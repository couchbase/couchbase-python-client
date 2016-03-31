/**
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#include "oputil.h"

void
pycbc_common_vars_finalize(struct pycbc_common_vars *cv, pycbc_Bucket *conn)
{
    if (cv->mctx) {
        cv->mctx->fail(cv->mctx);
        cv->mctx = NULL;
    }
    lcb_sched_fail(conn->instance);
    Py_XDECREF(cv->mres);

    if (conn->lockmode) {
        pycbc_oputil_conn_unlock(conn);
    }
}

int
pycbc_common_vars_wait(struct pycbc_common_vars *cv, pycbc_Bucket *self)
{
    Py_ssize_t nsched = cv->is_seqcmd ? 1 : cv->ncmds;

    if (cv->mctx) {
        cv->mctx->done(cv->mctx, cv->mres);
        cv->mctx = NULL;
    }
    lcb_sched_leave(self->instance);
    self->nremaining += nsched;

    if (self->flags & PYCBC_CONN_F_ASYNC) {
        /** For async, just do the right thing :) */
        cv->ret = (PyObject *)cv->mres;
        ((pycbc_AsyncResult *)cv->mres)->nops = nsched;

        /** INCREF once more so it's alive in the event loop */
        Py_INCREF(cv->ret);
        cv->mres = NULL;
        return 0;

    } else if (self->pipeline_queue) {
        cv->ret = Py_None;
        Py_INCREF(Py_None);
        return 0;
    }
    pycbc_oputil_wait_common(self);

    if (!pycbc_assert(self->nremaining == 0)) {
        fprintf(stderr, "Remaining count != 0. Adjusting");
        self->nremaining = 0;
    }

    if (pycbc_multiresult_maybe_raise(cv->mres)) {
        return -1;
    }

    cv->ret = pycbc_multiresult_get_result(cv->mres);
    Py_DECREF(cv->mres);
    cv->mres = NULL;

    if (cv->ret == NULL) {
        return -1;
    }

    return 0;
}

int
pycbc_common_vars_init(struct pycbc_common_vars *cv,
                       pycbc_Bucket *self,
                       int argopts,
                       Py_ssize_t ncmds,
                       int want_vals)
{
    if (-1 == pycbc_oputil_conn_lock(self)) {
        return -1;
    }

    cv->ncmds = ncmds;
    cv->mres = (pycbc_MultiResult*)pycbc_multiresult_new(self);
    cv->argopts = argopts;

    if (argopts & PYCBC_ARGOPT_SINGLE) {
        cv->mres->mropts |= PYCBC_MRES_F_SINGLE;
    }

    if (!cv->mres) {
        pycbc_oputil_conn_unlock(self);
        return -1;
    }

    lcb_sched_enter(self->instance);
    return 0;
}

/**
 * Check that the object is not one of Python's typical string types
 */
#define _is_not_strtype(o) \
    (PyBytes_Check(o) == 0 && PyByteArray_Check(o) == 0 && PyUnicode_Check(o) == 0)

int
pycbc_oputil_check_sequence(PyObject *sequence,
                            int allow_list,
                            Py_ssize_t *ncmds,
                            pycbc_seqtype_t *seqtype)
{
    int ret = 0;
    pycbc_seqtype_t dummy;
    if (!seqtype) {
        seqtype = &dummy;
    }

    *ncmds = 0;

    if (PyDict_Check(sequence)) {
        *ncmds = PyDict_Size(sequence);
        *seqtype = PYCBC_SEQTYPE_DICT;
        ret = 0;

    } else if (allow_list == 0 &&
            PyObject_IsInstance(sequence, pycbc_helpers.itmcoll_base_type) == 0) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                           "Keys must be a dictionary",
                           sequence);
        ret = -1;

    } else if (PyList_Check(sequence)) {
        *seqtype = PYCBC_SEQTYPE_LIST;
        *ncmds = PyList_GET_SIZE(sequence);

    } else if (PyTuple_Check(sequence)) {
        *seqtype = PYCBC_SEQTYPE_TUPLE;
        *ncmds = PyTuple_GET_SIZE(sequence);

    } else if (PyObject_IsInstance(sequence, pycbc_helpers.itmcoll_base_type)) {
        *ncmds = PyObject_Length(sequence);
        if (*ncmds == -1) {
            PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0,
                           "ItemCollection subclass did not return proper length");
            ret = -1;
        }
        *seqtype = PYCBC_SEQTYPE_GENERIC | PYCBC_SEQTYPE_F_ITM;
        if (PyObject_IsInstance(sequence, pycbc_helpers.itmopts_dict_type)) {
            *seqtype |= PYCBC_SEQTYPE_F_OPTS;
        }


    } else if (_is_not_strtype(sequence)) {
        /**
         * Previously we used PySequence_Check, but this failed on things
         * which didn't have __getitem__ (they had a length, but the elements
         * were not ordered, but we don't care about that here
         */
        *seqtype = PYCBC_SEQTYPE_GENERIC;
        *ncmds = PyObject_Length(sequence);

        if (*ncmds == -1) {
            PyErr_Clear();
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                               "Keys must be iterable and have known length",
                               sequence);
            ret = -1;
        }

    } else {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                           "Keys must be iterable and have known length",
                           sequence);
        ret = -1;
    }

    if (ret == 0 && *ncmds < 1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Key list is empty", sequence);
        ret = -1;
    }

    return ret;
}

int
pycbc_maybe_set_quiet(pycbc_MultiResult *mres, PyObject *quiet)
{
    /**
     * If quiet is 'None', then we default to Connection.quiet
     */
    int tmp = 0;
    if (quiet == NULL || quiet == Py_None) {
        mres->mropts |= (mres->parent->quiet) ? PYCBC_MRES_F_QUIET : 0;
        return 0;
    }

    tmp |= PyObject_IsTrue(quiet);

    if (tmp == -1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS,
                           0, "quiet must be True, False, or None'", quiet);
        return -1;
    }

    mres->mropts |= tmp ? PYCBC_MRES_F_QUIET : 0;

    return 0;
}

PyObject *
pycbc_oputil_iter_prepare(pycbc_seqtype_t seqtype,
                          PyObject *sequence,
                          PyObject **iter,
                          Py_ssize_t *dictpos)
{
    if (seqtype & PYCBC_SEQTYPE_GENERIC) {
        *iter = PyObject_GetIter(sequence);
        if (!*iter) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                               "Couldn't get iterator from object. Object "
                               "should implement __iter__",
                               sequence);
        }
        return *iter;
    } else if (seqtype & PYCBC_SEQTYPE_DICT) {
        *dictpos = 0;
    }
    *iter = NULL;
    return sequence;
}

/**
 * I thought it better to make the function call a bit more complex, so as to
 * have the iteration logic unified in a single place
 */
int
pycbc_oputil_sequence_next(pycbc_seqtype_t seqtype,
                           PyObject *seqobj,
                           Py_ssize_t *dictpos,
                           int ii,
                           PyObject **key,
                           PyObject **value)
{
    if (seqtype & PYCBC_SEQTYPE_DICT) {
        int rv = PyDict_Next(seqobj, dictpos, key, value);
        if (rv < 1) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_INTERNAL,
                               0, "Couldn't iterate", seqobj);
            return -1;
        }

        Py_XINCREF(*key);
        Py_XINCREF(*value);
        return 0;
    }

    *value = NULL;
    if (seqtype & PYCBC_SEQTYPE_LIST) {
        *key = PyList_GET_ITEM(seqobj, ii);
        Py_INCREF(*key);
    } else if (seqtype & PYCBC_SEQTYPE_TUPLE) {
        *key = PyTuple_GET_ITEM(seqobj, ii);
        Py_INCREF(*key);
    } else {
        *key = PyIter_Next(seqobj);
        if (!*key) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                               "PyIter_Next returned NULL", seqobj);
            return -1;
        }
    }

    return 0;
}

static int
extract_item_params(struct pycbc_common_vars *cv,
                    PyObject *k,
                    pycbc_Item **itm,
                    PyObject **options)
{
    /** Key will always be an item */
    Py_ssize_t tsz;

    if (!PyTuple_Check(k)) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Expected Tuple", k);
        return -1;
    }

    tsz = PyTuple_GET_SIZE(k);
    if (tsz != 2 && tsz != 1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                           "Tuple from __iter__ must return 1 or 2 items", k);
        return -1;
    }

    *itm = (pycbc_Item *) PyTuple_GET_ITEM(k, 0);
    if (!PyObject_IsInstance((PyObject*)*itm, (PyObject *)&pycbc_ItemType)) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                           "Expected 'Item' instance", (PyObject*)*itm);
        return -1;
    }

    if (tsz == 2) {
        *options = PyTuple_GET_ITEM(k, 1);

        if (*options == Py_None) {
            *options = NULL;

        } else if (!PyDict_Check(*options)) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS,
                               0, "Options must be None or dict", *options);
            return -1;
        }
    }

    if (! (*itm)->key) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                           "Item is missing key", (PyObject *)*itm);
        return -1;
    }

    /** Store the item inside the mres dictionary */
    PyDict_SetItem(pycbc_multiresult_dict(cv->mres),
                   (*itm)->key, (PyObject *)*itm);
    cv->mres->mropts |= PYCBC_MRES_F_UALLOCED;
    return 0;
}

int
pycbc_oputil_iter_multi(pycbc_Bucket *self,
                        pycbc_seqtype_t seqtype,
                        PyObject *collection,
                        struct pycbc_common_vars *cv,
                        int optype,
                        pycbc_oputil_keyhandler handler,
                        void *arg)
{
    int rv = 0;
    int ii;
    PyObject *iterobj;
    PyObject *seqobj;
    Py_ssize_t dictpos = 0;

    seqobj = pycbc_oputil_iter_prepare(seqtype, collection, &iterobj, &dictpos);
    if (seqobj == NULL) {
        return -1;
    }

    for (ii = 0; ii < cv->ncmds; ii++) {
        PyObject *k, *v = NULL, *options = NULL;
        PyObject *arg_k = NULL;
        pycbc_Item *itm = NULL;

        rv = pycbc_oputil_sequence_next(seqtype,
                                        seqobj, &dictpos, ii, &k, &v);
        if (rv < 0) {
            goto GT_ITER_DONE;
        }

        if (seqtype & PYCBC_SEQTYPE_F_ITM) {
            if ((rv = extract_item_params(cv, k, &itm, &options)) != 0) {
                goto GT_ITER_DONE;
            }
            arg_k = itm->key;

        } else {
            arg_k = k;
        }

        rv = handler(self, cv, optype, arg_k, v, options, itm, arg);

        GT_ITER_DONE:
        Py_XDECREF(k);
        Py_XDECREF(v);

        if (rv == -1) {
            break;
        }
    }

    Py_XDECREF(iterobj);
    return rv;
}

int
pycbc_oputil_conn_lock(pycbc_Bucket *self)
{
    int status;
    int mode;

    if (!self->lockmode) {
        return 0;
    }

    mode = self->lockmode == PYCBC_LOCKMODE_WAIT ? WAIT_LOCK : NOWAIT_LOCK;
    if (mode == WAIT_LOCK) {
        /**
         * We need to unlock the GIL here so that other objects can potentially
         * access the Connection (and thus unlock it).
         */
        Py_BEGIN_ALLOW_THREADS
        status = PyThread_acquire_lock(self->lock, mode);
        Py_END_ALLOW_THREADS
    } else {
        status = PyThread_acquire_lock(self->lock, mode);
    }

    if (!status) {
        PYCBC_EXC_WRAP(PYCBC_EXC_THREADING,
                       0,
                       "Couldn't lock. If LOCKMODE_WAIT was passed, "
                       "then this means that something has gone wrong "
                       "internally. Otherwise, this means you are using "
                       "the Connection object from multiple threads. This "
                       "is not allowed (without an explicit "
                       "lockmode=LOCKMODE_WAIT constructor argument");
        return -1;
    }
    return 0;
}

void
pycbc_oputil_conn_unlock(pycbc_Bucket *self)
{
    if (!self->lockmode) {
        return;
    }
    PyThread_release_lock(self->lock);
}

void
pycbc_oputil_wait_common(pycbc_Bucket *self)
{
    /**
     * If we have a 'lockmode' specified, check to see that nothing else is
     * using us. We lock in any event.
     *
     * We have two modes:
     *  - LOCKMODE_WAIT explicitly allows access from multiple threads.
     *      In this mode, we actually wait to acquire the lock.
     *
     *  - LOCKMODE_EXC  will raise an exception if it cannot lock immediately
     *
     * Note that LOCKMODE_EXC won't do strict checking - i.e. it's perfectly
     * possible
     */

    PYCBC_CONN_THR_BEGIN(self);
    lcb_wait3(self->instance, LCB_WAIT_NOCHECK);
    PYCBC_CONN_THR_END(self);
}

/**
 * Returns 1 if durability was found, 0 if durability was not found, and -1
 * on error.
 */
int
pycbc_handle_durability_args(pycbc_Bucket *self,
                             pycbc_dur_params *params,
                             char persist_to,
                             char replicate_to)
{
    if (self->dur_global.persist_to || self->dur_global.replicate_to) {
        if (persist_to == 0 && replicate_to == 0) {
            persist_to = self->dur_global.persist_to;
            replicate_to = self->dur_global.replicate_to;
        }
    }

    if (persist_to || replicate_to) {
        int nreplicas = lcb_get_num_replicas(self->instance);
        params->persist_to = persist_to;
        params->replicate_to = replicate_to;
        if (replicate_to > nreplicas || persist_to > (nreplicas + 1)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, LCB_DURABILITY_ETOOMANY,
                           "Durability requirements will never be satisfied");
            return -1;
        }

        return 1;
    }

    return 0;
}

int
pycbc_encode_sd_keypath(pycbc_Bucket *conn, PyObject *src,
                      pycbc_pybuffer *keybuf, pycbc_pybuffer *pathbuf)
{
    PyObject *kobj, *pthobj;
    int rv;

    if (!PyTuple_Check(src) || PyTuple_GET_SIZE(src) != 2) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                       "Sub-document key must be a 2-tuple");
        return -1;
    }

    kobj = PyTuple_GET_ITEM(src, 0);
    pthobj = PyTuple_GET_ITEM(src, 1);

    rv = pycbc_tc_encode_key(conn, kobj, keybuf);
    if (rv != 0) {
        return rv;
    }
    rv = pycbc_tc_simple_encode(pthobj, pathbuf, PYCBC_FMT_UTF8);
    if (rv != 0) {
        PYCBC_PYBUF_RELEASE(keybuf);
    }
    return rv;
}

static int
sd_convert_spec(PyObject *pyspec, lcb_SDSPEC *sdspec,
    pycbc_pybuffer *pathbuf, pycbc_pybuffer *valbuf)
{
    PyObject *path = NULL;
    PyObject *val = NULL;
    int op = 0, create = 0;

    if (!PyTuple_Check(pyspec)) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Expected tuple for spec", pyspec);
        return -1;
    }

    if (!PyArg_ParseTuple(pyspec, "iO|Oi", &op, &path, &val, &create)) {
        PYCBC_EXCTHROW_ARGS();
        return -1;
    }
    if (pycbc_tc_simple_encode(path, pathbuf, PYCBC_FMT_UTF8) != 0) {
        goto GT_ERROR;
    }

    sdspec->sdcmd = op;
    sdspec->options = create ? LCB_SDSPEC_F_MKINTERMEDIATES : 0;
    LCB_SDSPEC_SET_PATH(sdspec, pathbuf->buffer, pathbuf->length);
    if (val != NULL) {
        int is_multival = 0;

        if (PyObject_IsInstance(val, pycbc_helpers.sd_multival_type)) {
            /* Verify the operation allows it */
            switch (op) {
            case LCB_SDCMD_ARRAY_ADD_FIRST:
            case LCB_SDCMD_ARRAY_ADD_LAST:
            case LCB_SDCMD_ARRAY_INSERT:
                is_multival = 1;
                break;
            default:
                PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                    "MultiValue not supported for operation", pyspec);
                goto GT_ERROR;
            }
        }

        if (pycbc_tc_simple_encode(val, valbuf, PYCBC_FMT_JSON) != 0) {
            goto GT_ERROR;
        }

        if (is_multival) {
            /* Strip first and last [ */
            const char *buf = (const char *)valbuf->buffer;
            size_t len = valbuf->length;

            for (; isspace(*buf) && len; len--, buf++) {
            }
            for (; len && isspace(buf[len-1]); len--) {
            }
            if (len < 3 || buf[0] != '[' || buf[len-1] != ']') {
                PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ENCODING, 0,
                    "Serialized MultiValue shows invalid JSON (maybe empty?)",
                    pyspec);
                goto GT_ERROR;
            }

            buf++;
            len -= 2;
            valbuf->buffer = buf;
            valbuf->length = len;
        }

        LCB_SDSPEC_SET_VALUE(sdspec, valbuf->buffer, valbuf->length);
    }
    return 0;

    GT_ERROR:
    PYCBC_PYBUF_RELEASE(valbuf);
    PYCBC_PYBUF_RELEASE(pathbuf);
    return -1;
}

int
pycbc_sd_handle_speclist(pycbc_Bucket *self, pycbc_MultiResult *mres,
    PyObject *key, PyObject *spectuple, lcb_CMDSUBDOC *cmd)
{
    int rv;
    lcb_error_t err = LCB_SUCCESS;
    Py_ssize_t nspecs = 0;
    pycbc__SDResult *newitm = NULL;
    lcb_SDSPEC *specs = NULL, spec_s = { 0 };
    pycbc_pybuffer pathbuf_s = { NULL }, valbuf_s = { NULL };
    pycbc_pybuffer *pathbufs = NULL, *valbufs = NULL;

    if (!PyTuple_Check(spectuple)) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Value must be a tuple!");
        return -1;
    }

    nspecs = PyTuple_GET_SIZE(spectuple);
    if (nspecs == 0) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Need one or more commands!");
        return -1;
    }

    newitm = pycbc_sdresult_new(self, spectuple);
    newitm->key = key;
    Py_INCREF(newitm->key);

    if (nspecs == 1) {
        PyObject *single_spec = PyTuple_GET_ITEM(spectuple, 0);
        pathbufs = &pathbuf_s;
        valbufs = &valbuf_s;

        cmd->specs = &spec_s;
        cmd->nspecs = 1;
        rv = sd_convert_spec(single_spec, &spec_s, pathbufs, valbufs);
    } else {
        Py_ssize_t ii;
        specs = calloc(nspecs, sizeof *specs);
        pathbufs = calloc(nspecs, sizeof *pathbufs);
        valbufs = calloc(nspecs, sizeof *valbufs);

        cmd->specs = specs;
        cmd->nspecs = nspecs;

        for (ii = 0; ii < nspecs; ++ii) {
            PyObject *cur = PyTuple_GET_ITEM(spectuple, ii);
            rv = sd_convert_spec(cur, specs + ii, pathbufs + ii, valbufs + ii);
            if (rv != 0) {
                break;
            }
        }
    }

    if (rv == 0) {
        err = lcb_subdoc3(self->instance, mres, cmd);
        if (err == LCB_SUCCESS) {
            PyDict_SetItem((PyObject*)mres, key, (PyObject*)newitm);
            pycbc_assert(Py_REFCNT(newitm) == 2);
        }
    }

    free(specs);
    {
        size_t ii;
        for (ii = 0; ii < nspecs; ++ii) {
            PYCBC_PYBUF_RELEASE(pathbufs + ii);
            PYCBC_PYBUF_RELEASE(valbufs + ii);
        }
    }

    if (nspecs > 1) {
        free(pathbufs);
        free(valbufs);
    }

    Py_DECREF(newitm);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        return -1;
    } else if (rv != 0) {
        return -1;
    }
    return 0;
}
