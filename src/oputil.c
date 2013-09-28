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
pycbc_common_vars_finalize(struct pycbc_common_vars *cv, pycbc_Connection *conn)
{
    int ii;
    if (cv->enckeys) {
        for (ii = 0; ii < cv->ncmds; ii++) {
            Py_XDECREF(cv->enckeys[ii]);
        }
    }

    if (cv->encvals) {
        for (ii = 0; ii < cv->ncmds; ii++) {
            Py_XDECREF(cv->encvals[ii]);
        }
    }

    /**
     * We only free the other malloc'd structures if we had more than
     * one command.
     */
    if (cv->ncmds > 1) {
        free(cv->cmds.get);
        free((void*)cv->cmdlist.get);
        free(cv->enckeys);
        free(cv->encvals);
    }

    Py_XDECREF(cv->mres);

    if (conn->lockmode) {
        pycbc_oputil_conn_unlock(conn);
    }
}

int
pycbc_common_vars_wait(struct pycbc_common_vars *cv, pycbc_Connection *self)
{
    lcb_error_t err;
    Py_ssize_t nsched = cv->is_seqcmd ? 1 : cv->ncmds;
    self->nremaining += nsched;

    if (self->flags & PYCBC_CONN_F_ASYNC) {
        /** For async, just do the right thing :) */
        cv->ret = (PyObject *)cv->mres;
        ((pycbc_AsyncResult *)cv->mres)->nops = nsched;

        /** INCREF once more so it's alive in the event loop */
        Py_INCREF(cv->ret);
        cv->mres = NULL;
        return 0;
    }

    err = pycbc_oputil_wait_common(self);

    if (err != LCB_SUCCESS) {
        self->nremaining -= nsched;
        PYCBC_EXCTHROW_WAIT(err);
        return -1;
    }

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
                       pycbc_Connection *self,
                       int argopts,
                       Py_ssize_t ncmds,
                       size_t tsize,
                       int want_vals)
{
    int ok;


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

    /**
     * If we have a single command, use the stack-allocated space.
     */
    if (ncmds == 1) {
        cv->cmds.get = &cv->_single_cmd.get;
        cv->cmdlist.get = (void*)&cv->cmds.get;
        cv->enckeys = cv->_po_single;
        cv->encvals = cv->_po_single + 1;
        return 0;
    }

    /**
     * TODO: Maybe Python has a memory pool we can use?
     */
    cv->cmds.get = calloc(ncmds, tsize);
    cv->cmdlist.get = malloc(ncmds * sizeof(void*));
    cv->enckeys = calloc(ncmds, sizeof(PyObject*));

    if (want_vals) {
        cv->encvals = calloc(ncmds, sizeof(PyObject*));

    } else {
        cv->encvals = NULL;
    }

    ok = (cv->cmds.get && cv->cmdlist.get && cv->enckeys);
    if (ok) {
        ok = (want_vals == 0 || cv->encvals);
    }

    if (!ok) {
        pycbc_common_vars_finalize(cv, self);
        PyErr_SetNone(PyExc_MemoryError);
        return -1;
    }


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
    PyDict_SetItem((PyObject *)&cv->mres->dict, (*itm)->key, (PyObject *)*itm);
    cv->mres->mropts |= PYCBC_MRES_F_UALLOCED;
    return 0;
}

int
pycbc_oputil_iter_multi(pycbc_Connection *self,
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

    seqobj = pycbc_oputil_iter_prepare(seqtype,
                                       collection,
                                       &iterobj,
                                       &dictpos);
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

        rv = handler(self, cv, optype, arg_k, v, options, itm, ii, arg);

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
pycbc_oputil_conn_lock(pycbc_Connection *self)
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
pycbc_oputil_conn_unlock(pycbc_Connection *self)
{
    if (!self->lockmode) {
        return;
    }
    PyThread_release_lock(self->lock);
}

lcb_error_t
pycbc_oputil_wait_common(pycbc_Connection *self)
{
    lcb_error_t ret;
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
    ret = lcb_wait(self->instance);
    PYCBC_CONN_THR_END(self);


    return ret;
}
