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
/**
 * Covers 'lock', 'touch', and 'get_and_touch'
 */


static int
handle_single_key(pycbc_Connection *self,
                  PyObject *curkey,
                  PyObject *curval,
                  unsigned long ttl,
                  int ii,
                  int optype,
                  struct pycbc_common_vars *cv)
{
    int rv;
    char *key;
    size_t nkey;
    unsigned int lock = 0;

    rv = pycbc_tc_encode_key(self, &curkey, (void**)&key, &nkey);
    if (rv == -1) {
        return -1;
    }

    cv->enckeys[ii] = curkey;

    if (!nkey) {
        PYCBC_EXCTHROW_EMPTYKEY();
        return -1;
    }


    if (curval) {
        static char *kwlist[] = { "ttl", NULL };
        PyObject *ttl_O = NULL;
        if (ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS,
                           0,
                           "Both global and single TTL specified");
            return -1;
        }

        if (PyDict_Check(curval)) {
            rv = PyArg_ParseTupleAndKeywords(pycbc_DummyTuple, curval,
                                             "|O", kwlist, &ttl_O);
            if (!rv) {
                PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS, 0,
                                   "Couldn't get sub-parmeters for key",
                                   curkey);
                return -1;
            }
        } else {
            ttl_O = curval;
        }

        rv = pycbc_get_ttl(ttl_O, &ttl, 1);
        if (rv < 0) {
            return -1;
        }
    }
    switch (optype) {
    case PYCBC_CMD_GAT:
        if (!ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "GAT must have positive TTL");
            return -1;
        }
        goto GT_GET;

    case PYCBC_CMD_LOCK:
        if (!ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Lock must have an expiry");
            return -1;
        }
        lock = 1;
        goto GT_GET;

    case PYCBC_CMD_GET:
        GT_GET: {
            lcb_get_cmd_t *gcmd = cv->cmds.get + ii;
            gcmd->v.v0.lock = lock;
            gcmd->v.v0.key = key;
            gcmd->v.v0.nkey = nkey;
            gcmd->v.v0.exptime = ttl;
            cv->cmdlist.get[ii] = gcmd;
        }
        break;

    case PYCBC_CMD_TOUCH: {
        lcb_touch_cmd_t *tcmd = cv->cmds.touch + ii;
        tcmd->v.v0.key = key;
        tcmd->v.v0.nkey = nkey;
        tcmd->v.v0.exptime = ttl;
        cv->cmdlist.touch[ii] = tcmd;
        break;
    }
    }

    return 0;
}


static PyObject*
get_common(pycbc_Connection *self,
           PyObject *args,
           PyObject *kwargs,
           int optype,
           int argopts)
{
    int rv;
    int ii;
    Py_ssize_t ncmds = 0;
    size_t cmdsize;
    pycbc_seqtype_t seqtype;
    PyObject *kobj = NULL;
    PyObject *is_quiet = NULL;
    lcb_error_t err;
    PyObject *ttl_O = NULL;
    unsigned long ttl = 0;

    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = { "keys", "ttl", "quiet", NULL };

    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|OO",
                                     kwlist,
                                     &kobj,
                                     &ttl_O,
                                     &is_quiet);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS()
        return NULL;
    }

    rv = pycbc_get_ttl(ttl_O, &ttl, 1);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(kobj,
                                         optype,
                                         &ncmds,
                                         &seqtype);
        if (rv < 0) {
            return NULL;
        }

    } else {
        ncmds = 1;
    }

    switch (optype) {
    case PYCBC_CMD_GET:
    case PYCBC_CMD_LOCK:
    case PYCBC_CMD_GAT:
        cmdsize = sizeof(lcb_get_cmd_t);
        break;

    case PYCBC_CMD_TOUCH:
        cmdsize = sizeof(lcb_touch_cmd_t);
        break;
    }

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, cmdsize, 0);

    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        Py_ssize_t dictpos;
        PyObject *curseq, *iter = NULL;

        curseq = pycbc_oputil_iter_prepare(seqtype, kobj, &iter, &dictpos);
        if (!curseq) {
            rv = -1;
            goto GT_ITER_DONE;
        }

        for (ii = 0; ii < ncmds; ii++) {
            PyObject *curkey = NULL, *curvalue = NULL;
            rv = pycbc_oputil_sequence_next(seqtype,
                                            curseq,
                                            &dictpos,
                                            ii,
                                            &curkey,
                                            &curvalue);
            if (rv < 0) {
                goto GT_ITER_DONE;
            }

            rv = handle_single_key(self, curkey, curvalue, ttl, ii, optype, &cv);
            Py_XDECREF(curkey);
            Py_XDECREF(curvalue);

            if (rv < 0) {
                goto GT_ITER_DONE;
            }
        }

        GT_ITER_DONE:
        Py_XDECREF(iter);
        if (rv < 0) {
            goto GT_DONE;
        }

    } else {
        rv = handle_single_key(self, kobj, NULL, ttl, 0, optype, &cv);
        if (rv < 0) {
            goto GT_DONE;
        }
    }

    if (pycbc_maybe_set_quiet(cv.mres, is_quiet) == -1) {
        goto GT_DONE;
    }

    if (optype == PYCBC_CMD_TOUCH) {
        err = lcb_touch(self->instance, cv.mres, ncmds, cv.cmdlist.touch);

    } else {
        err = lcb_get(self->instance, cv.mres, ncmds, cv.cmdlist.get);
    }

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Connection_##name(pycbc_Connection *self, \
                                      PyObject *args, PyObject *kwargs) { \
    return get_common(self, args, kwargs, operation, mode); \
}


DECLFUNC(get, PYCBC_CMD_GET, PYCBC_ARGOPT_SINGLE)
DECLFUNC(touch, PYCBC_CMD_TOUCH, PYCBC_ARGOPT_SINGLE)
DECLFUNC(lock, PYCBC_CMD_LOCK, PYCBC_ARGOPT_SINGLE)
DECLFUNC(get_multi, PYCBC_CMD_GET, PYCBC_ARGOPT_MULTI)
DECLFUNC(touch_multi, PYCBC_CMD_TOUCH, PYCBC_ARGOPT_MULTI)
DECLFUNC(lock_multi, PYCBC_CMD_LOCK, PYCBC_ARGOPT_MULTI)
