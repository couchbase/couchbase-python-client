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
 * This file contains 'miscellaneous' operations. Functions contained here
 * might move to other files if they become more complex.
 *
 * More specifically, this contains 'key-only' operations that don't
 * require a value.
 */


/**
 * This is called during each iteration of delete/unlock
 */
static int
handle_single_keyop(pycbc_Connection *self,
                    PyObject *curkey,
                    PyObject *curval,
                    int ii,
                    int optype,
                    struct pycbc_common_vars *cv)
{
    int rv;
    char *key;
    size_t nkey;
    lcb_uint64_t cas = 0;

    rv = pycbc_tc_encode_key(self, &curkey, (void**)&key, &nkey);
    if (rv == -1) {
        return -1;
    }

    cv->enckeys[ii] = curkey;

    if (curval) {

        if (PyDict_Check(curval)) {
            PyObject *cas_o = PyDict_GetItemString(curval, "cas");

            if (!cas_o) {
                PyErr_Clear();
            }
            cas = pycbc_IntAsULL(cas_o);

        } else if (PYCBC_OPRES_CHECK(curval)) {
            /* If we're passed a Result object, just extract its CAS */
            cas = ((pycbc_OperationResult*)curval)->cas;

        } else if (PyNumber_Check(curval)) {
            cas = pycbc_IntAsULL(curval);

        }

        if (cas == (lcb_uint64_t)-1 && PyErr_Occurred()) {
            PyErr_Clear();
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Invalid CAS specified");
            return -1;
        }
    }

    if (optype == PYCBC_CMD_UNLOCK) {
        lcb_unlock_cmd_t *ucmd = cv->cmds.unlock + ii;

        if (!cas) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS,
                           0,
                           "CAS must be specified for unlock");
        }

        ucmd->v.v0.key = key;
        ucmd->v.v0.nkey = nkey;
        ucmd->v.v0.cas = cas;
        cv->cmdlist.unlock[ii] = ucmd;

        return 0;

    } else {
        lcb_remove_cmd_t *rcmd = cv->cmds.remove + ii;
        rcmd->v.v0.key = key;
        rcmd->v.v0.nkey = nkey;
        rcmd->v.v0.cas = cas;
        cv->cmdlist.remove[ii] = rcmd;

        return 0;
    }
}

static PyObject *
keyop_common(pycbc_Connection *self,
             PyObject *args,
             PyObject *kwargs,
             int optype,
             int argopts)
{
    int rv;
    int ii;
    Py_ssize_t ncmds = 0;
    pycbc_seqtype_t seqtype;
    PyObject *casobj = NULL;
    PyObject *is_quiet = NULL;
    PyObject *kobj = NULL;
    lcb_error_t err;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = { "keys", "cas", "quiet", NULL };

    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|OO",
                                     kwlist,
                                     &kobj,
                                     &casobj,
                                     &is_quiet);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(kobj, 1, &ncmds, &seqtype);
        if (rv < 0) {
            return NULL;
        }

        if (casobj && PyObject_IsTrue(casobj)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Can't pass CAS for multiple keys");
        }

    } else {
        ncmds = 1;
    }

    rv = pycbc_common_vars_init(&cv,
                                self,
                                argopts,
                                ncmds,
                                sizeof(lcb_remove_cmd_t),
                                0);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        Py_ssize_t dictpos = 0;
        PyObject *curseq, *iter = NULL;
        curseq = pycbc_oputil_iter_prepare(seqtype, kobj, &iter, &dictpos);

        if (!curseq) {
            goto GT_DONE;
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

            rv = handle_single_keyop(self, curkey, curvalue, ii, optype, &cv);

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
        rv = handle_single_keyop(self, kobj, casobj, 0, optype, &cv);
        if (rv < 0) {
            goto GT_DONE;
        }
    }


    if (optype == PYCBC_CMD_DELETE) {
        if (pycbc_maybe_set_quiet(cv.mres, is_quiet) == -1) {
            goto GT_DONE;
        }
        err = lcb_remove(self->instance, cv.mres, ncmds, cv.cmdlist.remove);

    } else {
        err = lcb_unlock(self->instance, cv.mres, ncmds, cv.cmdlist.unlock);
    }

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv);
    return cv.ret;
}



#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Connection_##name(pycbc_Connection *self, \
                                      PyObject *args, PyObject *kwargs) { \
    return keyop_common(self, args, kwargs, operation, mode); \
}

DECLFUNC(delete, PYCBC_CMD_DELETE, PYCBC_ARGOPT_SINGLE)
DECLFUNC(unlock, PYCBC_CMD_UNLOCK, PYCBC_ARGOPT_SINGLE)
DECLFUNC(delete_multi, PYCBC_CMD_DELETE, PYCBC_ARGOPT_MULTI)
DECLFUNC(unlock_multi, PYCBC_CMD_UNLOCK, PYCBC_ARGOPT_MULTI)


PyObject *
pycbc_Connection__stats(pycbc_Connection *self,
                        PyObject *args,
                        PyObject *kwargs)
{
    int rv;
    int ii;
    Py_ssize_t ncmds;
    lcb_error_t err;
    PyObject *keys = NULL;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    static char *kwlist[] = {  "keys", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &keys);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (keys == NULL || PyObject_IsTrue(keys) == 0) {
        keys = NULL;
        ncmds = 1;

    } else {
        if (!PySequence_Check(keys)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "keys argument must be sequence");
            return NULL;
        }
        ncmds = PySequence_Size(keys);
    }

    rv = pycbc_common_vars_init(&cv,
                                self,
                                PYCBC_ARGOPT_MULTI,
                                ncmds,
                                sizeof(lcb_server_stats_cmd_t),
                                0);
    if (rv < 0) {
        return NULL;
    }

    if (keys) {
        for (ii =0; ii < ncmds; ii++) {
            char *key;
            Py_ssize_t nkey;
            PyObject *newkey = NULL;

            PyObject *curkey = PySequence_GetItem(keys, ii);
            lcb_server_stats_cmd_t *cmd = cv.cmds.stats + ii;
            rv = pycbc_BufFromString(curkey, &key, &nkey, &newkey);
            if (rv < 0) {
                PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS,
                                   0,
                                   "bad key type in stats",
                                   curkey);
                goto GT_DONE;
            }

            cmd->v.v0.name = key;
            cmd->v.v0.nname = nkey;
            cv.cmdlist.stats[ii] = cmd;
            cv.enckeys[ii] = newkey;
        }

    } else {
        cv.cmdlist.stats[0] = cv.cmds.stats;
    }

    err = lcb_server_stats(self->instance, cv.mres, ncmds, cv.cmdlist.stats);
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv);
    return cv.ret;
}
