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

struct arithmetic_common_vars {
    lcb_int64_t delta;
    lcb_uint64_t initial;
    unsigned long ttl;
    int create;
};

static int
handle_single_arith(pycbc_Connection *self,
                    PyObject *curkey,
                    PyObject *curvalue,
                    struct arithmetic_common_vars *av,
                    int ii,
                    int optype,
                    struct pycbc_common_vars *cv)
{
    void *key;
    size_t nkey;
    int rv;
    lcb_arithmetic_cmd_t *acmd;
    struct arithmetic_common_vars my_params;
    static char *kwlist[] = { "delta", "initial", "ttl", NULL };

    memcpy(&my_params, av, sizeof(my_params));
    acmd = cv->cmds.arith + ii;

    rv = pycbc_tc_encode_key(self, &curkey, &key, &nkey);
    if (rv < 0) {
        return -1;
    }

    cv->enckeys[ii] = curkey;

    if (curvalue) {
        if (PyDict_Check(curvalue)) {
            PyObject *initial_O = NULL;
            rv = PyArg_ParseTupleAndKeywords(pycbc_DummyTuple,
                                             curvalue,
                                             "L|Ok",
                                             kwlist,
                                             &my_params.delta,
                                             &initial_O,
                                             &my_params.ttl);
            if (!rv) {
                PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS, 0,
                                   "Couldn't parse parameter for key",
                                   curkey);
                return -1;
            }

            if (initial_O) {
                if (PyNumber_Check(initial_O)) {
                    my_params.create = 1;
                    my_params.initial = pycbc_IntAsULL(initial_O);

                } else {
                    my_params.create = 0;
                }
            }

        } else if (PyNumber_Check(curvalue)) {
            my_params.delta = pycbc_IntAsLL(curvalue);
            if (optype == PYCBC_CMD_DECR) {
                my_params.delta = (-my_params.delta);
            }

        } else {
            PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS,
                               0,
                               "value for key must be an integer amount "
                               "or a dict of parameters",
                               curkey);
            return -1;
        }
    }

    acmd->v.v0.key = key;
    acmd->v.v0.nkey = nkey;
    acmd->v.v0.delta = my_params.delta;
    acmd->v.v0.create = my_params.create;
    acmd->v.v0.exptime = my_params.ttl;
    acmd->v.v0.initial = my_params.initial;
    cv->cmdlist.arith[ii] = acmd;

    return 0;
}

PyObject *
arithmetic_common(pycbc_Connection *self,
                                   PyObject *args,
                                   PyObject *kwargs,
                                   int optype,
                                   int argopts)
{
    int rv;
    int ii;
    Py_ssize_t ncmds;
    struct arithmetic_common_vars global_params = { 0 };
    pycbc_seqtype_t seqtype;
    PyObject *all_initial_O = NULL;
    PyObject *all_ttl_O = NULL;
    PyObject *collection;
    PyObject *ret = NULL;
    pycbc_MultiResult *mres = NULL;
    lcb_error_t err;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = { "keys", "delta", "initial", "ttl", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O|LOO", kwlist,
                                     &collection,
                                     &global_params.delta,
                                     &all_initial_O,
                                     &all_ttl_O);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    rv = pycbc_get_ttl(all_ttl_O, &global_params.ttl, 1);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(collection,
                            optype != PYCBC_CMD_ARITH,
                            &ncmds,
                            &seqtype);
        if (rv < 0) {
            return NULL;
        }
    } else {
        ncmds = 1;
    }


    if (all_initial_O && PyNumber_Check(all_initial_O)) {
        global_params.create = 1;
        global_params.initial = pycbc_IntAsULL(all_initial_O);
    }

    if (optype == PYCBC_CMD_DECR) {
        global_params.delta = -(global_params.delta);
    }

    rv = pycbc_common_vars_init(&cv, ncmds, sizeof(lcb_arithmetic_cmd_t), 0);

    if (argopts & PYCBC_ARGOPT_MULTI) {
        Py_ssize_t dictpos;
        PyObject *curseq, *iter;
        curseq = pycbc_oputil_iter_prepare(seqtype, collection, &iter, &dictpos);
        if (!curseq) {
            rv = -1;
            goto GT_ITER_DONE;
        }

        for (ii = 0; ii < ncmds; ii++) {
            PyObject *curkey = NULL, *curvalue = NULL;
            rv = pycbc_oputil_sequence_next(seqtype, curseq, &dictpos, ii,
                                            &curkey, &curvalue);
            if (rv < 0) {
                goto GT_ITER_DONE;
            }

            rv = handle_single_arith(self,
                                     curkey,
                                     curvalue,
                                     &global_params,
                                     ii,
                                     optype,
                                     &cv);

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
        rv = handle_single_arith(self,
                                 collection,
                                 NULL,
                                 &global_params,
                                 0,
                                 optype,
                                 &cv);
        if (rv < 0) {
            goto GT_DONE;
        }
    }

    mres = (pycbc_MultiResult*)pycbc_multiresult_new(self);

    err = lcb_arithmetic(self->instance, mres, ncmds, cv.cmdlist.arith);
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    err = pycbc_oputil_wait_common(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_WAIT(err);
        goto GT_DONE;
    }

    if (!pycbc_multiresult_maybe_raise(mres)) {
        ret = (PyObject*)mres;
    }


    GT_DONE:
    pycbc_common_vars_free(&cv);
    ret = pycbc_make_retval(argopts, &ret, &mres);
    Py_XDECREF(mres);
    return ret;
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Connection_##name(pycbc_Connection *self, \
                                      PyObject *args, PyObject *kwargs) { \
    return arithmetic_common(self, args, kwargs, operation, mode); \
}

DECLFUNC(arithmetic, PYCBC_CMD_ARITH, PYCBC_ARGOPT_SINGLE)
DECLFUNC(incr, PYCBC_CMD_INCR, PYCBC_ARGOPT_SINGLE)
DECLFUNC(decr, PYCBC_CMD_DECR, PYCBC_ARGOPT_SINGLE)

DECLFUNC(arithmetic_multi, PYCBC_CMD_ARITH, PYCBC_ARGOPT_MULTI)
DECLFUNC(incr_multi, PYCBC_CMD_INCR, PYCBC_ARGOPT_MULTI)
DECLFUNC(decr_multi, PYCBC_CMD_DECR, PYCBC_ARGOPT_MULTI)
