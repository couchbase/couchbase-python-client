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

struct getcmd_vars_st {
    int optype;
    int allow_dval;
    union {
        unsigned long ttl;
        struct {
            int strategy;
            short index;
        } replica;
    } u;
};

static int
handle_single_key(pycbc_Connection *self,
                  PyObject *curkey,
                  PyObject *curval,
                  int ii,
                  struct getcmd_vars_st *gv,
                  struct pycbc_common_vars *cv)
{
    int rv;
    char *key;
    size_t nkey;
    unsigned int lock = 0;
    unsigned long ttl = gv->u.ttl;

    rv = pycbc_tc_encode_key(self, &curkey, (void**)&key, &nkey);
    if (rv == -1) {
        return -1;
    }

    cv->enckeys[ii] = curkey;

    if (!nkey) {
        PYCBC_EXCTHROW_EMPTYKEY();
        return -1;
    }

    if (curval && gv->allow_dval) {
        static char *kwlist[] = { "ttl", NULL };
        PyObject *ttl_O = NULL;
        if (gv->u.ttl) {
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
    switch (gv->optype) {
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

#if LCB_VERSION >= 0x020007
    case PYCBC_CMD_GETREPLICA:
    case PYCBC_CMD_GETREPLICA_INDEX:
    case PYCBC_CMD_GETREPLICA_ALL: {
        lcb_get_replica_cmd_t *rcmd = cv->cmds.replica + ii;
        rcmd->version = 1;
        rcmd->v.v1.key = key;
        rcmd->v.v1.nkey = nkey;
        rcmd->v.v1.nkey = nkey;
        rcmd->v.v1.strategy = gv->u.replica.strategy;
        rcmd->v.v1.index = gv->u.replica.index;

        cv->cmdlist.replica[ii] = rcmd;
        break;
    }
#else
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                       "Need libcouchbase 2.0.7 or "
                       "greater for replica functionality");
        return -1;
#endif


    }

    return 0;
}

static int handle_replica_options(int *optype,
                                  struct getcmd_vars_st *gv,
                                  PyObject *replica_O)
{

#if LCB_VERSION >= 0x020007
    switch (*optype) {
    case PYCBC_CMD_GET:
        *optype = PYCBC_CMD_GETREPLICA;
        if (gv->u.ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS,
                           0, "TTL specified along with replica");
            return -1;
        }
        gv->u.replica.strategy = LCB_REPLICA_FIRST;
        return 0;

    case PYCBC_CMD_GETREPLICA:
        gv->u.replica.strategy = LCB_REPLICA_FIRST;
        return 0;

    case PYCBC_CMD_GETREPLICA_INDEX:
        gv->u.replica.strategy = LCB_REPLICA_SELECT;
        if (replica_O == NULL || replica_O == Py_None) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS,
                           0, "rgetix must have a valid replica index");
            return -1;
        }
        gv->u.replica.index = pycbc_IntAsL(replica_O);
        if (PyErr_Occurred()) {
            return -1;
        }
        return 0;

    case PYCBC_CMD_GETREPLICA_ALL:
        gv->u.replica.strategy = LCB_REPLICA_ALL;
        return 0;

    default:
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                       "Replica option not supported for this operation");
        return -1;
    }
#else
    PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Need libcouchbase >= 2.0.7");
    (void)optype;
    (void)gv;
    (void)replica_O;
#endif
    return -1;
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
    PyObject *replica_O = NULL;

    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    struct getcmd_vars_st gv = { 0 };
    static char *kwlist[] = { "keys", "ttl", "quiet", "replica", NULL };

    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|OOO",
                                     kwlist,
                                     &kobj,
                                     &ttl_O,
                                     &is_quiet,
                                     &replica_O);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS()
        return NULL;
    }

    gv.optype = optype;

    rv = pycbc_get_ttl(ttl_O, &gv.u.ttl, 1);
    if (rv < 0) {
        return NULL;
    }

    if (replica_O && replica_O != Py_None && replica_O != Py_False) {
        if (-1 == handle_replica_options(&optype, &gv, replica_O)) {
            return NULL;
        }
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
        gv.allow_dval = 1;
        cmdsize = sizeof(lcb_get_cmd_t);
        break;

    case PYCBC_CMD_TOUCH:
        gv.allow_dval = 1;
        cmdsize = sizeof(lcb_touch_cmd_t);
        break;

    case PYCBC_CMD_GETREPLICA:
    case PYCBC_CMD_GETREPLICA_INDEX:
    case PYCBC_CMD_GETREPLICA_ALL:
        cmdsize = sizeof(lcb_get_replica_cmd_t);
        gv.allow_dval = 0;
        break;

    default:
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "Unrecognized optype");
        return NULL;

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

            rv = handle_single_key(self, curkey, curvalue, ii, &gv, &cv);
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
        rv = handle_single_key(self, kobj, NULL, 0, &gv, &cv);
        if (rv < 0) {
            goto GT_DONE;
        }
    }

    if (pycbc_maybe_set_quiet(cv.mres, is_quiet) == -1) {
        goto GT_DONE;
    }

    if (optype == PYCBC_CMD_GET || optype == PYCBC_CMD_LOCK) {
        err = lcb_get(self->instance, cv.mres, ncmds, cv.cmdlist.get);

    } else if (optype == PYCBC_CMD_TOUCH) {
        err = lcb_touch(self->instance, cv.mres, ncmds, cv.cmdlist.touch);

    } else {
        err = lcb_get_replica(self->instance, cv.mres, ncmds, cv.cmdlist.replica);
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

DECLFUNC(_rget, PYCBC_CMD_GETREPLICA, PYCBC_ARGOPT_SINGLE)
DECLFUNC(_rget_multi, PYCBC_CMD_GETREPLICA, PYCBC_ARGOPT_MULTI)
DECLFUNC(_rgetix, PYCBC_CMD_GETREPLICA_INDEX, PYCBC_ARGOPT_SINGLE)
DECLFUNC(_rgetix_multi, PYCBC_CMD_GETREPLICA_INDEX, PYCBC_ARGOPT_MULTI)
DECLFUNC(_rgetall, PYCBC_CMD_GETREPLICA_ALL, PYCBC_ARGOPT_SINGLE)
DECLFUNC(_rgetall_multi, PYCBC_CMD_GETREPLICA_ALL, PYCBC_ARGOPT_MULTI)
