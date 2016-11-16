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
handle_single_key(pycbc_Bucket *self, struct pycbc_common_vars *cv, int optype,
    PyObject *curkey, PyObject *curval, PyObject *options, pycbc_Item *itm,
    void *arg)
{
    int rv;
    unsigned int lock = 0;
    struct getcmd_vars_st *gv = (struct getcmd_vars_st *)arg;
    unsigned long ttl = gv->u.ttl;
    lcb_error_t err;
    pycbc_pybuffer keybuf = { NULL };

    union {
        lcb_CMDBASE base;
        lcb_CMDGET get;
        lcb_CMDTOUCH touch;
        lcb_CMDGETREPLICA rget;
    } u_cmd;

    memset(&u_cmd, 0, sizeof u_cmd);
    (void)itm;

    rv = pycbc_tc_encode_key(self, curkey, &keybuf);
    if (rv == -1) {
        return -1;
    }

    LCB_CMD_SET_KEY(&u_cmd.base, keybuf.buffer, keybuf.length);

    if (curval && gv->allow_dval && options == NULL) {
        options = curval;
    }
    if (options) {
        static char *kwlist[] = { "ttl", NULL };
        PyObject *ttl_O = NULL;
        if (gv->u.ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Both global and single TTL specified");
            rv = -1;
            goto GT_DONE;
        }

        /* Note, options only comes when ItemOptionsCollection and friends
         * are used. When this is in effect, options is the options for the
         * current item, and value is NULL.
         */
        if (!curval) {
            curval = options;
        }

        if (PyDict_Check(curval)) {
            rv = PyArg_ParseTupleAndKeywords(pycbc_DummyTuple,
                curval, "|O", kwlist, &ttl_O);
            if (!rv) {
                PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS, 0, "Couldn't get sub-parmeters for key", curkey);
                rv = -1;
                goto GT_DONE;
            }
        } else {
            ttl_O = curval;
        }

        rv = pycbc_get_ttl(ttl_O, &ttl, 1);
        if (rv < 0) {
            rv = -1;
            goto GT_DONE;
        }
    }
    u_cmd.base.exptime = ttl;

    switch (optype) {
    case PYCBC_CMD_GAT:
        if (!ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "GAT must have positive TTL");
            rv = -1;
            goto GT_DONE;
        }
        goto GT_GET;

    case PYCBC_CMD_LOCK:
        if (!ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Lock must have an expiry");
            rv = -1;
            goto GT_DONE;
        }
        lock = 1;
        goto GT_GET;

    case PYCBC_CMD_GET:
        GT_GET:
        u_cmd.get.lock = lock;
        err = lcb_get3(self->instance, cv->mres, &u_cmd.get);
        break;

    case PYCBC_CMD_TOUCH:
        u_cmd.touch.exptime = ttl;
        err = lcb_touch3(self->instance, cv->mres, &u_cmd.touch);
        break;

    case PYCBC_CMD_GETREPLICA:
    case PYCBC_CMD_GETREPLICA_INDEX:
    case PYCBC_CMD_GETREPLICA_ALL:
        u_cmd.rget.strategy = gv->u.replica.strategy;
        u_cmd.rget.index = gv->u.replica.index;
        err = lcb_rget3(self->instance, cv->mres, &u_cmd.rget);
        break;
    default:
        err = LCB_ERROR;
        abort();
        break;
    }

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        rv = -1;
        goto GT_DONE;
    } else {
        rv = 0;
    }

    GT_DONE:
    PYCBC_PYBUF_RELEASE(&keybuf);
    return rv;
}

static int
handle_replica_options(int *optype, struct getcmd_vars_st *gv, PyObject *replica_O)
{
    switch (*optype) {
    case PYCBC_CMD_GET:
        *optype = PYCBC_CMD_GETREPLICA;
        if (gv->u.ttl) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "TTL specified along with replica");
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
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "rgetix must have a valid replica index");
            return -1;
        }
        gv->u.replica.index = (short)pycbc_IntAsL(replica_O);
        if (PyErr_Occurred()) {
            return -1;
        }
        return 0;

    case PYCBC_CMD_GETREPLICA_ALL:
        gv->u.replica.strategy = LCB_REPLICA_ALL;
        return 0;

    default:
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Replica option not supported for this operation");
        return -1;
    }
    return -1;
}


static PyObject*
get_common(pycbc_Bucket *self, PyObject *args, PyObject *kwargs, int optype,
    int argopts)
{
    int rv;
    Py_ssize_t ncmds = 0;
    pycbc_seqtype_t seqtype;
    PyObject *kobj = NULL;
    PyObject *is_quiet = NULL;
    PyObject *ttl_O = NULL;
    PyObject *replica_O = NULL;
    PyObject *nofmt_O = NULL;

    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    struct getcmd_vars_st gv = { 0 };
    static char *kwlist[] = {
            "keys", "ttl", "quiet", "replica", "no_format", NULL
    };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOOO", kwlist,
        &kobj, &ttl_O, &is_quiet, &replica_O, &nofmt_O);

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
        rv = pycbc_oputil_check_sequence(kobj, optype, &ncmds, &seqtype);
        if (rv < 0) {
            return NULL;
        }

    } else {
        ncmds = 1;
    }

    gv.allow_dval = 1;

    switch (optype) {
    case PYCBC_CMD_GET:
    case PYCBC_CMD_LOCK:
    case PYCBC_CMD_GAT:
        gv.allow_dval = 1;
        break;

    case PYCBC_CMD_TOUCH:
        gv.allow_dval = 1;
        break;

    case PYCBC_CMD_GETREPLICA:
    case PYCBC_CMD_GETREPLICA_INDEX:
    case PYCBC_CMD_GETREPLICA_ALL:
        gv.allow_dval = 0;
        break;

    default:
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "Unrecognized optype");
        return NULL;

    }

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, 0);

    if (rv < 0) {
        return NULL;
    }

    if (nofmt_O && nofmt_O != Py_None) {
        cv.mres->mropts |= PyObject_IsTrue(nofmt_O)
                ? PYCBC_MRES_F_FORCEBYTES : 0;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_iter_multi(self, seqtype, kobj, &cv, optype,
            handle_single_key, &gv);

    } else {
        rv = handle_single_key(self, &cv, optype, kobj, NULL, NULL, NULL, &gv);
    }
    if (rv < 0) {
        goto GT_DONE;
    }

    if (pycbc_maybe_set_quiet(cv.mres, is_quiet) == -1) {
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

static int
handle_single_lookup(pycbc_Bucket *self, struct pycbc_common_vars *cv, int optype,
    PyObject *curkey, PyObject *curval, PyObject *options, pycbc_Item *itm,
    void *arg)
{
    pycbc_pybuffer keybuf = { NULL };
    lcb_CMDSUBDOC cmd = { 0 };
    int rv = 0;

    if (itm) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Items not supported for subdoc!");
        return -1;
    }
    if (pycbc_tc_encode_key(self, curkey, &keybuf) != 0) {
        return -1;
    }
    LCB_CMD_SET_KEY(&cmd, keybuf.buffer, keybuf.length);
    rv = pycbc_sd_handle_speclist(self, cv->mres, curkey, curval, &cmd);
    PYCBC_PYBUF_RELEASE(&keybuf);
    return rv;
}

static PyObject *
sdlookup_common(pycbc_Bucket *self, PyObject *args, PyObject *kwargs, int argopts)
{
    Py_ssize_t ncmds;
    PyObject *kobj = NULL;
    PyObject *quiet_key = NULL;
    pycbc_seqtype_t seqtype;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    static char *kwlist[] = { "ks", "quiet", NULL };

    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "O|O", kwlist, &kobj, &quiet_key)) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (pycbc_oputil_check_sequence(kobj, 0, &ncmds, &seqtype) != 0) {
        return NULL;
    }

    if (pycbc_common_vars_init(&cv, self, argopts, ncmds, 1) != 0) {
        return NULL;
    }

    if (pycbc_oputil_iter_multi(
        self, seqtype, kobj, &cv, 0, handle_single_lookup, NULL) != 0) {
        goto GT_DONE;
    }

    if (pycbc_maybe_set_quiet(cv.mres, quiet_key) != 0) {
        goto GT_DONE;
    }

    pycbc_common_vars_wait(&cv, self);

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

PyObject *
pycbc_Bucket_lookup_in(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    return sdlookup_common(self, args, kwargs, PYCBC_ARGOPT_SINGLE);
}

PyObject *
pycbc_Bucket_lookup_in_multi(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    return sdlookup_common(self, args, kwargs, PYCBC_ARGOPT_MULTI);
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Bucket_##name(pycbc_Bucket *self, \
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
