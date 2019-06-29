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
#include "pycbc.h"

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
        } replica;
    } u;
};

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                int,
                handle_single_key,
                pycbc_oputil_keyhandler_raw_Bucket *original,
                pycbc_Collection_t *collection,
                struct pycbc_common_vars *cv,
                int optype,
                PyObject *curkey,
                PyObject *curval,
                PyObject *options,
                pycbc_Item *itm,
                void *arg)

{
    pycbc_Bucket *self = collection->bucket;
    int rv;
    unsigned int lock = 0;
    struct getcmd_vars_st *gv = (struct getcmd_vars_st *)arg;
    unsigned long ttl = gv->u.ttl;
    lcb_STATUS err = LCB_SUCCESS;
    pycbc_pybuffer keybuf = { NULL };

    PYCBC_DEBUG_LOG_CONTEXT(context,"Started processing")
    (void)itm;

    PYCBC_DEBUG_LOG_CONTEXT(context, "Encoding")
    rv = pycbc_tc_encode_key(self, curkey, &keybuf);
    PYCBC_DEBUG_LOG_CONTEXT(context, "Encoded")
    if (rv == -1) {
        return -1;
    }

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
#define COMMON_OPTS(X, NAME, CMDNAME)              \
    lcb_cmd##CMDNAME##_expiration(cmd, ttl);       \
    PYCBC_CMD_SET_KEY_SCOPE(CMDNAME, cmd, keybuf); \
    PYCBC_TRACECMD_TYPED(CMDNAME, cmd, context, cv->mres, curkey, self);

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
        GT_GET : {
            CMDSCOPE_NG(GET, get)
            {
                lcb_cmdget_locktime(cmd, lock);
                COMMON_OPTS(PYCBC_get_ATTR, get, get);
                err = pycbc_get(collection, cv->mres, cmd);
            }
        } break;

        case PYCBC_CMD_TOUCH: {
            CMDSCOPE_NG_V4(TOUCH, touch)
            {
                COMMON_OPTS(PYCBC_touch_ATTR, touch, touch);
                err = pycbc_touch(collection, cv->mres, cmd);
            }
        } break;

        case PYCBC_CMD_GETREPLICA:
        case PYCBC_CMD_GETREPLICA_INDEX:
        case PYCBC_CMD_GETREPLICA_ALL: {
            CMDSCOPE_NG_PARAMS(GETREPLICA, getreplica, gv->u.replica.strategy)
            {
                COMMON_OPTS(PYCBC_getreplica_ATTR, rget, getreplica);
                err = pycbc_rget(self->instance, cv->mres, cmd);
            }
        } break;
        default:
            err = LCB_ERROR;
            abort();
            break;
    }
    GT_ERR:
    if (err != LCB_SUCCESS) {
        PYCBC_DEBUG_LOG_CONTEXT(context, "Got result %d", err)
        PYCBC_EXCTHROW_SCHED(err);
        rv = -1;
        goto GT_DONE;
    } else {
        rv = 0;
    }

    GT_DONE:
        PYCBC_DEBUG_LOG_CONTEXT(context, "Got rv %d", rv)
        PYCBC_PYBUF_RELEASE(&keybuf);
        PYCBC_DEBUG_LOG_CONTEXT(context, "Finished processing")

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
        gv->u.replica.strategy = LCB_REPLICA_MODE_ANY;
        return 0;

    case PYCBC_CMD_GETREPLICA:
        gv->u.replica.strategy = LCB_REPLICA_MODE_ANY;
        return 0;

    case PYCBC_CMD_GETREPLICA_INDEX:
        if (replica_O == NULL || replica_O == Py_None) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "rgetix must have a valid replica index");
            return -1;
        }
        switch ((short)pycbc_IntAsL(replica_O)) {
        case 0:
            gv->u.replica.strategy = LCB_REPLICA_MODE_IDX0;
            break;
        case 1:
            gv->u.replica.strategy = LCB_REPLICA_MODE_IDX1;
            break;
        case 2:
            gv->u.replica.strategy = LCB_REPLICA_MODE_IDX2;
            break;
        default:
            break;
        }
        if (PyErr_Occurred()) {
            return -1;
        }
        return 0;

    case PYCBC_CMD_GETREPLICA_ALL:
        gv->u.replica.strategy = LCB_REPLICA_MODE_ALL;
        return 0;

    default:
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Replica option not supported for this operation");
        return -1;
    }
    return -1;
}

static PyObject*
get_common(pycbc_Bucket *self, PyObject *args, PyObject *kwargs, int optype,
    int argopts, pycbc_stack_context_handle context)
{
    Py_ssize_t ncmds = 0;
    pycbc_seqtype_t seqtype;
    PyObject *kobj = NULL;
    PyObject *is_quiet = NULL;
    PyObject *ttl_O = NULL;
    PyObject *replica_O = NULL;
    PyObject *nofmt_O = NULL;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    struct getcmd_vars_st gv = { 0 };
#define X(name, target, type) name,
    static char *kwlist[] = {
            "keys", "ttl", "quiet", "replica", "no_format", NULL};
#undef X
    pycbc_Collection_t collection = pycbc_Collection_as_value(self, kwargs);
    int rv = PyArg_ParseTupleAndKeywords(args,
                                         kwargs,
                                         "O|OOOO",
                                         kwlist,
                                         &kobj,
                                         &ttl_O,
                                         &is_quiet,
                                         &replica_O,
                                         &nofmt_O);

    if (!rv) {
        if (!PyErr_Occurred()) {
            PYCBC_EXCTHROW_ARGS()
        }
        return NULL;
    }

    gv.optype = optype;

    rv = pycbc_get_ttl(ttl_O, &gv.u.ttl, 1);
    if (rv < 0) {
        goto GT_FINALLY;
    }

    if (replica_O && replica_O != Py_None && replica_O != Py_False) {
        if (-1 == handle_replica_options(&optype, &gv, replica_O)) {
            goto GT_FINALLY;
        }
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(kobj, optype, &ncmds, &seqtype);
        if (rv < 0) {
            goto GT_FINALLY;
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
    {
        // temporary wrapping code until everything is migrated to collections

        if (argopts & PYCBC_ARGOPT_MULTI) {
            rv = PYCBC_OPUTIL_ITER_MULTI_COLLECTION(&collection,
                                                    seqtype,
                                                    kobj,
                                                    &cv,
                                                    optype,
                                                    handle_single_key,
                                                    &gv,
                                                    context);
        } else {
#ifndef PYCBC_UNIT_GEN
            rv = PYCBC_TRACE_WRAP_NOTERV(handle_single_key,
                                         kwargs,
                                         1,
                                         &cv,
                                         &context,
                                         self,
                                         NULL,
                                         &collection,
                                         &cv,
                                         optype,
                                         kobj,
                                         NULL,
                                         NULL,
                                         NULL,
                                         &gv);
#else
            rv = PYCBC_TRACE_WRAP_NOTERV(handle_single_key,
                                         kwargs,
                                         1,
                                         &cv,
                                         &context,
                                         self,
                                         NULL,
                                         unit,
                                         &cv,
                                         optype,
                                         kobj,
                                         NULL,
                                         NULL,
                                         NULL,
                                         &gv);
#endif
#ifndef PYCBC_GLOBAL_SCHED
            if (!rv) {
                cv.sched_cmds++;
            }
#endif
        }
    }
    PYCBC_DEBUG_LOG_CONTEXT(context,
                            "Got rv %d, cv.is_seqcmd %d and cv.sched_cmds %d",
                            rv,
                            cv.is_seqcmd,
                            cv.sched_cmds)
    if (rv < 0) {
        pycbc_wait_for_scheduled(self, kwargs, &context, &cv);
        goto GT_DONE;
    }

    if (pycbc_maybe_set_quiet(cv.mres, is_quiet) == -1) {
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }

GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
GT_FINALLY:
    pycbc_Collection_free_unmanaged_contents(&collection);
    return cv.ret;
}

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                int,
                handle_single_lookup,
                pycbc_oputil_keyhandler_raw_Bucket *handler,
                pycbc_Collection_t *collection,
                struct pycbc_common_vars *cv,
                int optype,
                PyObject *curkey,
                PyObject *curval,
                PyObject *options,
                pycbc_Item *itm,
                void *arg)
{
    pycbc_Bucket *self = collection->bucket;
    pycbc_pybuffer keybuf = {NULL};
    int rv = 0;
    if (itm) {
      PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Items not supported for subdoc!");
      return -1;
    }
    if (pycbc_tc_encode_key(self, curkey, &keybuf) != 0) {
      return -1;
    }
    CMDSCOPE_NG(SUBDOC, subdoc) {


        PYCBC_CMD_SET_KEY_SCOPE(subdoc, cmd, keybuf);
        rv = PYCBC_TRACE_WRAP(pycbc_sd_handle_speclist,
                              NULL,
                              collection,
                              cv->mres,
                              curkey,
                              curval,
                              cmd);
    }
    GT_ERR:
GT_DONE:
    PYCBC_PYBUF_RELEASE(&keybuf);
  return rv;
}
TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
static, PyObject *,
sdlookup_common, pycbc_Bucket *self, PyObject *args, PyObject *kwargs, int argopts)
{
    Py_ssize_t ncmds;
    PyObject *kobj = NULL;
    PyObject *quiet_key = NULL;
    pycbc_seqtype_t seqtype;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    static char *kwlist[] = { "ks", "quiet", NULL };
    pycbc_Collection_t collection = pycbc_Collection_as_value(self, kwargs);
    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "O|O", kwlist, &kobj, &quiet_key)) {
        PYCBC_EXCTHROW_ARGS();
        goto GT_FAIL;
    }

    if (pycbc_oputil_check_sequence(kobj, 0, &ncmds, &seqtype) != 0) {
        goto GT_FAIL;
    }

    if (pycbc_common_vars_init(&cv, self, argopts, ncmds, 1) != 0) {
        goto GT_FAIL;
    }

    if (PYCBC_OPUTIL_ITER_MULTI_COLLECTION(&collection,
                                           seqtype,
                                           kobj,
                                           &cv,
                                           0,
                                           handle_single_lookup,
                                           NULL,
                                           context) != 0) {
        pycbc_wait_for_scheduled(self, kwargs, &context, &cv);
        goto GT_DONE;
    }

    if (pycbc_maybe_set_quiet(cv.mres, quiet_key) != 0) {
        goto GT_DONE;
    }

    pycbc_common_vars_wait(&cv, self, context);

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    GT_FINAL:
        pycbc_Collection_free_unmanaged_contents(&collection);
        return cv.ret;
    GT_FAIL:
        cv.ret = NULL;
        goto GT_FINAL;
}

PyObject *
pycbc_Bucket_lookup_in(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    PyObject* result=NULL;
    PYCBC_TRACE_WRAP_TOPLEVEL(result, LCBTRACE_OP_REQUEST_ENCODING, sdlookup_common, self->tracer,
            self,
            args,
            kwargs,
            PYCBC_ARGOPT_SINGLE);
    return result;
}

PyObject *
pycbc_Bucket_lookup_in_multi(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    PyObject* result=NULL;
    PYCBC_TRACE_WRAP_TOPLEVEL(result,LCBTRACE_OP_REQUEST_ENCODING, sdlookup_common, self->tracer,
            self,
            args,
            kwargs,
            PYCBC_ARGOPT_MULTI);
    return result;
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Bucket_##name(pycbc_Bucket *self, \
                                      PyObject *args, PyObject *kwargs) { \
                                      PyObject* result;\
    PYCBC_TRACE_WRAP_TOPLEVEL(result,LCBTRACE_OP_REQUEST_ENCODING,get_common, self->tracer, self, args, kwargs, operation, mode); \
    return result;\
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
