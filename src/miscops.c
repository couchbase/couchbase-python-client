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
 * This file contains 'miscellaneous' operations. Functions contained here
 * might move to other files if they become more complex.
 *
 * More specifically, this contains 'key-only' operations that don't
 * require a value.
 */


/**
 * This is called during each iteration of delete/unlock
 */
TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                int,
                handle_single_keyop,
                pycbc_oputil_keyhandler_raw_Bucket *handler,
                pycbc_Collection_t *collection,
                struct pycbc_common_vars *cv,
                int optype,
                PyObject *curkey,
                PyObject *curval,
                PyObject *options,
                pycbc_Item *item,
                void *arg)
{
    int rv;
    pycbc_pybuffer keybuf = { NULL };
    lcb_uint64_t cas = 0;
    lcb_STATUS err = LCB_SUCCESS;
    pycbc_Bucket *self = collection->bucket;
    (void)options;
    (void)arg;
    (void)handler;

    if ( (optype == PYCBC_CMD_UNLOCK || optype == PYCBC_CMD_ENDURE)
            && PYCBC_OPRES_CHECK(curkey)) {
        curval = curkey;
        curkey = ((pycbc_OperationResult*)curkey)->key;
    }
    rv = pycbc_tc_encode_key(self, curkey, &keybuf);
    if (rv == -1) {
        return -1;
    }

    if (item) {
        cas = item->cas;
    } else if (curval) {
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
            rv = -1;
            goto GT_DONE;
        }
    }
#define COMMON_OPTS(CMD, X, NAME, CMDNAME)           \
    X((CMD), cas, cas);                              \
    PYCBC_CMD_SET_KEY_SCOPE(CMDNAME, (CMD), keybuf); \
    PYCBC_TRACECMD_TYPED(CMDNAME, (CMD), context, cv->mres, curkey, self);

    if (optype == PYCBC_CMD_UNLOCK) {
        if (!cas) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "CAS must be specified for unlock");
            rv = -1;
            goto GT_DONE;
        }
        {
            CMDSCOPE_NG(UNLOCK, unlock)
            {
                COMMON_OPTS(cmd, PYCBC_unlock_ATTR, unl, unlock);
                err = pycbc_unlock(collection, cv->mres, cmd);
            }
        }
    }
    else if (optype == PYCBC_CMD_ENDURE) {
        CMDSCOPE_NG_PARAMS(STORE,store, LCB_STORE_UPSERT) {
            COMMON_OPTS(cmd, PYCBC_endure_ATTR, endure, endure);
            err = cv->mctx->addcmd(cv->mctx, (lcb_CMDBASE *) cmd);
        }
    }
    else {

        CMDSCOPE_NG(REMOVE, remove)
        {
            PYCBC_DUR_INIT(err, cmd, remove, cv->mres->dur);
            if (err){
                CMDSCOPE_GENERIC_FAIL(,REMOVE,remove)
            }
            COMMON_OPTS(cmd, PYCBC_remove_ATTR, rm, remove);
            err = pycbc_remove(collection, cv->mres, cmd);
        }
    }
GT_ERR:
    if (err == LCB_SUCCESS) {
        rv = 0;
    } else {
        rv = -1;
        PYCBC_EXCTHROW_SCHED(err);
    }

    GT_DONE:
        PYCBC_PYBUF_RELEASE(&keybuf);
        return rv;
#undef COMMON_OPTS
}

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING, static, PyObject*, keyop_common, pycbc_Bucket *self, PyObject *args, PyObject *kwargs, int optype,
    int argopts)
{
    int rv;
    Py_ssize_t ncmds = 0;
    pycbc_seqtype_t seqtype = 0;
    PyObject *casobj = NULL;
    PyObject *is_quiet = NULL;
    PyObject *kobj = NULL;
    char persist_to = 0, replicate_to = 0;
    pycbc_DURABILITY_LEVEL durability_level =
            LCB_DURABILITYLEVEL_MAJORITY_AND_PERSIST_ON_MASTER;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = {"keys",
                             "cas",
                             "quiet",
                             "persist_to",
                             "replicate_to",
                             "durability_level",
                             NULL};

    pycbc_Collection_t collection = pycbc_Collection_as_value(self, kwargs);

    PYCBC_DEBUG_LOG_CONTEXT(context, "Parsing args %R", kwargs)
    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|OOBBB",
                                     kwlist,
                                     &kobj,
                                     &casobj,
                                     &is_quiet,
                                     &persist_to,
                                     &replicate_to,
                                     &durability_level);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        goto GT_FAIL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(kobj, 1, &ncmds, &seqtype);
        if (rv < 0) {
            goto GT_FAIL;
        }

        if (casobj && PyObject_IsTrue(casobj)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Can't pass CAS for multiple keys");
        }

    } else {
        ncmds = 1;
    }
    PYCBC_DEBUG_LOG_CONTEXT(context, "Got durability_level %d", durability_level)

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, 0);
    if (rv < 0) {
        goto GT_FAIL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = PYCBC_OPUTIL_ITER_MULTI_COLLECTION(&collection,
                                                seqtype,
                                                kobj,
                                                &cv,
                                                optype,
                                                handle_single_keyop,
                                                NULL,
                                                context);
    } else {
        rv = PYCBC_TRACE_WRAP_NOTERV(handle_single_keyop,
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
                                     casobj,
                                     NULL,
                                     NULL,
                                     NULL);
    }

    if (rv < 0) {
        pycbc_wait_for_scheduled(self, kwargs, &context, &cv);
        PYCBC_DEBUG_LOG_CONTEXT(context,"Got error from keyops")
        goto GT_DONE;
    }

    if (optype == PYCBC_CMD_DELETE) {
        rv = pycbc_handle_durability_args(self,
                                          &cv.mres->dur,
                                          persist_to,
                                          replicate_to,
                                          durability_level);
        PYCBC_DEBUG_LOG_CONTEXT(
                context, "Handling delete durability, got rv %d", rv)
        if (rv == 1) {
            cv.mres->mropts |= PYCBC_MRES_F_DURABILITY;

        } else if (rv == -1) {
            PYCBC_DEBUG_LOG_CONTEXT(context, "Problems with durability")
            goto GT_DONE;
        }
        if (pycbc_maybe_set_quiet(cv.mres, is_quiet) == -1) {
            PYCBC_DEBUG_LOG_CONTEXT(context, "Problems with maybe_set_quiet")
            goto GT_DONE;
        }
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    pycbc_Collection_free_unmanaged_contents(&collection);
    return cv.ret;
    GT_FAIL:
        cv.ret = NULL;
        goto GT_DONE;
}
#if PYCBC_ENDURE
TRACED_FUNCTION_WRAPPER(endure_multi, LCBTRACE_OP_REQUEST_ENCODING, Bucket)
{
    int rv;
    Py_ssize_t ncmds;
    pycbc_seqtype_t seqtype;
    char persist_to = 0, replicate_to = 0;
    lcb_durability_opts_t dopts = { 0 };
    PyObject *keys;
    PyObject *is_delete_O = Py_False;
    lcb_STATUS err;
    float timeout = 0.0;
    float interval = 0.00;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = {
            "keys",
            "persist_to",
            "replicate_to",
            "check_removed",
            "timeout",
            "interval",
            NULL
    };
    struct pycbc_Collection collection =
            pycbc_Collection_as_value(self, kwargs);
    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "OBB|Off", kwlist,
                                     &keys,
                                     &persist_to, &replicate_to,
                                     &is_delete_O, &timeout, &interval);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        goto GT_ERR;
    }

    rv = pycbc_oputil_check_sequence(keys, 1, &ncmds, &seqtype);
    if (rv < 0) {
        goto GT_ERR;
    }
    rv = pycbc_common_vars_init(&cv, self, PYCBC_ARGOPT_MULTI, ncmds, 0);
    if (rv < 0) {
        goto GT_ERR;
    }

    dopts.v.v0.cap_max = persist_to < 0 || replicate_to < 0;
    dopts.v.v0.check_delete = is_delete_O && PyObject_IsTrue(is_delete_O);
    dopts.v.v0.timeout = (lcb_uint32_t)(timeout * 1000000.0);
    dopts.v.v0.interval = (lcb_uint32_t)(interval * 1000000.0);
    dopts.v.v0.persist_to = persist_to;
    dopts.v.v0.replicate_to = replicate_to;
    cv.mctx = lcb_endure3_ctxnew(self->instance, &dopts, &err);
    if (cv.mctx == NULL) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    rv = PYCBC_OPUTIL_ITER_MULTI_COLLECTION(&collection,
                                            seqtype,
                                            keys,
                                            &cv,
                                            PYCBC_CMD_ENDURE,
                                            handle_single_keyop,
                                            NULL,
                                            context);
    if (rv < 0) {
        pycbc_wait_for_scheduled(self, kwargs, &context, &cv);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    GT_FINAL:
        pycbc_Collection_free_unmanaged_contents(&collection);
        return cv.ret;
    GT_ERR:
        cv.ret = NULL;
        goto GT_FINAL;
}
#else
TRACED_FUNCTION_WRAPPER(endure_multi, LCBTRACE_OP_REQUEST_ENCODING, Bucket)
{
    PYCBC_EXC_WRAP(
            LCB_ERRTYPE_INTERNAL, LCB_NOT_SUPPORTED, "Endure unavailable n V4");
    return NULL;
}
#endif

#define DECLFUNC(name, operation, mode)                           \
    PyObject *pycbc_Bucket_##name(                                \
            pycbc_Bucket *self, PyObject *args, PyObject *kwargs) \
    {                                                             \
        PyObject *result;                                         \
        PYCBC_TRACE_WRAP_TOPLEVEL(result,                         \
                                  "Bucket." #name,                \
                                  keyop_common,                   \
                                  self->tracer,                   \
                                  self,                           \
                                  args,                           \
                                  kwargs,                         \
                                  operation,                      \
                                  mode);                          \
        return result;                                            \
    }

DECLFUNC(remove, PYCBC_CMD_DELETE, PYCBC_ARGOPT_SINGLE)
DECLFUNC(unlock, PYCBC_CMD_UNLOCK, PYCBC_ARGOPT_SINGLE)
DECLFUNC(remove_multi, PYCBC_CMD_DELETE, PYCBC_ARGOPT_MULTI)
DECLFUNC(unlock_multi, PYCBC_CMD_UNLOCK, PYCBC_ARGOPT_MULTI)


TRACED_FUNCTION_WRAPPER(_stats,LCBTRACE_OP_REQUEST_ENCODING,Bucket)
{
    int rv;
    int ii;
    Py_ssize_t ncmds;
    lcb_STATUS err = LCB_ERROR;
    PyObject *keys = NULL, *is_keystats = NULL;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    static char *kwlist[] = {  "keys", "keystats", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "|OO", kwlist,
        &keys, &is_keystats);

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

    rv = pycbc_common_vars_init(&cv, self, PYCBC_ARGOPT_MULTI, ncmds, 0);
    if (rv < 0) {
        return NULL;
    }
    {
        CMDSCOPE_NG(STATS, stats)
        {
            if (keys) {
                for (ii = 0; ii < ncmds; ii++) {
                    char *key;
                    Py_ssize_t nkey;
                    PyObject *newkey = NULL;

                    PyObject *curkey = PySequence_GetItem(keys, ii);
                    rv = pycbc_BufFromString(curkey, &key, &nkey, &newkey);
                    if (rv < 0) {
                        PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS,
                                           0,
                                           "bad key type in stats",
                                           curkey);
                        goto GT_DONE;
                    }

                    LCB_CMD_SET_KEY(cmd, key, nkey);
                    if (is_keystats && PyObject_IsTrue(is_keystats)) {
                        pycbc_cmdstats_kv(cmd);
                    }
                    err = pycbc_stats(self->instance, cv.mres, cmd);
                    Py_XDECREF(newkey);
                }

            } else {
                err = pycbc_stats(self->instance, cv.mres, cmd);
            }
        }
    }
    GT_ERR:
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

TRACED_FUNCTION_WRAPPER(_ping,LCBTRACE_OP_REQUEST_ENCODING,Bucket)
{
    int rv;
    Py_ssize_t ncmds = 0;
    lcb_STATUS err = LCB_ERROR;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    CMDSCOPE_NG(PING, ping)
    {
        lcb_cmdping_all(cmd);
        lcb_cmdping_encode_json(cmd, 1, 1, 1);
        rv = pycbc_common_vars_init(&cv, self, PYCBC_ARGOPT_MULTI, ncmds, 0);
        if (rv < 0) {
            return NULL;
        }

        lcb_sched_enter(self->instance);
        err = pycbc_ping(self->instance, cv.mres, cmd);
    }
    GT_ERR:
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }
    lcb_sched_leave(self->instance);
GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

TRACED_FUNCTION_WRAPPER(_diagnostics,LCBTRACE_OP_REQUEST_ENCODING,Bucket)
{
    int rv;
    Py_ssize_t ncmds = 0;
    lcb_STATUS err = LCB_ERROR;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    CMDSCOPE_NG(DIAG, diag)
    {
        lcb_cmddiag_prettify(cmd, 1);
        lcb_cmddiag_report_id(cmd, "PYCBC", strlen("PYCBC"));
        rv = pycbc_common_vars_init(&cv, self, PYCBC_ARGOPT_MULTI, ncmds, 0);

        if (rv < 0) {
            return NULL;
        }

        lcb_sched_enter(self->instance);
        PYCBC_CONN_THR_BEGIN(self);
        err = lcb_diag(self->instance, cv.mres, cmd);
    }
    GT_ERR:
    PYCBC_CONN_THR_END(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }
    lcb_sched_leave(self->instance);

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}