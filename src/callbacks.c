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

#include <libcouchbase/api3.h>
#include "pycbc.h"

#define CB_THREADS

#ifdef CB_THREADS

static PyObject * mk_sd_tuple(const lcb_SDENTRY *ent);

/*
 * Add the sub-document error to the result list
 */
static void mk_sd_error(pycbc__SDResult *res, pycbc_MultiResult *mres, lcb_error_t rc, size_t ix);

static void
cb_thr_end(pycbc_Bucket *self)
{
    PYCBC_CONN_THR_END(self);
    Py_INCREF((PyObject *)self);
}

static void
cb_thr_begin(pycbc_Bucket *self)
{
#ifdef PYCBC_TRACING
    if (self && self->tracer) {
        PYCBC_DEBUG_LOG("propagating tracer from %p, %p", self, self->tracer);
        pycbc_Tracer_propagate(self->tracer);
    }
#endif
    if (Py_REFCNT(self) > 1) {
        Py_DECREF(self);
        PYCBC_CONN_THR_BEGIN(self);
        return;
    }

    pycbc_assert(self->unlock_gil == 0);
    Py_DECREF(self);
}

#define CB_THR_END                 \
    PYCBC_DEBUG_LOG("cb_thr_end"); \
    cb_thr_end
#define CB_THR_BEGIN                 \
    PYCBC_DEBUG_LOG("cb_thr_begin"); \
    cb_thr_begin
#else
#define CB_THR_END(x)
#define CB_THR_BEGIN(x)
#endif


enum {
    RESTYPE_BASE = 1 << 0,
    RESTYPE_VALUE = 1 << 1,
    RESTYPE_OPERATION = 1 << 2,

    /* Extra flag indicating it's ok if it already exists */
    RESTYPE_EXISTS_OK = 1 << 3,

    /* Don't modify "remaining" count */
    RESTYPE_VARCOUNT = 1 << 4
};

/* Returns true if an error has been added... */
static int
maybe_push_operr(pycbc_MultiResult *mres, pycbc_Result *res, lcb_error_t err,
    int check_enoent)
{
#ifdef PYCBC_TRACING
    pycbc_stack_context_handle parent_context =
            res ? (res->tracing_context ? res->tracing_context->parent : NULL)
                : NULL;
    PYCBC_DEBUG_LOG_CONTEXT(parent_context, "maybe_push_operr")
#endif
    if (err == LCB_SUCCESS || mres->errop) {
        return 0;
    }
#ifdef PYCBC_TRACING
    if (parent_context) {
        PYCBC_DEBUG_LOG_CONTEXT(parent_context, "maybe_push_operr")
        pycbc_Result_propagate_context(
                res, res->tracing_context, mres ? mres->parent : NULL);
    }
#endif
    if (check_enoent &&
            (mres->mropts & PYCBC_MRES_F_QUIET) &&
            (err == LCB_KEY_ENOENT || err == LCB_SUBDOC_PATH_ENOENT)) {
        return 0;
    }

    mres->errop = (PyObject*)res;
    Py_INCREF(mres->errop);
    return 1;
}

static void operation_completed3(pycbc_Bucket *self,
                                 pycbc_MultiResult *mres,
                                 pycbc_enhanced_err_info *err_info)
{
    pycbc_assert(self->nremaining);
    --self->nremaining;
    if (mres) {
        mres->err_info = err_info;
        Py_XINCREF(err_info);
    }
    if ((self->flags & PYCBC_CONN_F_ASYNC) == 0) {
        if (!self->nremaining) {
            lcb_breakout(self->instance);
        }
        return;
    }

    if (mres) {
        pycbc_AsyncResult *ares;
        ares = (pycbc_AsyncResult *)mres;
        if (--ares->nops) {
            return;
        }
        pycbc_asyncresult_invoke(ares, err_info);
    }
}


void pycbc_dict_add_text_kv(PyObject *dict, const char *key, const char *value)
{
    if (!key || !value || !dict) {
        PYCBC_DEBUG_LOG("one of key %p value %p dict %p is NULL", key, value, dict);
    }
    PYCBC_DEBUG_LOG("adding %s to %s on %p\n", value, key, dict);
    {
        PyObject *valstr = pycbc_SimpleStringZ(value);
        PyObject *keystr = pycbc_SimpleStringZ(key);
        PyDict_SetItem(dict, keystr, valstr);
        PYCBC_DECREF(valstr);
        PYCBC_DECREF(keystr);
    }
}

void pycbc_enhanced_err_register_entry(PyObject **dict,
                                       const char *key,
                                       const char *value)
{
    if (!value) {
        return;
    }
    if (!*dict) {
        *dict = PyDict_New();
    }
    pycbc_dict_add_text_kv(*dict, key, value);
}

static pycbc_enhanced_err_info *pycbc_enhanced_err_info_store(
        const lcb_RESPBASE *respbase, int cbtype)
{
    pycbc_enhanced_err_info *err_info = NULL;
    const char *ref = lcb_resp_get_error_ref(cbtype, respbase);
    const char *context = lcb_resp_get_error_context(cbtype, respbase);
    pycbc_enhanced_err_register_entry(&err_info, "ref", ref);
    pycbc_enhanced_err_register_entry(&err_info, "context", context);
    return err_info;
}

static void operation_completed_with_err_info(pycbc_Bucket *self,
                                              pycbc_MultiResult *mres,
                                              int cbtype,
                                              const lcb_RESPBASE *resp,
                                              pycbc_Result *res)
{
    pycbc_enhanced_err_info *err_info =
            pycbc_enhanced_err_info_store(resp, cbtype);
    pycbc_stack_context_handle context = PYCBC_RESULT_EXTRACT_CONTEXT(res);
    PYCBC_DEBUG_LOG("Completed context %p with %p, nremaining is %d",
                    context,
                    res ? (PyObject *)res : NULL,
                    self ? self->nremaining : 0)
    PYCBC_CONTEXT_DEREF(context, 0);
    operation_completed3(self, mres, err_info);
    Py_XDECREF(err_info);
}

/**
 * Call this function for each callback. Note that even if this function
 * returns nonzero, CB_THR_BEGIN() must still be called, and the `conn`
 * and `mres` out parameters are considered valid
 * @param resp base response object
 * @param[out] conn the bucket object
 * @param[out] res the result object for the individual operation
 * @param restype What type should `res` be if it needs to be created
 * @param[out] mres the context for the current operation
 * @return 0 if operation processing may proceed, nonzero if operation
 * processing has completed. In both cases the `conn` and `mres` paramters
 * are valid, however.
 */
static int
get_common_objects(const lcb_RESPBASE *resp, pycbc_Bucket **conn,
    pycbc_Result **res, int restype, pycbc_MultiResult **mres)

{
    PyObject *hkey;
    PyObject *mrdict;
    int rv;
    PyObject *pycbc_err[3] = {0};
#ifdef PYCBC_TRACING
    pycbc_stack_context_handle parent_context = NULL;
    pycbc_stack_context_handle decoding_context = NULL;
    int was_tracing_stub = 0;
#endif
    pycbc_assert(pycbc_multiresult_check(resp->cookie));
    *mres = (pycbc_MultiResult*)resp->cookie;
    *conn = (*mres)->parent;

    CB_THR_END(*conn);

    rv = pycbc_tc_decode_key(*conn, resp->key, resp->nkey, &hkey);

    if (rv < 0) {
        pycbc_multiresult_adderr(*mres);
        return -1;
    }
    pycbc_store_error(pycbc_err);
    {
        mrdict = pycbc_multiresult_dict(*mres);
        *res = (pycbc_Result *) PyDict_GetItem(mrdict, hkey);

#ifdef PYCBC_TRACING
        parent_context = PYCBC_MULTIRESULT_EXTRACT_CONTEXT(*mres, hkey, res);
        if (parent_context) {
            decoding_context =
                    pycbc_Result_start_context(parent_context,
                                               hkey,
                                               "get_common_objects",
                                               LCBTRACE_OP_RESPONSE_DECODING,
                                               LCBTRACE_REF_FOLLOWS_FROM);
        };
#endif
        if (*res) {
            int exists_ok = (restype & RESTYPE_EXISTS_OK) ||
                            ((*mres)->mropts & PYCBC_MRES_F_UALLOCED);
            was_tracing_stub = (*res)->is_tracing_stub;

            if (!exists_ok) {
                if ((*conn)->flags & PYCBC_CONN_F_WARNEXPLICIT) {
                    PyErr_WarnExplicit(PyExc_RuntimeWarning,
                                       "Found duplicate key",
                                       __FILE__,
                                       __LINE__,
                                       "couchbase._libcouchbase",
                                       NULL);

                } else {
                    PyErr_WarnEx(
                            PyExc_RuntimeWarning, "Found duplicate key", 1);
                }
                /**
                 * We need to destroy the existing object and re-create it.
                 */
                PyDict_DelItem(mrdict, hkey);
                *res = NULL;

            } else {
                Py_XDECREF(hkey);
            }
        }

        if (*res == NULL) {
            /* Now, get/set the result object */
            if ((*mres)->mropts & PYCBC_MRES_F_ITEMS) {
                PYCBC_DEBUG_LOG("Item creation");
                *res = (pycbc_Result *)pycbc_item_new(*conn);
            } else if (restype & RESTYPE_BASE) {
                PYCBC_DEBUG_LOG("Result creation");
                *res = (pycbc_Result *)pycbc_result_new(*conn);

            } else if (restype & RESTYPE_OPERATION) {
                PYCBC_DEBUG_LOG("Opresult creation");
                *res = (pycbc_Result *)pycbc_opresult_new(*conn);

            } else if (restype & RESTYPE_VALUE) {
                PYCBC_DEBUG_LOG("Valresult creation");
                *res = (pycbc_Result *)pycbc_valresult_new(*conn);
            } else {
                *res = (pycbc_Result *)pycbc_result_new(*conn);
                if ((*conn)->nremaining) {
                    --(*conn)->nremaining;
                }
            }
            if (*res) {
                PyDict_SetItem(mrdict, hkey, (PyObject *)*res);
                (*res)->key = hkey;
                PYCBC_DECREF(*res);

            } else {
                abort();
            }
        }
#ifdef PYCBC_TRACING
            if (res && *res) {
                pycbc_Result_propagate_context(*res, parent_context, *conn);
            }
            PYCBC_CONTEXT_DEREF(decoding_context, 1);
#define PYCBC_CLEAN_PARENT
#ifdef PYCBC_CLEAN_PARENT
            if (parent_context && parent_context->is_stub) {
                PYCBC_CONTEXT_DEREF(parent_context, 0);
            }
#endif
#endif
        if (resp->rc && res && *res) {
            (*res)->rc = resp->rc;
        }

        if (resp->rc != LCB_SUCCESS) {
            (*mres)->all_ok = 0;
        }
    }
#define PYCBC_RESTORE_PRE_CONTEXT_ERROR
    if (pycbc_err[0] || pycbc_err[1] || pycbc_err[2]) {
#ifdef PYCBC_RESTORE_PRE_CONTEXT_ERROR
        pycbc_fetch_error(pycbc_err);
#else

        PYCBC_XDECREF(pycbc_err[0]);
        PYCBC_XDECREF(pycbc_err[1]);
        PYCBC_XDECREF(pycbc_err[2]);
#endif
    }
    return 0;
}

static void
invoke_endure_test_notification(pycbc_Bucket *self, pycbc_Result *resp)
{
    PyObject *ret;
    PyObject *argtuple = Py_BuildValue("(O)", resp);
    ret = PyObject_CallObject(self->dur_testhook, argtuple);
    pycbc_assert(ret);

    Py_XDECREF(ret);
    Py_XDECREF(argtuple);
}

static void
dur_chain2(pycbc_Bucket *conn,
    pycbc_MultiResult *mres,
    pycbc_OperationResult *res, int cbtype, const lcb_RESPBASE *resp)
{
    lcb_error_t err;
    lcb_durability_opts_t dopts = { 0 };
    lcb_CMDENDURE cmd = { 0 };
    lcb_MULTICMD_CTX *mctx = NULL;
    int is_delete = cbtype == LCB_CALLBACK_REMOVE;
    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "durability chain callback")
    res->rc = resp->rc;
    if (resp->rc == LCB_SUCCESS) {
        const lcb_MUTATION_TOKEN *mutinfo = lcb_resp_get_mutation_token(cbtype, resp);
        Py_XDECREF(res->mutinfo);

        if (mutinfo && LCB_MUTATION_TOKEN_ISVALID(mutinfo)) {
            /* Create the mutation token tuple: (vb,uuid,seqno) */
            res->mutinfo = Py_BuildValue("HKKO",
                LCB_MUTATION_TOKEN_VB(mutinfo),
                LCB_MUTATION_TOKEN_ID(mutinfo),
                LCB_MUTATION_TOKEN_SEQ(mutinfo),
                conn->bucket);
        } else {
            Py_INCREF(Py_None);
            res->mutinfo = Py_None;
        }
        res->cas = resp->cas;
    }

    /** For remove, we check quiet */
    maybe_push_operr(mres, (pycbc_Result*)res, resp->rc, is_delete ? 1 : 0);

    if ((mres->mropts & PYCBC_MRES_F_DURABILITY) == 0 || resp->rc != LCB_SUCCESS) {
        operation_completed_with_err_info(
                conn, mres, cbtype, resp, (pycbc_Result *)res);
        CB_THR_BEGIN(conn);
        return;
    }

    if (conn->dur_testhook && conn->dur_testhook != Py_None) {
        invoke_endure_test_notification(conn, (pycbc_Result *)res);
    }

    /** Setup global options */
    dopts.v.v0.persist_to = mres->dur.persist_to;
    dopts.v.v0.replicate_to = mres->dur.replicate_to;
    dopts.v.v0.timeout = conn->dur_timeout;
    dopts.v.v0.check_delete = is_delete;
    if (mres->dur.persist_to < 0 || mres->dur.replicate_to < 0) {
        dopts.v.v0.cap_max = 1;
    }

    lcb_sched_enter(conn->instance);
    mctx = lcb_endure3_ctxnew(conn->instance, &dopts, &err);
    if (mctx == NULL) {
        goto GT_DONE;
    }

    cmd.cas = resp->cas;
    LCB_CMD_SET_KEY(&cmd, resp->key, resp->nkey);
    err = mctx->addcmd(mctx, (lcb_CMDBASE*)&cmd);
    if (err != LCB_SUCCESS) {
        goto GT_DONE;
    }

    err = mctx->done(mctx, mres);
    if (err == LCB_SUCCESS) {
        mctx = NULL;
        lcb_sched_leave(conn->instance);
    }

    GT_DONE:
    if (mctx) {
        mctx->fail(mctx);
    }
    if (err != LCB_SUCCESS) {
        res->rc = err;
        maybe_push_operr(mres, (pycbc_Result*)res, err, 0);
        operation_completed_with_err_info(
                conn, mres, cbtype, resp, (pycbc_Result *)res);
    }

    CB_THR_BEGIN(conn);
}

/**
 * Common handler for durability
 */
static void
durability_chain_common(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    pycbc_Bucket *conn;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres;
    int restype = RESTYPE_VARCOUNT;
    PYCBC_DEBUG_LOG("Durability chain callback")
    if (cbtype == LCB_CALLBACK_COUNTER) {
        restype |= RESTYPE_VALUE;
    } else {
        restype |= RESTYPE_OPERATION;
    }

    if (get_common_objects(resp, &conn, (pycbc_Result**)&res, restype, &mres) != 0) {
        operation_completed_with_err_info(
                conn, mres, cbtype, resp, (pycbc_Result *)res);
        PYCBC_DEBUG_LOG("Durability chain returning")

        CB_THR_BEGIN(conn);
        return;
    }
    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "durability_chain_common")
    dur_chain2(conn, mres, res, cbtype, resp);
}

static void
value_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    int rv;
    pycbc_Bucket *conn = NULL;
    pycbc_ValueResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    PYCBC_DEBUG_LOG("Value callback")
    rv = get_common_objects(resp, &conn, (pycbc_Result**)&res, RESTYPE_VALUE,
        &mres);

    if (rv < 0) {
        goto GT_DONE;
    }
    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "Value callback continues")

    if (resp->rc == LCB_SUCCESS) {
        res->cas = resp->cas;
    } else {
        maybe_push_operr(mres, (pycbc_Result*)res, resp->rc,
            cbtype != LCB_CALLBACK_COUNTER);
        goto GT_DONE;
    }

    if (cbtype == LCB_CALLBACK_GET || cbtype == LCB_CALLBACK_GETREPLICA) {
        const lcb_RESPGET *gresp = (const lcb_RESPGET *)resp;
        lcb_U32 eflags;

        res->flags = gresp->itmflags;
        if (mres->mropts & PYCBC_MRES_F_FORCEBYTES) {
            eflags = PYCBC_FMT_BYTES;
        } else {
            eflags = gresp->itmflags;
        }

        if (res->value) {
            Py_DECREF(res->value);
            res->value = NULL;
        }

        rv = pycbc_tc_decode_value(mres->parent, gresp->value, gresp->nvalue,
            eflags, &res->value);
        if (rv < 0) {
            pycbc_multiresult_adderr(mres);
        }
    } else if (cbtype == LCB_CALLBACK_COUNTER) {
        const lcb_RESPCOUNTER *cresp = (const lcb_RESPCOUNTER *)resp;
        res->value = pycbc_IntFromULL(cresp->value);
    }
    GT_DONE:
        operation_completed_with_err_info(
                conn, mres, cbtype, resp, (pycbc_Result *)res);
        CB_THR_BEGIN(conn);
        (void)instance;
}

static void
mk_sd_error(pycbc__SDResult *res,
    pycbc_MultiResult *mres, lcb_error_t rc, size_t ix)
{
    PyObject *spec = PyTuple_GET_ITEM(res->specs, ix);
    PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_LCBERR, rc, "Subcommand failure", spec);
    pycbc_multiresult_adderr(mres);
}

static PyObject *
mk_sd_tuple(const lcb_SDENTRY *ent)
{
    PyObject *val = NULL;
    PyObject *ret;
    if (ent->status == LCB_SUCCESS && ent->nvalue != 0) {
        int rv = pycbc_tc_simple_decode(&val, ent->value, ent->nvalue, PYCBC_FMT_JSON);
        if (rv != 0) {
            return NULL;
        }
    }

    if (val == NULL) {
        val = Py_None;
        Py_INCREF(Py_None);
    }

    ret = Py_BuildValue("(iO)", ent->status, val);
    Py_DECREF(val);
    return ret;

}

static void
subdoc_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb)
{
    int rv;
    pycbc_Bucket *conn;
    pycbc__SDResult *res;
    pycbc_MultiResult *mres;
    lcb_SDENTRY cur;
    size_t vii = 0, oix = 0;
    const lcb_RESPSUBDOC *resp = (const lcb_RESPSUBDOC *)rb;
    PYCBC_DEBUG_LOG("Subdoc callback")
    rv = get_common_objects(rb, &conn,
        (pycbc_Result**)&res, RESTYPE_EXISTS_OK, &mres);
    if (rv < 0) {
        goto GT_ERROR;
    }

    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "Subdoc callback continues")
    if (rb->rc == LCB_SUCCESS || rb->rc == LCB_SUBDOC_MULTI_FAILURE) {
        res->cas = rb->cas;
    } else {
        maybe_push_operr(mres, (pycbc_Result*)res, rb->rc, 0);
        goto GT_ERROR;
    }

    while ((lcb_sdresult_next(resp, &cur, &vii))) {
        size_t cur_index;
        PyObject *cur_tuple = mk_sd_tuple(&cur);

        if (cbtype == LCB_CALLBACK_SDMUTATE) {
            cur_index = cur.index;
        } else {
            cur_index = oix++;
        }

        if (cur_tuple == NULL) {
            pycbc_multiresult_adderr(mres);
            goto GT_ERROR;
        }

        if (cur.status != LCB_SUCCESS) {
            if (cbtype == LCB_CALLBACK_SDMUTATE) {
                mk_sd_error(res, mres, cur.status, cur_index);
            } else if (cur.status != LCB_SUBDOC_PATH_ENOENT) {
                mk_sd_error(res, mres, cur.status, cur_index);
            }
        }

        pycbc_sdresult_addresult(res, cur_index, cur_tuple);
        Py_DECREF(cur_tuple);
    }
    if (rb->rc == LCB_SUCCESS) {
        dur_chain2(conn, mres, (pycbc_OperationResult*)res, cbtype, (const lcb_RESPBASE*)resp);
        return;
    }

    GT_ERROR:
        operation_completed_with_err_info(
                conn, mres, cbtype, rb, (pycbc_Result *)res);
        CB_THR_BEGIN(conn);
        (void)instance;
}

static void
keyop_simple_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    int rv;
    int optflags = RESTYPE_OPERATION;
    pycbc_Bucket *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;

    if (cbtype == LCB_CALLBACK_ENDURE) {
        optflags |= RESTYPE_EXISTS_OK;
    }
    PYCBC_DEBUG_LOG("Keyop callback")
    rv = get_common_objects(resp, &conn, (pycbc_Result**)&res, optflags, &mres);
    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "Keyop callback continues")

    if (rv == 0) {
        res->rc = resp->rc;
        maybe_push_operr(mres, (pycbc_Result*)res, resp->rc, 0);
    }
    if (resp->cas) {
        res->cas = resp->cas;
    }

    operation_completed_with_err_info(
            conn, mres, cbtype, resp, (pycbc_Result *)res);
    CB_THR_BEGIN(conn);
    (void)instance;

}

static void
stats_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp_base)
{
    pycbc_MultiResult *mres;
    PyObject *value;
    PyObject *skey, *knodes;
    PyObject *mrdict;
    pycbc_Bucket *parent;
    pycbc_Result *res = NULL;
    const lcb_RESPSTATS *resp = (const lcb_RESPSTATS *)resp_base;
    int do_return = 0;

    mres = (pycbc_MultiResult*)resp->cookie;
    parent = mres->parent;
    CB_THR_END(parent);

    if (resp->rc != LCB_SUCCESS) {
        do_return = 1;
        if (mres->errop == NULL) {
            res = (pycbc_Result *)pycbc_result_new(parent);
            res->rc = resp->rc;
            res->key = Py_None; Py_INCREF(res->key);
            maybe_push_operr(mres, res, resp->rc, 0);
        }
    }
    if (resp->rflags & LCB_RESP_F_FINAL) {
        /* Note this can happen in both success and error cases! */
        do_return = 1;
        operation_completed_with_err_info(parent, mres, cbtype, resp_base, res);
    }
    if (do_return) {
        CB_THR_BEGIN(parent);
        return;
    }

    skey = pycbc_SimpleStringN(resp->key, resp->nkey);

    mrdict = pycbc_multiresult_dict(mres);
    knodes = PyDict_GetItem(mrdict, skey);

    value = pycbc_SimpleStringN(resp->value, resp->nvalue);
    {
        PyObject *intval = pycbc_maybe_convert_to_int(value);
        if (intval) {
            Py_DECREF(value);
            value = intval;

        } else {
            PyErr_Clear();
        }
    }

    if (!knodes) {
        knodes = PyDict_New();
        PyDict_SetItem(mrdict, skey, knodes);
    }

    PyDict_SetItemString(knodes, resp->server, value);
    Py_DECREF(skey);
    Py_DECREF(value);

    CB_THR_BEGIN(parent);
    (void)instance;
}



static void
observe_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp_base)
{
    int rv;
    pycbc_ObserveInfo *oi;
    pycbc_Bucket *conn;
    pycbc_ValueResult *vres = NULL;
    pycbc_MultiResult *mres;
    const lcb_RESPOBSERVE *oresp = (const lcb_RESPOBSERVE *)resp_base;
    PYCBC_DEBUG_LOG("observe callback")
    if (resp_base->rflags & LCB_RESP_F_FINAL) {
        mres = (pycbc_MultiResult*)resp_base->cookie;
        operation_completed_with_err_info(
                mres->parent, mres, cbtype, resp_base, (pycbc_Result *)vres);
        return;
    }

    rv = get_common_objects(resp_base, &conn, (pycbc_Result**)&vres,
        RESTYPE_VALUE|RESTYPE_EXISTS_OK|RESTYPE_VARCOUNT, &mres);

    if (rv < 0) {
        goto GT_DONE;
    }

    PYCBC_DEBUG_LOG_CONTEXT(vres ? vres->tracing_context : NULL,
                            "observe callback continues")

    if (resp_base->rc != LCB_SUCCESS) {
        maybe_push_operr(mres, (pycbc_Result*)vres, resp_base->rc, 0);
        goto GT_DONE;
    }

    if (!vres->value) {
        vres->value = PyList_New(0);
    }

    oi = pycbc_observeinfo_new(conn);
    if (oi == NULL) {
        pycbc_multiresult_adderr(mres);
        goto GT_DONE;
    }

    oi->from_master = oresp->ismaster;
    oi->flags = oresp->status;
    oi->cas = oresp->cas;
    PyList_Append(vres->value, (PyObject*)oi);
    Py_DECREF(oi);

    GT_DONE:
    CB_THR_BEGIN(conn);
    (void)instance; (void)cbtype;
}

static int
start_global_callback(lcb_t instance, pycbc_Bucket **selfptr)
{
    *selfptr = (pycbc_Bucket *)lcb_get_cookie(instance);
    if (!*selfptr) {
        return 0;
    }
    PYCBC_DEBUG_LOG("start of bootstrap callback on bucket %p", selfptr);
    CB_THR_END(*selfptr);
    Py_INCREF((PyObject *)*selfptr);
    return 1;
}

static void
end_global_callback(lcb_t instance, pycbc_Bucket *self)
{
    Py_DECREF((PyObject *)(self));

    self = (pycbc_Bucket *)lcb_get_cookie(instance);
    if (self) {
        CB_THR_BEGIN(self);
    }
    PYCBC_DEBUG_LOG("end of bootstrap callback on bucket %p", self);
}

static void
bootstrap_callback(lcb_t instance, lcb_error_t err)
{
    pycbc_Bucket *self;

    if (!start_global_callback(instance, &self)) {
        return;
    }
    PYCBC_DEBUG_LOG("bootstrap callback on bucket %p", self);
    pycbc_invoke_connected_event(self, err);
    end_global_callback(instance, self);
}

#define LCB_PING_FOR_ALL_TYPES(X) \
    X(KV, kv)            \
    X(VIEWS, views)      \
    X(N1QL, n1ql)        \
    X(FTS, fts)

#define LCB_PING_GET_TYPE_S(X, Y)  \
    case LCB_PINGSVC_##X: \
        return #Y;

const char *get_type_s(lcb_PINGSVCTYPE type)
{
    switch (type) {
        LCB_PING_FOR_ALL_TYPES(LCB_PING_GET_TYPE_S)
    default:
        break;
    }
    return "Unknown type";
}
#undef LCB_PING_GET_TYPE_S
#undef LCB_PING_FOR_ALL_TYPES

static void ping_callback(lcb_t instance,
                          int cbtype,
                          const lcb_RESPBASE *resp_base)
{
    pycbc_Bucket *parent;
    const lcb_RESPPING *resp = (const lcb_RESPPING *)resp_base;

    pycbc_MultiResult *mres = (pycbc_MultiResult *)resp->cookie;
    PyObject *resultdict = pycbc_multiresult_dict(mres);
    parent = mres->parent;
    CB_THR_END(parent);

    if (resp->rc != LCB_SUCCESS) {
        if (mres->errop == NULL) {
            pycbc_Result *res = (pycbc_Result *)pycbc_result_new(parent);
            res->rc = resp->rc;
            res->key = Py_None;
            Py_INCREF(res->key);
            maybe_push_operr(mres, res, resp->rc, 0);
        }
    }

    {
        PyObject *struct_services_dict = PyDict_New();

        lcb_SIZE ii;
        for (ii = 0; ii < resp->nservices; ii++) {
            lcb_PINGSVC *svc = &resp->services[ii];
            const char *type_s = get_type_s(svc->type);
            PyObject *struct_server_list =
                    PyDict_GetItemString(struct_services_dict, type_s);
            if (!struct_server_list) {
                struct_server_list = PyList_New(0);
                PyDict_SetItemString(
                        struct_services_dict, type_s, struct_server_list);
                Py_DECREF(struct_server_list);
            }
            {
                PyObject *mrdict = PyDict_New();
                PyList_Append(struct_server_list, mrdict);
                switch (svc->status) {
                    case LCB_PINGSTATUS_OK:
                        break;
                    case LCB_PINGSTATUS_TIMEOUT:
                        break;
                    default:
                        pycbc_dict_add_text_kv(mrdict,
                                               "details",
                                               lcb_strerror_long(svc->rc));
                }
                pycbc_dict_add_text_kv(
                        mrdict, "server", svc->server);
                PyDict_SetItemString(mrdict,
                                     "status",
                                     PyLong_FromLong((long)svc->status));
                PyDict_SetItemString(
                        mrdict,
                        "latency",
                        PyLong_FromUnsignedLong((unsigned long)svc->latency));
                Py_DECREF(mrdict);
            }
        }
        PyDict_SetItemString(
                resultdict, "services_struct", struct_services_dict);
        Py_DECREF(struct_services_dict);
    }
    if (resp->njson) {

        pycbc_dict_add_text_kv(resultdict, "services_json", resp->json);
    }
    if (resp->rflags & LCB_RESP_F_FINAL) {
        /* Note this can happen in both success and error cases!*/
        operation_completed_with_err_info(
                parent, mres, cbtype, resp_base, NULL);
    }
    CB_THR_BEGIN(parent);
}


static void diag_callback(lcb_t instance,
                          int cbtype,
                          const lcb_RESPBASE *resp_base)
{
    pycbc_Bucket *parent;
    const lcb_RESPDIAG *resp = (const lcb_RESPDIAG *)resp_base;

    pycbc_MultiResult *mres = (pycbc_MultiResult *)resp->cookie;
    PyObject *resultdict = pycbc_multiresult_dict(mres);
    pycbc_Result *res = NULL;
    parent = mres->parent;
    CB_THR_END(parent);
    if (resp->rc != LCB_SUCCESS) {
        if (mres->errop == NULL) {
            res = (pycbc_Result *)pycbc_result_new(parent);
            res->rc = resp->rc;
            res->key = Py_None;
            Py_INCREF(res->key);
            maybe_push_operr(mres, res, resp->rc, 0);
        }
    }

    if (resp->njson) {
        pycbc_dict_add_text_kv(resultdict, "health_json", resp->json);
    }
    if (resp->rflags & LCB_RESP_F_FINAL) {
        /* Note this can happen in both success and error cases!*/
        operation_completed_with_err_info(parent, mres, cbtype, resp_base, res);
    }

    CB_THR_BEGIN(parent);
}

void pycbc_generic_cb(lcb_t instance,
                      int cbtype,
                      const lcb_RESPBASE *rb,
                      const char *NAME)
{
    const lcb_RESPCOUNTER *resp = (const lcb_RESPCOUNTER *)rb;
    int rv;
    int optflags = RESTYPE_OPERATION;
    pycbc_Bucket *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;

    PYCBC_DEBUG_LOG("%s callback", NAME)
    rv = get_common_objects((const lcb_RESPBASE *)resp,
                            &conn,
                            (pycbc_Result **)&res,
                            optflags,
                            &mres);
    PYCBC_DEBUG_LOG_CONTEXT(
            res ? res->tracing_context : NULL, "%s callback continues", NAME)

    if (rv == 0) {
        res->rc = resp->rc;
        maybe_push_operr(mres, (pycbc_Result *)res, resp->rc, 0);
    }

    operation_completed_with_err_info(conn,
                                      mres,
                                      cbtype,
                                      (const lcb_RESPBASE *)resp,
                                      (pycbc_Result *)res);
    CB_THR_BEGIN(conn);
    (void)instance;
}

#define PYCBC_CALLBACK_GENERIC(NAME)                                   \
    void NAME##_cb(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) \
    {                                                                  \
        pycbc_generic_cb(instance, cbtype, rb, #NAME);                 \
    }

#ifdef PYCBC_EXTRA_CALLBACK_WRAPPERS
#define PYCBC_FOR_EACH_GEN_CALLBACK(X) \
    X(LCB_CALLBACK_VERSIONS)           \
    X(LCB_CALLBACK_VERBOSITY)          \
    X(LCB_CALLBACK_FLUSH)              \
    X(LCB_CALLBACK_CBFLUSH)            \
    X(LCB_CALLBACK_OBSEQNO)            \
    X(LCB_CALLBACK_STOREDUR)           \
    X(LCB_CALLBACK_COUNTER)

PYCBC_FOR_EACH_GEN_CALLBACK(PYCBC_CALLBACK_GENERIC)
#endif

void
pycbc_callbacks_init(lcb_t instance)
{
    lcb_install_callback3(instance, LCB_CALLBACK_STORE, durability_chain_common);
    lcb_install_callback3(instance, LCB_CALLBACK_REMOVE, durability_chain_common);
    lcb_install_callback3(instance, LCB_CALLBACK_UNLOCK, keyop_simple_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_TOUCH, keyop_simple_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_ENDURE, keyop_simple_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_GET, value_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_GETREPLICA, value_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_COUNTER, value_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_OBSERVE, observe_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_STATS, stats_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_PING, ping_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_DIAG, diag_callback);
#ifdef PYCBC_EXTRA_CALLBACK_WRAPPERS
#define X(NAME) lcb_install_callback3(instance, NAME, NAME##_cb);
    PYCBC_FOR_EACH_GEN_CALLBACK(X)
#undef X
#endif
    /* Subdoc */
    lcb_install_callback3(instance, LCB_CALLBACK_SDLOOKUP, subdoc_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_SDMUTATE, subdoc_callback);

    lcb_set_bootstrap_callback(instance, bootstrap_callback);

    pycbc_http_callbacks_init(instance);
}
