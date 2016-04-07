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
    if (Py_REFCNT(self) > 1) {
        Py_DECREF(self);
        PYCBC_CONN_THR_BEGIN(self);
        return;
    }

    pycbc_assert(self->unlock_gil == 0);
    Py_DECREF(self);
}

#define CB_THR_END cb_thr_end
#define CB_THR_BEGIN cb_thr_begin
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
    if (err == LCB_SUCCESS || mres->errop) {
        return 0;
    }

    if (check_enoent &&
            (mres->mropts & PYCBC_MRES_F_QUIET) &&
            (err == LCB_KEY_ENOENT || err == LCB_SUBDOC_PATH_ENOENT)) {
        return 0;
    }

    mres->errop = (PyObject*)res;
    Py_INCREF(mres->errop);
    return 1;
}


static void
operation_completed(pycbc_Bucket *self, pycbc_MultiResult *mres)
{
    pycbc_assert(self->nremaining);
    --self->nremaining;

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
        pycbc_asyncresult_invoke(ares);
    }
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

    pycbc_assert(pycbc_multiresult_check(resp->cookie));
    *mres = (pycbc_MultiResult*)resp->cookie;
    *conn = (*mres)->parent;

    CB_THR_END(*conn);

    rv = pycbc_tc_decode_key(*conn, resp->key, resp->nkey, &hkey);

    if (rv < 0) {
        pycbc_multiresult_adderr(*mres);
        return -1;
    }

    mrdict = pycbc_multiresult_dict(*mres);

    *res = (pycbc_Result*)PyDict_GetItem(mrdict, hkey);

    if (*res) {
        int exists_ok = (restype & RESTYPE_EXISTS_OK) ||
                ( (*mres)->mropts & PYCBC_MRES_F_UALLOCED);

        if (!exists_ok) {
            if ((*conn)->flags & PYCBC_CONN_F_WARNEXPLICIT) {
                PyErr_WarnExplicit(PyExc_RuntimeWarning,
                                   "Found duplicate key",
                                   __FILE__, __LINE__,
                                   "couchbase._libcouchbase",
                                   NULL);

            } else {
                PyErr_WarnEx(PyExc_RuntimeWarning,
                             "Found duplicate key",
                             1);
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
        if ( (*mres)->mropts & PYCBC_MRES_F_ITEMS) {
            *res = (pycbc_Result*)pycbc_item_new(*conn);

        } else if (restype & RESTYPE_BASE) {
            *res = (pycbc_Result*)pycbc_result_new(*conn);

        } else if (restype & RESTYPE_OPERATION) {
            *res = (pycbc_Result*)pycbc_opresult_new(*conn);

        } else if (restype & RESTYPE_VALUE) {
            *res = (pycbc_Result*)pycbc_valresult_new(*conn);

        } else {
            abort();
        }

        PyDict_SetItem(mrdict, hkey, (PyObject*)*res);

        (*res)->key = hkey;
        Py_DECREF(*res);
    }

    if (resp->rc) {
        (*res)->rc = resp->rc;
    }

    if (resp->rc != LCB_SUCCESS) {
        (*mres)->all_ok = 0;
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
        operation_completed(conn, mres);
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
        operation_completed(conn, mres);

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

    if (cbtype == LCB_CALLBACK_COUNTER) {
        restype |= RESTYPE_VALUE;
    } else {
        restype |= RESTYPE_OPERATION;
    }

    if (get_common_objects(resp, &conn, (pycbc_Result**)&res, restype, &mres) != 0) {
        operation_completed(conn, mres);
        CB_THR_BEGIN(conn);
        return;
    }

    dur_chain2(conn, mres, res, cbtype, resp);
}

static void
value_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    int rv;
    pycbc_Bucket *conn = NULL;
    pycbc_ValueResult *res = NULL;
    pycbc_MultiResult *mres = NULL;

    rv = get_common_objects(resp, &conn, (pycbc_Result**)&res, RESTYPE_VALUE,
        &mres);

    if (rv < 0) {
        goto GT_DONE;
    }

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
    operation_completed(conn, mres);
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

    rv = get_common_objects(rb, &conn,
        (pycbc_Result**)&res, RESTYPE_EXISTS_OK, &mres);
    if (rv < 0) {
        goto GT_ERROR;
    }

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
    operation_completed(conn, mres);
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

    rv = get_common_objects(resp, &conn, (pycbc_Result**)&res, optflags, &mres);

    if (rv == 0) {
        res->rc = resp->rc;
        maybe_push_operr(mres, (pycbc_Result*)res, resp->rc, 0);
    }
    if (resp->cas) {
        res->cas = resp->cas;
    }

    operation_completed(conn, mres);
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
    const lcb_RESPSTATS *resp = (const lcb_RESPSTATS *)resp_base;
    int do_return = 0;

    mres = (pycbc_MultiResult*)resp->cookie;
    parent = mres->parent;
    CB_THR_END(parent);

    if (resp->rc != LCB_SUCCESS) {
        do_return = 1;
        if (mres->errop == NULL) {
            pycbc_Result *res = (pycbc_Result*)pycbc_result_new(parent);
            res->rc = resp->rc;
            res->key = Py_None; Py_INCREF(res->key);
            maybe_push_operr(mres, res, resp->rc, 0);
        }
    }
    if (resp->rflags & LCB_RESP_F_FINAL) {
        /* Note this can happen in both success and error cases! */
        do_return = 1;
        operation_completed(parent, mres);
    }
    if (do_return) {
        CB_THR_BEGIN(parent);
        return;
    }

    skey = pycbc_SimpleStringN(resp->key, resp->nkey);
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

    mrdict = pycbc_multiresult_dict(mres);
    knodes = PyDict_GetItem(mrdict, skey);
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
    pycbc_ValueResult *vres;
    pycbc_MultiResult *mres;
    const lcb_RESPOBSERVE *oresp = (const lcb_RESPOBSERVE *)resp_base;

    if (resp_base->rflags & LCB_RESP_F_FINAL) {
        mres = (pycbc_MultiResult*)resp_base->cookie;
        operation_completed(mres->parent, mres);
        return;
    }

    rv = get_common_objects(resp_base, &conn, (pycbc_Result**)&vres,
        RESTYPE_VALUE|RESTYPE_EXISTS_OK|RESTYPE_VARCOUNT, &mres);
    if (rv < 0) {
        goto GT_DONE;
    }

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
}

static void
bootstrap_callback(lcb_t instance, lcb_error_t err)
{
    pycbc_Bucket *self;

    if (!start_global_callback(instance, &self)) {
        return;
    }
    pycbc_invoke_connected_event(self, err);
    end_global_callback(instance, self);
}

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

    /* Subdoc */
    lcb_install_callback3(instance, LCB_CALLBACK_SDLOOKUP, subdoc_callback);
    lcb_install_callback3(instance, LCB_CALLBACK_SDMUTATE, subdoc_callback);

    lcb_set_bootstrap_callback(instance, bootstrap_callback);

    pycbc_http_callbacks_init(instance);
}
