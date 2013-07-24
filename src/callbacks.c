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
#define CB_THR_END PYCBC_CONN_THR_END
#define CB_THR_BEGIN PYCBC_CONN_THR_BEGIN
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
    RESTYPE_VARCOUNT = 1 << 4,
};

static PyObject *
make_error_tuple(void)
{
    PyObject *type, *value, *traceback;
    PyObject *ret;

    pycbc_assert(PyErr_Occurred());

    PyErr_Fetch(&type, &value, &traceback);
    PyErr_Clear();

    if (value == NULL) {
        value = Py_None; Py_INCREF(value);
    }
    if (traceback == NULL) {
        traceback = Py_None; Py_INCREF(traceback);
    }

    ret = PyTuple_New(3);
    /** Steal references from PyErr_Fetch() */
    PyTuple_SET_ITEM(ret, 0, type);
    PyTuple_SET_ITEM(ret, 1, value);
    PyTuple_SET_ITEM(ret, 2, traceback);

    return ret;
}

static void
push_fatal_error(pycbc_MultiResult* mres)
{
    PyObject *etuple;
    mres->all_ok = 0;
    if (!mres->exceptions) {
        mres->exceptions = PyList_New(0);
    }

    etuple = make_error_tuple();
    PyList_Append(mres->exceptions, etuple);
    Py_DECREF(etuple);
}

static void
maybe_push_operr(pycbc_MultiResult *mres,
                 pycbc_Result *res,
                 lcb_error_t err,
                 int check_enoent)
{
    if (err == LCB_SUCCESS || mres->errop) {
        return;
    }

    if (check_enoent &&
            (mres->mropts & PYCBC_MRES_F_QUIET) &&
            err == LCB_KEY_ENOENT) {
        return;
    }

    mres->errop = (PyObject*)res;
    Py_INCREF(mres->errop);
}

static void
maybe_breakout(pycbc_Connection *self)
{
    pycbc_assert(self->nremaining);

    if (!--self->nremaining) {
        lcb_breakout(self->instance);
    }
}

static int
get_common_objects(PyObject *cookie,
                   const void *key,
                   size_t nkey,
                   lcb_error_t err,
                   pycbc_Connection **conn,
                   pycbc_Result **res,
                   int restype,
                   pycbc_MultiResult **mres)

{
    PyObject *hkey;
    int rv;

    pycbc_assert(Py_TYPE(cookie) == &pycbc_MultiResultType);
    *mres = (pycbc_MultiResult*)cookie;
    *conn = (*mres)->parent;

    if (!(restype & RESTYPE_VARCOUNT)) {
        maybe_breakout(*conn);
    }

    CB_THR_END(*conn);

    rv = pycbc_tc_decode_key(*conn, key, nkey, &hkey);

    if (rv < 0) {
        push_fatal_error(*mres);
        return -1;
    }

    *res = (pycbc_Result*)PyDict_GetItem((PyObject*)*mres, hkey);

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
            PyDict_DelItem((PyObject*)*mres, hkey);
            *res = NULL;

        } else {
            Py_XDECREF(hkey);
        }

    }

    if (*res == NULL) {
        /**
         * Now, get/set the result object
         */
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

        PyDict_SetItem((PyObject*)*mres, hkey, (PyObject*)*res);

        (*res)->key = hkey;
        Py_DECREF(*res);
    }

    if (err) {
        (*res)->rc = err;
    }

    if (err != LCB_SUCCESS) {
        (*mres)->all_ok = 0;
    }

    return 0;
}

static void
durability_callback(lcb_t instance,
                    const void *cookie,
                    lcb_error_t err,
                    const lcb_durability_resp_t *resp)
{
    pycbc_Connection *conn;
    pycbc_OperationResult *res;
    pycbc_MultiResult *mres;
    int rv;
    lcb_error_t effective_err;

    if (err != LCB_SUCCESS) {
        effective_err = err;
    } else {
        effective_err = resp->v.v0.err;
    }

    rv = get_common_objects((PyObject *)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            effective_err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_OPERATION|RESTYPE_EXISTS_OK,
                            &mres);
    if (rv == -1) {
        CB_THR_BEGIN(conn);
        return;
    }
    res->rc = effective_err;

    maybe_push_operr(mres, (pycbc_Result*)res, effective_err, 0);
    CB_THR_BEGIN(conn);
}

static void
store_callback(lcb_t instance,
               const void *cookie,
               lcb_storage_t op,
               lcb_error_t err,
               const lcb_store_resp_t *resp)
{
    pycbc_Connection *conn;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres;
    int rv;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_OPERATION|RESTYPE_VARCOUNT,
                            &mres);

    if (rv == -1) {
        maybe_breakout(conn);
        CB_THR_BEGIN(conn);
        return;
    }

    res->rc = err;
    res->cas = resp->v.v0.cas;
    maybe_push_operr(mres, (pycbc_Result*)res, err, 0);

    if (err == LCB_SUCCESS && (mres->mropts & PYCBC_MRES_F_DURABILITY)) {
        lcb_durability_opts_t dopts = { 0 };
        lcb_durability_cmd_t dcmd = { 0 };
        lcb_durability_cmd_t *dcmd_p = &dcmd;

        dopts.v.v0.persist_to = mres->durability_reqs[0];
        dopts.v.v0.replicate_to = mres->durability_reqs[1];
        dcmd.v.v0.cas = resp->v.v0.cas;
        dcmd.v.v0.key = resp->v.v0.key;
        dcmd.v.v0.nkey = resp->v.v0.nkey;

        err = lcb_durability_poll(instance,
                                  mres,
                                  &dopts,
                                  1,
                                  (const lcb_durability_cmd_t * const *)&dcmd_p);
        if (err != LCB_SUCCESS) {
            res->rc = err;
            maybe_push_operr(mres, (pycbc_Result*)res, err, 0);
        }
    } else {
        maybe_breakout(conn);
    }

    CB_THR_BEGIN(conn);

    (void)instance;
    (void)op;
}

static void
get_callback(lcb_t instance,
             const void *cookie,
             lcb_error_t err,
             const lcb_get_resp_t *resp)
{

    int rv;
    pycbc_Connection *conn = NULL;
    pycbc_ValueResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    lcb_uint32_t eflags;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_VALUE,
                            &mres);

    if (rv < 0) {
        CB_THR_BEGIN(conn);
        return;
    }

    res->flags = resp->v.v0.flags;
    res->cas = resp->v.v0.cas;

    maybe_push_operr(mres, (pycbc_Result*)res, err, 1);

    if (err != LCB_SUCCESS) {
        CB_THR_BEGIN(conn);
        return;
    }

    if (mres->mropts & PYCBC_MRES_F_FORCEBYTES) {
        eflags = PYCBC_FMT_BYTES;
    } else {
        eflags = resp->v.v0.flags;
    }

    rv = pycbc_tc_decode_value(mres->parent,
                               resp->v.v0.bytes,
                               resp->v.v0.nbytes,
                               eflags,
                               &res->value);
    if (rv < 0) {
        push_fatal_error(mres);
    }

    CB_THR_BEGIN(conn);
    (void)instance;
}

static void
delete_callback(lcb_t instance,
                const void *cookie,
                lcb_error_t err,
                const lcb_remove_resp_t *resp)
{
    int rv;
    pycbc_Connection *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key, resp->v.v0.nkey, err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_OPERATION,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
    }

    maybe_push_operr(mres, (pycbc_Result*)res, err, 1);

    CB_THR_BEGIN(conn);
    (void)instance;
}


static void
arithmetic_callback(lcb_t instance,
                    const void *cookie,
                    lcb_error_t err,
                    const lcb_arithmetic_resp_t *resp)
{
    int rv;
    pycbc_Connection *conn = NULL;
    pycbc_ValueResult *res = NULL;
    pycbc_MultiResult *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_VALUE,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
        res->rc = err;
        if (err == LCB_SUCCESS) {
            res->value = pycbc_IntFromULL(resp->v.v0.value);
        }

        maybe_push_operr(mres, (pycbc_Result*)res, err, 0);
    }

    CB_THR_BEGIN(conn);
    (void)instance;
}

static void
unlock_callback(lcb_t instance,
                const void *cookie,
                lcb_error_t err,
                const lcb_unlock_resp_t *resp)
{
    int rv;
    pycbc_Connection *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_OPERATION,
                            &mres);
    if (rv == 0) {
        res->rc = err;
        maybe_push_operr(mres, (pycbc_Result*)res, err, 0);
    }
    CB_THR_BEGIN(conn);
    (void)instance;
}

static void
touch_callback(lcb_t instance,
               const void *cookie,
               lcb_error_t err,
               const lcb_touch_resp_t *resp)
{
    int rv;
    pycbc_Connection *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;

    rv = get_common_objects((PyObject*) cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_Result**)&res,
                            RESTYPE_OPERATION,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
        res->rc = err;
        maybe_push_operr(mres, (pycbc_Result*)res, err, 1);
    }

    CB_THR_BEGIN(conn);
    (void)instance;
}

static void
stat_callback(lcb_t instance,
              const void *cookie,
              lcb_error_t err,
              const lcb_server_stat_resp_t *resp)
{
    pycbc_MultiResult *mres;
    PyObject *value;
    PyObject *skey, *knodes;


    mres = (pycbc_MultiResult*)cookie;
    CB_THR_END(mres->parent);

    if (!resp->v.v0.server_endpoint) {
        maybe_breakout(mres->parent);
    }

    if (err != LCB_SUCCESS) {
        if (mres->errop == NULL) {
            pycbc_Result *res =
                    (pycbc_Result*)pycbc_result_new(mres->parent);
            res->rc = err;
            res->key = Py_None; Py_INCREF(res->key);
            maybe_push_operr(mres, res, err, 0);
        }
        CB_THR_BEGIN(mres->parent);
        return;
    }

    if (!resp->v.v0.server_endpoint) {
        CB_THR_BEGIN(mres->parent);
        return;
    }

    skey = pycbc_SimpleStringN(resp->v.v0.key, resp->v.v0.nkey);
    value = pycbc_SimpleStringN(resp->v.v0.bytes, resp->v.v0.nbytes);
    {
        PyObject *intval = pycbc_maybe_convert_to_int(value);
        if (intval) {
            Py_DECREF(value);
            value = intval;

        } else {
            PyErr_Clear();
        }
    }

    knodes = PyDict_GetItem((PyObject*)mres, skey);
    if (!knodes) {
        knodes = PyDict_New();
        PyDict_SetItem((PyObject*)mres, skey, knodes);
        Py_DECREF(knodes);
    }

    PyDict_SetItemString(knodes, resp->v.v0.server_endpoint, value);

    Py_DECREF(skey);
    Py_DECREF(value);

    CB_THR_BEGIN(mres->parent);
    (void)instance;
}



static void
observe_callback(lcb_t instance,
                 const void *cookie,
                 lcb_error_t err,
                 const lcb_observe_resp_t *resp)
{
    int rv;
    pycbc_ObserveInfo *oi;
    pycbc_Connection *conn;
    pycbc_ValueResult *vres;
    pycbc_MultiResult *mres;

    if (!resp->v.v0.key) {
        mres = (pycbc_MultiResult*)cookie;;
        maybe_breakout(mres->parent);
        return;
    }

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_Result**)&vres,
                            RESTYPE_VALUE|RESTYPE_EXISTS_OK|RESTYPE_VARCOUNT,
                            &mres);
    if (rv < 0) {
        goto GT_DONE;
    }

    if (err != LCB_SUCCESS) {
        maybe_push_operr(mres, (pycbc_Result*)vres, err, 0);
        goto GT_DONE;
    }

    if (!vres->value) {
        vres->value = PyList_New(0);
    }

    oi = pycbc_observeinfo_new(conn);
    if (oi == NULL) {
        push_fatal_error(mres);
        goto GT_DONE;
    }

    oi->from_master = resp->v.v0.from_master;
    oi->flags = resp->v.v0.status;
    oi->cas = resp->v.v0.cas;
    PyList_Append(vres->value, (PyObject*)oi);
    Py_DECREF(oi);

    GT_DONE:
    CB_THR_BEGIN(conn);
    (void)instance;
}

static void
error_callback(lcb_t instance, lcb_error_t err, const char *msg)
{
    PyObject *errtuple;
    PyObject *result;
    pycbc_Connection *self = (pycbc_Connection*) lcb_get_cookie(instance);

    CB_THR_END(self);

    pycbc_assert(self->errors);
    errtuple = Py_BuildValue("(i,s)", err, msg);
    pycbc_assert(errtuple);
    result = PyObject_CallMethod(self->errors, "append", "(O)", errtuple);
    pycbc_assert(result);
    Py_DECREF(errtuple);
    Py_DECREF(result);

    CB_THR_BEGIN(self);
}


void
pycbc_callbacks_init(lcb_t instance)
{
    lcb_set_store_callback(instance, store_callback);
    lcb_set_unlock_callback(instance, unlock_callback);
    lcb_set_get_callback(instance, get_callback);
    lcb_set_touch_callback(instance, touch_callback);
    lcb_set_arithmetic_callback(instance, arithmetic_callback);
    lcb_set_remove_callback(instance, delete_callback);
    lcb_set_stat_callback(instance, stat_callback);
    lcb_set_error_callback(instance, error_callback);
    lcb_set_observe_callback(instance, observe_callback);
    lcb_set_durability_callback(instance, durability_callback);

    pycbc_http_callbacks_init(instance);
}
