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
    RESTYPE_BASE = 1,
    RESTYPE_VALUE,
    RESTYPE_OPERATION
};

static PyObject *make_error_tuple(void)
{
    PyObject *type, *value, *traceback;
    PyObject *ret;

    assert(PyErr_Occurred());

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

static void push_fatal_error(pycbc_MultiResultObject* mres)
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

static void maybe_push_operr(pycbc_MultiResultObject *mres,
                             pycbc_ResultBaseObject *res,
                             lcb_error_t err,
                             int check_enoent)
{
    if (err == LCB_SUCCESS || mres->errop) {
        return;
    }

    if (check_enoent && mres->no_raise_enoent != 0 && err == LCB_KEY_ENOENT) {
        return;
    }

    mres->errop = (PyObject*)res;
    Py_INCREF(mres->errop);
}

static int get_common_objects(PyObject *cookie,
                              const void *key,
                              size_t nkey,
                              lcb_error_t err,
                              pycbc_ConnectionObject **conn,
                              pycbc_ResultBaseObject **res,
                              int restype,
                              pycbc_MultiResultObject **mres)

{
    PyObject *hkey;
    int rv;

    assert(Py_TYPE(cookie) == &pycbc_MultiResultType);
    *mres = (pycbc_MultiResultObject*)cookie;
    *conn = (*mres)->parent;

    CB_THR_END(*conn);

    rv = pycbc_tc_decode_key(*conn, key, nkey, &hkey);

    if (rv < 0) {
        push_fatal_error(*mres);
        return -1;
    }

    /**
     * Now, get/set the result object
     */
    if (restype == RESTYPE_BASE) {
        *res = (pycbc_ResultBaseObject*)pycbc_result_new(*conn);

    } else if (restype == RESTYPE_OPERATION) {
        *res = (pycbc_ResultBaseObject*)pycbc_opresult_new(*conn);

    } else if (restype == RESTYPE_VALUE) {
        *res = (pycbc_ResultBaseObject*)pycbc_valresult_new(*conn);

    } else {
        abort();
    }
    assert(PyDict_Contains((PyObject*)*mres, hkey) == 0);

    PyDict_SetItem((PyObject*)*mres, hkey, (PyObject*)*res);
    Py_DECREF(*res);

    (*res)->key = hkey;
    (*res)->rc = err;

    if (err != LCB_SUCCESS) {
        (*mres)->all_ok = 0;
    }

    return 0;
}

static void store_callback(lcb_t instance,
                           const void *cookie,
                           lcb_storage_t op,
                           lcb_error_t err,
                           const lcb_store_resp_t *resp)
{
    pycbc_ConnectionObject *conn;
    pycbc_OperationResultObject *res;
    pycbc_MultiResultObject *mres;
    int rv;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_ResultBaseObject**)&res,
                            RESTYPE_OPERATION,
                            &mres);

    if (rv == -1) {
        CB_THR_BEGIN(conn);
        return;
    }

    res->rc = err;
    res->cas = resp->v.v0.cas;
    maybe_push_operr(mres, (pycbc_ResultBaseObject*)res, err, 0);
    CB_THR_BEGIN(conn);
}

static void get_callback(lcb_t instance, const void *cookie,
                         lcb_error_t err,
                         const lcb_get_resp_t *resp)
{

    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_ValueResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_ResultBaseObject**)&res,
                            RESTYPE_VALUE,
                            &mres);

    if (rv < 0) {
        CB_THR_BEGIN(conn);
        return;
    }

    res->flags = resp->v.v0.flags;
    res->cas = resp->v.v0.cas;

    maybe_push_operr(mres, (pycbc_ResultBaseObject*)res, err, 1);

    if (err != LCB_SUCCESS) {
        CB_THR_BEGIN(conn);
        return;
    }

    rv = pycbc_tc_decode_value(mres->parent,
                               resp->v.v0.bytes,
                               resp->v.v0.nbytes,
                               resp->v.v0.flags,
                               &res->value);
    if (rv < 0) {
        push_fatal_error(mres);
    }

    CB_THR_BEGIN(conn);
}

static void delete_callback(lcb_t instance, const void *cookie,
                            lcb_error_t err,
                            const lcb_remove_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_OperationResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;
    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key, resp->v.v0.nkey, err,
                            &conn,
                            (pycbc_ResultBaseObject**)&res,
                            RESTYPE_OPERATION,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
    }

    maybe_push_operr(mres, (pycbc_ResultBaseObject*)res, err, 1);

    CB_THR_BEGIN(conn);
}

static void arithmetic_callback(lcb_t instance, const void *cookie,
                                lcb_error_t err,
                                const lcb_arithmetic_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_ValueResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_ResultBaseObject**)&res,
                            RESTYPE_VALUE,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
        res->rc = err;
        if (err == LCB_SUCCESS) {
            res->value = pycbc_IntFromULL(resp->v.v0.value);
        }

        maybe_push_operr(mres, (pycbc_ResultBaseObject*)res, err, 0);
    }

    CB_THR_BEGIN(conn);
}

static void unlock_callback(lcb_t instance, const void *cookie,
                            lcb_error_t err,
                            const lcb_unlock_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_OperationResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_ResultBaseObject**)&res,
                            RESTYPE_OPERATION,
                            &mres);
    if (rv == 0) {
        res->rc = err;
        maybe_push_operr(mres, (pycbc_ResultBaseObject*)res, err, 0);
    }
    CB_THR_BEGIN(conn);
}

static void touch_callback(lcb_t isntance, const void *cookie,
                           lcb_error_t err,
                           const lcb_touch_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_OperationResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*) cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            (pycbc_ResultBaseObject**)&res,
                            RESTYPE_OPERATION,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
        res->rc = err;
        maybe_push_operr(mres, (pycbc_ResultBaseObject*)res, err, 1);
    }

    CB_THR_BEGIN(conn);
}

static void stat_callback(lcb_t instance,
                          const void *cookie,
                          lcb_error_t err,
                          const lcb_server_stat_resp_t *resp)
{
    pycbc_MultiResultObject *mres;
    PyObject *value;
    PyObject *skey, *knodes;


    mres = (pycbc_MultiResultObject*)cookie;
    CB_THR_END(mres->parent);

    if (err != LCB_SUCCESS) {
        if (mres->errop == NULL) {
            pycbc_ResultBaseObject *res =
                    (pycbc_ResultBaseObject*)pycbc_result_new(mres->parent);
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
}


/**
 * This callback does things a bit differently.
 * Instead of using a MultiResult, we use a single HttpResult object.
 * We won't ever have "multiple" http objects.
 */
static void http_complete_callback(lcb_http_request_t req,
                                   lcb_t instance,
                                   const void *cookie,
                                   lcb_error_t err,
                                   const lcb_http_resp_t *resp)
{
    pycbc_HttpResultObject *htres = (pycbc_HttpResultObject*)cookie;
    htres->rc = err;
    htres->htcode = resp->v.v0.status;

    CB_THR_END(htres->parent);

    if (resp->v.v0.nbytes) {
        pycbc_tc_simple_decode(&htres->http_data,
                               resp->v.v0.bytes,
                               resp->v.v0.nbytes,
                               htres->format);
        if (!htres->http_data) {
            PyErr_Clear();
            htres->http_data = PyBytes_FromStringAndSize(resp->v.v0.bytes,
                                                         resp->v.v0.nbytes);
        }

    } else {
        htres->http_data = Py_None;
        Py_INCREF(Py_None);
    }

    CB_THR_BEGIN(htres->parent);

}

static void error_callback(lcb_t instance, lcb_error_t err, const char *msg)
{
    PyObject *errtuple;
    PyObject *result;

    pycbc_ConnectionObject *self =
            (pycbc_ConnectionObject*) lcb_get_cookie(instance);

    CB_THR_END(self);

    assert(self->errors);
    errtuple = Py_BuildValue("(i,s)", err, msg);
    assert(errtuple);
    result = PyObject_CallMethod(self->errors, "append", "(O)", errtuple);
    assert(result);
    Py_DECREF(errtuple);
    Py_DECREF(result);

    CB_THR_BEGIN(self);
}


void pycbc_callbacks_init(lcb_t instance)
{
    lcb_set_store_callback(instance, store_callback);
    lcb_set_unlock_callback(instance, unlock_callback);
    lcb_set_get_callback(instance, get_callback);
    lcb_set_touch_callback(instance, touch_callback);
    lcb_set_arithmetic_callback(instance, arithmetic_callback);
    lcb_set_remove_callback(instance, delete_callback);
    lcb_set_stat_callback(instance, stat_callback);
    lcb_set_error_callback(instance, error_callback);
    lcb_set_http_complete_callback(instance, http_complete_callback);
}
