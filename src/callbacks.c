#include "pycbc.h"

#define CB_THREADS

#ifdef CB_THREADS
#define CB_THR_END PYCBC_CONN_THR_END
#define CB_THR_BEGIN PYCBC_CONN_THR_BEGIN
#else
#define CB_THR_END(x)
#define CB_THR_BEGIN(x)
#endif

static PyObject *make_error_tuple(void)
{
    PyObject *type, *value, *traceback;
    assert(PyErr_Occurred());
    PyErr_Fetch(&type, &value, &traceback);

    if (value == NULL) {
        value = Py_None; Py_INCREF(value);
    }

    return PyTuple_Pack(3, type, value, traceback);
}

static void push_fatal_error(pycbc_MultiResultObject* mres)
{
    mres->all_ok = 0;
    if (!mres->exceptions) {
        mres->exceptions = PyList_New(0);
    }

    PyList_Append(mres->exceptions, make_error_tuple());
    PyErr_Clear();
}

static void maybe_push_operr(pycbc_MultiResultObject *mres,
                             pycbc_ResultObject *res,
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
                              pycbc_ResultObject **res,
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
    *res = (pycbc_ResultObject*)pycbc_result_new(*conn);
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
    pycbc_ResultObject *res;
    pycbc_MultiResultObject *mres;
    int rv;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            &res,
                            &mres);

    if (rv == -1) {
        CB_THR_BEGIN(conn);
        return;
    }

    res->rc = err;
    res->cas = resp->v.v0.cas;
    maybe_push_operr(mres, res, err, 0);
    CB_THR_BEGIN(conn);
}

static void get_callback(lcb_t instance, const void *cookie,
                         lcb_error_t err,
                         const lcb_get_resp_t *resp)
{

    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_ResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            &res,
                            &mres);

    if (rv < 0) {
        CB_THR_BEGIN(conn);
        return;
    }

    res->flags = resp->v.v0.flags;
    res->cas = resp->v.v0.cas;

    maybe_push_operr(mres, res, err, 1);

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
    pycbc_ResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;
    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key, resp->v.v0.nkey, err,
                            &conn, &res, &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
    }

    maybe_push_operr(mres, res, err, 1);

    CB_THR_BEGIN(conn);
}

static void arithmetic_callback(lcb_t instance, const void *cookie,
                                lcb_error_t err,
                                const lcb_arithmetic_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_ResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn, &res, &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
        res->rc = err;
        if (err == LCB_SUCCESS) {
            res->value = pycbc_IntFromULL(resp->v.v0.value);
        }

        maybe_push_operr(mres, res, err, 0);
    }

    CB_THR_BEGIN(conn);
}

static void unlock_callback(lcb_t instance, const void *cookie,
                            lcb_error_t err,
                            const lcb_unlock_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_ResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*)cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn, &res, &mres);
    if (rv == 0) {
        res->rc = err;
        maybe_push_operr(mres, res, err, 0);
    }
    CB_THR_BEGIN(conn);
}

static void touch_callback(lcb_t isntance, const void *cookie,
                           lcb_error_t err,
                           const lcb_touch_resp_t *resp)
{
    int rv;
    pycbc_ConnectionObject *conn = NULL;
    pycbc_ResultObject *res = NULL;
    pycbc_MultiResultObject *mres = NULL;

    rv = get_common_objects((PyObject*) cookie,
                            resp->v.v0.key,
                            resp->v.v0.nkey,
                            err,
                            &conn,
                            &res,
                            &mres);
    if (rv == 0) {
        res->cas = resp->v.v0.cas;
        res->rc = err;
        maybe_push_operr(mres, res, err, 1);
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
            pycbc_ResultObject *res = (pycbc_ResultObject*)pycbc_result_new(mres->parent);
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
}
