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
#include "oputil.h"

static void
get_headers(pycbc_HttpResult *htres, const char * const *headers)
{
    const char * const *p;
    htres->headers = PyDict_New();

    if (!headers) {
        return;
    }

    for (p = headers; *p; p += 2) {
        PyObject *hval = pycbc_SimpleStringZ(p[1]);
        PyDict_SetItemString(htres->headers, p[0], hval);
        Py_DECREF(hval);
    }
}

void
pycbc_httpresult_add_data(pycbc_MultiResult *mres, pycbc_HttpResult *htres,
                          const void *bytes, size_t nbytes)
{
    PyObject *newbuf;
    if (!nbytes) {
        return;
    }

    newbuf = PyBytes_FromStringAndSize(bytes, nbytes);
    if (htres->http_data) {
        PyObject *old_s = htres->http_data;
        PyBytes_ConcatAndDel(&htres->http_data, newbuf);
        if (!htres->http_data) {
            htres->http_data = old_s;
            Py_XDECREF(newbuf);
            pycbc_multiresult_adderr(mres);
        }
    } else {
        htres->http_data = newbuf;
    }
}

static void
decode_data(pycbc_MultiResult *mres, pycbc_HttpResult *htres)
{
    int rv;
    lcb_U32 format = htres->format;
    const void *data;
    Py_ssize_t ndata;
    PyObject *tmp;
    int is_success = 1;

    if (!format) {
        /* Already bytes */
        return;
    }

    if (!htres->http_data) {
        htres->http_data = Py_None;
        Py_INCREF(Py_None);
        return;
    }


    if (htres->htcode < 200 || htres->htcode > 299) {
        /* Not a successful response. */
        is_success = 0;
    }

    /* Handle cases where we already have a failure. In this case failure should
     * be for the actual content or HTTP code, rather than on encoding. */
    #define MAYBE_ADD_ERR() \
        if (is_success) { pycbc_multiresult_adderr(mres); } \
        else { PyErr_Clear(); }


    rv = PyBytes_AsStringAndSize(htres->http_data, (char**)&data, &ndata);
    if (rv != 0) {
        MAYBE_ADD_ERR();
        return;
    }
    rv = pycbc_tc_simple_decode(&tmp, data, ndata, format);
    if (rv != 0) {
        MAYBE_ADD_ERR();
        return;
    }
    #undef MAYBE_ADD_ERR

    Py_DECREF(htres->http_data);
    htres->http_data = tmp;
}

#define HTTP_IS_OK(st) (st > 199 && st < 300)

void
pycbc_httpresult_complete(pycbc_HttpResult *htres, pycbc_MultiResult *mres,
                          lcb_error_t err, short status,
                          const char * const *headers)
{
    int should_raise = 0;
    pycbc_Bucket *bucket = htres->parent;

    if (htres->rc == LCB_SUCCESS) {
        htres->rc = err;
    }

    htres->htcode = status;
    htres->done = 1;
    htres->u.htreq = NULL;
    Py_XDECREF(htres->parent);
    htres->parent = NULL;

    if (err != LCB_SUCCESS) {
        should_raise = 1;
    } else if (status && !HTTP_IS_OK(status) &&
            (mres->mropts & PYCBC_MRES_F_QUIET) == 0) {
        should_raise = 1;
    }

    if (should_raise) {
        PYCBC_EXC_WRAP_EX(err ? PYCBC_EXC_LCBERR : PYCBC_EXC_HTTP, err,
                          "HTTP Request failed. Examine 'objextra' for "
                          "full result", htres->key, (PyObject*)htres);
        pycbc_multiresult_adderr(mres);
    }

    get_headers(htres, headers);
    decode_data(mres, htres);

    if ((bucket->flags & PYCBC_CONN_F_ASYNC) == 0) {
        if (!bucket->nremaining) {
            lcb_breakout(bucket->instance);
        }
        PYCBC_CONN_THR_BEGIN(bucket);
    } else {
        pycbc_AsyncResult *ares = (pycbc_AsyncResult *)mres;
        ares->nops--;
        Py_INCREF(ares);
        pycbc_asyncresult_invoke(ares);
        /* We don't handle the GIL in async mode */
    }
}

static void
complete_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb)
{
    pycbc_MultiResult *mres;
    pycbc_Bucket *bucket;
    pycbc_HttpResult *htres;
    const lcb_RESPHTTP *resp = (const lcb_RESPHTTP *)rb;

    mres = (pycbc_MultiResult *)resp->cookie;
    bucket = mres->parent;

    PYCBC_CONN_THR_END(bucket);

    htres = (pycbc_HttpResult*)PyDict_GetItem((PyObject*)mres, Py_None);
    pycbc_httpresult_add_data(mres, htres, resp->body, resp->nbody);
    pycbc_httpresult_complete(htres, mres, resp->rc, resp->htstatus, resp->headers);

    /* CONN_THR_BEGIN called by httpresult_complete() */
    (void)instance; (void)cbtype;
}

void
pycbc_http_callbacks_init(lcb_t instance)
{
    lcb_install_callback3(instance, LCB_CALLBACK_HTTP, complete_callback);
    pycbc_views_callbacks_init(instance);
}

PyObject *
pycbc_Bucket__http_request(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    int rv;
    int method;
    int reqtype;
    unsigned value_format = PYCBC_FMT_JSON;
    lcb_error_t err;

    const char *body = NULL;
    PyObject *ret = NULL;
    PyObject *quiet_O = NULL;
    pycbc_strlen_t nbody = 0;
    const char *path = NULL;
    const char *content_type = NULL;
    pycbc_HttpResult *htres = NULL;
    pycbc_MultiResult *mres = NULL;
    lcb_CMDHTTP htcmd = { 0 };

    static char *kwlist[] = {
            "type", "method", "path", "content_type", "post_data",
            "response_format", "quiet", NULL
    };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "iis|zz#IO", kwlist,
                                     &reqtype, &method, &path,
                                     &content_type, &body, &nbody,
                                     &value_format, &quiet_O);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }

    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "HTTP/View Requests cannot be executed in "
                       "pipeline context");
        goto GT_DONE;
    }

    mres = (pycbc_MultiResult*)pycbc_multiresult_new(self);
    htres = (pycbc_HttpResult*)PYCBC_TYPE_CTOR(&pycbc_HttpResultType);
    pycbc_httpresult_init(htres, mres);

    htres->key = pycbc_SimpleStringZ(path);
    htres->format = value_format;

    if (quiet_O != NULL && quiet_O != Py_None && PyObject_IsTrue(quiet_O)) {
        mres->mropts |= PYCBC_MRES_F_QUIET;
    }
    mres->mropts |= PYCBC_MRES_F_SINGLE;

    LCB_CMD_SET_KEY(&htcmd, path, strlen(path));
    htcmd.body = body;
    htcmd.nbody = nbody;
    htcmd.content_type = content_type;
    htcmd.method = method;
    htcmd.reqhandle = &htres->u.htreq;
    htcmd.type = reqtype;

    err = lcb_http3(self->instance, mres, &htcmd);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (!(self->flags & PYCBC_CONN_F_ASYNC)) {
        pycbc_oputil_wait_common(self);
        /* RC=1 (decref on done) */
        if (pycbc_multiresult_maybe_raise(mres)) {
            goto GT_DONE;
        }

        ret = pycbc_multiresult_get_result(mres);
        Py_DECREF(mres); /* Don't need multiresult anymore. Use ret */
    } else {
        ret = (PyObject*)mres;
    }

    mres = NULL; /* Avoid the DECREF on success */

    GT_DONE:
    Py_XDECREF(mres);
    pycbc_oputil_conn_unlock(self);
    return ret;
}

