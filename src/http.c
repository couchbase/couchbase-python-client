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
get_headers(pycbc_HttpResult *htres, const lcb_http_resp_t * resp)
{

    const char * const *p;
    PyObject *hval;

    if (!htres->headers) {
        return;
    }

    if (PyDict_Size(htres->headers)) {
        return;
    }

    if (!resp->v.v0.headers) {
        return;
    }

    for (p = resp->v.v0.headers; *p; p += 2) {
        hval = pycbc_SimpleStringZ(p[1]);
        PyDict_SetItemString(htres->headers, p[0], hval);
        Py_DECREF(hval);
    }
}

static void
get_data(pycbc_HttpResult *htres, const void *data, size_t ndata)
{
    PyObject *o = NULL;

    if (data == NULL) {
        if (!htres->http_data) {
            htres->http_data = Py_None;
            Py_INCREF(Py_None);
        }
        return;
    }

    pycbc_tc_simple_decode(&o, data, ndata, htres->format);

    if (!o) {
        PyErr_Clear();
        htres->http_data = PyBytes_FromStringAndSize(data, ndata);
    }

    if ((htres->htflags & PYCBC_HTRES_F_CHUNKED) &&
            htres->http_data &&
            PyList_Check(htres->http_data)) {
        /**
         * If we have a list here, then it means we got an error and have to
         * start populating it. Otherwise, we handle as usual
         */
        PyList_Append(htres->http_data, o);

    } else {
        Py_XDECREF(htres->http_data);
        htres->http_data = o;
    }

}

/**
 * This callback does things a bit differently.
 * Instead of using a MultiResult, we use a single HttpResult object.
 * We won't ever have "multiple" http objects.
 */
static void
http_complete_callback(lcb_http_request_t req,
                       lcb_t instance,
                       const void *cookie,
                       lcb_error_t err,
                       const lcb_http_resp_t *resp)
{
    pycbc_HttpResult *htres = (pycbc_HttpResult*)cookie;

    htres->htreq = NULL;

    if (!htres->parent) {
        return;
    }

    htres->rc = err;
    htres->htcode = resp->v.v0.status;

    if (htres->htflags & PYCBC_HTRES_F_CHUNKED) {
        /** No data here */
        if (!pycbc_assert(resp->v.v0.nbytes == 0)) {
            fprintf(stderr, "Unexpected payload in HTTP response callback\n");
        }

        if (!htres->parent->nremaining) {
            lcb_breakout(instance);
        }
        return;
    }


    PYCBC_CONN_THR_END(htres->parent);

    if (!--htres->parent->nremaining) {
        lcb_breakout(instance);
    }

    get_data(htres, resp->v.v0.bytes, resp->v.v0.nbytes);
    get_headers(htres, resp);

    PYCBC_CONN_THR_BEGIN(htres->parent);
    (void)instance;
    (void)req;
}

static void
http_vrow_callback(lcbex_vrow_ctx_t *rctx,
                   const void *cookie,
                   const lcbex_vrow_datum_t *row)
{
    pycbc_HttpResult *htres = (pycbc_HttpResult *)cookie;

    if (row->type == LCBEX_VROW_ROW) {
        if (row->ndata) {
            PyObject *s = PyUnicode_FromStringAndSize(row->data, row->ndata);
            PyList_Append(htres->rowsbuf, s);
            Py_DECREF(s);
        }

    } else {
        Py_XDECREF(htres->http_data);
        htres->http_data = NULL;
        get_data(htres, row->data, row->ndata);
    }

    (void)rctx;
}


static void
http_data_callback(lcb_http_request_t req,
                   lcb_t instance,
                   const void *cookie,
                   lcb_error_t err,
                   const lcb_http_resp_t *resp)
{
    pycbc_HttpResult *htres = (pycbc_HttpResult*)cookie;

    htres->htcode = resp->v.v0.status;
    htres->rc = err;

    PYCBC_CONN_THR_END(htres->parent);

    get_headers(htres, resp);

    if (err != LCB_SUCCESS || resp->v.v0.status < 200 || resp->v.v0.status > 299) {
        PyObject *old_data = htres->http_data;

        lcbex_vrow_free(htres->rctx);
        htres->rctx = NULL;
        htres->http_data = PyList_New(0);

        if (old_data) {
            if (old_data != Py_None) {
                PyList_Append(htres->http_data, old_data);
            }
            Py_DECREF(old_data);
        }
    }

    if (htres->rctx) {
        lcbex_vrow_feed(htres->rctx, resp->v.v0.bytes, resp->v.v0.nbytes);

    } else if (resp->v.v0.bytes) {
        get_data(htres, resp->v.v0.bytes, resp->v.v0.nbytes);
    }

    if (!htres->parent->nremaining) {
        lcb_breakout(instance);
    }

    PYCBC_CONN_THR_BEGIN(htres->parent);

    (void)req;
    (void)instance;
}

void
pycbc_http_callbacks_init(lcb_t instance)
{
    lcb_set_http_complete_callback(instance, http_complete_callback);
    lcb_set_http_data_callback(instance, http_data_callback);
}


static int
maybe_raise(pycbc_HttpResult *htres)
{
    if (htres->htflags & PYCBC_HTRES_F_QUIET) {
        return 0;
    }
    if (pycbc_httpresult_ok(htres)) {
        return 0;
    }


    PYCBC_EXC_WRAP_EX(htres->rc ? PYCBC_EXC_LCBERR : PYCBC_EXC_HTTP,
                      htres->rc,
                      "HTTP Request failed. Examine 'objextra' for "
                      "full result",
                      htres->key,
                      (PyObject*)htres);

    return 1;
}

PyObject *
pycbc_Connection__http_request(pycbc_Connection *self,
                               PyObject *args,
                               PyObject *kwargs)
{
    int rv;
    int method;
    int reqtype;
    unsigned short value_format = 0;
    lcb_error_t err;

    const char *body = NULL;
    PyObject *ret = NULL;
    PyObject *quiet_O = NULL;
    PyObject *chunked_O = NULL;
    PyObject *fetch_headers_O = Py_False;
    pycbc_strlen_t nbody = 0;
    const char *path = NULL;
    const char *content_type = NULL;
    pycbc_HttpResult *htres;
    lcb_http_request_t l_htreq;

    lcb_http_cmd_t htcmd = { 0 };

    static char *kwlist[] = {
            "type", "method", "path", "content_type", "post_data",
            "response_format", "quiet", "fetch_headers",
            "chunked", NULL
    };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "iis|zz#HOOO", kwlist,
                                     &reqtype,
                                     &method,
                                     &path,
                                     &content_type,
                                     &body,
                                     &nbody,
                                     &value_format,
                                     &quiet_O,
                                     &fetch_headers_O,
                                     &chunked_O);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }


    htres = pycbc_httpresult_new(self);
    htres->key = pycbc_SimpleStringZ(path);
    htres->format = value_format;
    htres->htflags = 0;

    if (quiet_O != NULL && quiet_O != Py_None && PyObject_IsTrue(quiet_O)) {
        htres->htflags |= PYCBC_HTRES_F_QUIET;
    }

    if (fetch_headers_O && PyObject_IsTrue(fetch_headers_O)) {
        htres->headers = PyDict_New();
    }

    if (chunked_O && PyObject_IsTrue(chunked_O)) {
        htcmd.v.v0.chunked = 1;
        htres->rctx = lcbex_vrow_create();
        htres->rctx->callback = http_vrow_callback;
        htres->rctx->user_cookie = htres;
        htres->htflags |= PYCBC_HTRES_F_CHUNKED;
    }

    htcmd.v.v1.body = body;
    htcmd.v.v1.nbody = nbody;
    htcmd.v.v1.content_type = content_type;
    htcmd.v.v1.path = path;
    htcmd.v.v1.npath = strlen(path);
    htcmd.v.v1.method = method;


    err = lcb_make_http_request(self->instance,
                                htres,
                                reqtype,
                                &htcmd,
                                &l_htreq);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    htres->htreq = l_htreq;

    if (htcmd.v.v0.chunked) {
        ret = (PyObject*)htres;
        htres = NULL;
        goto GT_DONE;
    }

    self->nremaining++;
    err = pycbc_oputil_wait_common(self);

    if (err != LCB_SUCCESS) {
        self->nremaining--;
        PYCBC_EXCTHROW_WAIT(err);
        goto GT_DONE;
    }

    if (maybe_raise(htres)) {
        goto GT_DONE;
    }

    ret = (PyObject*)htres;
    htres = NULL;

    GT_DONE:
    Py_XDECREF(htres);
    pycbc_oputil_conn_unlock(self);
    return ret;
}

/**
 * Fetches a bunch of results from the network. Returns False when
 * no more results remain.
 */
PyObject *
pycbc_HttpResult__fetch(pycbc_HttpResult *self)
{
    lcb_error_t err;
    PyObject *ret = NULL;

    if (-1 == pycbc_oputil_conn_lock(self->parent)) {
        return NULL;
    }

    if (!self->htreq) {
        ret = Py_None;
        Py_INCREF(ret);
        goto GT_RET;
    }

    if (!self->rowsbuf) {
        self->rowsbuf = PyList_New(0);
    }

    err = pycbc_oputil_wait_common(self->parent);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_WAIT(err);
        goto GT_RET;

    } else {
        if (maybe_raise(self)) {
            goto GT_RET;
        }

        ret = self->rowsbuf;
        self->rowsbuf = NULL;
    }

    if (!pycbc_assert(self->parent->nremaining == 0)) {
        fprintf(stderr, "Remaining count unexpected. Adjusting");
        self->parent->nremaining = 0;
    }

    GT_RET:
    pycbc_oputil_conn_unlock(self->parent);
    return ret;
}
