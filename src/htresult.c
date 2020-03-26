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
#include "pycbc_http.h"
#include "structmember.h"

int
pycbc_httpresult_ok(pycbc_HttpResult *self)
{
    if (self->rc == LCB_SUCCESS &&
            ((self->htcode < 300 && self->htcode > 199) || self->htcode == 0)) {
        return 1;
    }
    return 0;
}

static PyObject *
HttpResult_success(pycbc_HttpResult *self, void *unused)
{
    PyObject *ret = NULL;
    if (pycbc_httpresult_ok(self)) {
        ret = Py_True;
    } else {
        ret = Py_False;
    }

    Py_INCREF(ret);

    (void)unused;
    return ret;
}

static PyObject*
HttpResult_context(pycbc_HttpResult* self, void* unused) {
    (void)unused;
    if (!self->context) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->context);
    return self->headers;
}

static PyObject *
HttpResult_headers(pycbc_HttpResult *self, void *unused)
{
    (void)unused;
    if (!self->headers) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->headers);
    return self->headers;
}

#define PYCBC_FOR_EACH_HTTP_OPTYPE(X) \
    X(VIEW, vh, view)                 \
    X(QUERY, query, query)            \
    X(SEARCH, search, search)         \
    X(RAW, htreq, http)               \
    X(ANALYTICS, analytics, analytics)

static void
HttpResult_dealloc(pycbc_HttpResult *self)
{
    if (self->u.htreq) {
        if (self->parent) {
            switch (self->htype) {
#define PYCBC_CASE(UC, LC, ACCESSOR)                                       \
    case PYCBC_HTTP_H##UC:                                                 \
        PYCBC_DEBUG_LOG(                                                   \
                "Cancelling %s operation %S at %p", #UC, self, self->u.LC) \
        lcb_##ACCESSOR##_cancel(self->parent->instance, self->u.LC);       \
        break;
#define PYCBC_GEN_HTTP_OPS
#ifdef PYCBC_GEN_HTTP_OPS
                PYCBC_FOR_EACH_HTTP_OPTYPE(PYCBC_CASE);
#else
            case PYCBC_HTTP_HVIEW:
                lcb_view_cancel(self->parent->instance, self->u.vh);
                break;
            case PYCBC_HTTP_HQUERY:
                lcb_query_cancel(self->parent->instance, self->u.query);
                break;
            case PYCBC_HTTP_HSEARCH:
                lcb_fts_cancel(self->parent->instance, self->u.search);
                break;
            case PYCBC_HTTP_HRAW:
                lcb_http_cancel(self->parent->instance, self->u.htreq);
                break;
            case PYCBC_HTTP_HANALYTICS:
                lcb_analytics_cancel(self->parent->instance, self->u.analytics);
                break;
                ;
#endif
            }
        }
        self->u.htreq = NULL;
    }
    Py_XDECREF(self->http_data);
    Py_XDECREF(self->headers);
    Py_XDECREF(self->parent);
    pycbc_Result_dealloc((pycbc_Result*)self);
}


static struct PyMemberDef HttpResult_TABLE_members[] = {
        { "http_status",
                T_USHORT, offsetof(pycbc_HttpResult, htcode),
                READONLY, PyDoc_STR("HTTP Status Code")
        },

        { "value",
                T_OBJECT_EX, offsetof(pycbc_HttpResult, http_data),
                READONLY, PyDoc_STR("HTTP Payload")
        },

        { "url",
                T_OBJECT_EX, offsetof(pycbc_HttpResult, key),
                READONLY, PyDoc_STR("HTTP URI")
        },
        { "done",
                T_BOOL, offsetof(pycbc_HttpResult, done),
                READONLY, PyDoc_STR("If the result is done")
        },
        { NULL }
};

static PyGetSetDef HttpResult_TABLE_getset[] = {
        { "success", (getter)HttpResult_success, NULL,
                PyDoc_STR("Whether the HTTP request was successful")
        },

        { "headers", (getter)HttpResult_headers, NULL,
                PyDoc_STR("Headers dict for the request. ")
        },
        { "context", (getter)HttpResult_context, NULL,
                PyDoc_STR("Error context dict for the request. ")
        },

        { NULL }
};

static PyMethodDef HttpResult_TABLE_methods[] = {
        { NULL }
};

PyTypeObject pycbc_HttpResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

int
pycbc_HttpResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_HttpResultType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }
    p->tp_name = "HttpResult";
    p->tp_doc = PyDoc_STR("Generic object returned for HTTP operations\n");
    p->tp_new = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_HttpResult);
    p->tp_base = &pycbc_ResultType;
    p->tp_getset = HttpResult_TABLE_getset;
    p->tp_members = HttpResult_TABLE_members;
    p->tp_methods = HttpResult_TABLE_methods;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_dealloc = (destructor)HttpResult_dealloc;
    return pycbc_ResultType_ready(p, PYCBC_HTRESULT_BASEFLDS);
}

void
pycbc_httpresult_init(pycbc_HttpResult *self, pycbc_MultiResult *mres)
{
    PyDict_SetItem((PyObject*)mres, Py_None, (PyObject*)self);
    Py_DECREF(self);
    self->parent = mres->parent;
    Py_INCREF(self->parent);
}
