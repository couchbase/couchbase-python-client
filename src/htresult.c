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

static PyObject *
HttpResult_done(pycbc_HttpResult *self, void *unused)
{
    PyObject *res = self->htflags & PYCBC_HTRES_F_COMPLETE ? Py_True : Py_False;
    Py_INCREF(res);

    (void)unused;
    return res;
}

static void
HttpResult_dealloc(pycbc_HttpResult *self)
{
    pycbc_Connection *parent = self->parent;

    self->parent = NULL;

    if (self->htreq) {
        lcb_cancel_http_request(parent->instance, self->htreq);
        self->htreq = NULL;
    }

    Py_XDECREF(self->http_data);
    Py_XDECREF(parent);
    Py_XDECREF(self->headers);
    Py_XDECREF(self->rowsbuf);
    Py_XDECREF(self->callback);

    if (self->rctx) {
        lcbex_vrow_free(self->rctx);
        self->rctx = NULL;
    }
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

        { "_callback",
                T_OBJECT_EX, offsetof(pycbc_HttpResult, callback),
                0, PyDoc_STR("Callback to be invoked with row data")
        },

        { "_rows",
                T_OBJECT_EX, offsetof(pycbc_HttpResult, rowsbuf),
                READONLY, PyDoc_STR("List containing raw strings of rows")
        },

        { "done",
                T_UBYTE, offsetof(pycbc_HttpResult, done),
                READONLY, PyDoc_STR("Boolean. True if result is done")
        },

        { NULL }
};

static PyGetSetDef HttpResult_TABLE_getset[] = {
        { "success",
                (getter)HttpResult_success,
                NULL,
                PyDoc_STR("Whether the HTTP request was successful")
        },

        { "headers",
                (getter)HttpResult_headers,
                NULL,
                PyDoc_STR("Headers dict for the request. "
                        "None unless 'fetch_headers' was passed to the request")
        },

        { "done",
                (getter)HttpResult_done,
                NULL,
                PyDoc_STR("Return true if this request has no more data.\n"
                        "This is most useful when issuing a streaming request\n"
                        "where multiple chunks of data may arrive.\n")
        },

        { NULL }
};

static PyMethodDef HttpResult_TABLE_methods[] = {
        { "_fetch", (PyCFunction)pycbc_HttpResult__fetch, METH_NOARGS, NULL },
        { "_maybe_raise",
                (PyCFunction)pycbc_HttpResult__maybe_raise,
                METH_NOARGS, NULL },

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

pycbc_HttpResult *
pycbc_httpresult_new(pycbc_Connection *parent)
{
    pycbc_HttpResult* ret = (pycbc_HttpResult*)
            PyObject_CallFunction((PyObject*)&pycbc_HttpResultType, NULL, NULL);
    ret->parent = parent;
    ret->http_data = PyList_New(0);
    Py_INCREF(parent);
    return ret;
}
