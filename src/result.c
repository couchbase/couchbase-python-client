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

static PyObject *
Result_success(pycbc_Result *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong(self->rc == LCB_SUCCESS);
}

static PyObject *
Result_repr(pycbc_Result *self)
{
    return PyObject_CallFunction(pycbc_helpers.result_reprfunc, "O", self);
}


static PyObject *
Result_retnone(pycbc_Result *self, void *closure)
{
    (void)closure;
    (void)self;
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
Result_int0(pycbc_Result *self, void *closure)
{
    (void)closure;
    (void)self;
    return pycbc_IntFromL(0);
}

static PyObject *
Result_errstr(pycbc_Result *self, void *closure)
{
    (void)closure;
    return pycbc_lcb_errstr(NULL, self->rc);
}

static struct PyMemberDef Result_TABLE_members[] = {
        { "rc",
                T_INT, offsetof(pycbc_Result, rc),
                READONLY,
                PyDoc_STR("libcouchbase error code")
        },
        { "key", T_OBJECT_EX, offsetof(pycbc_Result, key),
                READONLY,
                PyDoc_STR("Key for the operation")
        },
        { "is_tracing_stub", T_BOOL, offsetof(pycbc_Result, is_tracing_stub),
                READONLY,
                PyDoc_STR("Whether this is a tracing stub")
        },

        { "tracing_output",
                T_OBJECT, offsetof(pycbc_Result, tracing_output),
          READONLY, PyDoc_STR("Tracing output")
        },
        { NULL }
};

PyObject *Result_tracing_context(pycbc_Result *self, void *closure)
{
    return PyLong_FromVoidPtr((void *)self->tracing_context);
}

static struct PyGetSetDef Result_TABLE_getset[] = {
        { "success",
                (getter)Result_success,
                NULL,
                PyDoc_STR("Determine whether operation succeeded or not")
        },
        { "value",
                (getter)Result_retnone,
                NULL, NULL,
        },
        { "errstr",
                (getter)Result_errstr,
                NULL,
                PyDoc_STR("Returns a textual representation of the error")
        },
        { "cas", (getter)Result_int0,
                NULL, NULL
        },
        { "tracing_context", (getter)Result_tracing_context,
                NULL, NULL
        },
        { NULL }
};

PyTypeObject pycbc_ResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyMethodDef Result_TABLE_methods[] = {
        { NULL }
};

static void
Result_dealloc(pycbc_Result *self)
{
    Py_XDECREF(self->key);
#ifdef PYCBC_EXTRA_STRICT
    pycbc_assert(!self->tracing_context);
#endif
    self->tracing_context = NULL;
    PYCBC_CONTEXT_DEREF_FROM_CONTEXT(self->tracing_context, 0, 0, NULL);
    Py_XDECREF(self->tracing_output);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

void
pycbc_Result_dealloc(pycbc_Result *self)
{
    Result_dealloc(self);
}
static int
pycbc_Result_init(PyObject *self_raw,
               PyObject *args, PyObject *kwargs)
{
    pycbc_Result* self = (pycbc_Result*)self_raw;
    self->is_tracing_stub = 0;
    self->tracing_context = NULL;
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    return 0;
}

int
pycbc_ResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ResultType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Result";
    p->tp_doc = PyDoc_STR(
            "The standard return type for Couchbase operations.\n"
            "\n"
            "This is a lightweight object and may be subclassed by other\n"
            "operations which may required additional fields.");

    p->tp_new = PyType_GenericNew;
    p->tp_init = pycbc_Result_init;
    p->tp_dealloc = (destructor)Result_dealloc;
    p->tp_basicsize = sizeof(pycbc_Result);
    p->tp_members = Result_TABLE_members;
    p->tp_methods = Result_TABLE_methods;
    p->tp_getset = Result_TABLE_getset;
    p->tp_repr = (reprfunc)Result_repr;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;

    return pycbc_ResultType_ready(p, PYCBC_RESULT_BASEFLDS);
}

PyObject *
pycbc_result_new(pycbc_Bucket *parent)
{
    PyObject *obj = PyObject_CallFunction((PyObject*) &pycbc_ResultType,
                                          NULL,
                                          NULL);
    (void)parent;
    return obj;
}

int
pycbc_ResultType_ready(PyTypeObject *p, int flags)
{
    int rv;
    PyObject *flags_o;

    rv = PyType_Ready(p);
    if (rv) {
        return rv;
    }

    flags_o = pycbc_IntFromUL(flags);
    PyDict_SetItemString(p->tp_dict, PYCBC_RESPROPS_NAME, flags_o);
    Py_DECREF(flags_o);

    return rv;
}
