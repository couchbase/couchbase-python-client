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

static PyObject *ResultBase_success(pycbc_ResultBaseObject *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong(self->rc == LCB_SUCCESS);
}

static PyObject *ResultBase_repr(pycbc_ResultBaseObject *self)
{
    return PyObject_CallFunction(pycbc_helpers.result_reprfunc, "O", self);
}


static PyObject *ResultBase_retnone(pycbc_ResultBaseObject *self, void *closure)
{
    (void)closure;
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *ResultBase_int0(pycbc_ResultBaseObject *self, void *closure)
{
    (void)closure;
    return pycbc_IntFromL(0);
}

static PyObject *ResultBase_errstr(pycbc_ResultBaseObject *self, void *closure)
{
    (void)closure;
    return pycbc_lcb_errstr(NULL, self->rc);
}

static struct PyMemberDef ResultBase_members[] = {
        { "rc",
                T_INT, offsetof(pycbc_ResultBaseObject, rc),
                READONLY, "libcouchbase error code"
        },
        { "key", T_OBJECT_EX, offsetof(pycbc_ResultBaseObject, key),
                READONLY, "Key for the operation"
        },

        { NULL }
};

struct PyGetSetDef ResultBase_getset[] = {
        { "success",
                (getter)ResultBase_success,
                NULL,
                "Determine whether operation succeeded or not"
        },
        { "value",
                (getter)ResultBase_retnone,
                NULL, NULL,
        },
        { "errstr",
                (getter)ResultBase_errstr,
                NULL,
                "Returns a textual representation of the error"
        },
        { "cas", (getter)ResultBase_int0,
                NULL, NULL
        },
        { NULL }
};

PyTypeObject pycbc_ResultBaseType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyMethodDef ResultBase_methods[] = {
        { NULL }
};

static void Result_dealloc(pycbc_ResultBaseObject *self)
{
    Py_XDECREF(self->key);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

void pycbc_ResultBase_dealloc(pycbc_ResultBaseObject *self)
{
    Result_dealloc(self);
}

int pycbc_ResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ResultBaseType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Result";
    p->tp_doc =
            "The standard return type for Couchbase operations.\n"
            "\n"
            "This is a lightweight object and may be subclassed by other\n"
            "operations which may required additional fields.";

    p->tp_new = PyType_GenericNew;
    p->tp_dealloc = (destructor)Result_dealloc;
    p->tp_basicsize = sizeof(pycbc_ResultBaseObject);
    p->tp_members = ResultBase_members;
    p->tp_methods = ResultBase_methods;
    p->tp_getset = ResultBase_getset;
    p->tp_repr = (reprfunc)ResultBase_repr;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;

    return PyType_Ready(p);
}

PyObject *pycbc_result_new(pycbc_ConnectionObject *parent)
{
    PyObject *obj = PyObject_CallFunction((PyObject*) &pycbc_ResultBaseType,
                                          NULL,
                                          NULL);
    return obj;
}
