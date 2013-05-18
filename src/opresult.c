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
ValueResult_value(pycbc_ValueResult *self, void *closure)
{
    (void)closure;

    if (!self->value) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    Py_INCREF(self->value);
    return self->value;
}

static void
OperationResult_dealloc(pycbc_OperationResult *self)
{
    pycbc_Result_dealloc((pycbc_Result*)self);
}

static void
ValueResult_dealloc(pycbc_ValueResult *self)
{
    Py_XDECREF(self->value);
    OperationResult_dealloc((pycbc_OperationResult*)self);
}


static struct PyMemberDef OperationResult_TABLE_members[] = {
        { "cas",
                T_ULONGLONG, offsetof(pycbc_OperationResult, cas),
                READONLY, PyDoc_STR("CAS For the key")
        },
        { NULL }
};

static struct PyMemberDef ValueResult_TABLE_members[] = {
        { "flags",
                T_ULONG, offsetof(pycbc_ValueResult, flags),
                READONLY, PyDoc_STR("Flags for the value")
        },
        { NULL }
};

static PyGetSetDef ValueResult_TABLE_getset[] = {
        { "value",
                (getter)ValueResult_value,
                NULL,
                PyDoc_STR("Value for the operation")
        },
        { NULL }
};

PyTypeObject pycbc_OperationResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

PyTypeObject pycbc_ValueResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

int
pycbc_ValueResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ValueResultType;
    *ptr = (PyObject*)p;


    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "ValueResult";
    p->tp_doc =  PyDoc_STR(
            "The result type returned for operations which retrieve a value\n");
    p->tp_new = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_ValueResult);
    p->tp_base = &pycbc_OperationResultType;
    p->tp_getset = ValueResult_TABLE_getset;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_members = ValueResult_TABLE_members;
    p->tp_dealloc = (destructor)ValueResult_dealloc;

    return PyType_Ready(p);
}

int
pycbc_OperationResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_OperationResultType;

    *ptr = (PyObject*)&pycbc_OperationResultType;
    if (pycbc_OperationResultType.tp_name) {
        return 0;
    }

    p->tp_name = "OperationResult";
    p->tp_doc = PyDoc_STR(
            "Result type returned for operations which do not fetch data\n");
    p->tp_basicsize = sizeof(pycbc_OperationResult);
    p->tp_base = &pycbc_ResultType;
    p->tp_members = OperationResult_TABLE_members;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_dealloc = (destructor)OperationResult_dealloc;

    return PyType_Ready(p);
}

pycbc_ValueResult *
pycbc_valresult_new(pycbc_Connection *parent)
{
    (void)parent;
    return (pycbc_ValueResult*)
            PyObject_CallFunction((PyObject*)&pycbc_ValueResultType, NULL, NULL);
}

pycbc_OperationResult *
pycbc_opresult_new(pycbc_Connection *parent)
{
    (void)parent;
    return (pycbc_OperationResult*)
            PyObject_CallFunction((PyObject*)&pycbc_OperationResultType, NULL, NULL);
}
