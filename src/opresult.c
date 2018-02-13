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
    Py_CLEAR(self->mutinfo);
    pycbc_Result_dealloc((pycbc_Result*)self);
}

static void
ValueResult_dealloc(pycbc_ValueResult *self)
{
    Py_XDECREF(self->value);
    OperationResult_dealloc((pycbc_OperationResult*)self);
}

static void
SDResult_dealloc(pycbc__SDResult *self)
{
    Py_CLEAR(self->results);
    Py_CLEAR(self->specs);
    OperationResult_dealloc((pycbc_OperationResult*)self);
}

static void
Item_dealloc(pycbc_Item *self)
{
    Py_XDECREF(self->vdict);
    ValueResult_dealloc((pycbc_ValueResult*)self);
}

static int
Item__init__(pycbc_Item *item, PyObject *args, PyObject *kwargs)
{
    if (pycbc_ValueResultType.tp_init((PyObject *)item, args, kwargs) != 0) {
        return -1;
    }

    if (!item->vdict) {
        item->vdict = PyDict_New();
    }
    return 0;
}

static struct PyMemberDef OperationResult_TABLE_members[] = {
        { "cas",
                T_ULONGLONG, offsetof(pycbc_OperationResult, cas),
                READONLY, PyDoc_STR("CAS For the key")
        },
        { "_mutinfo",
                T_OBJECT_EX, offsetof(pycbc_OperationResult, mutinfo),
                READONLY, PyDoc_STR("Mutation info")
        },
        { NULL }
};

static struct PyMemberDef ValueResult_TABLE_members[] = {
        { "flags",
                T_ULONG, offsetof(pycbc_ValueResult, flags),
                READONLY
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

static struct PyMemberDef SDResult_TABLE_members[] = {
        { "_results", T_OBJECT_EX, offsetof(pycbc__SDResult, results), READONLY },
        { "_specs", T_OBJECT_EX, offsetof(pycbc__SDResult, specs), READONLY },
        { NULL }
};

/**
 * We need to re-define all these fields again and indicate their permissions
 * as being writable
 */
static PyMemberDef Item_TABLE_members[] = {
        { "__dict__",
            T_OBJECT_EX, offsetof(pycbc_Item, vdict), READONLY
        },

        { "value",
            T_OBJECT_EX, offsetof(pycbc_Item, value), 0,
            PyDoc_STR("The value of the Item.\n\n"
                    "For storage operations, this value is read. For retrieval\n"
                    "operations, this field is set\n")
        },

        { "cas",
            T_ULONGLONG, offsetof(pycbc_Item, cas), 0,
            PyDoc_STR("The CAS of the Item.\n\n"
                    "This field is always updated. On storage operations,\n"
                    "this field (if not ``0``) is used as the CAS for the\n"
                    "current operation. If the CAS on the server does not\n"
                    "match the value in this property, the operation will\n"
                    "fail.\n"
                    "For retrieval operations, this field is simply\n"
                    "set with the current CAS of the Item\n")
        },

        { "flags",
            T_ULONG, offsetof(pycbc_Item, flags), 0,
            PyDoc_STR("The flags (format) of the Item.\n\n"
                    "This field is set\n"
                    "During a retrieval operation. It is not read for a \n"
                    "storage operation\n")
        },

        {"key",
            T_OBJECT_EX, offsetof(pycbc_Item, key), 0,
            PyDoc_STR("This is the key for the Item. It *must* be set\n"
                    "before passing this item along in any operation\n")
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

PyTypeObject pycbc_ItemType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

PyTypeObject pycbc__SDResultType = {
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
    return pycbc_ResultType_ready(p, PYCBC_VALRESULT_BASEFLDS);
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
    return pycbc_ResultType_ready(p, PYCBC_OPRESULT_BASEFLDS);
}

int pycbc_SDResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc__SDResultType;
    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "_SDResult";
    p->tp_basicsize = sizeof(pycbc__SDResult);
    p->tp_base = &pycbc_OperationResultType;
    p->tp_members = SDResult_TABLE_members;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_dealloc = (destructor)SDResult_dealloc;
    return pycbc_ResultType_ready(p, PYCBC_OPRESULT_BASEFLDS);
}

int pycbc_ItemType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ItemType;
    *ptr = (PyObject *)p;
    if (p->tp_name) {
        return 0;
    }
    p->tp_name = "Item";
    p->tp_doc = PyDoc_STR(
            "Subclass of a :class:`~couchbase.result.ValueResult`.\n"
            "This can contain user-defined fields\n"
            "This can also be used as an item in either a\n"
            ":class:`ItemOptionDict` or a :class:`ItemSequence` object which\n"
            "can then be passed along to one of the ``_multi`` operations\n"
            "\n");
    p->tp_basicsize = sizeof(pycbc_Item);
    p->tp_base = &pycbc_ValueResultType;
    p->tp_members = Item_TABLE_members;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_init = (initproc)Item__init__;
    p->tp_dealloc = (destructor)Item_dealloc;
    p->tp_dictoffset = offsetof(pycbc_Item, vdict);
    return pycbc_ResultType_ready(p, PYCBC_VALRESULT_BASEFLDS);
}

pycbc_ValueResult *
pycbc_valresult_new(pycbc_Bucket *parent)
{
    (void)parent;
    return (pycbc_ValueResult*)
            PyObject_CallFunction((PyObject*)&pycbc_ValueResultType, NULL, NULL);
}

pycbc_OperationResult *
pycbc_opresult_new(pycbc_Bucket *parent)
{
    (void)parent;
    return (pycbc_OperationResult*)
            PyObject_CallFunction((PyObject*)&pycbc_OperationResultType, NULL, NULL);
}

pycbc_Item *
pycbc_item_new(pycbc_Bucket *parent)
{
    (void)parent;
    return (pycbc_Item *)
            PyObject_CallFunction((PyObject*)&pycbc_ItemType, NULL, NULL);
}

pycbc__SDResult *
pycbc_sdresult_new(pycbc_Bucket *parent, PyObject *specs)
{
    pycbc__SDResult *res =
            (pycbc__SDResult*)PyObject_CallFunction(
                pycbc_helpers.sd_result_type, NULL, NULL);
    if (res != NULL) {
        res->specs = specs;
        Py_INCREF(specs);
    }
    return res;
}

void
pycbc_sdresult_addresult(pycbc__SDResult *obj, size_t ii, PyObject *item)
{
    if (obj->results == NULL) {
        obj->results = PyList_New(PyTuple_GET_SIZE(obj->specs));
    }
    pycbc_assert(ii < (size_t)PyTuple_GET_SIZE(obj->specs));
    PyList_SetItem(obj->results, ii, item);
    Py_INCREF(item); /* To normalize refcount semantics */
}
