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


static struct PyMemberDef MultiResult_TABLE_members[] = {
        { "all_ok",
                T_INT, offsetof(pycbc_MultiResult, all_ok),
                READONLY,
                PyDoc_STR("Whether all the items in this result are successful")
        },
        { NULL }
};

PyTypeObject pycbc_MultiResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyMethodDef MultiResult_TABLE_methods[] = {
        { NULL }
};


static int
MultiResultType__init__(pycbc_MultiResult *self, PyObject *args, PyObject *kwargs)
{
    if (PyDict_Type.tp_init((PyObject*)self, args, kwargs) < 0) {
        PyErr_Print();
        abort();
        return -1;
    }
    self->all_ok = 1;
    self->exceptions = NULL;
    self->errop = NULL;
    self->mropts = 0;

    return 0;
}

static void
MultiResult_dealloc(pycbc_MultiResult *self)
{
    Py_XDECREF(self->parent);
    Py_XDECREF(self->exceptions);
    Py_XDECREF(self->errop);
    PyDict_Type.tp_dealloc((PyObject*)self);
}

int
pycbc_MultiResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_MultiResultType;

    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_base = &PyDict_Type;
    p->tp_init = (initproc)MultiResultType__init__;
    p->tp_dealloc = (destructor)MultiResult_dealloc;

    p->tp_name = "MultiResult";
    p->tp_doc = PyDoc_STR(
            ":class:`dict` subclass to hold :class:`Result` objects\n"
            "\n"
            "This object also contains some of the heavy lifting, but this\n"
            "is not currently exposed in python-space\n."
            "\n"
            "An additional method is :meth:`all_ok`, which allows to see\n"
            "if all commands completed successfully\n"
            "\n");

    p->tp_basicsize = sizeof(pycbc_MultiResult);
    p->tp_members = MultiResult_TABLE_members;
    p->tp_methods = MultiResult_TABLE_methods;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;

    return PyType_Ready(p);
}

PyObject *
pycbc_multiresult_new(pycbc_Connection *parent)
{
    pycbc_MultiResult *ret =
            (pycbc_MultiResult*) PyObject_CallFunction((PyObject*) &pycbc_MultiResultType,
                                                             NULL,
                                                             NULL);
    if (!ret) {
        PyErr_Print();
        abort();
    }
    ret->parent = parent;
    Py_INCREF(parent);

    return (PyObject*)ret;
}

/**
 * This function raises exceptions from the MultiResult object, as required
 */
int
pycbc_multiresult_maybe_raise(pycbc_MultiResult *self)
{
    PyObject *type = NULL, *value = NULL, *traceback = NULL;

    if (self->errop == NULL && self->exceptions == NULL) {
        return 0;
    }

    if (self->exceptions) {
        PyObject *tuple = PyList_GetItem(self->exceptions, 0);

        pycbc_assert(tuple);
        pycbc_assert(PyTuple_Size(tuple) == 3);

        type = PyTuple_GetItem(tuple, 0);
        value = PyTuple_GetItem(tuple, 1);
        traceback = PyTuple_GetItem(tuple, 2);
        PyErr_NormalizeException(&type, &value, &traceback);
        Py_XINCREF(type);
        Py_XINCREF(value);
        Py_XINCREF(traceback);

        pycbc_assert(PyObject_IsInstance(value,
                                         pycbc_helpers.default_exception));

    } else {
        pycbc_Result *res = (pycbc_Result*)self->errop;

        /** Craft an exception based on the operation */
        PYCBC_EXC_WRAP_KEY(PYCBC_EXC_LCBERR, res->rc, "Operational Error", res->key);

        /** Now we have an exception. Let's fetch it back */
        PyErr_Fetch(&type, &value, &traceback);
        PyObject_SetAttrString(value, "result", (PyObject*)res);
    }

    PyObject_SetAttrString(value, "all_results", (PyObject*)self);
    PyErr_Restore(type, value, traceback);

    /**
     * This is needed since the exception object will later contain
     * a reference to ourselves. If we don't free the original exception,
     * then we'll be stuck with a circular reference
     */
    Py_XDECREF(self->exceptions);
    Py_XDECREF(self->errop);
    self->exceptions = NULL;
    self->errop = NULL;


    return 1;
}
