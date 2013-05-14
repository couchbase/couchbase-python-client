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


static struct PyMemberDef MultiResult_members[] = {
        { "all_ok",
                T_INT, offsetof(pycbc_MultiResultObject, all_ok),
                READONLY, "Whether all the items in this result are successful"
        },
        { NULL }
};

PyTypeObject pycbc_MultiResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyMethodDef methods_dummy[] = {
        { NULL }
};


static int MultiResultType__init__(pycbc_MultiResultObject *self,
                                   PyObject *args,
                                   PyObject *kwargs)
{
    if (PyDict_Type.tp_init((PyObject*)self, args, kwargs) < 0) {
        PyErr_Print();
        abort();
        return -1;
    }
    self->all_ok = 1;
    self->exceptions = NULL;
    self->errop = NULL;
    self->no_raise_enoent = 0;

    return 0;
}

static void MultiResult_dealloc(pycbc_MultiResultObject *self)
{
    Py_XDECREF(self->parent);
    Py_XDECREF(self->exceptions);
    Py_XDECREF(self->errop);
    PyDict_Type.tp_dealloc((PyObject*)self);
}

int pycbc_MultiResultType_init(PyObject **ptr)
{
    *ptr = (PyObject*)&pycbc_MultiResultType;
    if (pycbc_MultiResultType.tp_name) {
        return 0;
    }

    pycbc_MultiResultType.tp_base = &PyDict_Type;
    pycbc_MultiResultType.tp_init = (initproc)MultiResultType__init__;
    pycbc_MultiResultType.tp_dealloc = (destructor)MultiResult_dealloc;

    pycbc_MultiResultType.tp_name = "MultiResult";
    pycbc_MultiResultType.tp_doc =
            ":class:`dict` subclass to hold :class:`Result` objects\n"
            "\n"
            "This object also contains some of the heavy lifting, but this\n"
            "is not currently exposed in python-space\n."
            "\n"
            "An additional method is :meth:`all_ok`, which allows to see\n"
            "if all commands completed successfully\n"
            "\n";

    pycbc_MultiResultType.tp_basicsize = sizeof(pycbc_MultiResultObject);
    pycbc_MultiResultType.tp_members = MultiResult_members;
    pycbc_MultiResultType.tp_methods = methods_dummy;
    pycbc_MultiResultType.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;

    return PyType_Ready(&pycbc_MultiResultType);
}

PyObject* pycbc_multiresult_new(pycbc_ConnectionObject *parent)
{
    pycbc_MultiResultObject *ret =
            (pycbc_MultiResultObject*) PyObject_CallFunction((PyObject*) &pycbc_MultiResultType,
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
int pycbc_multiresult_maybe_raise(pycbc_MultiResultObject *self)
{
    PyObject *type = NULL, *value = NULL, *traceback = NULL;

    if (self->errop == NULL && self->exceptions == NULL) {
        return 0;
    }

    if (self->exceptions) {
        PyObject *tuple = PyList_GetItem(self->exceptions, 0);
        assert(tuple);
        assert(PyTuple_Size(tuple) == 3);

        type = PyTuple_GetItem(tuple, 0); Py_INCREF(type);
        value = PyTuple_GetItem(tuple, 1); Py_INCREF(value);
        traceback = PyTuple_GetItem(tuple, 2); Py_INCREF(traceback);

        PyErr_NormalizeException(&type, &value, &traceback);

    } else {
        pycbc_ResultBaseObject *res = (pycbc_ResultBaseObject*)self->errop;
        /** Craft an exception based on the operation */
        PYCBC_EXC_WRAP_KEY(PYCBC_EXC_LCBERR, res->rc, "Operational Error", res->key);
        PyErr_Fetch(&type, &value, &traceback);
        PyObject_SetAttrString(value, "result", (PyObject*)res);
    }

    PyObject_SetAttrString(value, "all_results", (PyObject*)self);
    PyErr_Restore(type, value, traceback);

    return 1;
}
