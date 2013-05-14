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
#if PY_MAJOR_VERSION == 2

unsigned PY_LONG_LONG pycbc_IntAsULL(PyObject *o)
{
    if (PyLong_Check(o)) {
        return PyLong_AsUnsignedLongLong(o);
    } else {
        long tmp =  PyInt_AsLong(o);
        if (tmp < 0) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_TypeError, "value must be unsigned");
            }
            return -1;
        }
        return tmp;
    }
}

PY_LONG_LONG pycbc_IntAsLL(PyObject *o)
{
    if (PyLong_Check(o)) {
        return PyLong_AsLongLong(o);
    } else {
        return PyInt_AsLong(o);
    }
}

long pycbc_IntAsL(PyObject *o)
{
    if (PyInt_Check(o)) {
        return PyInt_AsLong(o);
    }
    return PyLong_AsLong(o);
}

unsigned long pycbc_IntAsUL(PyObject *o)
{
    if (PyInt_Check(o)) {
        long l = PyInt_AsLong(o);
        if (l < 0) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_TypeError, "value must be unsigned");
            }
            return -1;
        }
        return l;
    }
    return PyLong_AsUnsignedLong(o);
}

#endif /* PY_MAJOR_VERSION == 2 */

PyObject *pycbc_maybe_convert_to_int(PyObject *o)
{
    PyObject *args, *result;
    args = Py_BuildValue("(O)", o);

#if PY_MAJOR_VERSION == 2
    result = PyObject_CallObject((PyObject*)&PyInt_Type, args);
    if (result) {
        Py_DECREF(args);
        return result;
    }

    PyErr_Clear();
#endif

    result = PyObject_CallObject((PyObject*)&PyLong_Type, args);
    Py_DECREF(args);

    if (result) {
        return result;
    }

    return NULL;
}

/**
 * Py3-specific stuff
 */

#if PY_MAJOR_VERSION == 3
int pycbc_BufFromString(PyObject *obj, char **key, Py_ssize_t *nkey, PyObject **newkey)
{
    int rv;
    if (PyBytes_Check(obj)) {
        *newkey = NULL;
        return PyBytes_AsStringAndSize(obj, key, nkey);
    }

    *newkey = PyUnicode_AsUTF8String(obj);
    if (!*newkey) {
        return -1;
    }
    rv = PyBytes_AsStringAndSize(*newkey, key, nkey);
    if (rv < 0) {
        Py_DECREF(*newkey);
        *newkey = NULL;
    }
    return rv;
}

#else
int pycbc_BufFromString(PyObject *obj, char **key, Py_ssize_t *nkey, PyObject **newkey)
{
    int rv;
    rv = PyBytes_AsStringAndSize(obj, key, nkey);
    if (rv < 0) {
        *newkey = NULL;
        return -1;
    }
    *newkey = obj;
    Py_INCREF(obj);
    return 0;
}

#endif /* PY_MAJOR_VERSION == 3*/
