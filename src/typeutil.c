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

/**
 * This file contains various conversion utilities between C and Python
 * types. This also does a lot of the porting and handling between Python
 * major versions as well.
 */
#include "pycbc.h"
#if PY_MAJOR_VERSION == 2

unsigned PY_LONG_LONG
pycbc_IntAsULL(PyObject *o)
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

PY_LONG_LONG
pycbc_IntAsLL(PyObject *o)
{
    if (PyLong_Check(o)) {
        return PyLong_AsLongLong(o);
    } else {
        return PyInt_AsLong(o);
    }
}

long
pycbc_IntAsL(PyObject *o)
{
    if (PyInt_Check(o)) {
        return PyInt_AsLong(o);
    }
    return PyLong_AsLong(o);
}

unsigned long
pycbc_IntAsUL(PyObject *o)
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

PyObject *
pycbc_maybe_convert_to_int(PyObject *o)
{
    PyObject *args, *result;
    args = Py_BuildValue("(O)", o);

    if (!args) {
        return NULL;
    }

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
int
pycbc_BufFromString(PyObject *obj, char **key, Py_ssize_t *nkey, PyObject **newkey)
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
int
pycbc_BufFromString(PyObject *obj, char **key, Py_ssize_t *nkey, PyObject **newkey)
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



int
pycbc_get_duration(PyObject *obj, unsigned long *ttl, int canbezero)
{
    int rc=0;
    if (obj == NULL || PyObject_IsTrue(obj) == 0) {
        if (!canbezero) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "duration must be specified "
                           "and must not be 0 or False or None",
                           obj);
            rc=-1;
            goto GT_DONE;
        }
        *ttl = 0;
        goto GT_DONE;
    }

    if (!PyNumber_Check(obj)) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "duration must be numeric", obj);
        rc=-1;
        goto GT_DONE;
    }
    *ttl = pycbc_IntAsUL(obj);
    if (*ttl == (unsigned long)-1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                           "duration must be a valid Unix timestamp ", obj);
        rc=-1;
        goto GT_DONE;
    }
    GT_DONE:

    return rc;
}

int
pycbc_get_u32(PyObject *obj, lcb_uint32_t *out)
{

    unsigned long val = pycbc_IntAsUL(obj);
    if (PyErr_Occurred()) {
        return -1;
    }

    /**
     * Python won't check for an overflow if our long type is not 32 bits,
     * so we need to do it ourselves
     */
#if LONG_MAX > 0xFFFFFFFFUL
    if ( (val & 0xFFFFFFFFUL) != val) {
        PyErr_SetString(PyExc_OverflowError, "Value must be smaller " \
                        "than 32 bits");
        return -1;
    }
#endif

    *out = (lcb_uint32_t)val;
    return 0;

}
