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
/**
 * This file contains boilerplate for the module itself
 */

struct pycbc_helpers_ST pycbc_helpers;


static PyObject *
_libcouchbase_init_helpers(PyObject *self, PyObject *args, PyObject *kwargs) {

#define X(n) \
    pycbc_helpers.n = PyDict_GetItemString(kwargs, #n); \
    if (!pycbc_helpers.n) { \
        PyErr_SetString(PyExc_EnvironmentError, "Can't find " #n); \
        return NULL; \
    } \

    PYCBC_XHELPERS(X);
#undef X

#define X(n) \
    Py_XINCREF(pycbc_helpers.n);
    PYCBC_XHELPERS(X)
#undef X

    (void)self;
    (void)args;

    Py_RETURN_NONE;
}

static PyObject *
_libcouchbase_strerror(PyObject *self, PyObject *args, PyObject *kw)
{
    int rv;
    int rc = 0;
    rv = PyArg_ParseTuple(args, "i", &rc);
    if (!rv) {
        return NULL;
    }

    (void)self;
    (void)kw;

    return pycbc_lcb_errstr(NULL, rc);
}

static PyMethodDef _libcouchbase_methods[] = {
        { "_init_helpers", (PyCFunction)_libcouchbase_init_helpers,
                METH_VARARGS|METH_KEYWORDS,
                "internal function to initialize python-language helpers"
        },
        { "_strerror", (PyCFunction)_libcouchbase_strerror,
                METH_VARARGS|METH_KEYWORDS,
                "Internal function to map errors"
        },

        { NULL }
};


#if PY_MAJOR_VERSION >= 3

#define PyString_FromString PyUnicode_FromString

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        PYCBC_MODULE_NAME,
        NULL,
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
};
#endif

#if PY_MAJOR_VERSION >= 3
PyObject *PyInit__libcouchbase(void)
#define INITERROR return NULL

#else
#define INITERROR return
PyMODINIT_FUNC
init_libcouchbase(void)
#endif
{
    PyObject *m = NULL;

#ifndef PYCBC_CPYCHECKER
    PyObject *result_type = NULL;
    PyObject *connection_type = NULL;
    PyObject *mresult_type = NULL;
    PyObject *valresult_type = NULL;
    PyObject *opresult_type = NULL;
    PyObject *htresult_type = NULL;
    PyObject *transcoder_type = NULL;
    PyObject *arg_type = NULL;

    if (pycbc_ConnectionType_init(&connection_type) < 0) {
        INITERROR;
    }

    if (pycbc_ResultType_init(&result_type) < 0) {
        INITERROR;
    }

    if (pycbc_OperationResultType_init(&opresult_type) < 0) {
        INITERROR;
    }

    if (pycbc_ValueResultType_init(&valresult_type) < 0) {
        INITERROR;
    }

    if (pycbc_MultiResultType_init(&mresult_type) < 0) {
        INITERROR;
    }

    if (pycbc_HttpResultType_init(&htresult_type) < 0) {
        INITERROR;
    }

    if (pycbc_ArgumentType_init(&arg_type) < 0) {
        INITERROR;
    }

    if (pycbc_TranscoderType_init(&transcoder_type) < 0) {
        INITERROR;
    }

#endif /* PYCBC_CPYCHECKER */

#if PY_MAJOR_VERSION >= 3
    moduledef.m_methods = _libcouchbase_methods;
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule(PYCBC_MODULE_NAME, _libcouchbase_methods);
#endif
    if (m == NULL) {
        INITERROR;
    }

#ifndef PYCBC_CPYCHECKER
    /**
     * Add the type:
     */
    PyModule_AddObject(m, "Connection", connection_type);
    PyModule_AddObject(m, "Result", result_type);
    PyModule_AddObject(m, "ValueResult", valresult_type);
    PyModule_AddObject(m, "OperationResult", opresult_type);
    PyModule_AddObject(m, "MultiResult", mresult_type);
    PyModule_AddObject(m, "HttpResult", htresult_type);
    PyModule_AddObject(m, "Arguments", arg_type);
    PyModule_AddObject(m, "Transcoder", transcoder_type);
#endif /* PYCBC_CPYCHECKER */

    /**
     * Initialize the helper names
     */
    pycbc_helpers.tcname_decode_key = pycbc_SimpleStringZ(PYCBC_TCNAME_DECODE_KEY);
    pycbc_helpers.tcname_encode_key = pycbc_SimpleStringZ(PYCBC_TCNAME_ENCODE_KEY);
    pycbc_helpers.tcname_decode_value = pycbc_SimpleStringZ(PYCBC_TCNAME_DECODE_VALUE);
    pycbc_helpers.tcname_encode_value = pycbc_SimpleStringZ(PYCBC_TCNAME_ENCODE_VALUE);

    pycbc_init_pyconstants(m);

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
