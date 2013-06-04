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

/**
 * Very simple file that simply adds LCB constants to the module
 */

#define XERR(X) \
    X(SUCCESS) \
    X(AUTH_CONTINUE) \
    X(AUTH_ERROR) \
    X(DELTA_BADVAL) \
    X(E2BIG) \
    X(EBUSY) \
    X(ENOMEM) \
    X(ERANGE) \
    X(ERROR) \
    X(ETMPFAIL) \
    X(KEY_EEXISTS) \
    X(KEY_ENOENT) \
    X(DLOPEN_FAILED) \
    X(DLSYM_FAILED) \
    X(NETWORK_ERROR) \
    X(NOT_MY_VBUCKET) \
    X(NOT_STORED) \
    X(NOT_SUPPORTED) \
    X(UNKNOWN_HOST) \
    X(PROTOCOL_ERROR) \
    X(ETIMEDOUT) \
    X(BUCKET_ENOENT) \
    X(CONNECT_ERROR) \
    X(EBADHANDLE) \
    X(SERVER_BUG) \
    X(PLUGIN_VERSION_MISMATCH) \
    X(INVALID_HOST_FORMAT) \
    X(INVALID_CHAR)

#define XHTTP(X) \
    X(HTTP_METHOD_GET) \
    X(HTTP_METHOD_POST) \
    X(HTTP_METHOD_PUT) \
    X(HTTP_METHOD_DELETE)



#define XSTORAGE(X) \
    X(ADD) \
    X(REPLACE) \
    X(SET) \
    X(APPEND) \
    X(PREPEND)

void
pycbc_init_pyconstants(PyObject *module)
{
#define X(b) \
    PyModule_AddIntMacro(module, LCB_##b);
    XERR(X);
    XSTORAGE(X);
    XHTTP(X);
#undef X

    PyModule_AddIntMacro(module, PYCBC_CMD_GET);
    PyModule_AddIntMacro(module, PYCBC_CMD_LOCK);
    PyModule_AddIntMacro(module, PYCBC_CMD_TOUCH);
    PyModule_AddIntMacro(module, PYCBC_CMD_GAT);
    PyModule_AddIntMacro(module, PYCBC_CMD_INCR);
    PyModule_AddIntMacro(module, PYCBC_CMD_DECR);

    PyModule_AddIntMacro(module, PYCBC_EXC_ARGUMENTS);
    PyModule_AddIntMacro(module, PYCBC_EXC_ENCODING);
    PyModule_AddIntMacro(module, PYCBC_EXC_LCBERR);
    PyModule_AddIntMacro(module, PYCBC_EXC_INTERNAL);

    PyModule_AddIntMacro(module, LCB_TYPE_BUCKET);
    PyModule_AddIntMacro(module, LCB_TYPE_CLUSTER);
    PyModule_AddIntMacro(module, LCB_HTTP_TYPE_VIEW);
    PyModule_AddIntMacro(module, LCB_HTTP_TYPE_MANAGEMENT);

    PyModule_AddIntMacro(module, PYCBC_RESFLD_CAS);
    PyModule_AddIntMacro(module, PYCBC_RESFLD_FLAGS);
    PyModule_AddIntMacro(module, PYCBC_RESFLD_KEY);
    PyModule_AddIntMacro(module, PYCBC_RESFLD_VALUE);
    PyModule_AddIntMacro(module, PYCBC_RESFLD_RC);
    PyModule_AddIntMacro(module, PYCBC_RESFLD_HTCODE);
    PyModule_AddIntMacro(module, PYCBC_RESFLD_URL);

    PyModule_AddIntConstant(module, "FMT_JSON", PYCBC_FMT_JSON);
    PyModule_AddIntConstant(module, "FMT_BYTES", PYCBC_FMT_BYTES);
    PyModule_AddIntConstant(module, "FMT_UTF8", PYCBC_FMT_UTF8);
    PyModule_AddIntConstant(module, "FMT_PICKLE", PYCBC_FMT_PICKLE);
    PyModule_AddIntConstant(module, "FMT_MASK", PYCBC_FMT_MASK);
}


PyObject *
pycbc_lcb_errstr(lcb_t instance, lcb_error_t err)
{
#if PY_MAJOR_VERSION == 3

    return PyUnicode_InternFromString(lcb_strerror(instance, err));
#else
    return PyString_InternFromString(lcb_strerror(instance, err));
#endif

}
