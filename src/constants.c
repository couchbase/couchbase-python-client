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


#define XSTORAGE(X) \
    X(ADD) \
    X(REPLACE) \
    X(SET) \
    X(APPEND) \
    X(PREPEND)

void pycbc_init_pyconstants(PyObject *module)
{
#define X(b) \
    PyModule_AddIntMacro(module, LCB_##b);
    XERR(X);
    XSTORAGE(X);
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

    PyModule_AddIntConstant(module, "FMT_JSON", PYCBC_FMT_JSON);
    PyModule_AddIntConstant(module, "FMT_BYTES", PYCBC_FMT_BYTES);
    PyModule_AddIntConstant(module, "FMT_UTF8", PYCBC_FMT_UTF8);
    PyModule_AddIntConstant(module, "FMT_PICKLE", PYCBC_FMT_PICKLE);
    PyModule_AddIntConstant(module, "FMT_MASK", PYCBC_FMT_MASK);
}


PyObject *pycbc_lcb_errstr(lcb_t instance, lcb_error_t err)
{
#if PY_MAJOR_VERSION == 3

    return PyUnicode_InternFromString(lcb_strerror(instance, err));
#else
    return PyString_InternFromString(lcb_strerror(instance, err));
#endif

}
