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

#include "iops.h"
#include "pycbc_http.h"
#include <libcouchbase/crypto.h>

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
    X(EINVAL) \
    X(CLIENT_ETMPFAIL) \
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
    X(INVALID_CHAR) \
    X(DURABILITY_ETOOMANY) \
    X(DUPLICATE_COMMANDS) \
    X(HTTP_ERROR) \
    X(SUBDOC_PATH_ENOENT) \
    X(SUBDOC_PATH_MISMATCH) \
    X(SUBDOC_PATH_EINVAL) \
    X(SUBDOC_DOC_E2DEEP) \
    X(SUBDOC_VALUE_E2DEEP) \
    X(SUBDOC_VALUE_CANTINSERT) \
    X(SUBDOC_DOC_NOTJSON) \
    X(SUBDOC_NUM_ERANGE) \
    X(SUBDOC_BAD_DELTA) \
    X(SUBDOC_PATH_EEXISTS) \
    X(SUBDOC_MULTI_FAILURE) \
    X(EMPTY_PATH)

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

typedef void *(*pycbc_constant_handler)(PyObject *,
                                        const char *,
                                        long long int);

static void setup_compression_map(PyObject *module, PyObject* public_constants,
                                  pycbc_constant_handler handler);

static void setup_tracing_map(PyObject *module, pycbc_constant_handler handler);

static void setup_crypto_exceptions(PyObject *module,
                                    pycbc_constant_handler handler);

static void
do_all_constants(PyObject *module, pycbc_constant_handler handler)
{
    PyObject* public_constants = PyDict_New();
    PyModule_AddObject(module, "PUBLIC_CONSTANTS", public_constants);

    #define ADD_MACRO(sym) handler(module, #sym, sym)
    #define ADD_CONSTANT(name, val) handler(module, name, val)
    #define ADD_STRING(name) PyModule_AddObject(module, #name, pycbc_SimpleStringZ(name) )
    #define LCB_CONSTANT(postfix, ...) ADD_CONSTANT(#postfix, LCB_##postfix)
    #define ADD_MACRO_TERM(sym) ADD_MACRO(sym);

#define LCB_PUBLIC_CONSTANT(postfix, ...)                              \
    {                                                                  \
        PyObject *py_longlong =                                        \
                PyLong_FromLongLong((PY_LONG_LONG)LCB_##postfix);      \
        PyDict_SetItemString(public_constants, #postfix, py_longlong); \
        Py_DecRef(py_longlong);                                        \
    }
    #define X(b) ADD_MACRO(LCB_##b);
    XERR(X);
    XHTTP(X);
#undef X
    XSTORAGE(LCB_STORE_WRAPPER)

    ADD_MACRO(LCB_MAX_ERROR);
    ADD_MACRO(PYCBC_CMD_GET);
    ADD_MACRO(PYCBC_CMD_LOCK);
    ADD_MACRO(PYCBC_CMD_TOUCH);
    ADD_MACRO(PYCBC_CMD_GAT);

    ADD_MACRO(PYCBC_EXC_ARGUMENTS);
    ADD_MACRO(PYCBC_EXC_ENCODING);
    ADD_MACRO(PYCBC_EXC_LCBERR);
    ADD_MACRO(PYCBC_EXC_INTERNAL);
    ADD_MACRO(PYCBC_EXC_HTTP);
    ADD_MACRO(PYCBC_EXC_THREADING);
    ADD_MACRO(PYCBC_EXC_DESTROYED);
    ADD_MACRO(PYCBC_EXC_PIPELINE);

    ADD_MACRO(LCB_TYPE_BUCKET);
    ADD_MACRO(LCB_TYPE_CLUSTER);
    ADD_MACRO(LCB_HTTP_TYPE_VIEW);
    ADD_MACRO(LCB_HTTP_TYPE_CBAS);
    ADD_MACRO(LCB_HTTP_TYPE_RAW);
    ADD_MACRO(LCB_HTTP_TYPE_MANAGEMENT);

    ADD_MACRO(PYCBC_RESFLD_CAS);
    ADD_MACRO(PYCBC_RESFLD_FLAGS);
    ADD_MACRO(PYCBC_RESFLD_KEY);
    ADD_MACRO(PYCBC_RESFLD_VALUE);
    ADD_MACRO(PYCBC_RESFLD_RC);
    ADD_MACRO(PYCBC_RESFLD_HTCODE);
    ADD_MACRO(PYCBC_RESFLD_URL);
    ADD_MACRO(PYCBC_LCB_API);
    ADD_MACRO(PYCBC_ENDURE);
    ADD_CONSTANT("FMT_JSON", (lcb_U32)PYCBC_FMT_JSON);
    ADD_CONSTANT("FMT_BYTES", (lcb_U32)PYCBC_FMT_BYTES);
    ADD_CONSTANT("FMT_UTF8", (lcb_U32)PYCBC_FMT_UTF8);
    ADD_CONSTANT("FMT_PICKLE", (lcb_U32)PYCBC_FMT_PICKLE);
    ADD_CONSTANT("FMT_LEGACY_MASK", (lcb_U32)PYCBC_FMT_LEGACY_MASK);
    ADD_CONSTANT("FMT_COMMON_MASK", (lcb_U32)PYCBC_FMT_COMMON_MASK);

    ADD_CONSTANT("OBS_PERSISTED", LCB_OBSERVE_PERSISTED);
    ADD_CONSTANT("OBS_FOUND", LCB_OBSERVE_FOUND);
    ADD_CONSTANT("OBS_NOTFOUND", LCB_OBSERVE_NOT_FOUND);
    ADD_CONSTANT("OBS_LOGICALLY_DELETED",
                 LCB_OBSERVE_PERSISTED| LCB_OBSERVE_NOT_FOUND);

    ADD_CONSTANT("OBS_MASK",
                 LCB_OBSERVE_PERSISTED|LCB_OBSERVE_FOUND| LCB_OBSERVE_NOT_FOUND);

    ADD_CONSTANT("LOCKMODE_WAIT", PYCBC_LOCKMODE_WAIT);
    ADD_CONSTANT("LOCKMODE_EXC", PYCBC_LOCKMODE_EXC);
    ADD_CONSTANT("LOCKMODE_NONE", PYCBC_LOCKMODE_NONE);

    ADD_MACRO(PYCBC_CONN_F_WARNEXPLICIT);
    ADD_MACRO(PYCBC_CONN_F_CLOSED);
    ADD_MACRO(PYCBC_CONN_F_ASYNC);
    ADD_MACRO(PYCBC_CONN_F_ASYNC_DTOR);

    ADD_MACRO(PYCBC_EVACTION_WATCH);
    ADD_MACRO(PYCBC_EVACTION_UNWATCH);
    ADD_MACRO(PYCBC_EVACTION_SUSPEND);
    ADD_MACRO(PYCBC_EVACTION_RESUME);
    ADD_MACRO(PYCBC_EVACTION_CLEANUP);
    ADD_MACRO(PYCBC_EVSTATE_INITIALIZED);
    ADD_MACRO(PYCBC_EVSTATE_ACTIVE);
    ADD_MACRO(PYCBC_EVSTATE_SUSPENDED);
    ADD_MACRO(PYCBC_EVTYPE_IO);
    ADD_MACRO(PYCBC_EVTYPE_TIMER);
    ADD_MACRO(LCB_READ_EVENT);
    ADD_MACRO(LCB_WRITE_EVENT);
    ADD_MACRO(LCB_RW_EVENT);


    /* For CNTL constants */
    ADD_MACRO(LCB_CNTL_OP_TIMEOUT);
    ADD_MACRO(LCB_CNTL_VIEW_TIMEOUT);
    ADD_MACRO(LCB_CNTL_SSL_MODE);
    ADD_MACRO(LCB_SSL_ENABLED);
    ADD_MACRO(LCB_CNTL_N1QL_TIMEOUT);
	ADD_MACRO(LCB_CNTL_COMPRESSION_OPTS);
    ADD_MACRO(LCB_CNTL_LOG_REDACTION);
    ADD_STRING(LCB_LOG_MD_OTAG);
    ADD_STRING(LCB_LOG_MD_CTAG);
    ADD_STRING(LCB_LOG_SD_OTAG);
    ADD_STRING(LCB_LOG_SD_CTAG);
    ADD_STRING(LCB_LOG_UD_OTAG);
    ADD_STRING(LCB_LOG_UD_CTAG);

    /* View options */
    ADD_MACRO(LCB_CMDVIEWQUERY_F_INCLUDE_DOCS);
    ADD_MACRO(LCB_CMDVIEWQUERY_F_SPATIAL);
    ADD_MACRO(LCB_SDCMD_REPLACE);
    ADD_MACRO(LCB_SDCMD_DICT_ADD);
    ADD_MACRO(LCB_SDCMD_DICT_UPSERT);
    ADD_MACRO(LCB_SDCMD_ARRAY_ADD_FIRST);
    ADD_MACRO(LCB_SDCMD_ARRAY_ADD_LAST);
    ADD_MACRO(LCB_SDCMD_ARRAY_ADD_UNIQUE);
    ADD_MACRO(LCB_SDCMD_EXISTS);
    ADD_MACRO(LCB_SDCMD_GET);
    ADD_MACRO(LCB_SDCMD_COUNTER);
    ADD_MACRO(LCB_SDCMD_REMOVE);
    ADD_MACRO(LCB_SDCMD_ARRAY_INSERT);
    /* Bucket types */
    ADD_MACRO(LCB_BTYPE_UNSPEC);
    ADD_MACRO(LCB_BTYPE_COUCHBASE);
    ADD_MACRO(LCB_BTYPE_EPHEMERAL);
    ADD_MACRO(LCB_BTYPE_MEMCACHED);
    ADD_MACRO(PYCBC_DURABILITY);
    /* Encryption options */
    PYCBC_PP_ENCRYPT_CONSTANTS(ADD_MACRO_TERM)

    LCB_CONSTANT(VERSION);
    ADD_STRING(LCB_VERSION_STRING);
    ADD_MACRO(PYCBC_CRYPTO_VERSION);
    ADD_CONSTANT("PYCBC_TRACING",1);
    setup_tracing_map(module, handler);
#define PYCBC_DURLEVEL(X) ADD_MACRO(LCB_DURABILITYLEVEL_##X);
    PYCBC_X_DURLEVEL(PYCBC_DURLEVEL);
#define PYCBC_ERR(ERRNAME, ...) ADD_MACRO(ERRNAME);
    PYCBX_X_SYNCREPERR(PYCBC_ERR)
    PYCBC_LCB_ERRTYPES(ADD_MACRO)
    setup_compression_map(module, public_constants, handler);
    setup_crypto_exceptions(module, handler);
    PyModule_AddObject(
            module, "CRYPTO_EXCEPTIONS", pycbc_gen_crypto_exception_map());
#ifdef LCB_N1XSPEC_F_DEFER
    ADD_MACRO(LCB_N1XSPEC_F_DEFER);
#endif
}

static void setup_crypto_exceptions(PyObject *module,
                                    pycbc_constant_handler handler)
{
#define X(NAME, ...) ADD_MACRO(NAME)
    {
        void *result[] = {PP_FOR_EACH_CRYPTO_EXCEPTION(X)};
        (void)result;
    };
#undef X
}

static void setup_tracing_map(PyObject *module,
                                       pycbc_constant_handler handler) {
#define LCB_CNTL_CONSTANT(postfix, ...) ADD_CONSTANT(#postfix, LCB_CNTL_##postfix)
#define LCB_FOR_EACH_THRESHOLD_PARAM(X, DIV)\
    X(TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL, convert_timevalue) DIV\
    X(TRACING_ORPHANED_QUEUE_SIZE, convert_u32) DIV\
    X(TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL, convert_timevalue) DIV\
    X(TRACING_THRESHOLD_QUEUE_SIZE, convert_u32) DIV\
    X(TRACING_THRESHOLD_KV, convert_timevalue) DIV\
    X(TRACING_THRESHOLD_N1QL, convert_timevalue) DIV\
    X(TRACING_THRESHOLD_VIEW, convert_timevalue) DIV\
    X(TRACING_THRESHOLD_FTS, convert_timevalue) DIV\
    X(TRACING_THRESHOLD_ANALYTICS, convert_timevalue)

    PyObject* convert_timevalue = pycbc_SimpleStringZ("timeout");
#define convert_timevalue_desc "The %S, in fractions of a second."
#define convert_timevalue_desc_val "0.5"
#define convert_timevalue_desc_val_units " seconds"
    PyObject* convert_u32 = pycbc_SimpleStringZ("uint32_t");
#define convert_u32_desc "The %S."
#define convert_u32_desc_val "100"
#define convert_u32_desc_val_units " entries"
    PyObject* convert_intbool = pycbc_SimpleStringZ("int");
#define convert_intbool_desc "Whether %S is set. "
#define convert_intbool_desc_val "True"
#define convert_intbool_desc_val_units ""
    PyObject *result = PyDict_New();
    LCB_FOR_EACH_THRESHOLD_PARAM(LCB_CNTL_CONSTANT, ;);
#define X(NAME, TYPE)                                       \
    {                                                       \
        PyObject *attrdict = PyDict_New();                  \
        PyObject *val = PyLong_FromLong(LCB_CNTL_##NAME);   \
        PyDict_SetItemString(attrdict, "op", val);          \
        PyDict_SetItemString(attrdict, "value_type", TYPE); \
        PyDict_SetItemString(result, #NAME, attrdict);      \
        Py_DecRef(val);                                     \
        Py_DecRef(attrdict);                                \
    }
    LCB_FOR_EACH_THRESHOLD_PARAM(X, ;);
#undef X

#define X(NAME,TYPE)\
    {\
        PyObject* as_string = pycbc_SimpleStringZ(#NAME);\
        PyObject* as_lower_case = PyObject_CallMethod(as_string, "lower","");\
        PyObject* as_words = PyObject_CallMethod(as_lower_case, "replace", "ss", "_", " ");\
        pycbc_replace_str(&as_words, "analytics", "for analytics");\
        pycbc_replace_str(&as_words, "n1ql", "for N1QL");\
        pycbc_replace_str(&as_words, "kv", "for KV");\
        pycbc_replace_str(&as_words, "fts", "for FTS");\
        pycbc_replace_str(&as_words, "view", "for View");\
        pycbc_print_pyformat(\
        "@property\n"\
        "def %S(self):\n"\
        "    \"\"\"\n"\
        "    " TYPE##_desc "\n"\
        "\n"\
        "    ::\n"\
        "        # Set %S to " TYPE##_desc_val TYPE##_desc_val_units "\n"\
        "        cb.%S=" TYPE##_desc_val "\n" \
        "\n"\
        "    \"\"\"\n"\
        "    \n"\
        "    return self._cntl(op=_LCB." #NAME ", value_type=\"%S\")\n\n", as_lower_case, as_words, as_words, as_lower_case, TYPE  );\
        pycbc_print_pyformat(\
        "@%S.setter\n"\
        "def %S(self, val):\n"\
        "    return self._cntl(op=_LCB." #NAME ", value=val, value_type=\"%S\")\n\n", as_lower_case, as_lower_case, TYPE);\
        Py_DecRef(as_words);\
        Py_DecRef(as_lower_case);\
        Py_DecRef(as_string);\
    }

#ifdef PYCBC_GEN_PYTHON
LCB_FOR_EACH_THRESHOLD_PARAM(X, ; );
#endif
#undef X
    Py_DecRef(convert_timevalue);
    Py_DecRef(convert_u32);
    Py_DecRef(convert_intbool);
    PyModule_AddObject(module, "TRACING", result);
#undef LCB_FOR_EACH_THRESHOLD_PARAM
}

static void setup_compression_map(PyObject *module, PyObject* public_constants,
                                  pycbc_constant_handler handler)
{
/* Compression options */
#define LCB_FOR_EACH_COMPRESS_TYPE(X)                                       \
    X(COMPRESS_NONE, "Do not perform compression in any direction.");       \
    X(COMPRESS_IN,                                                          \
      "Decompress incoming data, if the data has been compressed at the "   \
      "server.");                                                           \
    X(COMPRESS_OUT, " Compress outgoing data.");                            \
    X(COMPRESS_INOUT);                                                      \
    X(COMPRESS_FORCE,                                                       \
      "Setting this flag will force the client to assume that all servers " \
      "support compression despite a HELLO not having been initially "      \
      "negotiated.")

    PyObject *result = PyDict_New();
    LCB_FOR_EACH_COMPRESS_TYPE(LCB_CONSTANT);
    LCB_FOR_EACH_COMPRESS_TYPE(LCB_PUBLIC_CONSTANT);
#undef LCB_FOR_EACH_COMPRESS_TYPE
#define PP_FOR_EACH(FUNC)                \
    FUNC(on, LCB_COMPRESS_INOUT);        \
    FUNC(off, LCB_COMPRESS_NONE);        \
    FUNC(inflate_only, LCB_COMPRESS_IN); \
    FUNC(force, LCB_COMPRESS_INOUT | LCB_COMPRESS_FORCE)

#define X(NAME, VALUE)                                 \
    {                                                  \
        PyObject *addition = PyLong_FromLong(VALUE);   \
        PyDict_SetItemString(result, #NAME, addition); \
        Py_DecRef(addition);                           \
    }
    PP_FOR_EACH(X);
#undef X
#undef PP_FOR_EACH
    PyModule_AddObject(module, "COMPRESSION", result);
}

#undef LCB_CONSTANT

static void *do_constmod(PyObject *module, const char *name, long long value)
{
    PyObject *o = PyLong_FromLongLong((PY_LONG_LONG)value);
    PyModule_AddObject(module, name, o);
    return NULL;
}

void
pycbc_init_pyconstants(PyObject *module)
{
    do_all_constants(module, do_constmod);
    /* We support built-in include_docs now! */
    PyModule_AddIntConstant(module, "_IMPL_INCLUDE_DOCS", 1);
}


PyObject *
pycbc_lcb_errstr(lcb_t instance, lcb_STATUS err)
{
#if PY_MAJOR_VERSION == 3

    return PyUnicode_InternFromString(lcb_strerror(instance, err));
#else
    return PyString_InternFromString(lcb_strerror(instance, err));
#endif
}

static void *do_printmod(PyObject *module, const char *name, PY_LONG_LONG value)
{
    printf("%s = %lld\n", name, value);
    return NULL;
}

PyObject *
pycbc_print_constants(PyObject *mod, PyObject *args)
{
    do_all_constants(NULL, do_printmod);
    (void)mod;
    (void)args;
    Py_RETURN_NONE;
}
