/**
 *     Copyright 2018 Couchbase, Inc.
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
#include <libcouchbase/crypto.h>
#include <string.h>

static int
CryptoProvider__init(pycbc_CryptoProvider *self,
                     PyObject *args,
                     PyObject *kwargs);

static void
CryptoProvider_dtor(pycbc_CryptoProvider *self);

lcb_STATUS pycbc_cstrndup(char **key, size_t *key_len, PyObject *result)
{
    const char *data = NULL;
    lcb_STATUS lcb_result = LCB_ERR_SDK_INTERNAL;

    data = PYCBC_CSTRN(result, key_len);;
    if (data) {
        lcb_result = LCB_SUCCESS;
        PYCBC_DEBUG_LOG(
                "Got string from %p: %.*s", result, (int)*key_len, data);
        *key = calloc(1, *key_len + 1);
        memcpy((void *)*key, (void *)data, *key_len);
        (*key)[*key_len] = '\0';
        PYCBC_DEBUG_LOG("Copied string from %p: %.*s",
                        result,
                        (int)*key_len,
                        (char *)*key);
    } else {
        PYCBC_DEBUG_PYFORMAT(
                "Problems extracting key from %p: %S", result, result);
    }

    return lcb_result;
}

const char *pycbc_cstrdup_or_default_and_exception(
        PyObject *object, const char *fallback)
{
    const char *buffer_data = PYCBC_CSTR(object);

    if (!buffer_data) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR,
                       LCB_ERR_TEMPORARY_FAILURE,
                       "CryptoProviderMissingPublicKeyException")
    }
    return buffer_data ? buffer_data : fallback;
}

lcb_STATUS pycbc_is_true(uint8_t *key, const size_t key_len, PyObject *result) {
    return (result && PyObject_IsTrue(result) && !PyErr_Occurred())
                   ? LCB_SUCCESS
                   : LCB_ERR_SDK_INTERNAL;
}

#define PYCBC_SIZE(X) (sizeof(X)/sizeof(char))

void pycbc_report_method_exception(lcb_STATUS errflags, const char* fmt, ...) {
    char buffer[1000];
    va_list args;
    va_start(args,fmt);
    vsnprintf(buffer, PYCBC_SIZE(buffer), fmt,args);
    va_end(args);
    PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, errflags, buffer);
}

static PyObject* pycbc_retrieve_method(lcbcrypto_PROVIDER* provider, const char* method_name){
    pycbc_CryptoProvider *py_provider =
            provider ? (pycbc_CryptoProvider *)provider->cookie : NULL;
    PyObject *method = py_provider
                               ? PyObject_GetAttrString((PyObject *)py_provider,
                                                        method_name)
                               : NULL;
    if (!method || !PyObject_IsTrue(method)) {
        pycbc_report_method_exception(LCB_ERR_TEMPORARY_FAILURE,
                                      "Method %s does not exist",
                                      method_name);
        return NULL;
    }
    PYCBC_DEBUG_LOG("Got method pointer %p for %s", method, method_name);
    PYCBC_DEBUG_PYFORMAT("i.e. %S for %s", method, method_name);
    return method;
}
#define PYCBC_CRYPTO_GET_METHOD(PROVIDER,METHOD) pycbc_retrieve_method(PROVIDER,#METHOD)

PyObject* pycbc_python_proxy(PyObject *method, PyObject *args, const char* method_name) {
    PyObject* result = NULL;
    pycbc_assert(method && PyObject_IsTrue(method));
    PYCBC_DEBUG_PYFORMAT("Calling %R with %R", method, args);
    if (PyErr_Occurred() || !args)
    {
        return NULL;
    };
    result = PyObject_CallObject(method, args);
    PYCBC_DEBUG_PYFORMAT("Called %R with %R, got %p", method, args, result);
    PYCBC_DEBUG_PYFORMAT("%p is %S", result, pycbc_none_or_value(result));
    if (!result || PyErr_Occurred()) {
        pycbc_report_method_exception(
                LCB_ERR_SDK_INTERNAL, "Problem calling method %s", method_name);
        result =NULL;
    };

    return result;
}
typedef struct pycbc_crypto_buf {
    const uint8_t *data;
    size_t len;
} pycbc_crypto_buf;

typedef struct pycbc_crypto_buf_array {
    pycbc_crypto_buf* buffers;
    size_t len;
} pycbc_crypto_buf_array;

#define PYCBC_GEN_LIST(TYPE)\
PyObject *pycbc_gen_list_##TYPE(const TYPE* array, size_t len,\
                                        PyObject *(*converter)(const TYPE *))\
{\
    PyObject* result = PyList_New(0);\
    size_t i;\
    for (i=0; i<len; ++i)\
    {\
        TYPE sigv = array[i];\
        PyObject *input = converter(&sigv);\
        PyList_Append(result, input);\
        Py_DECREF(input);\
    }\
    return result;\
}\

PYCBC_GEN_LIST(lcbcrypto_SIGV);
PYCBC_GEN_LIST(uint8_t);

PyObject *pycbc_convert_uint8_t(const pycbc_crypto_buf buf) {
#if PY_MAJOR_VERSION >= 3
    return PyBytes_FromStringAndSize((const char *) buf.data, buf.len);
#else
    return PyString_FromStringAndSize((const char *)buf.data, buf.len);
#endif
}

PyObject *pycbc_convert_lcbcrypto_SIGV(const lcbcrypto_SIGV *sigv) {
    const pycbc_crypto_buf buf = {sigv->data, sigv->len};
    return pycbc_convert_uint8_t(buf);
}

PyObject* pycbc_convert_char_p(const char* string)
{
    return pycbc_SimpleStringZ(string);
}

pycbc_crypto_buf pycbc_gen_buf(const uint8_t *data, size_t len)
{
    pycbc_crypto_buf result;
    result.data=data;
    result.len = len;
    return result;
}

#define PYCBC_SIZED_ARRAY(TYPE,NAME) ,const TYPE* NAME, size_t NAME##_num
#define PYCBC_SIZED_ARRAY_FWD(TYPE,NAME) ,NAME, NAME##_num

#define PYCBC_ARG_CSTRN(TYPE,NAME)  , TYPE* NAME, size_t NAME##_len
#define PYCBC_ARG_CSTRN_FWD(TYPE,NAME)  ,NAME##_cstrn
#define PYCBC_STORE_CSTRN(TYPE,NAME) \
    PyObject *NAME##_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(NAME,NAME##_len));
#define PYCBC_ARG_CSTRN_FREE(TYPE,NAME) Py_DecRef(NAME##_cstrn);
#define PYCBC_ARG_CSTRN_FWD_PASSTHRU(TYPE, NAME) , NAME, NAME##_len
#define PYCBC_ARG_CSTRN_TRIM_NEWLINE(TYPE,NAME)  , TYPE* NAME, size_t NAME##_len
#define PYCBC_ARG_CSTRN_TRIM_NEWLINE_FWD(TYPE,NAME)  ,NAME##_cstrn
#define PYCBC_STORE_CSTRN_TRIM_NEWLINE(TYPE,NAME) \
    PyObject *NAME##_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(NAME,(NAME##_len>0)?(NAME##_len-1):0));

#define PYCBC_ARG_PASSTHRU(TYPE,NAME) ,TYPE NAME
#define PYCBC_ARG_PASSTHRU_FWD(TYPE, NAME) , NAME
#define PYCBC_DUMMY_ARG_ACTOR(TYPE,NAME)
#define PYCBC_ARG_FWD(TYPE,NAME) ,pycbc_convert_##TYPE(NAME)
#define PYCBC_ARG_NAME_AND_TYPE(TYPE,NAME) ,TYPE NAME
#define PYCBC_PARAM_NAME_AND_TYPE(TYPE,NAME) ,TYPE NAME

#define PYCBC_PARAM_P_NAME_AND_TYPE(TYPE,NAME) ,const TYPE* NAME
#define PYCBC_PARAM_P_CONVERTED(TYPE,NAME) ,NAME##_converted
#define PYCBC_PARAM_P_STORE(TYPE,NAME) \
    PyObject *NAME##_converted = pycbc_convert_##TYPE##_p(NAME);
#define PYCBC_PARAM_P_DEREF(TYPE,NAME) Py_DecRef(NAME##_converted);

#define PYCBC_STORE_LIST(TYPE,NAME) \
    PyObject *NAME##_list = pycbc_gen_list_##TYPE(NAME,NAME##_num,pycbc_convert_##TYPE);
#define PYCBC_DEREF_LIST(TYPE,NAME) Py_DecRef(NAME##_list);
#define PYCBC_GET_LIST(TYPE,NAME) ,NAME##_list

#define PARAM_FMT(TYPE,NAME) "O"
#define PARAM_P_FMT(TYPE,NAME) "O"
#define PARAM_L_FMT(TYPE,NAME) "O"
#define PARAM_CSTRN_FMT(TYPE,NAME) "O"
#define PARAM_PASSTHRU_FMT(TYPE,NAME)

#define PYCBC_LOAD_KEY_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU) PARAM(lcbcrypto_KEYTYPE,type)\
    PARAM_P(char,keyid)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)
#define PYCBC_GENERATE_IV_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)
#define PYCBC_SIGN_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU) PARAM_L(lcbcrypto_SIGV,inputs)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)
#define PYCBC_VER_SIGN_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU) PARAM_L(lcbcrypto_SIGV,inputs)\
    PARAM_CSTRN(uint8_t, subject)
#define PYCBC_V0_ENCRYPT_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU)\
    PARAM_CSTRN_TRIM_NEWLINE(const uint8_t,input) PARAM_CSTRN(const uint8_t, key) PARAM_CSTRN(const uint8_t,iv)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)
#define PYCBC_V0_DECRYPT_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU)\
    PARAM_CSTRN(const uint8_t,input) PARAM_CSTRN(const uint8_t, key) PARAM_CSTRN(const uint8_t,iv)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)

#define PYCBC_V1_ENCRYPT_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU)\
    PARAM_CSTRN_TRIM_NEWLINE(const uint8_t,input) PARAM_CSTRN(const uint8_t,iv)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)
#define PYCBC_V1_DECRYPT_TYPES(PARAM, PARAM_P, PARAM_L, PARAM_CSTRN, PARAM_CSTRN_TRIM_NEWLINE, PARAM_PASSTHRU)\
    PARAM_CSTRN(const uint8_t,input) PARAM_CSTRN(const uint8_t,iv)\
    PARAM_PASSTHRU(uint8_t**, subject) PARAM_PASSTHRU(size_t*, subject_len)

#define PYCBC_V1_GET_KID_TYPES(PARAM,                    \
                               PARAM_P,                  \
                               PARAM_L,                  \
                               PARAM_CSTRN,              \
                               PARAM_CSTRN_TRIM_NEWLINE, \
                               PARAM_PASSTHRU)

typedef const char *PYCBC_CSTR_T;
PYCBC_CSTR_T PYCBC_CSTR_T_ERRVALUE = "[VALUE NOT FOUND]";
lcb_STATUS lcb_STATUS_ERRVALUE = LCB_ERR_TEMPORARY_FAILURE;

#define PYCBC_CSTRDUP_WRAPPER(DUMMY, DUMMY2, OBJECT) \
    pycbc_cstrdup_or_default_and_exception(          \
            OBJECT, PYCBC_CSTR_T_ERRVALUE)

#define PYCBC_CSTRNDUP_WRAPPER(BUF, BUF_LEN, OBJECT) \
    pycbc_cstrndup((char **)BUF, BUF_LEN, OBJECT)

#define EXC(X) ((lcb_STATUS)X),((lcb_STATUS)PYCBC_CRYPTO_PROVIDER_KEY_SIZE_EXCEPTION),

#define PYCBC_X_COMMON_CRYPTO_METHODS(X)         \
    X(lcb_STATUS,                               \
      generic,                                   \
      generate_iv,                               \
      PYCBC_CSTRNDUP_WRAPPER,                    \
      PYCBC_GENERATE_IV_TYPES,                   \
      EXC(PYCBC_CRYPTO_EXECUTION_ERROR))         \
    X(lcb_STATUS,                               \
      generic,                                   \
      sign,                                      \
      PYCBC_CSTRNDUP_WRAPPER,                    \
      PYCBC_SIGN_TYPES,                          \
      EXC(PYCBC_CRYPTO_PROVIDER_SIGNING_FAILED)) \
    X(lcb_STATUS,                               \
      generic,                                   \
      verify_signature,                          \
      pycbc_is_true,                             \
      PYCBC_VER_SIGN_TYPES,                      \
      EXC(PYCBC_CRYPTO_ERROR))

#define PYCBC_X_V0_ONLY_CRYPTO_METHODS(X) \
    X(lcb_STATUS,                        \
      v0,                                 \
      load_key,                           \
      PYCBC_CSTRNDUP_WRAPPER,             \
      PYCBC_LOAD_KEY_TYPES,               \
      EXC(PYCBC_CRYPTO_ERROR))            \
    X(lcb_STATUS,                        \
      v0,                                 \
      encrypt,                            \
      PYCBC_CSTRNDUP_WRAPPER,             \
      PYCBC_V0_ENCRYPT_TYPES)             \
    X(lcb_STATUS,                        \
      v0,                                 \
      decrypt,                            \
      PYCBC_CSTRNDUP_WRAPPER,             \
      PYCBC_V0_DECRYPT_TYPES,             \
      EXC(PYCBC_CRYPTO_ERROR))

#define PYCBC_X_V0_CRYPTO_METHODS(X)\
PYCBC_X_V0_ONLY_CRYPTO_METHODS(X)\
PYCBC_X_COMMON_CRYPTO_METHODS(X)

#define PYCBC_X_V1_ONLY_CRYPTO_METHODS(X)        \
    X(lcb_STATUS,                               \
      v1,                                        \
      encrypt,                                   \
      PYCBC_CSTRNDUP_WRAPPER,                    \
      PYCBC_V1_ENCRYPT_TYPES,                    \
      EXC(PYCBC_CRYPTO_PROVIDER_ENCRYPT_FAILED)) \
    X(lcb_STATUS,                               \
      v1,                                        \
      decrypt,                                   \
      PYCBC_CSTRNDUP_WRAPPER,                    \
      PYCBC_V1_DECRYPT_TYPES,                    \
      EXC(PYCBC_CRYPTO_PROVIDER_DECRYPT_FAILED)) \
    X(PYCBC_CSTR_T,                              \
      V1,                                        \
      get_key_id,                                \
      PYCBC_CSTRDUP_WRAPPER,                     \
      PYCBC_V1_GET_KID_TYPES,                    \
      EXC(PYCBC_CRYPTO_ERROR))

#define PYCBC_X_V1_CRYPTO_METHODS(X)\
PYCBC_X_V1_ONLY_CRYPTO_METHODS(X)\
PYCBC_X_COMMON_CRYPTO_METHODS(X)

#define PYCBC_X_ALL_CRYPTO_FUNCTIONS(X) \
    PYCBC_X_COMMON_CRYPTO_METHODS(X)    \
    PYCBC_X_V1_ONLY_CRYPTO_METHODS(X)

#define PYCBC_SIG_METHOD(RTYPE, VERSION, METHOD, PROCESSOR, X_ARGS, ...)       \
    static RTYPE pycbc_crypto_##VERSION##_##METHOD(                            \
            lcbcrypto_PROVIDER *provider X_ARGS(PYCBC_ARG_NAME_AND_TYPE,       \
                                                PYCBC_PARAM_P_NAME_AND_TYPE,   \
                                                PYCBC_SIZED_ARRAY,             \
                                                PYCBC_ARG_CSTRN,               \
                                                PYCBC_ARG_CSTRN_TRIM_NEWLINE,  \
                                                PYCBC_ARG_PASSTHRU))           \
    {                                                                          \
        RTYPE lcb_result = RTYPE##_ERRVALUE;                                   \
        PyObject *method = !PyErr_Occurred()                                   \
                                   ? PYCBC_CRYPTO_GET_METHOD(provider, METHOD) \
                                   : NULL;                                     \
        if (method) {                                                          \
            X_ARGS(PYCBC_DUMMY_ARG_ACTOR,                                      \
                   PYCBC_PARAM_P_STORE,                                        \
                   PYCBC_STORE_LIST,                                           \
                   PYCBC_STORE_CSTRN,                                          \
                   PYCBC_STORE_CSTRN_TRIM_NEWLINE,                             \
                   PYCBC_DUMMY_ARG_ACTOR)                                      \
            const char *PYARGS_FMTSTRING = "(" X_ARGS(PARAM_FMT,               \
                                                      PARAM_P_FMT,             \
                                                      PARAM_L_FMT,             \
                                                      PARAM_CSTRN_FMT,         \
                                                      PARAM_CSTRN_FMT,         \
                                                      PARAM_PASSTHRU_FMT) ")"; \
            PyObject *args = Py_BuildValue(                                    \
                    PYARGS_FMTSTRING X_ARGS(PYCBC_ARG_FWD,                     \
                                            PYCBC_PARAM_P_CONVERTED,           \
                                            PYCBC_GET_LIST,                    \
                                            PYCBC_ARG_CSTRN_FWD,               \
                                            PYCBC_ARG_CSTRN_TRIM_NEWLINE_FWD,  \
                                            PYCBC_DUMMY_ARG_ACTOR));           \
            PyObject *result = pycbc_python_proxy(method, args, #METHOD);      \
            if (result) {                                                      \
                lcb_result = PROCESSOR(subject, subject_len, result);          \
            }                                                                  \
            Py_DecRef(result);                                                 \
            Py_DecRef(args);                                                   \
            X_ARGS(PYCBC_DUMMY_ARG_ACTOR,                                      \
                   PYCBC_PARAM_P_DEREF,                                        \
                   PYCBC_DEREF_LIST,                                           \
                   PYCBC_ARG_CSTRN_FREE,                                       \
                   PYCBC_ARG_CSTRN_FREE,                                       \
                   PYCBC_DUMMY_ARG_ACTOR)                                      \
        }                                                                      \
        return lcb_result;                                                     \
    }

pycbc_NamedCryptoProvider *pycbc_extract_named_crypto_provider(
        const lcbcrypto_PROVIDER *provider)
{
    return provider ? (pycbc_NamedCryptoProvider *)(provider->cookie) : NULL;
}

void pycbc_exc_wrap_obj(pycbc_NamedCryptoProvider *named_crypto_provider,
                        lcb_STATUS err_code)
{
    PyObject *name =
            named_crypto_provider
                    ? (named_crypto_provider->name ? named_crypto_provider->name
                                                   : Py_None)
                    : Py_None;
    PyObject* attrib_dict = PyDict_New();
    PyDict_SetItemString(attrib_dict, "alias", name);
    PYCBC_DEBUG_PYFORMAT(
            "About to raise exception from err_code %d, alias is %S",
            err_code,
            name);
    PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_LCBERR, err_code, "", attrib_dict);
    //Py_DecRef(attrib_dict);
}

#if PYCBC_CRYPTO_VERSION > 0
#    define PYCBC_CRYPTO_VVERSION v1
#    define PYCBC_CRYPTO_METHODS(X) PYCBC_X_V1_CRYPTO_METHODS(X)
#else
#define PYCBC_CRYPTO_VVERSION v0
#define PYCBC_CRYPTO_METHODS(X) PYCBC_X_V0_CRYPTO_METHODS(X)
#endif

#define PYCBC_WRAP_CRYPTO_EXCEPTION(provider, code, ...) \
    pycbc_exc_wrap_obj(provider, code)
#define PYCBC_CRYPTO_EXC_WRAPPER(                                             \
        RTYPE, VERSION, METHOD, PROCESSOR, X_ARGS, ...)                       \
    static RTYPE pycbc_crypto_exc_wrap_##VERSION##_##METHOD(                  \
            lcbcrypto_PROVIDER *provider X_ARGS(PYCBC_ARG_NAME_AND_TYPE,      \
                                                PYCBC_PARAM_P_NAME_AND_TYPE,  \
                                                PYCBC_SIZED_ARRAY,            \
                                                PYCBC_ARG_CSTRN,              \
                                                PYCBC_ARG_CSTRN_TRIM_NEWLINE, \
                                                PYCBC_ARG_PASSTHRU))          \
    {                                                                         \
        pycbc_NamedCryptoProvider *named_crypto_provider =                    \
                pycbc_extract_named_crypto_provider(provider);                \
        lcbcrypto_PROVIDER *orig_lcb_provider =                               \
                named_crypto_provider->orig_py_provider->lcb_provider;        \
        RTYPE lcb_result = RTYPE##_ERRVALUE;                                  \
        if (PyErr_Occurred()) {                                               \
            goto FAIL;                                                        \
        }                                                                     \
        if (named_crypto_provider) {                                          \
            lcb_result = orig_lcb_provider->v.PYCBC_CRYPTO_VVERSION.METHOD(   \
                    orig_lcb_provider X_ARGS(PYCBC_ARG_FWD,                   \
                                             PYCBC_ARG_PASSTHRU_FWD,          \
                                             PYCBC_SIZED_ARRAY_FWD,           \
                                             PYCBC_ARG_CSTRN_FWD_PASSTHRU,    \
                                             PYCBC_ARG_CSTRN_FWD_PASSTHRU,    \
                                             PYCBC_ARG_PASSTHRU_FWD));        \
        }                                                                     \
        if (lcb_result == RTYPE##_ERRVALUE) {                                 \
            PYCBC_WRAP_CRYPTO_EXCEPTION(named_crypto_provider,                \
                                        __VA_ARGS__ lcb_STATUS_ERRVALUE);    \
        }                                                                     \
    FAIL:                                                                     \
        return lcb_result;                                                    \
    }

PyObject *pycbc_va_list_v(lcb_STATUS sentinel, va_list errs)
{
    PyObject *err_list = PyList_New(0);
    do {
        PyObject *py_enum;
        lcb_STATUS val = va_arg(errs, lcb_STATUS);
        if (val == sentinel)
            break;
        py_enum = PyLong_FromLong(val);
        PyList_Append(err_list, py_enum);
        PYCBC_DECREF(py_enum);
    } while (1);
    return err_list;
}

void pycbc_set_var_items_dict(PyObject *dict,
                              const char *key,
                              lcb_STATUS sentinel,
                              ...)
{
    PyObject *err_list;
    va_list errs;
    va_start(errs, sentinel);
    err_list = pycbc_va_list_v(sentinel, errs);
    va_end(errs);
    PyDict_SetItemString(dict, key, err_list);
    PYCBC_DECREF(err_list);
}

PyObject *pycbc_gen_crypto_exception_map(void)
{
    PyObject *exception_map = PyDict_New();
#define PYCBC_CRYPTO_EXC_MAP_WRAPPER(                                      \
        RTYPE, VERSION, METHOD, PROCESSOR, X_ARGS, ...)                    \
    pycbc_set_var_items_dict(exception_map,                                \
                             #METHOD,                                      \
                             (lcb_STATUS)PYCBC_CRYPTO_PROVIDER_ERROR_MAX, \
                             __VA_ARGS__(lcb_STATUS)                      \
                                     PYCBC_CRYPTO_PROVIDER_ERROR_MAX);
    PYCBC_CRYPTO_METHODS(PYCBC_CRYPTO_EXC_MAP_WRAPPER)

    return exception_map;
}

#ifdef PYCBC_GEN_METHODS
PYCBC_X_ALL_CRYPTO_FUNCTIONS(PYCBC_SIG_METHOD)
PYCBC_CRYPTO_METHODS(PYCBC_CRYPTO_EXC_WRAPPER)
#else

static lcb_STATUS pycbc_crypto_generic_generate_iv(lcbcrypto_PROVIDER *provider, uint8_t **subject,
                                                    size_t *subject_len) {
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "generate_iv") : ((void *) 0);
    if (method) {
        const char *PYARGS_FMTSTRING = "(" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING);
        PyObject *result = pycbc_python_proxy(method, args, "generate_iv");
        if (result) {
            lcb_result = pycbc_cstrndup((char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
    }
    return lcb_result;
}

static lcb_STATUS
pycbc_crypto_generic_sign(lcbcrypto_PROVIDER *provider, const lcbcrypto_SIGV *inputs, size_t inputs_num,
                          uint8_t **subject, size_t *subject_len) {
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "sign") : ((void *) 0);
    if (method) {
        PyObject *inputs_list = pycbc_gen_list_lcbcrypto_SIGV(inputs, inputs_num, pycbc_convert_lcbcrypto_SIGV);
        const char *PYARGS_FMTSTRING = "(" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, inputs_list);
        PyObject *result = pycbc_python_proxy(method, args, "sign");
        if (result) {
            lcb_result = pycbc_cstrndup((char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(inputs_list);
    }
    return lcb_result;
}

static lcb_STATUS
pycbc_crypto_generic_verify_signature(lcbcrypto_PROVIDER *provider, const lcbcrypto_SIGV *inputs, size_t inputs_num,
                                      uint8_t *subject, size_t subject_len) {
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "verify_signature") : ((void *) 0);
    if (method) {
        PyObject *inputs_list = pycbc_gen_list_lcbcrypto_SIGV(inputs, inputs_num, pycbc_convert_lcbcrypto_SIGV);
        PyObject *subject_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(subject, subject_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, inputs_list, subject_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "verify_signature");
        if (result) { lcb_result = pycbc_is_true(subject, subject_len, result); }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(inputs_list);
        Py_DecRef(subject_cstrn);
    }
    return lcb_result;
}


static lcb_STATUS
pycbc_crypto_v1_encrypt(lcbcrypto_PROVIDER *provider, const uint8_t *input, size_t input_len, const uint8_t *iv,
                        size_t iv_len, uint8_t **subject, size_t *subject_len) {
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "encrypt") : ((void *) 0);
    if (method) {
        PyObject *input_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(input, (input_len > 0) ? (input_len - 1) : 0));
        PyObject *iv_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(iv, iv_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, input_cstrn, iv_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "encrypt");
        if (result) {
            lcb_result = pycbc_cstrndup((char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(input_cstrn);
        Py_DecRef(iv_cstrn);
    }
    return lcb_result;
}

static lcb_STATUS
pycbc_crypto_v1_decrypt(lcbcrypto_PROVIDER *provider, const uint8_t *input, size_t input_len, const uint8_t *iv,
                        size_t iv_len, uint8_t **subject, size_t *subject_len) {
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "decrypt") : ((void *) 0);
    if (method) {
        PyObject *input_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(input, input_len));
        PyObject *iv_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(iv, iv_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, input_cstrn, iv_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "decrypt");
        if (result) {
            lcb_result = pycbc_cstrndup((char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(input_cstrn);
        Py_DecRef(iv_cstrn);
    }
    return lcb_result;
}

static PYCBC_CSTR_T pycbc_crypto_V1_get_key_id(lcbcrypto_PROVIDER *provider)
{
    PYCBC_CSTR_T lcb_result = PYCBC_CSTR_T_ERRVALUE;
    PyObject *method = !PyErr_Occurred()
                               ? pycbc_retrieve_method(provider, "get_key_id")
                               : ((void *)0);
    if (method) {
        const char *PYARGS_FMTSTRING =
                "("
                ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING);
        PyObject *result = pycbc_python_proxy(method, args, "get_key_id");
        if (result) {
            lcb_result = pycbc_cstrdup_or_default_and_exception(
                    result, PYCBC_CSTR_T_ERRVALUE);
        }
        Py_DecRef(result);
        Py_DecRef(args);
    }
    return lcb_result;
}

static lcb_STATUS pycbc_crypto_exc_wrap_v1_encrypt(
        lcbcrypto_PROVIDER *provider,
        const uint8_t *input,
        size_t input_len,
        const uint8_t *iv,
        size_t iv_len,
        uint8_t **subject,
        size_t *subject_len)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            pycbc_extract_named_crypto_provider(provider);
    lcbcrypto_PROVIDER *orig_lcb_provider =
            named_crypto_provider->orig_py_provider->lcb_provider;
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    if (PyErr_Occurred()) {
        goto FAIL;
    }
    if (named_crypto_provider) {
        lcb_result = orig_lcb_provider->v.v1.encrypt(orig_lcb_provider,
                                                     input,
                                                     input_len,
                                                     iv,
                                                     iv_len,
                                                     subject,
                                                     subject_len);
    }
    if (lcb_result == lcb_STATUS_ERRVALUE) {
        pycbc_exc_wrap_obj(named_crypto_provider,
                           ((lcb_STATUS)PYCBC_CRYPTO_PROVIDER_ENCRYPT_FAILED));
    }
FAIL:
    return lcb_result;
}

static lcb_STATUS pycbc_crypto_exc_wrap_v1_decrypt(
        lcbcrypto_PROVIDER *provider,
        const uint8_t *input,
        size_t input_len,
        const uint8_t *iv,
        size_t iv_len,
        uint8_t **subject,
        size_t *subject_len)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            pycbc_extract_named_crypto_provider(provider);
    lcbcrypto_PROVIDER *orig_lcb_provider =
            named_crypto_provider->orig_py_provider->lcb_provider;
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    if (PyErr_Occurred()) {
        goto FAIL;
    }
    if (named_crypto_provider) {
        lcb_result = orig_lcb_provider->v.v1.decrypt(orig_lcb_provider,
                                                     input,
                                                     input_len,
                                                     iv,
                                                     iv_len,
                                                     subject,
                                                     subject_len);
    }
    if (lcb_result == lcb_STATUS_ERRVALUE) {
        pycbc_exc_wrap_obj(named_crypto_provider,
                           ((lcb_STATUS)PYCBC_CRYPTO_PROVIDER_DECRYPT_FAILED));
    }
FAIL:
    return lcb_result;
}

static PYCBC_CSTR_T pycbc_crypto_exc_wrap_V1_get_key_id(
        lcbcrypto_PROVIDER *provider)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            pycbc_extract_named_crypto_provider(provider);
    lcbcrypto_PROVIDER *orig_lcb_provider =
            named_crypto_provider->orig_py_provider->lcb_provider;
    PYCBC_CSTR_T lcb_result = PYCBC_CSTR_T_ERRVALUE;
    if (PyErr_Occurred()) {
        goto FAIL;
    }
    if (named_crypto_provider) {
        lcb_result = orig_lcb_provider->v.v1.get_key_id(orig_lcb_provider);
    }
    if (lcb_result == PYCBC_CSTR_T_ERRVALUE) {
        pycbc_exc_wrap_obj(named_crypto_provider,
                           ((lcb_STATUS)PYCBC_CRYPTO_ERROR));
    }
FAIL:
    return lcb_result;
}

static lcb_STATUS pycbc_crypto_exc_wrap_generic_generate_iv(
        lcbcrypto_PROVIDER *provider, uint8_t **subject, size_t *subject_len)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            pycbc_extract_named_crypto_provider(provider);
    lcbcrypto_PROVIDER *orig_lcb_provider =
            named_crypto_provider->orig_py_provider->lcb_provider;
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    if (PyErr_Occurred()) {
        goto FAIL;
    }
    if (named_crypto_provider) {
        lcb_result = orig_lcb_provider->v.v1.generate_iv(
                orig_lcb_provider, subject, subject_len);
    }
    if (lcb_result == lcb_STATUS_ERRVALUE) {
        pycbc_exc_wrap_obj(named_crypto_provider,
                           ((lcb_STATUS)PYCBC_CRYPTO_EXECUTION_ERROR));
    }
FAIL:
    return lcb_result;
}

static lcb_STATUS pycbc_crypto_exc_wrap_generic_sign(
        lcbcrypto_PROVIDER *provider,
        const lcbcrypto_SIGV *inputs,
        size_t inputs_num,
        uint8_t **subject,
        size_t *subject_len)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            pycbc_extract_named_crypto_provider(provider);
    lcbcrypto_PROVIDER *orig_lcb_provider =
            named_crypto_provider->orig_py_provider->lcb_provider;
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    if (PyErr_Occurred()) {
        goto FAIL;
    }
    if (named_crypto_provider) {
        lcb_result = orig_lcb_provider->v.v1.sign(
                orig_lcb_provider, inputs, inputs_num, subject, subject_len);
    }
    if (lcb_result == lcb_STATUS_ERRVALUE) {
        pycbc_exc_wrap_obj(named_crypto_provider,
                           ((lcb_STATUS)PYCBC_CRYPTO_PROVIDER_SIGNING_FAILED));
    }
FAIL:
    return lcb_result;
}

static lcb_STATUS pycbc_crypto_exc_wrap_generic_verify_signature(
        lcbcrypto_PROVIDER *provider,
        const lcbcrypto_SIGV *inputs,
        size_t inputs_num,
        uint8_t *subject,
        size_t subject_len)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            pycbc_extract_named_crypto_provider(provider);
    lcbcrypto_PROVIDER *orig_lcb_provider =
            named_crypto_provider->orig_py_provider->lcb_provider;
    lcb_STATUS lcb_result = lcb_STATUS_ERRVALUE;
    if (PyErr_Occurred()) {
        goto FAIL;
    }
    if (named_crypto_provider) {
        lcb_result = orig_lcb_provider->v.v1.verify_signature(
                orig_lcb_provider, inputs, inputs_num, subject, subject_len);
    }
    if (lcb_result == lcb_STATUS_ERRVALUE) {
        pycbc_exc_wrap_obj(named_crypto_provider,
                           ((lcb_STATUS)PYCBC_CRYPTO_ERROR));
    }
FAIL:
    return lcb_result;
}
#endif

#ifdef PYCBC_GEN_PYTHON_STUBS
#define PYCBC_PY_PARAM(TYPE,NAME) "," #NAME
#define PYCBC_PY_PARAM_L(TYPE,NAME) "," #NAME
#define PYCBC_PY_PARAM_CSTRN(TYPE,NAME) "," #NAME
#define PYCBC_PY_PASSTHRU(TYPE,NAME) ""

#define PYCBC_PY_GEN(VERSION,METHOD, PROCESSOR, X_ARGS)\
printf("\n    @abc.abstractmethod\n    def %s(self%s):\n        pass\n",#METHOD,\
       X_ARGS(PYCBC_PY_PARAM, PYCBC_PY_PARAM, PYCBC_PY_PARAM_L,\
              PYCBC_PY_PARAM_CSTRN, PYCBC_PY_PARAM_CSTRN, PYCBC_PY_PASSTHRU));

void pycbc_generate_stubs()
{
    printf("\nimport abc\nclass PythonCryptoProvider(CryptoProvider):\n    __metaclass__ = abc.ABCMeta\n");
    PYCBC_CRYPTO_METHODS(PYCBC_PY_GEN);
}
#endif

void pycbc_release_bytes(lcbcrypto_PROVIDER *provider, void *bytes)
{
    PYCBC_FREE(bytes);
};

#define PYCBC_POPULATE_STRUCT_REAL(                          \
        POSTFIX, RTYPE, VERSION, METHOD, DUMMY, X_ARGS, ...) \
    self->lcb_provider->v.PYCBC_CRYPTO_VVERSION.METHOD =     \
            pycbc_crypto_##POSTFIX##VERSION##_##METHOD;

#define PYCBC_POPULATE_STRUCT(                                              \
        POSTFIX, RTYPE, VERSION, METHOD, DUMMY, X_ARGS, ...)                \
    method = PyObject_HasAttrString((PyObject *)self, #METHOD)              \
                     ? PyObject_GetAttrString((PyObject *)self, #METHOD)    \
                     : NULL;                                                \
    if (method) {                                                           \
        PYCBC_POPULATE_STRUCT_REAL(                                         \
                POSTFIX, RTYPE, VERSION, METHOD, DUMMY, X_ARGS __VA_ARGS__) \
        Py_DecRef(method);                                                  \
    } else if (!self->lcb_provider->v.PYCBC_CRYPTO_VVERSION.METHOD) {       \
        pycbc_report_method_exception(                                      \
                LCB_ERR_INVALID_ARGUMENT, "Missing method %s", #METHOD);    \
    }

void pycbc_named_crypto_provider_destructor(lcbcrypto_PROVIDER *provider)
{
    pycbc_NamedCryptoProvider *named_crypto_provider =
            provider ? provider->cookie : NULL;
    PYCBC_XDECREF(named_crypto_provider);
    PYCBC_FREE(provider);
}

static int NamedCryptoProvider__init(pycbc_NamedCryptoProvider *self,
                                     PyObject *args,
                                     PyObject *kwargs)
{
    int result = -1;
    PyObject *name = kwargs ? PyDict_GetItemString(kwargs, "name") : NULL;
    PyObject *provider =
            kwargs ? PyDict_GetItemString(kwargs, "provider") : NULL;

    if (!provider || !name) {
        PYCBC_EXCTHROW_ARGS()
        goto END;
    }
    self->name = name;
    PYCBC_DEBUG_PYFORMAT(
            "Registering provider %S as %S at %p", provider, name, self);
    Py_XINCREF(name);
    self->orig_py_provider = (pycbc_CryptoProvider *)provider;
    Py_XINCREF(provider);
    self->lcb_provider = PYCBC_CALLOC_TYPED(1, lcbcrypto_PROVIDER);
    // lcbcrypto_ref(self->lcb_provider);

    PYCBC_INCREF(self);
    self->lcb_provider->cookie = self;
    self->lcb_provider->destructor = pycbc_named_crypto_provider_destructor;
    self->lcb_provider->version = PYCBC_CRYPTO_VERSION;
    self->lcb_provider->v.PYCBC_CRYPTO_VVERSION.release_bytes =
            self->orig_py_provider->lcb_provider->v.PYCBC_CRYPTO_VVERSION
                    .release_bytes;
#define PYCBC_POPULATE_STRUCT_EXC_WRAP(             \
        RTYPE, VERSION, METHOD, DUMMY, X_ARGS, ...) \
    PYCBC_POPULATE_STRUCT_REAL(                     \
            exc_wrap_, RTYPE, VERSION, METHOD, DUMMY, X_ARGS, ...)

    PYCBC_CRYPTO_METHODS(PYCBC_POPULATE_STRUCT_EXC_WRAP);
    result = 0;
END:
    return result;
}

static void NamedCryptoProvider_dtor(pycbc_NamedCryptoProvider *self)
{
    /*
    if (self->lcb_provider) {
        lcbcrypto_unref(self->lcb_provider);
        self->lcb_provider = NULL;
    }*/

    // PYCBC_XDECREF(self->orig_py_provider);
    // PYCBC_XDECREF(self->name);

    Py_TYPE(self)->tp_free((PyObject *)self);
}
void pycbc_crypto_provider_destructor(lcbcrypto_PROVIDER *provider)
{
    pycbc_CryptoProvider *crypto_provider = provider ? provider->cookie : NULL;
    PYCBC_XDECREF(crypto_provider);
    PYCBC_FREE(provider);
}

static int CryptoProvider__init(pycbc_CryptoProvider *self,
                                PyObject *args,
                                PyObject *kwargs)
{
    // create instance here
    PyObject *provider =
            kwargs ? PyDict_GetItemString(kwargs, "provider") : NULL;
    if (provider) {
        if (PyObject_IsInstance(provider, (PyObject *)&PyLong_Type)) {
            self->lcb_provider = PyLong_AsVoidPtr(provider);

            if (PyErr_Occurred()) {
                PYCBC_EXCTHROW_ARGS();
                return -1;
            }
        }

    } else {
        PyObject *method = NULL;
        self->lcb_provider = calloc(1, sizeof(lcbcrypto_PROVIDER));
        PYCBC_INCREF(self);
        self->lcb_provider->cookie = self;
        self->lcb_provider->destructor = pycbc_crypto_provider_destructor;
        self->lcb_provider->version = PYCBC_CRYPTO_VERSION;
        self->lcb_provider->v.PYCBC_CRYPTO_VVERSION.release_bytes =
                pycbc_release_bytes;
#define PYCBC_POPULATE_STRUCT_VANILLA(              \
        RTYPE, VERSION, METHOD, DUMMY, X_ARGS, ...) \
    PYCBC_POPULATE_STRUCT(, RTYPE, VERSION, METHOD, DUMMY, X_ARGS, ...)
        PYCBC_CRYPTO_METHODS(PYCBC_POPULATE_STRUCT_VANILLA);
#undef PYCBC_POPULATE_STRUCT
    }
    if (PyErr_Occurred() || !self->lcb_provider) {
        return -1;
    }
#ifdef PYCBC_GEN_PYTHON_STUBS
    pycbc_generate_stubs();
#endif
    return 0;
}

static void
CryptoProvider_dtor(pycbc_CryptoProvider *self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}

#define PYCBC_DUMMY_METHOD_USE(RTYPE, VERSION, METHOD, PROCESSOR, X_ARGS, ...) \
    (void)pycbc_crypto_##VERSION##_##METHOD;

static void pycbc_CryptoProvideType_extra_init(PyObject **ptr)
{
    PYCBC_X_ALL_CRYPTO_FUNCTIONS(PYCBC_DUMMY_METHOD_USE)
}

#define PYCBC_TYPE_INIT(NAME, DESCRIPTION, ...)                        \
    static PyMethodDef NAME##_TABLE_methods[] = {{NULL, NULL, 0, NULL} \
                                                                       \
    };                                                                 \
    int pycbc_##NAME##Type_init(PyObject **ptr)                        \
    {                                                                  \
        PyTypeObject *p = &pycbc_##NAME##Type;                         \
        *ptr = (PyObject *)p;                                          \
                                                                       \
        if (p->tp_name) {                                              \
            return 0;                                                  \
        }                                                              \
                                                                       \
        p->tp_name = #NAME;                                            \
        p->tp_new = PyType_GenericNew;                                 \
        p->tp_init = (initproc)NAME##__init;                           \
        p->tp_dealloc = (destructor)NAME##_dtor;                       \
                                                                       \
        p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;        \
        p->tp_doc = PyDoc_STR(DESCRIPTION);                            \
        p->tp_basicsize = sizeof(pycbc_##NAME);                        \
        p->tp_methods = NAME##_TABLE_methods;                          \
        __VA_ARGS__;                                                   \
        return PyType_Ready(p);                                        \
    }

#undef PYCBC_DUMMY_METHOD_USE

PYCBC_CRYPTO_TYPES(PYCBC_TYPE_INIT)
