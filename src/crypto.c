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

static PyTypeObject CryptoProviderType = {
    PYCBC_POBJ_HEAD_INIT(NULL)
    0
};

lcb_error_t pycbc_cstrndup(const char **key,
                           size_t *key_len,
                           PyObject *result)
{
    const char *data = NULL;
    lcb_error_t lcb_result = LCB_EINTERNAL;

    data = PYCBC_CSTRN(result, key_len);;
    if (data) {
        lcb_result = LCB_SUCCESS;
        PYCBC_DEBUG_LOG("Got string from %p: %.*s", result, (int)key_len, data);
        *key = calloc(1, *key_len);
        memcpy((void *)*key, (void *)data, *key_len);
        PYCBC_DEBUG_LOG("Copied string from %p: %.*s",
                        result,
                        (int)key_len,
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
                       LCB_GENERIC_TMPERR,
                       "CryptoProviderMissingPublicKeyException")
    }
    return buffer_data ? buffer_data : fallback;
}

lcb_error_t pycbc_is_true(uint8_t *key, const size_t key_len, PyObject *result) {
    return (result && PyObject_IsTrue(result) && !PyErr_Occurred())?
           LCB_SUCCESS:
           LCB_EINTERNAL;
}

#define PYCBC_SIZE(X) (sizeof(X)/sizeof(char))

void pycbc_report_method_exception(lcb_error_t errflags, const char* fmt, ...) {
    char buffer[1000];
    va_list args;
    va_start(args,fmt);
    vsnprintf(buffer, PYCBC_SIZE(buffer), fmt,args);
    va_end(args);
    PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, errflags, buffer);
}

static PyObject* pycbc_retrieve_method(lcbcrypto_PROVIDER* provider, const char* method_name){
    PyObject* method = PyObject_GetAttrString((PyObject *) provider->cookie, method_name);
    if (!method || !PyObject_IsTrue(method)) {
        pycbc_report_method_exception(LCB_GENERIC_TMPERR, "Method %s does not exist", method_name);
        return NULL;
    }
    return method;
}
#define PYCBC_CRYPTO_GET_METHOD(PROVIDER,METHOD) pycbc_retrieve_method(PROVIDER,#METHOD)

PyObject* pycbc_python_proxy(PyObject *method, PyObject *args, const char* method_name) {
    PyObject* result = NULL;
    pycbc_assert(method && PyObject_IsTrue(method));
    if (PyErr_Occurred() || !args)
    {
        return NULL;
    };
    result = PyObject_CallObject(method, args);
    if (!result || PyErr_Occurred()) {
        PyErr_Print();
        pycbc_report_method_exception(LCB_EINTERNAL, "Problem calling method %s",
                                      method_name);
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
    return PyString_FromStringAndSize(buf.data, buf.len);
#endif
}

PyObject *pycbc_convert_lcbcrypto_SIGV(const lcbcrypto_SIGV *sigv) {
    const pycbc_crypto_buf buf = {sigv->data, sigv->len};
    return pycbc_convert_uint8_t(buf);
}

PyObject* pycbc_convert_lcbcrypto_KEYTYPE(const lcbcrypto_KEYTYPE type)
{
    return PyLong_FromLong(type);
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

#define PYCBC_ARG_CSTRN_TRIM_NEWLINE(TYPE,NAME)  , TYPE* NAME, size_t NAME##_len
#define PYCBC_ARG_CSTRN_TRIM_NEWLINE_FWD(TYPE,NAME)  ,NAME##_cstrn
#define PYCBC_STORE_CSTRN_TRIM_NEWLINE(TYPE,NAME) \
    PyObject *NAME##_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(NAME,(NAME##_len>0)?(NAME##_len-1):0));

#define PYCBC_ARG_PASSTHRU(TYPE,NAME) ,TYPE NAME
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
lcb_error_t lcb_error_t_ERRVALUE = LCB_GENERIC_TMPERR;

#define PYCBC_CSTRDUP_WRAPPER(DUMMY, DUMMY2, OBJECT) \
    pycbc_cstrdup_or_default_and_exception(          \
            OBJECT, PYCBC_CSTR_T_ERRVALUE)

#define PYCBC_CSTRNDUP_WRAPPER(BUF, BUF_LEN, OBJECT) \
    pycbc_cstrndup((const char **)BUF, BUF_LEN, OBJECT)

#define PYCBC_X_COMMON_CRYPTO_METHODS(X)                                    \
    X(lcb_error_t,                                                          \
      generic,                                                              \
      generate_iv,                                                          \
      PYCBC_CSTRNDUP_WRAPPER,                                               \
      PYCBC_GENERATE_IV_TYPES)                                              \
    X(lcb_error_t, generic, sign, PYCBC_CSTRNDUP_WRAPPER, PYCBC_SIGN_TYPES) \
    X(lcb_error_t,                                                          \
      generic,                                                              \
      verify_signature,                                                     \
      pycbc_is_true,                                                        \
      PYCBC_VER_SIGN_TYPES)

#define PYCBC_X_V0_ONLY_CRYPTO_METHODS(X)                                      \
    X(lcb_error_t, v0, load_key, PYCBC_CSTRNDUP_WRAPPER, PYCBC_LOAD_KEY_TYPES) \
    X(lcb_error_t,                                                             \
      v0,                                                                      \
      encrypt,                                                                 \
      PYCBC_CSTRNDUP_WRAPPER,                                                  \
      PYCBC_V0_ENCRYPT_TYPES)                                                  \
    X(lcb_error_t, v0, decrypt, PYCBC_CSTRNDUP_WRAPPER, PYCBC_V0_DECRYPT_TYPES)

#define PYCBC_X_V0_CRYPTO_METHODS(X)\
PYCBC_X_V0_ONLY_CRYPTO_METHODS(X)\
PYCBC_X_COMMON_CRYPTO_METHODS(X)

#define PYCBC_X_V1_ONLY_CRYPTO_METHODS(X) \
    X(lcb_error_t,                        \
      v1,                                 \
      encrypt,                            \
      PYCBC_CSTRNDUP_WRAPPER,             \
      PYCBC_V1_ENCRYPT_TYPES)             \
    X(lcb_error_t,                        \
      v1,                                 \
      decrypt,                            \
      PYCBC_CSTRNDUP_WRAPPER,             \
      PYCBC_V1_DECRYPT_TYPES)             \
    X(PYCBC_CSTR_T,                       \
      V1,                                 \
      get_key_id,                         \
      PYCBC_CSTRDUP_WRAPPER,              \
      PYCBC_V1_GET_KID_TYPES)

#define PYCBC_X_V1_CRYPTO_METHODS(X)\
PYCBC_X_V1_ONLY_CRYPTO_METHODS(X)\
PYCBC_X_COMMON_CRYPTO_METHODS(X)

#define PYCBC_X_ALL_CRYPTO_FUNCTIONS(X) \
    PYCBC_X_COMMON_CRYPTO_METHODS(X)    \
    PYCBC_X_V0_ONLY_CRYPTO_METHODS(X)   \
    PYCBC_X_V1_ONLY_CRYPTO_METHODS(X)

#define PYCBC_SIG_METHOD(RTYPE, VERSION, METHOD, PROCESSOR, X_ARGS)            \
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

#if PYCBC_CRYPTO_VERSION == 1
#define PYCBC_CRYPTO_VVERSION v1
#define PYCBC_CRYPTO_METHODS(X) PYCBC_X_V1_CRYPTO_METHODS(X)
#else
#define PYCBC_CRYPTO_VVERSION v0
#define PYCBC_CRYPTO_METHODS(X) PYCBC_X_V0_CRYPTO_METHODS(X)
#endif

#ifdef PYCBC_GEN_METHODS
PYCBC_X_ALL_CRYPTO_FUNCTIONS(PYCBC_SIG_METHOD)
#else

static lcb_error_t pycbc_crypto_generic_generate_iv(lcbcrypto_PROVIDER *provider, uint8_t **subject,
                                                    size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "generate_iv") : ((void *) 0);
    if (method) {
        const char *PYARGS_FMTSTRING = "(" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING);
        PyObject *result = pycbc_python_proxy(method, args, "generate_iv");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
    }
    return lcb_result;
}

static lcb_error_t
pycbc_crypto_generic_sign(lcbcrypto_PROVIDER *provider, const lcbcrypto_SIGV *inputs, size_t inputs_num,
                          uint8_t **subject, size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "sign") : ((void *) 0);
    if (method) {
        PyObject *inputs_list = pycbc_gen_list_lcbcrypto_SIGV(inputs, inputs_num, pycbc_convert_lcbcrypto_SIGV);
        const char *PYARGS_FMTSTRING = "(" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, inputs_list);
        PyObject *result = pycbc_python_proxy(method, args, "sign");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(inputs_list);
    }
    return lcb_result;
}

static lcb_error_t
pycbc_crypto_generic_verify_signature(lcbcrypto_PROVIDER *provider, const lcbcrypto_SIGV *inputs, size_t inputs_num,
                                      uint8_t *subject, size_t subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
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

static lcb_error_t
pycbc_crypto_v0_load_key(lcbcrypto_PROVIDER *provider, lcbcrypto_KEYTYPE type, const char *keyid, uint8_t **subject,
                         size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "load_key") : ((void *) 0);
    if (method) {
        PyObject *keyid_converted = pycbc_convert_char_p(keyid);
        const char *PYARGS_FMTSTRING = "(" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, pycbc_convert_lcbcrypto_KEYTYPE(type), keyid_converted);
        PyObject *result = pycbc_python_proxy(method, args, "load_key");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(keyid_converted);
    }
    return lcb_result;
}

static lcb_error_t
pycbc_crypto_v0_encrypt(lcbcrypto_PROVIDER *provider, const uint8_t *input, size_t input_len, const uint8_t *key,
                        size_t key_len, const uint8_t *iv, size_t iv_len, uint8_t **subject, size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "encrypt") : ((void *) 0);
    if (method) {
        PyObject *input_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(input, (input_len > 0) ? (input_len - 1) : 0));
        PyObject *key_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(key, key_len));
        PyObject *iv_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(iv, iv_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, input_cstrn, key_cstrn, iv_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "encrypt");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(input_cstrn);
        Py_DecRef(key_cstrn);
        Py_DecRef(iv_cstrn);
    }
    return lcb_result;
}

static lcb_error_t
pycbc_crypto_v0_decrypt(lcbcrypto_PROVIDER *provider, const uint8_t *input, size_t input_len, const uint8_t *key,
                        size_t key_len, const uint8_t *iv, size_t iv_len, uint8_t **subject, size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "decrypt") : ((void *) 0);
    if (method) {
        PyObject *input_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(input, input_len));
        PyObject *key_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(key, key_len));
        PyObject *iv_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(iv, iv_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, input_cstrn, key_cstrn, iv_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "decrypt");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(input_cstrn);
        Py_DecRef(key_cstrn);
        Py_DecRef(iv_cstrn);
    }
    return lcb_result;
}

static lcb_error_t
pycbc_crypto_v1_encrypt(lcbcrypto_PROVIDER *provider, const uint8_t *input, size_t input_len, const uint8_t *iv,
                        size_t iv_len, uint8_t **subject, size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "encrypt") : ((void *) 0);
    if (method) {
        PyObject *input_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(input, (input_len > 0) ? (input_len - 1) : 0));
        PyObject *iv_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(iv, iv_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, input_cstrn, iv_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "encrypt");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
        }
        Py_DecRef(result);
        Py_DecRef(args);
        Py_DecRef(input_cstrn);
        Py_DecRef(iv_cstrn);
    }
    return lcb_result;
}

static lcb_error_t
pycbc_crypto_v1_decrypt(lcbcrypto_PROVIDER *provider, const uint8_t *input, size_t input_len, const uint8_t *iv,
                        size_t iv_len, uint8_t **subject, size_t *subject_len) {
    lcb_error_t lcb_result = lcb_error_t_ERRVALUE;
    PyObject *method = !PyErr_Occurred() ? pycbc_retrieve_method(provider, "decrypt") : ((void *) 0);
    if (method) {
        PyObject *input_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(input, input_len));
        PyObject *iv_cstrn = pycbc_convert_uint8_t(pycbc_gen_buf(iv, iv_len));
        const char *PYARGS_FMTSTRING = "(" "O" "O" ")";
        PyObject *args = Py_BuildValue(PYARGS_FMTSTRING, input_cstrn, iv_cstrn);
        PyObject *result = pycbc_python_proxy(method, args, "decrypt");
        if (result) {
            lcb_result =
                    pycbc_cstrndup((const char **)subject, subject_len, result);
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
#endif

static int
CryptoProvider___setattr__(PyObject *self, PyObject *attr_name, PyObject *v) {
    int result= PyObject_GenericSetAttr((PyObject*)self, attr_name, v);
    size_t name_n;
    const char* name = PYCBC_CSTRN(attr_name, &name_n);
    if (result || PyErr_Occurred() || !v || !attr_name || !PyObject_IsTrue(v) ||
        !PyObject_IsInstance(v, (PyObject*)&PyMethod_Type)) {
        return result;
    }
#define PYCBC_INSERT_METHOD(RTYPE, VERSION, METHOD, DUMMY, X_ARGS)          \
    if (name_n == PYCBC_SIZE(#METHOD) && !strncmp(name, #METHOD, name_n)) { \
        ((pycbc_CryptoProvider *)self)                                      \
                ->provider->v.PYCBC_CRYPTO_VVERSION.METHOD =                \
                pycbc_crypto_##VERSION##_##METHOD;                          \
        return result;                                                      \
    }

    PYCBC_CRYPTO_METHODS(PYCBC_INSERT_METHOD);
    return result;
}

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

static PyMethodDef CryptoProvider_TABLE_methods[] = {
        { NULL, NULL, 0, NULL }

};

void pycbc_crypto_provider_destructor(lcbcrypto_PROVIDER *provider) {
    PYCBC_FREE(provider);
}
void pycbc_release_bytes(lcbcrypto_PROVIDER *provider, void *bytes)
{
    PYCBC_FREE(bytes);
};

static int
CryptoProvider__init(pycbc_CryptoProvider *self,
                       PyObject *args, PyObject *kwargs)
{
    // create instance here
    PyObject* provider = kwargs?PyDict_GetItemString(kwargs,"provider"):NULL;
    self->provider = NULL;
    if (provider) {
        self->provider = PyLong_AsVoidPtr(provider);
        if (PyErr_Occurred())
        {
            PYCBC_EXCTHROW_ARGS();
            return -1;
        }
    } else{
        self->provider = calloc(1, sizeof(lcbcrypto_PROVIDER));
        self->provider->destructor=pycbc_crypto_provider_destructor;
        self->provider->version = PYCBC_CRYPTO_VERSION;
    }
    {
        PyObject *method = NULL;
#define PYCBC_POPULATE_STRUCT(RTYPE, VERSION, METHOD, DUMMY, X_ARGS)     \
    method = PyObject_HasAttrString((PyObject *)self, #METHOD)           \
                     ? PyObject_GetAttrString((PyObject *)self, #METHOD) \
                     : NULL;                                             \
    if (method) {                                                        \
        self->provider->v.PYCBC_CRYPTO_VVERSION.METHOD =                 \
                pycbc_crypto_##VERSION##_##METHOD;                       \
        Py_DecRef(method);                                               \
    } else if (!self->provider->v.PYCBC_CRYPTO_VVERSION.METHOD) {        \
        pycbc_report_method_exception(                                   \
                LCB_EINVAL, "Missing method %s", #METHOD);               \
    }

        PYCBC_CRYPTO_METHODS(PYCBC_POPULATE_STRUCT);
#undef PYCBC_POPULATE_STRUCT
        self->provider->v.PYCBC_CRYPTO_VVERSION.release_bytes =
                pycbc_release_bytes;
    }
    if (PyErr_Occurred() || !self->provider)
    {
        return -1;
    }
#ifdef PYCBC_GEN_PYTHON_STUBS
    pycbc_generate_stubs();
#endif
    self->provider->cookie = self;
    lcbcrypto_ref(self->provider);
    return 0;
}

static void
CryptoProvider_dtor(pycbc_CryptoProvider *self)
{
    if (self->provider) {
        lcbcrypto_unref(self->provider);
        self->provider = NULL;
    }

    Py_TYPE(self)->tp_free((PyObject*)self);
}

int
pycbc_CryptoProviderType_init(PyObject **ptr)
{
    PyTypeObject *p = &CryptoProviderType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "CryptoProvider";
    p->tp_new = PyType_GenericNew;
    p->tp_init = (initproc)CryptoProvider__init;
    p->tp_dealloc = (destructor)CryptoProvider_dtor;

    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_doc = PyDoc_STR("The encryption provider to encrypt/decrypt document fields");
    p->tp_basicsize = sizeof(pycbc_CryptoProvider);
    p->tp_setattro = CryptoProvider___setattr__;
    p->tp_methods = CryptoProvider_TABLE_methods;
#define PYCBC_DUMMY_METHOD_USE(RTYPE, VERSION, METHOD, PROCESSOR, X_ARGS) \
    (void)pycbc_crypto_##VERSION##_##METHOD;
    PYCBC_X_ALL_CRYPTO_FUNCTIONS(PYCBC_DUMMY_METHOD_USE)
#undef PYCBC_DUMMY_METHOD_USE
    return PyType_Ready(p);
}