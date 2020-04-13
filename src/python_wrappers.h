/**
 *     Copyright 2019 Couchbase, Inc.
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

#ifndef COUCHBASE_PYTHON_CLIENT_PYTHON_WRAPPERS_H
#define COUCHBASE_PYTHON_CLIENT_PYTHON_WRAPPERS_H
#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "util_wrappers.h"

void pycbc_dict_add_text_kv_strn(PyObject *dict,
                                 pycbc_strn_base_const strn_key,
                                 pycbc_strn_base_const strn_value);

void pycbc_dict_add_text_kv_strn2(PyObject* dict,
                                  const char* key,
                                  const char* val,
                                  size_t val_len);

void pycbc_dict_add_text_kv(PyObject *dict, const char *key, const char *value);

pycbc_strn_unmanaged pycbc_strn_from_managed(PyObject *source);

typedef struct pycbc_pybuffer_real {
    PyObject *pyobj;
    const void *buffer;
    size_t length;
} pycbc_pybuffer;

#define PYCBC_PYBUF_RELEASE(buf) do { \
    Py_XDECREF((buf)->pyobj); \
    (buf)->pyobj = NULL; \
} while (0)

#define KEYWORDS_STRING(s) #s,
#define KEYWORDS_CREDENTIAL(s) KEYWORDS_STRING(s)
#define KEYWORDS_TYPEOP(s, ARGNAME) #ARGNAME,
#define KEYWORDS_BUILD_CRED(U, P) U P
#define KEYWORDS_OBJECT(s) #s,
#define KEYWORDS_TARGET_OBJECT(TARGET, s) #s,
#define KEYWORDS_TARGET_OBJECTALIAS(TARGET, s, DEST) #s,
#define KEYWORDS_STRINGALIAS(s, ALIAS) #s,
#define KEYWORDS_INT(s) KEYWORDS_TARGET_INT(, s)
#define KEYWORDS_UINT(s) KEYWORDS_TARGET_UINT(, s)
#define KEYWORDS_TARGET_INT(TARGET, s) #s,
#define KEYWORDS_TARGET_UINT(TARGET, s) #s,
#define KEYWORDS_TARGET_UINTALIAS(TARGET, X, DEST) \
    KEYWORDS_TARGET_UINT(TARGET, X)

#define ARGSPEC_STRING(s) "z#"
#define ARGSPEC_CREDENTIAL(s) ARGSPEC_STRING(s)
#define ARGSPEC_STRINGALIAS(s, TARGET) "z#"
#define ARGSPEC_TYPEOP(s, ARGNAME) "i"
#define ARGSPEC_BUILD_CRED(U, P) U P
#define ARGSPEC_TARGET_OBJECT(TARGET, s) "O"
#define ARGSPEC_TARGET_OBJECTALIAS(TARGET, s, DEST) "O"
#define ARGSPEC_OBJECT(s) ARGSPEC_TARGET_OBJECT(, s)
#define ARGSPEC_TARGET_INT(TARGET, s) "i"
#define ARGSPEC_TARGET_UINT(TARGET, s) "I"
#define ARGSPEC_INT(s) ARGSPEC_TARGET_INT(, s)
#define ARGSPEC_UINT(s) ARGSPEC_TARGET_UINT(, s)
#define ARGSPEC_TARGET_UINTALIAS(TARGET, X, DEST) \
    ARGSPEC_TARGET_UINT(TARGET, DEST)

#define PYCBC_KWSTRUCT(XCTOR_ARGS, struct_name) \
    typedef struct {                            \
        XCTOR_ARGS(STRUCT)                      \
    } struct_name##_t;

#define PYCBC_KWLIST(XCTOR_ARGS, struct_name)                           \
    static char *kwlist[] = {XCTOR_ARGS(KEYWORDS) NULL};                \
    static char *argspec = "|" XCTOR_ARGS(ARGSPEC);                     \
    PYCBC_KWSTRUCT(XCTOR_ARGS, struct_name);                            \
    struct_name##_t struct_name = {0};                                  \
    rv = PyArg_ParseTupleAndKeywords(                                   \
            args, kwargs, argspec, kwlist, XCTOR_ARGS(MEMACCESS) NULL); \
    if (!rv) {                                                          \
        PYCBC_EXCTHROW_ARGS();                                          \
        return -1;                                                      \
    }

#define STRUCT_TYPEOP(X, ARGNAME) lcb_INSTANCE_TYPE X;
#define STRUCT_STRING(X) \
    const char *X;       \
    size_t X##_len;
#define STRUCT_CREDENTIAL(X) STRUCT_STRING(X)
#define STRUCT_STRINGALIAS(...)
#define STRUCT_BUILD_CRED(U, P) U P
#define STRUCT_TARGET_OBJECT(TARGET, X)
#define STRUCT_TARGET_OBJECTALIAS(TARGET, X, DEST)
#define STRUCT_OBJECT(X) PyObject *X;
#define STRUCT_TARGET_INT(TARGET, X)
#define STRUCT_TARGET_UINT(TARGET, X)
#define STRUCT_TARGET_UINTALIAS(TARGET, X, DEST) \
    STRUCT_TARGET_UINT(TARGET, DEST)
#define STRUCT_INT(X) int X;
#define STRUCT_UINT(X) unsigned int X;

#define MEMACCESS_TYPEOP(X, ARGNAME) &opts.X,
#define MEMACCESS_STRING(X) &opts.X, &opts.X##_len,
#define MEMACCESS_CREDENTIAL(X) MEMACCESS_STRING(X)
#define MEMACCESS_STRINGALIAS(X, TARGET) &opts.TARGET, &opts.TARGET##_len,
#define MEMACCESS_BUILD_CRED(U, P) U P
#define MEMACCESS_TARGET_OBJECT(TARGET, X) &(TARGET).X,
#define MEMACCESS_TARGET_OBJECTALIAS(TARGET, X, DEST) &(TARGET).DEST,
#define MEMACCESS_OBJECT(X) MEMACCESS_TARGET_OBJECT(opts, X)
#define MEMACCESS_TARGET_INT(TARGET, X) &(TARGET).X,
#define MEMACCESS_TARGET_UINT(TARGET, X) &(TARGET).X,
#define MEMACCESS_INT(X) MEMACCESS_TARGET_INT(opts, X)
#define MEMACCESS_UINT(X) MEMACCESS_TARGET_UINT(opts, X)
#define MEMACCESS_TARGET_UINTALIAS(TARGET, X, DEST) \
    MEMACCESS_TARGET_UINT(TARGET, DEST)
#endif // COUCHBASE_PYTHON_CLIENT_PYTHON_WRAPPERS_H
