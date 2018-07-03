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

PyObject *pycbc_log_handler = NULL;

static void log_handler(struct lcb_logprocs_st *procs, unsigned int iid,
    const char *subsys, int severity, const char *srcfile, int srcline,
    const char *fmt, va_list ap);

struct lcb_logprocs_st pycbc_lcb_logprocs = { 0 };

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

static void
get_helper_field(const char *s, PyObject *key, PyObject **cand, PyObject ***out)
{
    PyObject *sk = pycbc_SimpleStringZ(s);
    if (PyUnicode_Compare(sk, key) == 0) {
        *out = cand;
    }
    Py_DECREF(sk);
}

static PyObject *
_libcouchbase_modify_helpers(PyObject *self, PyObject *args, PyObject *kwargs)
{
    Py_ssize_t dictpos = 0;
    PyObject *curkey;
    PyObject *curval;
    PyObject *ret;

    if (kwargs == NULL || PyDict_Check(kwargs) == 0) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    ret = PyDict_New();

    while (PyDict_Next(kwargs, &dictpos, &curkey, &curval)) {
        PyObject **field = NULL;
        PyObject *dent = curval;

        #define X(name) \
        if (!field) { \
            get_helper_field(#name, \
                             curkey, \
                             &pycbc_helpers.name, \
                             &field); \
        }

        PYCBC_XHELPERS(X)
        #undef X

        if (!field) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Unknown helper", curkey);
            Py_DECREF(ret);
            return NULL;
        }

        if (*field) {
            dent = *field;
        } else {
            dent = Py_None;
            Py_INCREF(dent);
        }

        PyDict_SetItem(ret, curkey, dent);
        Py_DECREF(dent);

        Py_INCREF(curval);
        *field = curval;
    }

    (void)args;
    (void)self;
    return ret;
}

static PyObject *
_libcouchbase_get_helper(PyObject *self, PyObject *arg)
{
    PyObject *key = NULL;
    PyObject **field = NULL;
    int rv;
    (void)self;

    rv = PyArg_ParseTuple(arg, "O", &key);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    #define X(name) \
    if (!field) { \
        get_helper_field(#name, key, &pycbc_helpers.name, &field); \
    }

    PYCBC_XHELPERS(X)
    #undef X

    if (!field) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Unknown helper", key);
        return NULL;
    }
    if (*field) {
        Py_INCREF(*field);
        return *field;
    } else {
        Py_RETURN_NONE;
    }

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

static PyObject *
pycbc_lcb_version(pycbc_Bucket *self)
{
    const char *verstr;
    lcb_uint32_t vernum;
    PyObject *ret;

    verstr = lcb_get_version(&vernum);
    ret = Py_BuildValue("(s,k)", verstr, vernum);

    (void)self;

    return ret;
}

static PyObject *
set_log_handler(PyObject *self, PyObject *args)
{
    PyObject *val_O = NULL;
    PyObject *oldval = pycbc_log_handler;

    int rv;

    rv = PyArg_ParseTuple(args, "|O", &val_O);
    if (!rv) {
        return NULL;
    }

    if (val_O) {
        pycbc_log_handler = val_O;
        if (val_O != Py_None) {
            Py_INCREF(val_O);
            pycbc_log_handler = val_O;
        } else {
            pycbc_log_handler = NULL;
        }

        if (oldval) {
            return oldval;
        } else {
            Py_RETURN_NONE;
        }
    } else {
        /* Simple GET */
        if (oldval) {
            Py_INCREF(oldval);
            return oldval;
        } else {
            Py_RETURN_NONE;
        }
    }
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
        { "_modify_helpers", (PyCFunction)_libcouchbase_modify_helpers,
                METH_VARARGS|METH_KEYWORDS,
                "Internal function to modify helpers during runtime"
        },
        { "_get_helper", (PyCFunction)_libcouchbase_get_helper,
                METH_VARARGS,
                "Get a helper by name"
        },
        { "_get_errtype", (PyCFunction) pycbc_exc_get_categories,
                METH_VARARGS,
                "Get error categories for a given code"
        },
        { "lcb_version",
                (PyCFunction)pycbc_lcb_version,
                METH_NOARGS,
                PyDoc_STR(
                "Get `libcouchbase` version information\n"
                "\n"
                ":return: a tuple of ``(version_string, version_number)``\n"
                "  corresponding to the underlying libcouchbase version\n"

                "Show the versions ::\n" \
                "   \n"
                "   verstr, vernum = Connection.lcb_version()\n"
                "   print('0x{0:x}'.format(vernum))\n"
                "   # 0x020005\n"
                "   \n"
                "   print(verstr)\n"
                "   # 2.0.5\n"
                "\n"
                "\n")
        },
        { "lcb_logging", (PyCFunction)set_log_handler, METH_VARARGS,
                PyDoc_STR("Get/Set logging callback")
        },
        { "dump_constants",
                (PyCFunction)pycbc_print_constants, METH_NOARGS,
                PyDoc_STR("Print the constants to standard output")
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

/**
 * Each of our types has an initializer function that accepts a single
 * PyObject** pointer which is set to the newly created class object.
 *
 * More types should be added to this list. The first field is the type
 * name (not a string but a literal) and the second is the name of the
 * function used to initialize it
 */
#define PYCBC_CRYPTO_TYPES_ADAPTER(NAME, DOC, ...) \
    X(NAME, pycbc_##NAME##Type_init)
#define X_PYTYPES_NOTRACING(X)                         \
    X(Bucket, pycbc_BucketType_init)                   \
    /** Remember to keep base classes in order */      \
    X(Result, pycbc_ResultType_init)                   \
    X(OperationResult, pycbc_OperationResultType_init) \
    X(ValueResult, pycbc_ValueResultType_init)         \
    X(MultiResult, pycbc_MultiResultType_init)         \
    X(HttpResult, pycbc_HttpResultType_init)           \
    X(ViewResult, pycbc_ViewResultType_init)           \
    X(Transcoder, pycbc_TranscoderType_init)           \
    X(ObserveInfo, pycbc_ObserveInfoType_init)         \
    X(Item, pycbc_ItemType_init)                       \
    X(Event, pycbc_EventType_init)                     \
    X(IOEvent, pycbc_IOEventType_init)                 \
    X(TimerEvent, pycbc_TimerEventType_init)           \
    X(AsyncResult, pycbc_AsyncResultType_init)         \
    X(_IOPSWrapper, pycbc_IOPSWrapperType_init)        \
    X(_SDResult, pycbc_SDResultType_init)              \
    PYCBC_CRYPTO_TYPES(PYCBC_CRYPTO_TYPES_ADAPTER)
#ifdef PYCBC_TRACING
#define X_PYTYPES(X)       \
    X_PYTYPES_NOTRACING(X) \
    X(Tracer, pycbc_TracerType_init)
#else
#define X_PYTYPES(X) X_PYTYPES_NOTRACING(X)
#endif

#define X(name, inf) PyObject *cls_##name;
    X_PYTYPES(X)
#undef X

#define X(name, infunc) \
    if (infunc(&cls_##name) < 0) { INITERROR; }
    X_PYTYPES(X)
#undef X

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
#define X(name, infunc) \
    PyModule_AddObject(m, #name, cls_##name);
    X_PYTYPES(X)
#undef X
#endif /* PYCBC_CPYCHECKER */

    /**
     * Initialize the helper names
     */
#define X(var, val) pycbc_helpers.var = pycbc_SimpleStringZ(val);
    PYCBC_XHELPERS_STRS(X);
#undef X

    pycbc_helpers.fmt_auto =
            PyObject_CallFunction((PyObject*)&PyBaseObject_Type, NULL, NULL);
    PyModule_AddObject(m, "FMT_AUTO", pycbc_helpers.fmt_auto);

    pycbc_init_pyconstants(m);

    /* Add various implementation specific flags */
    PyModule_AddIntConstant(m, "_IMPL_INCLUDE_DOCS", 0);

    /* Initialize the logging routines */
    pycbc_lcb_logprocs.v.v0.callback = log_handler;

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}

#ifndef va_copy
#define va_copy(a, b) (a) = (b)
#endif

/* Logging functionality */
static void log_handler(struct lcb_logprocs_st *procs,
                        unsigned int iid,
                        const char *subsys,
                        int severity,
                        const char *srcfile,
                        int srcline,
                        const char *fmt,
                        va_list ap)
{
    PyGILState_STATE gil_prev;
    PyObject *tmp = NULL;
    PyObject *kwargs;
#define PYCBC_LOG_BUFSZ 1000
    char stackbuf[PYCBC_LOG_BUFSZ] = {0};
    char *heapbuf = NULL;
    char *tempbuf = stackbuf;
    va_list vacp;
    int length = 0;
    if (!pycbc_log_handler) {
        return;
    }

    gil_prev = PyGILState_Ensure();

    kwargs = PyDict_New();
    va_copy(vacp, ap);
    length = vsnprintf(stackbuf, PYCBC_LOG_BUFSZ, fmt, vacp);
    if (length > PYCBC_LOG_BUFSZ) {
        heapbuf = PYCBC_CALLOC_TYPED(length, char);
        va_copy(vacp, ap);
        length = vsnprintf(heapbuf, length, fmt, vacp);
        tempbuf = heapbuf;
    }
    if (length >= 0) {
        tmp = pycbc_SimpleStringN(tempbuf, length);
    } else {
        va_copy(vacp, ap);
        PYCBC_DEBUG_LOG("Got negative result %d from FMT %s", length, fmt);
        PYCBC_DEBUG_LOG("should be:");
        PYCBC_DEBUG_LOG(fmt, vacp);
    }
    va_end(ap);
#undef PYCBC_LOG_BUFSZ
    if (heapbuf) // in case we ever-hit a noncompliant C implementation
    {
        PYCBC_FREE(heapbuf);
    }
    if (length < 0 || !tmp || PyErr_Occurred())
        goto FAIL;
    PyDict_SetItemString(kwargs, "message", tmp);
    Py_DECREF(tmp);
    tmp = pycbc_IntFromL(iid);
    PyDict_SetItemString(kwargs, "id", tmp); Py_DECREF(tmp);

    tmp = pycbc_IntFromL(severity);
    PyDict_SetItemString(kwargs, "level", tmp); Py_DECREF(tmp);

    tmp = Py_BuildValue("(s,i)", srcfile, srcline);
    PyDict_SetItemString(kwargs, "c_src", tmp); Py_DECREF(tmp);

    tmp = pycbc_SimpleStringZ(subsys);
    PyDict_SetItemString(kwargs, "subsys", tmp); Py_DECREF(tmp);

    PYCBC_STASH_EXCEPTION(
            PyObject_Call(pycbc_log_handler, pycbc_DummyTuple, kwargs));
FAIL:
    Py_DECREF(kwargs);
    PyGILState_Release(gil_prev);
}


#include <stdio.h>
#include <libcouchbase/tracing.h>
#ifdef _WIN32
#define PRIx64 "I64x"
#define PRId64 "I64d"
#else
#include <inttypes.h>
#endif

#include "oputil.h"
#if PY_MAJOR_VERSION < 3
const char *pycbc_cstrn(PyObject *object, Py_ssize_t *length)
{
    const char *buffer = NULL;
    *length = 0;
    if (!object) {
        goto FAIL;
    }

    if (PyUnicode_Check(object)) {
        buffer = PyUnicode_AS_DATA(object);
        *length = PyUnicode_GetSize(object);
    } else if (PyString_Check(object)) {
        PyString_AsStringAndSize(object, (char **)&buffer, length);
    }
FAIL:
    return buffer;
}

#else

const char *pycbc_cstrn(PyObject *object, Py_ssize_t *length)
{
    char *buffer = NULL;
    *length = 0;
    if (!object) {
        goto FAIL;
    }
    if (PyBytes_Check(object)) {
        PyBytes_AsStringAndSize(object, &buffer, length);
    } else if (PyUnicode_Check(object)) {
        buffer = PyUnicode_AsUTF8AndSize(object, length);
    }
FAIL:
    return buffer;
}
#endif

const char *pycbc_cstr(PyObject *object)
{
    Py_ssize_t dummy = 0;
    return pycbc_cstrn(object, &dummy);
}

void pycbc_fetch_error(PyObject *err[3])
{
    PyErr_Restore(err[0], err[1], err[2]);
}

void pycbc_store_error(PyObject *err[3])
{
    PyErr_Fetch(&err[0], &err[1], &err[2]);
}

#ifdef PYCBC_DEBUG
void *malloc_and_log(const char *file,
                     const char *func,
                     int line,
                     size_t quant,
                     size_t size,
                     const char *typename)
{
    size_t total_bytes = quant * size;
    void *result = malloc(total_bytes);
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            file,
            func,
            line,
            "malloced %llu * [(%s) = %llu] = %llu bytes at %p",
            (long long unsigned)quant,
            typename ? typename : "unknown",
            (long long unsigned)size,
            (long long unsigned)total_bytes,
            result);
    return result;
}
void *calloc_and_log(const char *file,
                     const char *func,
                     int line,
                     size_t quant,
                     size_t size,
                     const char *type_name)
{
    void *result = calloc(size, quant);
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            file,
            func,
            line,
            "calloced (%llu * [ %s == %llu ])= %llu at %p",
            (long long unsigned)quant,
            type_name,
            (long long unsigned)size,
            (long long unsigned)(quant * size),
            result);
    return result;
}
void pycbc_exception_log(const char *file,
                         const char *func,
                         int line,
                         int clear)
{

    if (PyErr_Occurred()) {
        PyObject* type, *value, *traceback;
        PyErr_Fetch(&type,&value,&traceback);
        PYCBC_DEBUG_PYFORMAT_FILE_FUNC_AND_LINE(
                file,
                func,
                line,
                "***** EXCEPTION:[%R], [%R] *****",
                type,
                value);
        if (clear)
        {
            Py_XDECREF(type);
            Py_XDECREF(value);
            Py_XDECREF(traceback);
        }
        else
        {
            PyErr_Restore(type,value,traceback);
        }
    }
}
#endif
void pycbc_print_pyformat(const char *format, ...)
{
    va_list v1;
    PyObject *type = NULL, *value = NULL, *traceback = NULL;
    PyObject *formatted;
    PyErr_Fetch(&type, &value, &traceback);
    va_start(v1, format);
    formatted = PyUnicode_FromFormatV(format, v1);
    va_end(v1);
    if (!formatted || PyErr_Occurred()) {
        PYCBC_EXCEPTION_LOG
    } else {
        fprintf(stderr, "%s", PYCBC_CSTR(formatted));
    }
    Py_XDECREF(formatted);
    PyErr_Print();
    if (type || value || traceback) {
        PyErr_Restore(type, value, traceback);
    }
}

PyObject* pycbc_replace_str(PyObject** string, const char* pat, const char* replace)
{
    PyObject* result = PyObject_CallMethod(*string, "replace", "ss", pat, replace);
    Py_DecRef(*string);
    *string = result;
    return result;
}

PyObject *pycbc_none_or_value(PyObject *maybe_value)
{
    return maybe_value ? maybe_value : Py_None;
}

PyObject *pycbc_null_or_value(PyObject *tracer)
{
    return (tracer && PyObject_IsTrue(tracer)) ? tracer : NULL;
}

#ifdef PYCBC_TRACING

void *pycbc_null_or_capsule_value(PyObject *maybe_capsule,
                                  const char *capsule_name)
{
    return (maybe_capsule ? PyCapsule_GetPointer(maybe_capsule, capsule_name)
                          : NULL);
}

void *pycbc_capsule_value_or_null(PyObject *capsule, const char *capsule_name)
{
    return capsule ? PyCapsule_GetPointer(capsule, capsule_name) : NULL;
}

static void _pycbc_Context_set_ref_count(pycbc_stack_context_handle context,
                                         size_t count);
void pycbc_Context_dec_ref_count(pycbc_stack_context_handle context);

void pycbc_Context_capsule_destructor(PyObject *context_capsule)
{
    pycbc_stack_context_handle handle =
            pycbc_capsule_value_or_null(context_capsule, "tracing_context");
    PYCBC_CONTEXT_DEREF(handle, 0);
}

PyObject *pycbc_Context_capsule(pycbc_stack_context_handle context)
{
    PYCBC_REF_CONTEXT(context);
    return PyCapsule_New(
            context, "tracing_context", pycbc_Context_capsule_destructor);
}

void *pycbc_Context_capsule_value(PyObject *context_capsule)
{
    return pycbc_capsule_value_or_null(context_capsule, "tracing_context");
}

typedef struct pycbc_tracer_payload pycbc_tracer_payload_t;
pycbc_tracer_payload_t *pycbc_persist_span(lcbtrace_SPAN *span);
PyObject *pycbc_tracer_payload_start_span_args(
        const pycbc_tracer_payload_t *payload);

void pycbc_log_context(pycbc_stack_context_handle context)
{
    {
        PYCBC_DEBUG_LOG_CONTEXT(context, "Persisting %p", context);
        if (PYCBC_CHECK_CONTEXT(context)) {
            pycbc_tracer_payload_t *output = pycbc_persist_span(context->span);
            PyObject *pyoutput = pycbc_tracer_payload_start_span_args(output);
            PyObject *repr = PyObject_Repr(pyoutput);
            PYCBC_DEBUG_LOG_CONTEXT(context,
                                    "context %p dereffed: %d references now:%s",
                                    output,
                                    (int)PYCBC_CONTEXT_GET_REF_COUNT(context),
                                    PYCBC_CSTR(repr));
            Py_DecRef(pyoutput);
            Py_DecRef(repr);
            free(output);
        }
    }
}
static void _pycbc_Context_set_ref_count(pycbc_stack_context_handle context,
                                         size_t count)
{
    context->ref_count = count;
}
size_t pycbc_Context_get_ref_count(pycbc_stack_context_handle context)
{
    return context ? context->ref_count : 0;
}
size_t pycbc_Context_get_ref_count_debug(const char *FILE,
                                         const char *FUNC,
                                         int line,
                                         pycbc_stack_context_handle context)
{
    if (!context) {
        PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
                FILE, FUNC, line, "Trying to get ref_count from NULL");
    }
    return pycbc_Context_get_ref_count(context);
}

void pycbc_Context_dec_ref_count(pycbc_stack_context_handle context)
{
#ifdef PYCBC_STRICT
    pycbc_assert(pycbc_Context_get_ref_count(context));
#endif
    if (!context->ref_count) {
        PYCBC_DEBUG_LOG_CONTEXT(
                context,
                "*** %p ref_count is already zero, dangling pointer? ***",
                context)
        return;
    }

    context->ref_count--;
}

void pycbc_ref_context(pycbc_stack_context_handle parent_context)
{
    if (parent_context) {
        parent_context->ref_count++;
    }
};

pycbc_stack_context_handle pycbc_Context_deref_debug(
        const char *file,
        const char *func,
        int line,
        pycbc_stack_context_handle context,
        int should_be_final,
        int dealloc_children,
        pycbc_stack_context_handle from_context)
{
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_LINE_CONTEXT_NEWLINE(
            file,
            func,
            line,
            context,
            "dereffing %p, %s",
            context,
            should_be_final ? "should be final" : "not necessarily final");
    return pycbc_Context_deref(
            context, should_be_final, dealloc_children, from_context);
}

void pycbc_Context__dtor(pycbc_stack_context_handle context)
{
    if (context) {
        if (context->span) {
            lcbtrace_span_finish(context->span, 0);
            context->span = NULL;
        } else {
            PYCBC_DEBUG_LOG("Has null span already")
#ifdef PYCBC_STRICT
            abort();
#endif
        }
    }
    PYCBC_FREE(context);
}

#ifdef PYCBC_REF_ACCOUNTING
static int pycbc_Context_account_for_children(
        pycbc_stack_context_handle context);

int pycbc_unregister_child_context(pycbc_stack_context_handle from_context)
{
    int result = 0;
    pycbc_stack_context_handle context = from_context->parent;
    if (!context || !context->children) {
        PYCBC_DEBUG_LOG("Invalid parent context");
        return -1;
    }
    if (PYCBC_CONTEXT_GET_REF_COUNT(from_context)) {
        PYCBC_DEBUG_LOG("count should be zero for %p", from_context);
#ifdef PYCBC_STRICT
        abort();
#endif
        _pycbc_Context_set_ref_count(from_context, 0);
    }

    PYCBC_DEBUG_LOG_CONTEXT(
            context, "freeing one child of %p, %p", context, from_context);
    {
        pycbc_context_children *predecessor_node = from_context->acc_node;
        pycbc_context_children *current_node = context->children;
        if (predecessor_node) {
            if (predecessor_node->next) {
                current_node = predecessor_node->next;
                predecessor_node->next = current_node->next;
            } else {
                PYCBC_DEBUG_LOG_CONTEXT(context,
                                        "from context %p, children %p, "
                                        "predecessor_node node is %p, "
                                        "predecessor node next is NULL, "
                                        "corrupt accounting list",
                                        from_context,
                                        context->children,
                                        predecessor_node);
#ifdef PYCBC_STRICT
                abort();
#endif
                return -1;
            }
        }
        PYCBC_FREE(current_node);
        if (current_node == context->children) {
            context->children = NULL;
        }
    }
    return result;
}
static int pycbc_Context_account_for_children(
        pycbc_stack_context_handle context)
{
    pycbc_context_children *current_node = context->children;
    int result = 0;
    PYCBC_DEBUG_LOG_CONTEXT(
            context,
            "*** freeing children of %p, should already all be free! ***",
            context);
    if (!current_node) {
        return 0;
    }
    while (current_node) {
        pycbc_context_children *next_node = current_node->next;
        PYCBC_DEBUG_LOG_CONTEXT(context,
                                "context %p: belatedly freeing node %p",
                                context,
                                current_node->value);
#ifdef PYCBC_LOG_CONTEXT_CONTENTS
        pycbc_log_context(current_node->value);
#endif
        ++result;
        result += pycbc_Context_account_for_children(current_node->value);
#ifdef PYCBC_REF_CLEANUP
        pycbc_Context__dtor(current_node->value);
#endif
        PYCBC_FREE(current_node);
        current_node = next_node;
    }

    context->children = NULL;
    PYCBC_DEBUG_LOG_CONTEXT(
            context,
            "finished freeing remaining children of %p, %d references cleaned",
            context,
            result);
#ifdef PYCBC_STRICT
    abort();
#endif
    return result;
}

#endif

pycbc_stack_context_handle pycbc_Context_deref(
        pycbc_stack_context_handle context,
        int should_be_final,
        int account_for_children,
        pycbc_stack_context_handle from_context)
{
    pycbc_stack_context_handle parent = NULL;
    if (!PYCBC_CHECK_CONTEXT(context)) {
        PYCBC_DEBUG_LOG("Invalid context %p, trying to decrease refcount",
                        context);
        return NULL;
    }

    PYCBC_DEBUG_LOG_CONTEXT(context,
                            "Dereffing %p, should be final %d, "
                            "account_for_children %d, from_context %p",
                            context,
                            should_be_final,
                            account_for_children,
                            from_context);
    if (PYCBC_CHECK_CONTEXT(context)) {
        if (PYCBC_CONTEXT_GET_REF_COUNT(context) == 0) {
            PYCBC_DEBUG_LOG("Already zero reference count! Wrong! %p", context);
#ifdef PYCBC_STRICT
            abort();
#endif
            return context;
        }

        pycbc_Context_dec_ref_count(context);
        if (PYCBC_CONTEXT_GET_REF_COUNT(context) == 0) {
#ifdef PYCBC_REF_ACCOUNTING
            if (account_for_children &&
                pycbc_Context_account_for_children(context)) {
                PYCBC_DEBUG_LOG_CONTEXT(
                        context, "ref accounting anomalies found cleaning up");
#ifdef PYCBC_STRICT
                abort();
#endif
            }
            parent = context->parent;
            if (parent) {
#ifdef PYCBC_REF_ACCOUNTING
                pycbc_unregister_child_context(context);
#endif
                pycbc_Context_deref(parent, 0, 0, context);
            }

#endif
            PYCBC_DEBUG_LOG_CONTEXT(context, "closing span %p", context->span);
            pycbc_Context__dtor(context);
        } else {
#ifdef PYCBC_REF_ACCOUNTING
            if (should_be_final && context->children) {
                PYCBC_DEBUG_LOG_CONTEXT(
                        context,
                        "*** %p Should have lost all children by now ***",
                        context);
                if (account_for_children &&
                    pycbc_Context_account_for_children(context)) {
                    PYCBC_DEBUG_LOG_CONTEXT(
                            context,
                            "ref accounting anomalies found cleaning up");
#ifdef PYCBC_STRICT
                    abort();
#endif
                }
            }
#endif
        }
    };
    return parent;
}

int pycbc_wrap_and_pop(pycbc_stack_context_handle *context, int result)
{
    if (context && PYCBC_CHECK_CONTEXT(*context)) {
        *context = PYCBC_CONTEXT_DEREF(*context, 0);
    }
    return result;
}

static void pycbc_Context_ref(pycbc_stack_context_handle context,
                              pycbc_stack_context_handle child)
{
#ifdef PYCBC_REF_ACCOUNTING
    pycbc_context_children *child_node =
            PYCBC_CALLOC_TYPED(1, pycbc_context_children);
    child_node->value = child;
    child_node->next = context->children;
    if (context->children && context->children->value) {
        context->children->value->acc_node = child_node;
    }
    context->children = child_node;
#endif
    PYCBC_DEBUG_LOG_CONTEXT(
            context,
            "About to ref %p, val %llu, from child %p",
            context,
            (unsigned long long)pycbc_Context_get_ref_count(context),
            child);
    PYCBC_REF_CONTEXT(context);
    PYCBC_DEBUG_LOG_CONTEXT(
            context,
            "Reffed %p, val %llu, from child %p",
            context,
            (unsigned long long)pycbc_Context_get_ref_count(context),
            child);
}
#ifdef PYCBC_DEBUG
static void pycbc_Context_ref_debug(const char *FILE,
                                    const char *FUNC,
                                    int line,
                                    pycbc_stack_context_handle ref_context,
                                    pycbc_stack_context_handle child)
{
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_LINE_CONTEXT_NEWLINE(
            FILE,
            FUNC,
            line,
            ref_context,
            "pre-ref: ontext %p: %d reffed by %p",
            ref_context,
            (int)pycbc_Context_get_ref_count(ref_context),
            child);
    pycbc_Context_ref(ref_context, child);
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_LINE_CONTEXT_NEWLINE(
            FILE,
            FUNC,
            line,
            ref_context,
            "context %p: %d reffed by %p",
            ref_context,
            (int)pycbc_Context_get_ref_count(ref_context),
            child);
}

#define PYCBC_CONTEXT_REF(ref_context, child) \
    pycbc_Context_ref_debug(                  \
            __FILE__, __FUNCTION_NAME__, __LINE__, ref_context, child)
#else
#define PYCBC_CONTEXT_REF(ref_context, child)\
    pycbc_Context_ref(ref_context, child)
#endif

pycbc_stack_context_handle pycbc_Context_init(
        pycbc_Tracer_t *py_tracer,
        const char *operation,
        lcb_uint64_t now,
        pycbc_stack_context_handle ref_context,
        lcbtrace_REF_TYPE ref_type,
        const char *component)
{
    pycbc_stack_context_handle context =
            PYCBC_CALLOC_TYPED(1, pycbc_stack_context);

    lcbtrace_REF ref;
    ref.type = ref_context ? ref_type : LCBTRACE_REF_NONE;
    ref.span = ref_context ? ref_context->span : NULL;
    pycbc_assert(py_tracer);
    context->tracer = py_tracer;
    context->span = lcbtrace_span_start(
            py_tracer->tracer, operation, now, ref_context ? &ref : NULL);
    _pycbc_Context_set_ref_count(context, 1);
    context->parent = NULL;
    if (ref_context) {
        switch (ref_type) {
        case LCBTRACE_REF_CHILD_OF:
#ifdef PYCBC_TABBED_CONTEXTS
            context->depth = ref_context->depth + 1;
#endif
            context->parent = ref_context;
            PYCBC_CONTEXT_REF(ref_context, context);
            break;
        case LCBTRACE_REF_FOLLOWS_FROM:
            if (ref_context->parent) {
                context->parent = ref_context->parent;
#ifdef PYCBC_TABBED_CONTEXTS
                context->depth = ref_context->parent->depth + 1;
#endif
                PYCBC_CONTEXT_REF(ref_context->parent, context);
            }
            break;
        case LCBTRACE_REF_NONE:
        case LCBTRACE_REF__MAX:
        default:
            break;
        }
    }
    lcbtrace_span_add_tag_str(context->span, LCBTRACE_TAG_COMPONENT, component);
    PYCBC_DEBUG_LOG_CONTEXT(
            context,
            "Created context %p with span %p: component: %s, operation %s, "
            "ref_context %p, ref count %llu",
            context,
            context->span,
            component,
            operation,
            context->parent,
            (unsigned long long)PYCBC_CONTEXT_GET_REF_COUNT(context));
    return context;
}

pycbc_stack_context_handle pycbc_Context_check(
        pycbc_stack_context_handle context,
        const char *file,
        const char *func,
        int line)
{
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            file, func, line, "checking context %p", context);
    if (!(context)) {
        PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
                file, func, line, "warning: got null context");
    } else if (!(context)->tracer) {
        PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
                file, func, line, "warning: got null tracer");
    } else if (!(context)->span) {
        PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
                file, func, line, "warning: got null span");
    } else if (!(context)->tracer->tracer) {
        PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
                file, func, line, "warning: got null lcb_tracer");
    }
    else {
        PYCBC_DEBUG_LOG_WITH_FILE_FUNC_LINE_CONTEXT_NEWLINE(
                file,
                func,
                line,
                context,
                "got valid context %p with ref_count %llu",
                context,
                (unsigned long long)PYCBC_CONTEXT_GET_REF_COUNT(context));
        return context;
    }
    return NULL;
}

pycbc_stack_context_handle pycbc_Tracer_start_span(
        pycbc_Tracer_t *py_tracer,
        PyObject *kwargs,
        const char *operation,
        lcb_uint64_t now,
        pycbc_stack_context_handle *context,
        lcbtrace_REF_TYPE ref_type,
        const char *component)
{
    PyObject *tracer = kwargs?PyDict_GetItemString(kwargs, "tracer"):NULL;
    pycbc_stack_context_handle result = NULL;
    if (!(py_tracer || (tracer && PyArg_ParseTuple(tracer, "O!", &pycbc_TracerType, &py_tracer) && py_tracer)))
    {
        PYCBC_EXCEPTION_LOG;
        return NULL;
    }

    result = pycbc_Context_init(py_tracer,
                                operation,
                                now,
                                context ? *context : NULL,
                                ref_type,
                                component);
    if (context) {
        *context = result;
    }
    return result;
}

pycbc_stack_context_handle pycbc_Result_start_context(
        pycbc_stack_context_handle parent_context,
        PyObject *hkey,
        char *component,
        char *operation)
{
    pycbc_stack_context_handle stack_context_handle = NULL;
    if (PYCBC_CHECK_CONTEXT(parent_context)) {
        pycbc_Tracer_t *py_tracer = (parent_context)->tracer;
        if (py_tracer) {
            stack_context_handle = pycbc_Context_init(py_tracer,
                                                      operation,
                                                      0,
                                                      parent_context,
                                                      LCBTRACE_REF_CHILD_OF,
                                                      component);
        }
        PYCBC_DEBUG_PYFORMAT_CONTEXT(
                parent_context, "starting new context on key:[%S]", hkey);
    }
    return stack_context_handle;
}

void pycbc_payload_dealloc(pycbc_tracer_payload_t *pPayload);
PyObject *pycbc_set_args_from_payload_abbreviated(lcbtrace_SPAN *span,
                                                  pycbc_Bucket *bucket);

void pycbc_Result_propagate_context(pycbc_Result_t *res,
                                    pycbc_stack_context_handle parent_context,
                                    pycbc_Bucket *bucket)
{
    if (PYCBC_CHECK_CONTEXT(parent_context)) {
        if (!res->tracing_output) {
            res->tracing_output = pycbc_set_args_from_payload_abbreviated(
                    parent_context->span, bucket);
            PYCBC_DEBUG_PYFORMAT_CONTEXT(parent_context,
                                         "got output %R from parent context %p",
                                         res->tracing_output,
                                         parent_context);
        }
    }
    else
    {
        res->tracing_context = NULL;
    }
    res->is_tracing_stub = 0;
}

pycbc_stack_context_handle pycbc_MultiResult_extract_context(
        pycbc_MultiResult *self, PyObject *hkey, pycbc_Result **res)
{
    PyObject *mrdict = pycbc_multiresult_dict(self);
    pycbc_stack_context_handle parent_context = NULL;
    if (*res) {
        PYCBC_DEBUG_PYFORMAT(
                "[%R]"
                "&res %p:  coming back from callback on key: [%R]",
                mrdict,
                res,
                hkey);
        PYCBC_DEBUG_LOG("res %p", *res);

        parent_context = PYCBC_CHECK_CONTEXT((*res)->tracing_context);
        if (parent_context)
            PYCBC_REF_CONTEXT(parent_context);
        if ((*res)->is_tracing_stub) {
            PyDict_DelItem(mrdict, hkey);
            PYCBC_DECREF(*res);
            *res = NULL;
        }
    }
    return parent_context;
}

pycbc_stack_context_handle pycbc_Result_extract_context(
        const pycbc_Result_t *res)
{
    return res ? (res->tracing_context) : NULL;
}

void pycbc_MultiResult_init_context(pycbc_MultiResult *self, PyObject *curkey,
                                    pycbc_stack_context_handle context, pycbc_Bucket *bucket) {
    PyObject* mres_dict = pycbc_multiresult_dict(self);
    pycbc_pybuffer keybuf={0};
    pycbc_Result *item;
    if (!context) {
        return;
    }
    pycbc_tc_encode_key(bucket, curkey, &keybuf);
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    pycbc_tc_decode_key(bucket, keybuf.buffer, keybuf.length, &curkey);
    item=(pycbc_Result*)PyDict_GetItem(mres_dict, curkey);
    if (!item) {
        PYCBC_DEBUG_PYFORMAT_CONTEXT(
                context, "Prior to insertion:[%R]", mres_dict);
        PYCBC_EXCEPTION_LOG_NOCLEAR;
        item = (pycbc_Result *)pycbc_valresult_new(bucket);
        PyDict_SetItem(mres_dict, curkey, (PyObject*)item);
        item->is_tracing_stub = 1;
    }
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    PYCBC_REF_CONTEXT(context);
    item->tracing_context = context;
    PYCBC_DEBUG_PYFORMAT_CONTEXT(context,
                                 "res %p: binding context %p to [%R]",
                                 item,
                                 context,
                                 curkey);
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    PYCBC_DEBUG_PYFORMAT_CONTEXT(context, "After insertion:[%R]", mres_dict);
}

int pycbc_is_async_or_pipeline(const pycbc_Bucket *self) { return self->flags & PYCBC_CONN_F_ASYNC || self->pipeline_queue; }

#define LOGARGS(instance, lvl) instance->settings, "bootstrap", LCB_LOG_##lvl, __FILE__, __LINE__
#endif

void pycbc_set_dict_kv_object(PyObject *dict,
                              PyObject *key,
                              const char *value_str)
{
    PyObject *value = pycbc_SimpleStringZ(value_str);
    PYCBC_DEBUG_PYFORMAT("adding [%R], value %S to [%R]", key, value, dict);
    PyDict_SetItem(dict, key, value);
    PYCBC_DECREF(value);
}


void pycbc_set_kv_ull(PyObject *dict, PyObject *keystr, lcb_uint64_t parenti_id) {
    PyObject *pULL = PyLong_FromUnsignedLongLong(parenti_id);
    PYCBC_DEBUG_PYFORMAT("adding [%R], value %S to [%R]", keystr, pULL, dict);
    PyDict_SetItem(dict, keystr, pULL);
    PYCBC_DECREF(pULL);
}
#define PYCBC_TYPE_DEF(NAME, DOC, ...) \
    PyTypeObject pycbc_##NAME##Type = {PYCBC_POBJ_HEAD_INIT(NULL) 0};

PYCBC_AUTODEF_TYPES(PYCBC_TYPE_DEF)

#ifdef PYCBC_TRACING

#define PYCBC_X_SPAN_ARGS(TEXT, ULL, TAGS, IDNUM) \
    TEXT(operation_name)                          \
    ULL(start_time)                               \
    IDNUM(child_of)                               \
    IDNUM(id)                                     \
    TAGS(tags)

#define PYCBC_X_FINISH_ARGS(TEXT, ULL) ULL(finish_time)

#define PYCBC_X_LITERALTAGNAMES(TEXT, ULL, IDNUM) \
    TEXT(DB_TYPE)                                 \
    ULL(PEER_LATENCY)                             \
    ULL(OPERATION_ID)                             \
    TEXT(SERVICE)                                 \
    TEXT(COMPONENT)                               \
    TEXT(PEER_ADDRESS)                            \
    TEXT(LOCAL_ADDRESS)                           \
    TEXT(DB_INSTANCE)                             \
    IDNUM(child_of)                               \
    IDNUM(id)
#undef X

#define X(NAME) PyObject* pycbc_##NAME;
PYCBC_X_SPAN_ARGS(X, X, X, X)
PYCBC_X_LITERALTAGNAMES(X, X, X)
PYCBC_X_FINISH_ARGS(X,X)
#undef X

#define ORIG_ID "couchbase.orig_id"
#define PYCBC_X_TAG_TO_OP_CONTEXT(FORWARD_TAG,        \
                                  FORWARD_TAG_ULL,    \
                                  FORWARD_TIMEOUT,    \
                                  OP_NAME,            \
                                  FORWARD_AGGREGATE,  \
                                  TAG_VALUE,          \
                                  FORWARD_OP_ID)      \
    FORWARD_AGGREGATE(s, TAG_VALUE(SERVICE), OP_NAME) \
    FORWARD_TAG(c, LOCAL_ID)                          \
    FORWARD_OP_ID(i, ORIG_ID)                         \
    FORWARD_TAG(b, DB_INSTANCE)                       \
    FORWARD_TAG(l, LOCAL_ADDRESS)                     \
    FORWARD_TAG(r, PEER_ADDRESS)                      \
    FORWARD_TIMEOUT(t)

#define PYCBC_ABBREV_NAME(NAME) pycbc_##NAME##_abbrev
#define GENERIC_ABBREV(NAME, ...) PyObject *PYCBC_ABBREV_NAME(NAME);
PYCBC_X_TAG_TO_OP_CONTEXT(GENERIC_ABBREV,
                          GENERIC_ABBREV,
                          GENERIC_ABBREV,
                          GENERIC_ABBREV,
                          GENERIC_ABBREV,
                          GENERIC_ABBREV,
                          GENERIC_ABBREV)

#undef FORWARD_OP_NAME
#undef GENERIC_ABBREV

PyObject* pycbc_default_key;

void pycbc_Tracer_init_constants(void)
{

#define X(NAME) pycbc_##NAME=pycbc_SimpleStringZ(#NAME);
    PYCBC_X_SPAN_ARGS(X, X, X, X)
    PYCBC_X_FINISH_ARGS(X,X)
#undef X
#define X(NAME) pycbc_##NAME=pycbc_SimpleStringZ(LCBTRACE_TAG_##NAME);
#define ID(NAME) pycbc_##NAME = pycbc_SimpleStringZ(#NAME);
    PYCBC_X_LITERALTAGNAMES(X, X, ID);
#undef X
#define GENERIC_ABBREV(NAME, ...) \
    PYCBC_ABBREV_NAME(NAME) = pycbc_SimpleStringZ(#NAME);
#define BLANK(NAME,...)

    PYCBC_X_TAG_TO_OP_CONTEXT(GENERIC_ABBREV,
                              GENERIC_ABBREV,
                              GENERIC_ABBREV,
                              GENERIC_ABBREV,
                              GENERIC_ABBREV,
                              GENERIC_ABBREV,
                              GENERIC_ABBREV);
#undef BLANK
}

#define PYCBC_TAG_TEXT(NAME) char *NAME;
#define PYCBC_TAG_ULL(NAME) lcb_uint64_t* NAME;

typedef struct pycbc_tracer_tags {
    PYCBC_X_LITERALTAGNAMES(PYCBC_TAG_TEXT, PYCBC_TAG_ULL, PYCBC_TAG_ULL)
} pycbc_tracer_tags_t;

#define PYCBC_TAG_STRUCT(NAME) pycbc_tracer_tags_t* NAME;

struct pycbc_tracer_span_args {
    PYCBC_X_SPAN_ARGS(PYCBC_TAG_TEXT,
                      PYCBC_TAG_ULL,
                      PYCBC_TAG_STRUCT,
                      PYCBC_TAG_ULL)
};

typedef struct pycbc_tracer_finish_args
{
    PYCBC_X_FINISH_ARGS(PYCBC_TAG_TEXT,PYCBC_TAG_ULL)
} pycbc_tracer_finish_args_t;

#undef PYCBC_TAG_ULL
#undef PYCBC_TAG_TEXT
#undef PYCBC_TAG_STRUCT

#define PYCBC_TRACING_ADD_TEXT(DICT, KEY, VALUE)    \
    {                                               \
        const char *VAL = VALUE;                    \
        (DICT)->KEY = (VAL) ? strdup((VAL)) : NULL; \
    };
#define PYCBC_TRACING_ADD_U64(DICT, KEY, VALUE)        \
    (DICT)->KEY = PYCBC_MALLOC_TYPED(1, lcb_uint64_t); \
    *((DICT)->KEY) = VALUE;

#define PYCBC_TEXT_TO_DICT_IMPL(ARGS, NAME, KEY)             \
    if ((ARGS)->NAME) {                                      \
        pycbc_set_dict_kv_object(dict, KEY, ((ARGS)->NAME)); \
    }
#define PYCBC_ULL_TO_DICT_IMPL(ARGS, NAME, KEY)       \
    if ((ARGS)->NAME) {                               \
        pycbc_set_kv_ull(dict, KEY, *((ARGS)->NAME)); \
    }

#define PYCBC_TEXT_TO_DICT(NAME) \
    PYCBC_TEXT_TO_DICT_IMPL(args, NAME, pycbc_##NAME)
#define PYCBC_ULL_TO_DICT(NAME) PYCBC_ULL_TO_DICT_IMPL(args, NAME, pycbc_##NAME)
#define PYCBC_TEXT_TO_DICT_ABBREV(NAME) PYCBC_TEXT_TO_DICT_IMPL(NAME, ABBREV)
#define PYCBC_ULL_TO_DICT_ABBREV(NAME) PYCBC_ULL_TO_DICT_IMPL(NAME, ABBREV)

PyObject* pycbc_set_tags_from_payload(pycbc_tracer_tags_t *args) {
    PyObject *dict = PyDict_New();
    PYCBC_X_LITERALTAGNAMES(
            PYCBC_TEXT_TO_DICT, PYCBC_ULL_TO_DICT, PYCBC_ULL_TO_DICT)
    return dict;
}

#define TAGS_IMPL(NAME, POSTFIX)                                   \
    if (args->NAME) {                                              \
        PyDict_SetItem(dict,                                       \
                       pycbc_##NAME,                               \
                       pycbc_set_tags_from_payload((args->NAME))); \
    }
#define PYCBC_CCBC_TO_OT_ID_IMPL(NAME, POSTFIX)               \
    if (args->NAME) {                                         \
        PYCBC_TRACING_ADD_U64(args->tags, NAME, *args->NAME); \
    }

#define TAGS(NAME) TAGS_IMPL(NAME, )
#define PYCBC_CCBC_TO_OT_ID(NAME) PYCBC_CCBC_TO_OT_ID_IMPL(NAME, )

typedef struct pycbc_tracer_span_args pycbc_tracer_span_args_t;
PyObject *pycbc_set_args_from_payload(pycbc_tracer_span_args_t *args);

PyObject* pycbc_set_args_from_payload(pycbc_tracer_span_args_t *args) {
    PyObject* dict = PyDict_New();

    PYCBC_X_SPAN_ARGS(
            PYCBC_TEXT_TO_DICT, PYCBC_ULL_TO_DICT, TAGS, PYCBC_CCBC_TO_OT_ID);
    return dict;
}

struct pycbc_tracer_payload {
    pycbc_tracer_span_args_t *span_start_args;
    pycbc_tracer_finish_args_t *span_finish_args;
    pycbc_tracer_payload_t *next;
};

PyObject* pycbc_set_finish_args_from_payload(pycbc_tracer_finish_args_t *args) {
    PyObject* dict = PyDict_New();
    PYCBC_X_FINISH_ARGS(TEXT,PYCBC_ULL_TO_DICT)
    return dict;
}
char *pycbc_get_string_tag_basic(pycbc_stack_context_handle context,
                                 const char *tagname)
{
    char *component = NULL;
    size_t length = 0;
    if (context && context->span) {
        lcbtrace_span_get_tag_str(context->span, tagname, &component, &length);
        component = realloc(component, length + 1);
        component[length] = '\0';
    }
    return component;
}

char *pycbc_dupe_string_tag(const lcbtrace_SPAN *span,
                            const char *tagname,
                            char **target_orig)
{
    size_t nbuf = 0;
    char* target_temp = target_orig?*target_orig:NULL;
    char **target = target_orig?target_orig:&target_temp;
    lcb_error_t result = lcbtrace_span_get_tag_str(
            (lcbtrace_SPAN *)span, tagname, target, &nbuf);
    if (result == LCB_SUCCESS && target && *target && nbuf && **target) {
        char *old_target = *target;
        PYCBC_DEBUG_LOG("Looking for tagname %s from %p, got something: [%.*s]",
                        tagname,
                        span,
                        (int)nbuf,
                        *target);
#ifdef PYCBC_SILIST
        *target = PYCBC_CALLOC_TYPED(nbuf + 1, char);
        memcpy(*target, old_target, nbuf);
#else
        *target = realloc(old_target, nbuf + 1);
#endif

        (*target)[nbuf] = '\0';
        PYCBC_DEBUG_LOG("Looking for tagname %s from %p, got something: [%s]",
                        tagname,
                        span,
                        *target);
    } else {
        if (target) {
            *target = NULL;
        }
        PYCBC_DEBUG_LOG(
                "Looking for tagname %s from %p, got nothing", tagname, span);
    }

    return target ? *target : NULL;
}

#ifdef PYCBC_DEBUG

char *pycbc_dupe_string_tag_debug(const char *FILE,
                                  const char *FUNC,
                                  int LINE,
                                  const lcbtrace_SPAN *span,
                                  const char *tagname,
                                  char **target_orig)
{
    char *result;
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            FILE,
            FUNC,
            LINE,
            "Span %p: duping tag %s to %p",
            span,
            tagname,
            target_orig);
    result = pycbc_dupe_string_tag(span, tagname, target_orig);
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            FILE,
            FUNC,
            LINE,
            "Span %p: duped tag %s to %p: %s",
            span,
            tagname,
            target_orig,
            result ? result : "Nothing");

    return result;
}

#define PYCBC_DUPE_STRING_TAG(SPAN, TAGNAME, TARGET_ORIG) \
    pycbc_dupe_string_tag_debug(                          \
            __FILE__, __FUNCTION_NAME__, __LINE__, SPAN, TAGNAME, TARGET_ORIG)
#else
#define PYCBC_DUPE_STRING_TAG(SPAN, TAGNAME, TARGET_ORIG) \
    pycbc_dupe_string_tag(SPAN,TAGNAME,TARGET_ORIG)
#endif

void pycbc_forward_string_tag(const lcbtrace_SPAN *span,
                              PyObject *dict,
                              PyObject *key,
                              const char *tagname)
{
    char *dest = NULL;
    PYCBC_DUPE_STRING_TAG(span, tagname, &dest);
    PYCBC_DEBUG_LOG("Got tagname %s from %p, result %s",
                    tagname,
                    span,
                    dest ? dest : "NOTHING");
    if (dest) {
        pycbc_set_dict_kv_object(dict, key, dest);
        PYCBC_FREE(dest);
    }
}

char *pycbc_get_string_tag(const lcbtrace_SPAN *span, const char *tagname)
{
    char *dest = NULL;
    dest = PYCBC_DUPE_STRING_TAG(span, tagname, &dest);
    return dest;
}

#ifdef PYCBC_DEBUG
char *pycbc_get_string_tag_debug(const char *FILE,
                                 const char *FUNC,
                                 int LINE,
                                 const lcbtrace_SPAN *span,
                                 const char *tagname)
{
    char *dest = NULL;
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            FILE, FUNC, LINE, "Span %p: Get String Tag %s", span, tagname)
    dest = pycbc_dupe_string_tag_debug(FILE, FUNC, LINE, span, tagname, &dest);
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            FILE,
            FUNC,
            LINE,
            "Span %p: Got String Tag %s: %s",
            span,
            tagname,
            dest ? dest : "Nothing")
    return dest;
}

#define PYCBC_GET_STRING_TAG(SPAN, TAGNAME) \
    pycbc_get_string_tag_debug(             \
            __FILE__, __FUNCTION_NAME__, __LINE__, SPAN, TAGNAME)
#else
#define PYCBC_GET_STRING_TAG(SPAN, TAGNAME) \
    pycbc_get_string_tag(SPAN, TAGNAME)
#endif

lcb_uint64_t pycbc_get_uint64_tag(const lcbtrace_SPAN *span,
                                  const char *tagname,
                                  lcb_error_t *result)
{
    lcb_uint64_t value = 0;
    (*result) = lcbtrace_span_get_tag_uint64(
            (lcbtrace_SPAN *)span, tagname, &value);
    return value;
}

void pycbc_dupe_uint64_tag(const lcbtrace_SPAN *span,
                           const char *tagname,
                           lcb_uint64_t **latency)
{
    lcb_uint64_t value;
    lcb_error_t result = lcbtrace_span_get_tag_uint64(
            (lcbtrace_SPAN *)span, tagname, &value);
    if (result == LCB_SUCCESS) {
        *latency = PYCBC_MALLOC_TYPED(1, uint64_t);
        **(latency) = value;
    }
}

void pycbc_forward_uint64_tag(const lcbtrace_SPAN *span,
                              PyObject *dict,
                              PyObject *key,
                              const char *tagname)
{
    lcb_uint64_t *value = NULL;
    pycbc_dupe_uint64_tag(span, tagname, &value);
    if (value) {
        pycbc_set_kv_ull(dict, key, *value);
        PYCBC_FREE(value);
    }
}

void pycbc_print_aggregate(PyObject *dict,
                           char *FIRST,
                           char *SECOND,
                           PyObject *key)
{
#define PYCBC_TRACING_BUFSZ 1000
        char buf[PYCBC_TRACING_BUFSZ] = {0};
        snprintf(buf,
                 PYCBC_TRACING_BUFSZ,
                 "%s:%s",
                 FIRST ? FIRST : "Nothing",
                 SECOND ? SECOND : "Nothing");
        pycbc_set_dict_kv_object(dict, key, buf);
        PYCBC_FREE(FIRST);
        PYCBC_FREE(SECOND);
}

lcb_uint64_t pycbc_get_timeout(pycbc_Bucket *bucket, int timeout_type)
{
    lcb_uint32_t timeout = 0;
    lcb_cntl(bucket->instance, LCB_CNTL_GET, timeout_type, &timeout);
    return timeout;
}

PyObject *pycbc_set_args_from_payload_abbreviated(lcbtrace_SPAN *span,
                                                  pycbc_Bucket *bucket)
{
    PyObject *dict = PyDict_New();

#define FORWARD_TAG(NAME, VALUE) \
    pycbc_forward_string_tag(    \
            span, dict, PYCBC_ABBREV_NAME(NAME), LCBTRACE_TAG_##VALUE);
#define FORWARD_TAG_ULL(NAME, VALUE) \
    pycbc_forward_uint64_tag(        \
            span, dict, PYCBC_ABBREV_NAME(NAME), LCBTRACE_TAG_##VALUE);
#define FORWARD_OP_NAME (char *)PYCBC_DUPE_STRING_TAG(span, "opcode", NULL)
#define FORWARD_AGGREGATE(NAME, TAG_VALUE, SECOND) \
    pycbc_print_aggregate(dict, (TAG_VALUE), (SECOND), PYCBC_ABBREV_NAME(NAME));

#define FORWARD_TIMEOUT(NAME)                 \
    pycbc_set_kv_ull(dict,                    \
                     PYCBC_ABBREV_NAME(NAME), \
                     pycbc_get_timeout(bucket, LCB_CNTL_OP_TIMEOUT));

#define TAG_VALUE(NAME) pycbc_get_string_tag(span, LCBTRACE_TAG_##NAME)
#define FORWARD_TAG_ULL_NOPREFIX(NAME, VALUE) \
    pycbc_forward_uint64_tag(span, dict, PYCBC_ABBREV_NAME(NAME), VALUE);
#ifdef PYCBC_GEN_OP_CONTEXT
    PYCBC_X_TAG_TO_OP_CONTEXT(FORWARD_TAG,
                              FORWARD_TAG_ULL,
                              FORWARD_TIMEOUT,
                              FORWARD_OP_NAME,
                              FORWARD_AGGREGATE,
                              TAG_VALUE,
                              FORWARD_TAG_ULL_NOPREFIX)


#else
    pycbc_print_aggregate(
            dict,
            (PYCBC_GET_STRING_TAG(span, "couchbase.service")),
            ((char *)PYCBC_DUPE_STRING_TAG(span, "opcode", ((void *)0))),
            pycbc_s_abbrev);
    pycbc_forward_string_tag(span, dict, pycbc_c_abbrev, "couchbase.local_id");
    pycbc_forward_uint64_tag(span, dict, pycbc_i_abbrev, "couchbase.orig_id");
    pycbc_forward_string_tag(span, dict, pycbc_b_abbrev, "db.instance");
    pycbc_forward_string_tag(span, dict, pycbc_l_abbrev, "local.address");
    pycbc_forward_string_tag(span, dict, pycbc_r_abbrev, "peer.address");
    pycbc_set_kv_ull(dict, pycbc_t_abbrev, pycbc_get_timeout(bucket, 0x00));

#endif


#undef FORWARD_TAG_ULL_NOPREFIX
#undef PYCBC_TRACING_BUFSZ
#undef FORWARD_TAG
#undef FORWARD_TAG_ULL
#undef FORWARD_TIMEOUT
#undef FORWARD_OP_NAME
#undef FORWARD_AGGREGATE
#undef TAG_VALUE
    return dict;
}

void pycbc_propagate_tag_str(const lcbtrace_SPAN *span,
                             lcbtrace_SPAN *dest,
                             const char *tagname)
{
    char *orig = PYCBC_GET_STRING_TAG(dest, tagname);
    char *value = NULL;
    if (orig) {
        return;
    }
    while (span){
        PYCBC_DEBUG_LOG("Span %p: Get better value than %s",
                        span,
                        value ? value : "Nothing");
        {
            char *better_value = PYCBC_GET_STRING_TAG(span, tagname);
            if (better_value) {
                PYCBC_DEBUG_LOG("Span %p: Got better value %s", span, better_value);
                PYCBC_FREE(value);
                value = better_value;
            }
            PYCBC_DEBUG_LOG("Span %p: looking for parent", span)
            span = lcbtrace_span_get_parent((lcbtrace_SPAN *) span);
        }
    }
    PYCBC_DEBUG_LOG("Span %p: finished searching, got %s, propagating to %p",
                    span,
                    value ? value : "Nothing",
                    dest)
    if (value) {
        lcbtrace_span_add_tag_str(dest, tagname, value);
        PYCBC_FREE(value);
    }
    PYCBC_FREE(orig);
}

#ifdef PYCBC_DEBUG
void pycbc_propagate_tag_str_debug(const char *FILE,
                                   const char *FUNC,
                                   int LINE,
                                   const lcbtrace_SPAN *span,
                                   lcbtrace_SPAN *dest,
                                   const char *tagname)
{
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(
            FILE,
            FUNC,
            LINE,
            "Propagate tag %s from span %p to span %p",
            tagname,
            span,
            dest)
    pycbc_propagate_tag_str(span, dest, tagname);
}

#define PYCBC_PROPAGATE_TAG_STR(SPAN, DEST, TAGNAME) \
    pycbc_propagate_tag_str_debug(                   \
            __FILE__, __FUNCTION_NAME__, __LINE__, SPAN, DEST, TAGNAME)
#else
#define PYCBC_PROPAGATE_TAG_STR(SPAN, DEST, TAGNAME) \
    pycbc_propagate_tag_str(                   \
            SPAN, DEST, TAGNAME)
#endif

void pycbc_propagate_tag_ull(const lcbtrace_SPAN *span,
                             lcbtrace_SPAN *dest,
                             const char *tagname)
{
    lcb_error_t result = LCB_NOT_STORED;
    pycbc_get_uint64_tag(dest, tagname, &result);
    if (result != LCB_SUCCESS) {
        lcb_uint64_t value = pycbc_get_uint64_tag(span, tagname, &result);
        if (result == LCB_SUCCESS) {
            lcbtrace_span_add_tag_uint64(dest, tagname, value);
        }
    }
}

void pycbc_forward_opname_to_tag(lcbtrace_SPAN *span, lcbtrace_SPAN *dest) {
#define PYCBC_TRACING_BUFSZ 1000
    char opcode[PYCBC_TRACING_BUFSZ] = {0};
    char *opcode_pointer = opcode;
    size_t size = PYCBC_TRACING_BUFSZ;
#undef PYCBC_TRACING_BUFSZ
    lcb_error_t opcode_res =
            lcbtrace_span_get_tag_str(span, "opcode", &opcode_pointer, &size);
    PYCBC_DEBUG_LOG("got opcode %p", opcode)
    if (opcode_res || !opcode_pointer || !*opcode_pointer || !size) {
        opcode_pointer = (char *)lcbtrace_span_get_operation(span);
        PYCBC_DEBUG_LOG("got no opcode tag, got opname instead: %s",
                        opcode_pointer ? opcode_pointer : "NOTHING")
        size = opcode_pointer ? strlen(opcode_pointer) : 0;
    }

    if (opcode_pointer && *opcode_pointer && size) {
        lcbtrace_span_add_tag_str(dest, "opcode", opcode_pointer);
    }
}

void pycbc_forward_opid_to_tag(lcbtrace_SPAN *span, lcbtrace_SPAN *dest, const char *tagname) {
    lcb_uint64_t value;
    lcb_error_t lcb_result=lcbtrace_span_get_tag_uint64(span, tagname, &value);
    if (lcb_result)
    {
        value=lcbtrace_span_get_span_id(span);
    }
    lcbtrace_span_add_tag_uint64(dest, tagname, value );
}

void pycbc_propagate_context_info(lcbtrace_SPAN *span,
                                  lcbtrace_SPAN *dest)
{
#define FORWARD_TAG(NAME, VALUE) \
    PYCBC_PROPAGATE_TAG_STR(span, dest, LCBTRACE_TAG_##VALUE);
#define FORWARD_TAG_ULL(NAME, VALUE) \
    pycbc_propagate_tag_ull(span, dest, LCBTRACE_TAG_##VALUE);
#define FORWARD_OP_NAME(NAME)  pycbc_forward_opname_to_tag(span, dest);
#define FORWARD_AGGREGATE(NAME, FIRST, SECOND) \
    pycbc_propagate_tag_str(span, dest, FIRST);  SECOND(NAME)

#define FORWARD_TIMEOUT(NAME)
#define TAG_VALUE(NAME) LCBTRACE_TAG_##NAME
#define FORWARD_OP_ID(NAME, VALUE)            pycbc_forward_opid_to_tag(span, dest, VALUE);

#ifdef PYCBC_GEN_OP_CONTEXT
    PYCBC_X_TAG_TO_OP_CONTEXT(FORWARD_TAG,
                              FORWARD_TAG_ULL,
                              FORWARD_TIMEOUT,
                              FORWARD_OP_NAME,
                              FORWARD_AGGREGATE,
                              TAG_VALUE,
                              FORWARD_OP_ID)

#else

    PYCBC_PROPAGATE_TAG_STR(span, dest, "couchbase.service");
    pycbc_forward_opname_to_tag(span, dest);
    PYCBC_PROPAGATE_TAG_STR(span, dest, "couchbase.local_id");
    pycbc_forward_opid_to_tag(span, dest, "couchbase.orig_id");
    PYCBC_PROPAGATE_TAG_STR(span, dest, "db.instance");
    PYCBC_PROPAGATE_TAG_STR(span, dest, "local.address");
    PYCBC_PROPAGATE_TAG_STR(span, dest, "peer.address");

#endif
#undef FORWARD_TAG
#undef FORWARD_TAG_ULL
#undef FORWARD_TIMEOUT
#undef FORWARD_OP_NAME
#undef FORWARD_AGGREGATE
#undef TAG_VALUE
}

#undef TAGS

#undef PYCBC_ULL_TO_DICT
#undef PYCBC_TEXT_TO_DICT

#define PYCBC_TEXT_FREE(NAME)           \
    if (args->NAME) {                   \
        PYCBC_FREE((void *)args->NAME); \
        args->NAME = NULL;              \
    }
#define PYCBC_ULL_FREE(NAME)            \
    if (args->NAME) {                   \
        PYCBC_FREE((void *)args->NAME); \
        args->NAME = NULL;              \
    }
void pycbc_span_tags_args_dealloc(pycbc_tracer_tags_t* args) {
    PYCBC_DEBUG_LOG("deallocing span tags args %p", args);

#ifdef PYCBC_GEN_ARGS
    PYCBC_X_LITERALTAGNAMES(PYCBC_TEXT_FREE, PYCBC_ULL_FREE, PYCBC_ULL_FREE)
#else
    if (args->DB_TYPE) {
        free((void *)args->DB_TYPE);
        args->DB_TYPE = ((void *)0);
    }
    if (args->PEER_LATENCY) {
        free((void *)args->PEER_LATENCY);
        args->PEER_LATENCY = ((void *)0);
    }
    if (args->OPERATION_ID) {
        free((void *)args->OPERATION_ID);
        args->OPERATION_ID = ((void *)0);
    }
    if (args->SERVICE) {
        free((void *)args->SERVICE);
        args->SERVICE = ((void *)0);
    }
    if (args->COMPONENT) {
        free((void *)args->COMPONENT);
        args->COMPONENT = ((void *)0);
    }
    if (args->PEER_ADDRESS) {
        free((void *)args->PEER_ADDRESS);
        args->PEER_ADDRESS = ((void *)0);
    }
    if (args->LOCAL_ADDRESS) {
        free((void *)args->LOCAL_ADDRESS);
        args->LOCAL_ADDRESS = ((void *)0);
    }
    if (args->DB_INSTANCE) {
        free((void *)args->DB_INSTANCE);
        args->DB_INSTANCE = ((void *)0);
    }
    if (args->child_of) {
        free((void *)args->child_of);
        args->child_of = ((void *)0);
    }
    if (args->id) {
        free((void *)args->id);
        args->id = ((void *)0);
    }
#endif
    PYCBC_FREE(args);
}

#define PYCBC_TAGS_FREE(NAME)                     \
    if (args->NAME) {                             \
        pycbc_span_tags_args_dealloc(args->NAME); \
        args->NAME = NULL;                        \
    }
void pycbc_span_args_dealloc(pycbc_tracer_span_args_t *args) {
    PYCBC_DEBUG_LOG("deallocing span args %p", args);

#ifdef PYCBC_GEN_SPAN_ARGS
    PYCBC_X_SPAN_ARGS(
            PYCBC_TEXT_FREE, PYCBC_ULL_FREE, PYCBC_TAGS_FREE, PYCBC_ULL_FREE)
#else
    if (args->operation_name) {
        PYCBC_FREE((void *)args->operation_name);
        args->operation_name = ((void *)0);
    }
    if (args->start_time) {
        PYCBC_FREE((void *)args->start_time);
        args->start_time = ((void *)0);
    }
    if (args->child_of) {
        PYCBC_FREE((void *)args->child_of);
        args->child_of = ((void *)0);
    }
    if (args->id) {
        PYCBC_FREE((void *)args->id);
        args->id = ((void *)0);
    }
    if (args->tags) {
        pycbc_span_tags_args_dealloc(args->tags);
        args->tags = ((void *)0);
    }
#endif
    PYCBC_FREE(args);
}
void pycbc_span_finish_args_dealloc(struct pycbc_tracer_finish_args *args) {
    PYCBC_DEBUG_LOG("deallocing finish args %p", args);

#ifdef PYCBC_GEN_ARGS
    PYCBC_X_FINISH_ARGS(PYCBC_TEXT_FREE, PYCBC_ULL_FREE);
#endif
    PYCBC_FREE(args);
}
#undef PYCBC_TEXT_FREE
#undef PYCBC_ULL_FREE
#undef PYCBC_X_FINISH_ARGS
#undef PYCBC_X_SPAN_ARGS



typedef struct pycbc_tracer_state {
    pycbc_tracer_payload_t *root;
    pycbc_tracer_payload_t *last;
    PyObject* parent;
    PyObject *start_span_method;
    lcbtrace_TRACER *child;
    PyObject *id_map;
} pycbc_tracer_state;

void pycbc_init_span_args(pycbc_tracer_payload_t *payload)
{
    payload->span_start_args = PYCBC_CALLOC_TYPED(1, pycbc_tracer_span_args_t);
    payload->span_start_args->tags = PYCBC_CALLOC_TYPED(1, pycbc_tracer_tags_t);
    payload->span_finish_args =
            PYCBC_CALLOC_TYPED(1, pycbc_tracer_finish_args_t);
}

void pycbc_payload_dealloc(pycbc_tracer_payload_t *pPayload)
{
    PYCBC_DEBUG_LOG("deallocing Payload %p", pPayload);
    pycbc_span_args_dealloc(pPayload->span_start_args);
    pycbc_span_finish_args_dealloc(pPayload->span_finish_args);
    PYCBC_FREE(pPayload);
}

void pycbc_Tracer_enqueue_payload(pycbc_tracer_state *state,
                                  pycbc_tracer_payload_t *payload)
{
    if (state->last) {
        state->last->next = payload;
    }
    state->last = payload;
    if (state->root == NULL) {
        state->root = payload;
    }
}

void pycbc_span_report(lcbtrace_TRACER *tracer, lcbtrace_SPAN *span)
{
    pycbc_tracer_state *state = NULL;
    pycbc_tracer_payload_t *payload;
    lcbtrace_SPAN *parent = lcbtrace_span_get_parent((lcbtrace_SPAN *)span);

    if (tracer == NULL) {
        return;
    }

    state = tracer->cookie;
    if (state == NULL) {
        return;
    }

    if (state->child) {
        state->child->v.v0.report(state->child, span);
    }

    pycbc_propagate_context_info(span, parent);
    if (!state->parent) {
        return;
    }

    payload = pycbc_persist_span(span);
    pycbc_Tracer_enqueue_payload(state, payload);
}

pycbc_tracer_payload_t *pycbc_persist_span(lcbtrace_SPAN *span)
{
    lcbtrace_SPAN *parent = lcbtrace_span_get_parent((lcbtrace_SPAN *)span);

    pycbc_tracer_payload_t *payload =
            PYCBC_CALLOC_TYPED(1, pycbc_tracer_payload_t);
    pycbc_init_span_args(payload);
    {
        pycbc_tracer_span_args_t *span_args = payload->span_start_args;
        pycbc_tracer_tags_t *tags_p = span_args->tags;
        pycbc_tracer_finish_args_t *span_finish_args =
                payload->span_finish_args;
        PYCBC_DEBUG_LOG("got span %p", span);

        PYCBC_TRACING_ADD_TEXT(
                span_args,
                operation_name,
                lcbtrace_span_get_operation((lcbtrace_SPAN *)span));
        if (parent) {
            PYCBC_TRACING_ADD_U64(
                    span_args, child_of, lcbtrace_span_get_trace_id(parent));
        }
        PYCBC_TRACING_ADD_U64(span_args,
                              id,
                              lcbtrace_span_get_span_id((lcbtrace_SPAN *)span));
        PYCBC_TRACING_ADD_U64(
                span_finish_args,
                finish_time,
                lcbtrace_span_get_finish_ts((lcbtrace_SPAN *)span));
        PYCBC_TRACING_ADD_U64(
                span_args,
                start_time,
                lcbtrace_span_get_start_ts((lcbtrace_SPAN *)span));
        {
#define TEXT_TO_PAYLOAD(tagname) \
    PYCBC_DUPE_STRING_TAG(span, LCBTRACE_TAG_##tagname, &tags_p->tagname);
#define ULL_TO_PAYLOAD(tagname) \
    pycbc_dupe_uint64_tag(span, LCBTRACE_TAG_##tagname, &tags_p->tagname);
#define IDNUM_TO_DUMMY(tagname)

#ifdef PYCBC_GEN_TAGS
            PYCBC_X_LITERALTAGNAMES(
                    TEXT_TO_PAYLOAD, ULL_TO_PAYLOAD, IDNUM_TO_DUMMY)
#else
            PYCBC_DUPE_STRING_TAG(span, "db.type", &tags_p->DB_TYPE);
            pycbc_dupe_uint64_tag(span, "peer.latency", &tags_p->PEER_LATENCY);
            pycbc_dupe_uint64_tag(
                    span, "couchbase.operation_id", &tags_p->OPERATION_ID);
            PYCBC_DUPE_STRING_TAG(span, "couchbase.service", &tags_p->SERVICE);
            PYCBC_DUPE_STRING_TAG(span, "component", &tags_p->COMPONENT);
            PYCBC_DUPE_STRING_TAG(span, "peer.address", &tags_p->PEER_ADDRESS);
            PYCBC_DUPE_STRING_TAG(
                    span, "local.address", &tags_p->LOCAL_ADDRESS);
            PYCBC_DUPE_STRING_TAG(span, "db.instance", &tags_p->DB_INSTANCE);
#endif
        }
    }

    return payload;
}

PyObject *pycbc_tracer_payload_start_span_args(
        const pycbc_tracer_payload_t *payload)
{
    return pycbc_set_args_from_payload(payload->span_start_args);
}

void pycbc_Tracer_span_finish(const pycbc_tracer_payload_t *payload,
                              const pycbc_tracer_state *state,
                              PyObject *fresh_span);
pycbc_tracer_payload_t *pycbc_Tracer_propagate_span(
        pycbc_Tracer_t *tracer, pycbc_tracer_payload_t *payload)
{
    pycbc_tracer_state *state = (pycbc_tracer_state *) tracer->tracer->cookie;
    PyObject *ptype = NULL, *pvalue = NULL, *ptraceback = NULL;
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);
    pycbc_assert(state->parent);
    if (state->start_span_method && PyObject_IsTrue(state->start_span_method)) {
        PyObject *start_span_args =
                pycbc_tracer_payload_start_span_args(payload);
        if (payload->span_start_args->child_of) {
            PyObject *key = PyLong_FromUnsignedLongLong(
                    *payload->span_start_args->child_of);
            PyObject *parent_span = PyDict_GetItem(state->id_map, key);
            Py_DecRef(key);
            if (parent_span) {
                PyDict_SetItem(start_span_args, pycbc_child_of, parent_span);
            } else {
                // PYCBC_DEBUG_LOG("requeueing %p", payload);
                // return payload;
            }

        }
        PYCBC_DEBUG_PYFORMAT("calling start method: %R ( %R )",
                             state->start_span_method,
                             start_span_args);

        {
            PyObject *fresh_span = PyObject_Call(state->start_span_method,
                                                 pycbc_DummyTuple,
                                                 start_span_args);
            if (fresh_span) {
                pycbc_Tracer_span_finish(payload, state, fresh_span);

            } else {
                PYCBC_DEBUG_LOG("Yielded no span!");
            }
        }
        PYCBC_EXCEPTION_LOG;
        PYCBC_DECREF(start_span_args);

    }
    if (ptype || pvalue || ptraceback) {
        PyErr_Restore(ptype, pvalue, ptraceback);
    }
    return NULL;
}

void pycbc_Tracer_span_finish(const pycbc_tracer_payload_t *payload,
                              const pycbc_tracer_state *state,
                              PyObject *fresh_span)
{
    PyObject *key = PyLong_FromUnsignedLongLong(*payload->span_start_args->id);
    PyObject *finish_method;
    PyDict_SetItem(state->id_map, key, fresh_span);
    Py_DecRef(key);
    finish_method = PyObject_GetAttrString(fresh_span, "finish");
    PYCBC_DEBUG_PYFORMAT("Got span'[%R]", fresh_span);
    pycbc_assert(finish_method);
    PYCBC_DEBUG_PYFORMAT("Got finish method'[%R]", finish_method);
    if (finish_method) {
        PyObject *span_finish_args =
                pycbc_set_finish_args_from_payload(payload->span_finish_args);
        PYCBC_DEBUG_PYFORMAT("calling finish method with;[%R]",
                             span_finish_args);
        PyObject_Call(finish_method, pycbc_DummyTuple, span_finish_args);
        PYCBC_EXCEPTION_LOG;
        PYCBC_XDECREF(span_finish_args);
    }

    PYCBC_XDECREF(finish_method);
    PYCBC_DECREF(fresh_span);
}

void pycbc_tracer_flush(pycbc_Tracer_t *tracer)
{
    pycbc_tracer_state *state = NULL;
    if (tracer == NULL) {
        return;
    }
    if (tracer->tracer == NULL) {
        return;
    }
    state = tracer->tracer->cookie;
    if (state == NULL) {
        return;
    }
    if (state->root == NULL || !state->root->span_start_args) {
        return;
    }
    {
        pycbc_tracer_payload_t *ptr = state->root;
        PYCBC_DEBUG_LOG("flushing");
        while (ptr) {
            pycbc_tracer_payload_t *tmp = ptr;
            ptr = ptr->next;
            if (state->parent) {
                pycbc_Tracer_propagate_span(tracer, tmp);
            }
            pycbc_payload_dealloc(tmp);
        }
    }
    state->root = state->last = NULL;
}

void pycbc_Tracer_propagate(pycbc_Tracer_t *tracer)
{
    PYCBC_DEBUG_LOG("flushing pycbc_Tracer_t %p", tracer);
    pycbc_tracer_flush(tracer);
}

void pycbc_tracer_destructor(lcbtrace_TRACER *tracer)
{
    if (tracer) {
        pycbc_tracer_state *state = tracer->cookie;
        PYCBC_DEBUG_LOG("freeing lcbtrace_TRACER %p", tracer);
        if (state) {
            Py_XDECREF(state->parent);
            Py_XDECREF(state->id_map);
            Py_XDECREF(state->start_span_method);
            PYCBC_FREE(state);
            tracer->cookie = NULL;
        }
        PYCBC_FREE(tracer);
    }
}

lcbtrace_TRACER *pycbc_tracer_new(PyObject *parent,
                                  lcbtrace_TRACER *child_tracer)
{
    lcbtrace_TRACER *tracer = PYCBC_CALLOC_TYPED(1, lcbtrace_TRACER);
    pycbc_tracer_state *pycbc_tracer =
            PYCBC_CALLOC_TYPED(1, pycbc_tracer_state);

    tracer->destructor = pycbc_tracer_destructor;
    tracer->flags = 0;
    tracer->version = 0;
    tracer->v.v0.report = pycbc_span_report;
    pycbc_tracer->root = NULL;
    pycbc_tracer->last = NULL;
    tracer->cookie = pycbc_tracer;
    pycbc_tracer->id_map = PyDict_New();
    pycbc_tracer->child = child_tracer;

    if (parent) {
        PYCBC_DEBUG_PYFORMAT("initialising tracer start_span method from:[%R]",
                             parent);
        pycbc_tracer->start_span_method = PyObject_GetAttrString(parent, "start_span");
        if (!PyErr_Occurred() && pycbc_tracer->start_span_method) {
            PYCBC_DEBUG_PYFORMAT("got start_span method:[%R]",
                                 pycbc_tracer->start_span_method);
            pycbc_tracer->parent = parent;
            PYCBC_INCREF(parent);
        }
        else
        {
            PYCBC_EXCEPTION_LOG_NOCLEAR;
            PYCBC_DEBUG_LOG("Falling back to internal tracing only");
            pycbc_tracer->parent = NULL;
        }
    }
    return tracer;
}

void pycbc_Tracer_set_child(pycbc_Tracer_t *py_Tracer,
                            lcbtrace_TRACER *child_tracer)
{
    ((pycbc_tracer_state *)(py_Tracer->tracer->cookie))->child = child_tracer;
}

static PyObject *Tracer_parent(pycbc_Tracer_t *self, void *unused)
{
    pycbc_tracer_state *tracer_state =
            (self && self->tracer) ? (pycbc_tracer_state *)self->tracer->cookie
                                   : NULL;
    pycbc_assert(tracer_state);
    {
        PyObject *result = pycbc_none_or_value(tracer_state->parent);
        PYCBC_INCREF(result);
        return result;
    }
}

static int Tracer__init__(pycbc_Tracer_t *self,
                          PyObject *args,
                          PyObject *kwargs)
{
    int rv = 0;
    PyObject *tracer = PyTuple_GetItem(args, 0);
    PyObject *threshold_tracer_capsule = PyTuple_GetItem(args, 1);
    PyObject *parent = pycbc_null_or_value(tracer);
    lcbtrace_TRACER *child_tracer =
            (lcbtrace_TRACER *)pycbc_null_or_capsule_value(
                    threshold_tracer_capsule, "threshold_tracer");
    self->tracer = pycbc_tracer_new(parent, child_tracer);

    PYCBC_EXCEPTION_LOG_NOCLEAR;
    return rv;
}

static void Tracer_dtor(pycbc_Tracer_t *self)
{
    PYCBC_DEBUG_LOG("Destroying pycbc_Tracer_t");
    pycbc_tracer_flush(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyGetSetDef pycbc_Tracer_TABLE_getset[] = {
        { "parent",
                (getter)Tracer_parent,
                NULL,
                PyDoc_STR("Optional parent tracer to propagate spans to.\n")
        },
        { NULL }
};

static struct PyMemberDef pycbc_Tracer_TABLE_members[] = {
        { NULL }
};

static PyMethodDef pycbc_Tracer_TABLE_methods[] = {

#define OPFUNC(name, doc) \
{ #name, (PyCFunction)pycbc_Tracer_##name, METH_VARARGS|METH_KEYWORDS, \
    PyDoc_STR(doc) }

#undef OPFUNC

        { NULL, NULL, 0, NULL }
};

int pycbc_TracerType_init(PyObject **ptr) {
    PyTypeObject *p = &pycbc_TracerType;
    *ptr = (PyObject *) p;
    if (p->tp_name) { return 0; }
    p->tp_name = "Tracer";
    p->tp_new = PyType_GenericNew;
    p->tp_init = (initproc) Tracer__init__;
    p->tp_dealloc = (destructor) Tracer_dtor;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_doc = "The Tracer Object";
    p->tp_basicsize = sizeof(pycbc_Tracer_t);
    p->tp_methods = pycbc_Tracer_TABLE_methods;
    p->tp_members = pycbc_Tracer_TABLE_members;
    p->tp_getset = pycbc_Tracer_TABLE_getset;
    pycbc_Tracer_init_constants();
    return PyType_Ready(p);
}

#endif
