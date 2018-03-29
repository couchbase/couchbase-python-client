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
#define X_PYTYPES_NOTRACING(X) \
    X(Bucket,       pycbc_BucketType_init) \
    /** Remember to keep base classes in order */ \
    X(Result,           pycbc_ResultType_init) \
    X(OperationResult,  pycbc_OperationResultType_init) \
    X(ValueResult,      pycbc_ValueResultType_init) \
    X(MultiResult,      pycbc_MultiResultType_init) \
    X(HttpResult,       pycbc_HttpResultType_init) \
    X(ViewResult,       pycbc_ViewResultType_init) \
    X(Transcoder,       pycbc_TranscoderType_init) \
    X(CryptoProvider,   pycbc_CryptoProviderType_init) \
    X(ObserveInfo,      pycbc_ObserveInfoType_init) \
    X(Item,             pycbc_ItemType_init) \
    X(Event,            pycbc_EventType_init) \
    X(IOEvent,          pycbc_IOEventType_init) \
    X(TimerEvent,       pycbc_TimerEventType_init) \
    X(AsyncResult,      pycbc_AsyncResultType_init) \
    X(_IOPSWrapper,     pycbc_IOPSWrapperType_init) \
    X(_SDResult,        pycbc_SDResultType_init)
#ifdef PYCBC_TRACING
#define X_PYTYPES(X) \
    X_PYTYPES_NOTRACING(X) \
    X(Tracer,           pycbc_TracerType_init)
#else
#define X_PYTYPES(X) \
    X_PYTYPES_NOTRACING(X)
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
    PyObject *tmp;
    PyObject *kwargs;
    va_list vacp;

    if (!pycbc_log_handler) {
        return;
    }

    gil_prev = PyGILState_Ensure();

    kwargs = PyDict_New();
    va_copy(vacp, ap);
    tmp = PyUnicode_FromFormatV(fmt, vacp);
    va_end(ap);

    PyDict_SetItemString(kwargs, "message", tmp); Py_DECREF(tmp);

    tmp = pycbc_IntFromL(iid);
    PyDict_SetItemString(kwargs, "id", tmp); Py_DECREF(tmp);

    tmp = pycbc_IntFromL(severity);
    PyDict_SetItemString(kwargs, "level", tmp); Py_DECREF(tmp);

    tmp = Py_BuildValue("(s,i)", srcfile, srcline);
    PyDict_SetItemString(kwargs, "c_src", tmp); Py_DECREF(tmp);

    tmp = pycbc_SimpleStringZ(subsys);
    PyDict_SetItemString(kwargs, "subsys", tmp); Py_DECREF(tmp);

    PyObject_Call(pycbc_log_handler, pycbc_DummyTuple, kwargs);
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



void pycbc_print_string( PyObject *curkey) {
#if PYTHON_ABI_VERSION >= 3
    {
        const char *keyname = PyUnicode_AsUTF8(curkey);
        PYCBC_DEBUG_LOG_RAW("%s",  keyname);//(int)length, keyname);
        PYCBC_EXCEPTION_LOG_NOCLEAR;
    }
#else
    {
        PYCBC_DEBUG_LOG("%s",PyString_AsString(curkey));
    };
#endif
}


void pycbc_print_repr( PyObject *pobj) {
    PyObject* x,*y,*z;
    PyErr_Fetch(&x,&y,&z);
    {
        PyObject *curkey = PyObject_Repr(pobj);
        PYCBC_EXCEPTION_LOG;
        PyErr_Restore(x, y, z);
        if (!curkey) {
            PYCBC_DEBUG_LOG("got null repr from %p", pobj);
            return;
        }
        pycbc_print_string(curkey);
        PYCBC_XDECREF(curkey);
    }
}


void pycbc_exception_log(const char* file, int line, int clear)
{

    if (PyErr_Occurred()) {
        PyObject* type, *value, *traceback;
        PYCBC_DEBUG_LOG_WITH_FILE_AND_LINE_POSTFIX(file, line, "***** EXCEPTION:[", "");
        PyErr_Fetch(&type,&value,&traceback);
        pycbc_print_repr(type);
        PYCBC_DEBUG_LOG_RAW(",");
        pycbc_print_repr(value);
        PYCBC_DEBUG_LOG_RAW(",");
        if (0){
            /* See if we can get a full traceback */
            PyObject* module_name = PyString_FromString("traceback");
            PyObject* pyth_module = PyImport_Import(module_name);
            PYCBC_DECREF(module_name);

            if (pyth_module) {
                PyObject* pyth_func = PyObject_GetAttrString(pyth_module, "format_exception");
                if (pyth_func && PyCallable_Check(pyth_func)) {
                    PyObject *pyth_val;

                    pyth_val = PyObject_CallFunctionObjArgs(pyth_func, type, value, traceback, NULL);
                    pycbc_print_string(pyth_val);
                    PYCBC_DECREF(pyth_val);
                }
            }
        }
        PYCBC_DEBUG_LOG_WITH_FILE_AND_LINE_NEWLINE(file,line, "]: END OF EXCEPTION *****");
        if (clear)
        {
            PYCBC_XDECREF(type);
            PYCBC_XDECREF(value);
            PYCBC_XDECREF(traceback);
        }
        else
        {
            PyErr_Restore(type,value,traceback);

        }
    }
}
#ifdef PYCBC_TRACING

pycbc_stack_context_handle
pycbc_Context_init(pycbc_Tracer_t *py_tracer, const char *operation, lcb_uint64_t now, lcbtrace_REF *ref, const char* component) {
    pycbc_stack_context_handle context = malloc(sizeof(pycbc_stack_context));
    pycbc_assert(py_tracer);
    context->tracer = py_tracer;
    context->span = lcbtrace_span_start(py_tracer->tracer, operation, now, ref);
    lcbtrace_span_add_tag_str(context->span, LCBTRACE_TAG_COMPONENT, component);
    PYCBC_DEBUG_LOG("Created context %p with span %p: component: %s, operation %s",context, context->span, component, operation);
    return context;
}

PyObject* pycbc_Context_finish(pycbc_stack_context_handle context )
{
    if (PYCBC_CHECK_CONTEXT(context)) {
        PYCBC_DEBUG_LOG("closing span %p",context->span);
        lcbtrace_span_finish(context->span, 0);
        (context)->span = NULL;
    };
    return Py_None;
}
pycbc_stack_context_handle pycbc_check_context(pycbc_stack_context_handle CONTEXT, const char* file, int line)
{
    if (!(CONTEXT)){
        PYCBC_DEBUG_LOG_WITH_FILE_AND_LINE_NEWLINE(file,line,"warning: got null context");
    } else if (!(CONTEXT)->tracer){\
        PYCBC_DEBUG_LOG_WITH_FILE_AND_LINE_NEWLINE(file,line,"warning: got null tracer");
    } else if (!(CONTEXT)->span){
        PYCBC_DEBUG_LOG_WITH_FILE_AND_LINE_NEWLINE(file,line,"warning: got null span");
    }
    else {
        return CONTEXT;
    }
    return NULL;
}

pycbc_stack_context_handle
pycbc_Tracer_span_start(pycbc_Tracer_t *py_tracer, PyObject *kwargs, const char *operation, lcb_uint64_t now,
                        pycbc_stack_context_handle context, lcbtrace_REF_TYPE ref_type, const char* component) {

    PyObject *tracer = kwargs?PyDict_GetItemString(kwargs, "tracer"):NULL;
    if (!(py_tracer || (tracer && PyArg_ParseTuple(tracer, "O!", &pycbc_TracerType, &py_tracer) && py_tracer)))
    {
        PYCBC_EXCEPTION_LOG;
        printf("Warning - got NULL tracer\n");
        return NULL;
    }

    if (context )
    {
        lcbtrace_REF ref;
        ref.type = ref_type;
        ref.span = context->span;
        return pycbc_Context_init(py_tracer, operation, now, &ref, component);
    }
    else
    {
        return pycbc_Context_init(py_tracer, operation, now, NULL, component);
    }
}

void pycbc_init_traced_result(pycbc_Bucket *self, PyObject* mres_dict, PyObject *curkey,
                              pycbc_stack_context_handle context) {
    pycbc_pybuffer keybuf={0};
    pycbc_Result *item;
    pycbc_tc_encode_key(self, curkey, &keybuf);
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    pycbc_tc_decode_key(self, keybuf.buffer, keybuf.length, &curkey);
    item=(pycbc_Result*)PyDict_GetItem(mres_dict, curkey);
    if (!item) {
        item = (pycbc_Result*) pycbc_valresult_new(self);
        PyDict_SetItem(mres_dict, curkey, (PyObject*)item);
        item->is_tracing_stub = 1;
    }
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    item->tracing_context = context;
    PYCBC_DEBUG_LOG_WITHOUT_NEWLINE("\nres %p: binding context %p to [", item, context);
    pycbc_print_repr(curkey);
    PYCBC_DEBUG_LOG_RAW("]\n");
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    PYCBC_DEBUG_LOG("\nPrior to insertion:[");
    pycbc_print_repr(mres_dict);
    PYCBC_DEBUG_LOG_RAW("]\n");
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    PYCBC_DEBUG_LOG_WITHOUT_NEWLINE("After insertion:[");
    pycbc_print_repr(mres_dict);
    PYCBC_DEBUG_LOG("]\n");
}


int pycbc_is_async_or_pipeline(const pycbc_Bucket *self) { return self->flags & PYCBC_CONN_F_ASYNC || self->pipeline_queue; }

void pycbc_tracer_destructor(lcbtrace_TRACER *tracer)
{
    if (tracer) {
        if (tracer->cookie) {
            free(tracer->cookie);
            tracer->cookie = NULL;
        }
        free(tracer);
    }
}

#define LOGARGS(instance, lvl) instance->settings, "bootstrap", LCB_LOG_##lvl, __FILE__, __LINE__
#endif

void pycbc_set_dict_kv_object(PyObject *dict, PyObject *key, const char* value_str) {
    PYCBC_DEBUG_LOG_WITHOUT_NEWLINE("adding [");
    pycbc_print_repr(key);
    PYCBC_DEBUG_LOG_RAW("], value %s to [", value_str);
    pycbc_print_repr(dict);
    PYCBC_DEBUG_LOG_RAW("]\n");
    {
        PyObject *value = pycbc_SimpleStringZ(value_str);
        PyDict_SetItem(dict, key, value);
        PYCBC_DECREF(value);
    }
}


void pycbc_set_kv_ull(PyObject *dict, PyObject *keystr, lcb_uint64_t parenti_id) {
    PYCBC_DEBUG_LOG_WITHOUT_NEWLINE("adding [");
    pycbc_print_repr(keystr);
    PYCBC_DEBUG_LOG_RAW("], value %" PRId64 " to [", parenti_id);
    pycbc_print_repr(dict);
    PYCBC_DEBUG_LOG_RAW("]\n");
    {
        PyObject *pULL = PyLong_FromUnsignedLongLong(parenti_id);
        PyDict_SetItem(dict, keystr, pULL);
        PYCBC_DECREF(pULL);
    }
}

#ifdef PYCBC_TRACING

#define PYCBC_X_SPAN_ARGS(TEXT,ULL,TAGS)\
    TEXT(operation_name) \
    ULL(child_of) \
    ULL(start_time) \
    TAGS(tags)

#define PYCBC_X_FINISH_ARGS(TEXT,ULL)\
    ULL(finish_time)

#define PYCBC_X_LITERALTAGNAMES(TEXT,ULL)\
    TEXT(DB_TYPE) \
    ULL(PEER_LATENCY) \
    ULL(OPERATION_ID) \
    TEXT(COMPONENT) \
    TEXT(PEER_ADDRESS) \
    TEXT(LOCAL_ADDRESS) \
    TEXT(DB_INSTANCE)
#undef X

#define X(NAME) PyObject* pycbc_##NAME;
PYCBC_X_SPAN_ARGS(X,X,X)
PYCBC_X_LITERALTAGNAMES(X,X)
PYCBC_X_FINISH_ARGS(X,X)
#undef X

PyObject* pycbc_default_key;

void pycbc_Tracer_init_constants()
{
    pycbc_default_key = pycbc_SimpleStringZ("__PYCBC_DEFAULT_KEY");

#define X(NAME) pycbc_##NAME=pycbc_SimpleStringZ(#NAME);
    PYCBC_X_SPAN_ARGS(X,X,X)
    PYCBC_X_FINISH_ARGS(X,X)
#undef X
#define X(NAME) pycbc_##NAME=pycbc_SimpleStringZ(LCBTRACE_TAG_##NAME);
    PYCBC_X_LITERALTAGNAMES(X,X);
#undef X
}

#define PYCBC_TAG_TEXT(NAME) const char* NAME;
#define PYCBC_TAG_ULL(NAME) lcb_uint64_t* NAME;

typedef struct pycbc_tracer_tags {
    PYCBC_X_LITERALTAGNAMES(PYCBC_TAG_TEXT,PYCBC_TAG_ULL)
} pycbc_tracer_tags_t;

#define PYCBC_TAG_STRUCT(NAME) pycbc_tracer_tags_t* NAME;

typedef struct pycbc_tracer_span_args {
    PYCBC_X_SPAN_ARGS(PYCBC_TAG_TEXT,PYCBC_TAG_ULL,PYCBC_TAG_STRUCT)
} pycbc_tracer_span_args_t;

typedef struct pycbc_tracer_finish_args
{
    PYCBC_X_FINISH_ARGS(PYCBC_TAG_TEXT,PYCBC_TAG_ULL)
} pycbc_tracer_finish_args_t;

#undef PYCBC_TAG_ULL
#undef PYCBC_TAG_TEXT
#undef PYCBC_TAG_STRUCT

#define PYCBC_TRACING_ADD_TEXT(DICT, KEY, VALUE) (DICT)->KEY=strdup((VALUE));
#define PYCBC_TRACING_ADD_U64(DICT, KEY, VALUE) (DICT)->KEY=malloc(sizeof(lcb_uint64_t));\
    *((DICT)->KEY)=VALUE;

#define PYCBC_TEXT_TO_DICT(NAME) if (args->NAME) { pycbc_set_dict_kv_object(dict, pycbc_##NAME, (args->NAME)); }
#define PYCBC_ULL_TO_DICT(NAME) if(args->NAME) { pycbc_set_kv_ull(dict, pycbc_##NAME, *(args->NAME)); }

PyObject* pycbc_set_tags_from_payload(pycbc_tracer_tags_t *args) {
    PyObject *dict = PyDict_New();
    PYCBC_X_LITERALTAGNAMES(PYCBC_TEXT_TO_DICT,PYCBC_ULL_TO_DICT)
    return dict;
}

#define TAGS(NAME) if(args->NAME) {PyDict_SetItem(dict, pycbc_##NAME, pycbc_set_tags_from_payload((args->NAME))); }


PyObject* pycbc_set_args_from_payload(pycbc_tracer_span_args_t *args) {
    PyObject* dict = PyDict_New();
    PYCBC_X_SPAN_ARGS(PYCBC_TEXT_TO_DICT,PYCBC_ULL_TO_DICT,TAGS)
    return dict;
}

PyObject* pycbc_set_finish_args_from_payload(pycbc_tracer_finish_args_t *args) {
    PyObject* dict = PyDict_New();
    PYCBC_X_FINISH_ARGS(TEXT,PYCBC_ULL_TO_DICT)
    return dict;
}

#undef TAGS

#undef PYCBC_ULL_TO_DICT
#undef PYCBC_TEXT_TO_DICT

#define PYCBC_TEXT_FREE(NAME) free((void*)args->NAME);
#define PYCBC_ULL_FREE(NAME) free((void*)args->NAME);
void pycbc_span_tags_args_dealloc(pycbc_tracer_tags_t* args) {
    PYCBC_X_LITERALTAGNAMES(PYCBC_TEXT_FREE, PYCBC_ULL_FREE)
    free(args);
}

#define PYCBC_TAGS_FREE(NAME) if(args->NAME) { pycbc_span_tags_args_dealloc(args->NAME); }
void pycbc_span_args_dealloc(pycbc_tracer_span_args_t *args) {
    PYCBC_X_SPAN_ARGS(PYCBC_TEXT_FREE,PYCBC_ULL_FREE, PYCBC_TAGS_FREE)
    free(args);
}

void pycbc_span_finish_args_dealloc(struct pycbc_tracer_finish_args *args) {
    PYCBC_X_FINISH_ARGS(PYCBC_TEXT_FREE, PYCBC_ULL_FREE);
    free(args);
}
#undef PYCBC_TEXT_FREE
#undef PYCBC_ULL_FREE

struct pycbc_tracer_payload;
struct pycbc_tracer_span_args;
struct pycbc_tracer_finish_args;
typedef struct pycbc_tracer_payload {
    struct pycbc_tracer_span_args* span_start_args;
    struct pycbc_tracer_finish_args* span_finish_args;
    struct pycbc_tracer_payload *next;
} pycbc_tracer_payload;


typedef struct pycbc_tracer_state {
    pycbc_tracer_payload *root;
    pycbc_tracer_payload *last;
    pycbc_Bucket* bucket;
    PyObject* parent;
    PyObject *start_span_method;
} pycbc_tracer_state;


void pycbc_init_span_args(pycbc_tracer_payload* payload)
{
    payload->span_start_args=malloc(sizeof(pycbc_tracer_span_args_t));
    memset((payload->span_start_args),0, sizeof(pycbc_tracer_span_args_t));
    payload->span_start_args->tags=malloc(sizeof(pycbc_tracer_tags_t));
    memset((payload->span_start_args->tags),0, sizeof(pycbc_tracer_tags_t));
    payload->span_finish_args=malloc(sizeof(pycbc_tracer_finish_args_t));
    memset((payload->span_finish_args),0, sizeof(pycbc_tracer_finish_args_t));
}

void pycbc_payload_dealloc(pycbc_tracer_payload *pPayload) {
    pycbc_span_args_dealloc(pPayload->span_start_args);
    pycbc_span_finish_args_dealloc(pPayload->span_finish_args);
}

void pycbc_span_report(lcbtrace_TRACER *tracer, lcbtrace_SPAN *span)
{
    pycbc_tracer_state *state = NULL;
    if (tracer == NULL) {
        return;
    }
    state = tracer->cookie;
    if (state == NULL) {
        return;
    }

    {

#define PYCBC_TRACING_BUFSZ 1000
        size_t nbuf = PYCBC_TRACING_BUFSZ;
        char bufarray[PYCBC_TRACING_BUFSZ]={0};
        char* buf = &bufarray[0];
        lcbtrace_SPAN *parent;
        lcb_uint64_t start;
        pycbc_tracer_payload *payload = calloc(1, sizeof(pycbc_tracer_payload));
        pycbc_init_span_args(payload);
        {
            pycbc_tracer_span_args_t* span_args = payload->span_start_args;
            pycbc_tracer_tags_t* tags_p = span_args->tags;
            pycbc_tracer_finish_args_t* span_finish_args = payload->span_finish_args;
            PYCBC_DEBUG_LOG("got span %p\n", span);

            buf = calloc(nbuf, sizeof(char));
            PYCBC_TRACING_ADD_TEXT(span_args, operation_name, lcbtrace_span_get_operation(span));
            parent = lcbtrace_span_get_parent(span);
            if (parent) {
                lcb_uint64_t parenti_id = lcbtrace_span_get_trace_id(parent);
                snprintf(buf, nbuf, "%" PRIx64, parenti_id);
            }
            start = lcbtrace_span_get_start_ts(span);
            PYCBC_TRACING_ADD_U64(span_finish_args, finish_time, lcbtrace_span_get_finish_ts(span));
            PYCBC_TRACING_ADD_U64(span_args, start_time, start);
            {
                nbuf = PYCBC_TRACING_BUFSZ;
                if (lcbtrace_span_get_tag_str(span, LCBTRACE_TAG_DB_TYPE, &buf, &nbuf) == LCB_SUCCESS) {
                    buf[nbuf] = '\0';
                    PYCBC_TRACING_ADD_TEXT(tags_p, DB_TYPE, buf);
                }
            }

            {
                lcb_uint64_t latency, operation_id;
                if (lcbtrace_span_get_tag_uint64(span, LCBTRACE_TAG_PEER_LATENCY, &latency) == LCB_SUCCESS) {
                    PYCBC_TRACING_ADD_U64(tags_p, PEER_LATENCY, latency);
                }
                if (lcbtrace_span_get_tag_uint64(span, LCBTRACE_TAG_OPERATION_ID, &operation_id) == LCB_SUCCESS) {
                    PYCBC_TRACING_ADD_U64(tags_p, OPERATION_ID, operation_id);
                }
                nbuf = PYCBC_TRACING_BUFSZ;
                if (lcbtrace_span_get_tag_str(span, LCBTRACE_TAG_COMPONENT, &buf, &nbuf) == LCB_SUCCESS) {
                    buf[nbuf] = '\0';
                    PYCBC_TRACING_ADD_TEXT(tags_p, COMPONENT, buf);
                }
                nbuf = PYCBC_TRACING_BUFSZ;
                if (lcbtrace_span_get_tag_str(span, LCBTRACE_TAG_PEER_ADDRESS, &buf, &nbuf) == LCB_SUCCESS) {
                    buf[nbuf] = '\0';
                    PYCBC_TRACING_ADD_TEXT(tags_p, PEER_ADDRESS, buf);
                }
                nbuf = PYCBC_TRACING_BUFSZ;
                if (lcbtrace_span_get_tag_str(span, LCBTRACE_TAG_LOCAL_ADDRESS, &buf, &nbuf) == LCB_SUCCESS) {
                    buf[nbuf] = '\0';
                    PYCBC_TRACING_ADD_TEXT(tags_p, LOCAL_ADDRESS, buf);
                }
                nbuf = PYCBC_TRACING_BUFSZ;
                if (lcbtrace_span_get_tag_str(span, LCBTRACE_TAG_DB_INSTANCE, &buf, &nbuf) == LCB_SUCCESS) {
                    buf[nbuf] = '\0';
                    PYCBC_TRACING_ADD_TEXT(tags_p, DB_INSTANCE, buf);
                }
            }
        }

        if (state->last) {
            state->last->next = payload;
        }
        state->last = payload;
        if (state->root == NULL) {
            state->root = payload;
        }
    }
#undef PYCBC_TRACING_BUFSZ
}



void pycbc_Tracer_propagate_span(pycbc_Tracer_t *tracer, struct pycbc_tracer_payload *payload) {
    pycbc_tracer_state *state = (pycbc_tracer_state *) tracer->tracer->cookie;
    pycbc_assert(state->parent);
    if (state->start_span_method && PyObject_IsTrue(state->start_span_method)) {
        PyObject* start_span_args = pycbc_set_args_from_payload(payload->span_start_args);
        PyObject *fresh_span;

        PYCBC_DEBUG_LOG_WITHOUT_NEWLINE("calling start method:[");
        pycbc_print_repr(state->start_span_method);
        PYCBC_DEBUG_LOG_RAW("]");
        PYCBC_DEBUG_LOG("\nabout to call: %p[\n", state->start_span_method);
        PYCBC_DEBUG_LOG("] on %p=[", state->parent);
        pycbc_print_repr(state->parent);
        PYCBC_DEBUG_LOG_WITHOUT_NEWLINE("full args to start_span:[");
        pycbc_print_repr(start_span_args);
        PYCBC_DEBUG_LOG_RAW("\n");

        fresh_span= PyObject_Call(state->start_span_method, pycbc_DummyTuple,
                                  start_span_args);
        if(fresh_span)
        {
            PyObject *finish_method = PyObject_GetAttrString(fresh_span, "finish");
            PYCBC_DEBUG_LOG("Got span'[");
            pycbc_print_repr(fresh_span);
            PYCBC_DEBUG_LOG("]\n");
            pycbc_assert(finish_method);
            PYCBC_DEBUG_LOG("Got finish method'[");
            pycbc_print_repr(finish_method);
            PYCBC_DEBUG_LOG("]\n");
            if (finish_method) {
                PyObject* span_finish_args = pycbc_set_finish_args_from_payload(payload->span_finish_args);
                PYCBC_DEBUG_LOG("calling finish method with;[");
                pycbc_print_repr(span_finish_args);
                PYCBC_DEBUG_LOG("]\n");
                PyObject_Call(finish_method, pycbc_DummyTuple, span_finish_args);
                PYCBC_XDECREF(span_finish_args);
            }

            PYCBC_DECREF(finish_method);
            PYCBC_DECREF(fresh_span);
        } else{
            PYCBC_DEBUG_LOG("Yielded no span!\n");
        }
        PYCBC_DECREF(start_span_args);
        PYCBC_EXCEPTION_LOG_NOCLEAR;

    }
}

void pycbc_tracer_flush(pycbc_Tracer_t *tracer)
{
    pycbc_tracer_state *state = NULL;

    if (tracer == NULL) {
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
        pycbc_tracer_payload *ptr = state->root;
        PYCBC_DEBUG_LOG("flushing\n");
        while (ptr) {
            pycbc_tracer_payload *tmp = ptr;
            if (tracer->parent) {
                pycbc_Tracer_propagate_span(tracer, tmp);
            }
            ptr = ptr->next;
            pycbc_payload_dealloc(tmp);
        }
    }
    state->root = state->last = NULL;
}


void pycbc_Tracer_propagate(  pycbc_Tracer_t *tracer) {
    pycbc_tracer_flush(tracer);
}



lcbtrace_TRACER *pycbc_tracer_new(PyObject *parent, pycbc_Bucket *bucket)
{
    lcbtrace_TRACER *tracer = calloc(1, sizeof(lcbtrace_TRACER));
    pycbc_tracer_state *pycbc_tracer = calloc(1, sizeof(pycbc_tracer_state));
    tracer->destructor = pycbc_tracer_destructor;
    tracer->flags = 0;
    tracer->version = 0;
    tracer->v.v0.report = pycbc_span_report;
    pycbc_tracer->root = NULL;
    pycbc_tracer->last = NULL;
    tracer->cookie = pycbc_tracer;
    pycbc_tracer->bucket = bucket;
    if (parent) {
        PYCBC_DEBUG_LOG("\ninitialising tracer start_span method from:[");
        pycbc_print_repr(parent);
        PYCBC_DEBUG_LOG("]\n");
        pycbc_tracer->start_span_method = PyObject_GetAttrString(parent, "start_span");
        if (pycbc_tracer->start_span_method)
        {
            PYCBC_DEBUG_LOG("got start_span method:[");
            pycbc_print_repr(pycbc_tracer->start_span_method);
            PYCBC_DEBUG_LOG("]\n");
            pycbc_tracer->parent = parent;
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

static PyGetSetDef pycbc_Tracer_TABLE_getset[] = {
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


static int
Tracer__init__(pycbc_Tracer_t *self,
               PyObject *args, PyObject* kwargs)
{
    int rv = 0;
    PyObject* tracer =PyTuple_GetItem(args, 0);
    pycbc_Bucket* bucket= (pycbc_Bucket*) PyTuple_GetItem(args,1);
    self->parent=(tracer && PyObject_IsTrue(tracer))?tracer:NULL;
    self->tracer= pycbc_tracer_new(self->parent, bucket);
    PYCBC_EXCEPTION_LOG_NOCLEAR;

    return rv;
}

static void
Tracer_dtor(pycbc_Tracer_t *self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyTypeObject pycbc_TracerType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
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
};
#endif