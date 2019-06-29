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
#ifndef PYCBC_H_
#define PYCBC_H_
/**
 * This file contains the base header for the Python Couchbase Client
 * @author Mark Nunberg
 */

#define PYCBC_TRACE_FINISH_SPANS
#define PYCBC_GLOBAL_SCHED
#define PYCBC_POSTINCREMENT
#define PYCBC_FREE_ACCOUNTING
#define PYCBC_FREE_CONTEXTS

#ifdef PYCBC_REF_ACCOUNTING_ENABLE
#define PYCBC_REF_ACCOUNTING
#endif

#define PYCBC_REF_CLEANUP_ENABLE

#ifdef PYCBC_REF_ACCOUNTING
#ifdef PYCBC_REF_CLEANUP_ENABLE
#define PYCBC_REF_CLEANUP
#endif
#endif


#define PYCBC_GC 3
#define PYCBC_AUTO_DEREF_FAILED

#include "python_wrappers.h"
#define PYCBC_COLLECTIONS

/**
 * This code supports both PYCBC V2 and PYCBC V3. The
idea is to wrap the LCB V3 API and the features it supports
in the V4 API, or V4-style functions/macros.

Everything is, as far as possible,
written against the V4 API or an abstraction thereof.

Mostly the V4 API changes
affect:

1. creational logic:
 wrapped using CMDSCOPE_NG etc, as defined in pycbc.h

2. 'shared' attributes of structures.
 The backport of the V4 API (see lcb_v4_backport.h) to
 LCB API V3 replicates these interface changes as far as possible, so one
 unified set of code can be used with both LCB API V3 and LCB API V4.

 There are some mutually exclusive features between LCB API V3 and LCB API V4
 (e.g. standalone ENDURE/OBSERVE commands)
 that are currently implemented via conditional compilation.

 PYCBC_LCB_API represents LCB_API version,
 but we use 0x02FF0x for V3 alphas (e.g. 0x02FF04 for 3.0.0 Alpha 4).
 */

#if PYCBC_LCB_API > 0x02FF00
#include "lcb_v4_wrapper.h"
#else
#include "lcb_v4_backport.h"
#endif

// TODO: fix in libcouchbase
#ifdef _WIN32
//#if _MSC_VER >= 1600
//        #include <cstdint>
//#else
    typedef __int8              int8_t;
    typedef __int16             int16_t;
//#endif
#include "libcouchbase/sysdefs.h"
#define uint8_t lcb_uint8_t
#define uint16_t lcb_uint16_t
#define uint64_t lcb_uint64_t
#endif

#include <libcouchbase/crypto.h>
#include "../build/lcb_min_version.h"
#if LCB_VERSION < LCB_MIN_VERSION
#pragma message "Couchbase Python SDK requires libcouchbase " LCB_MIN_VERSION_TEXT " or greater"
#error "Please upgrade libcouchbase accordingly"
#endif

#include <pythread.h>
#include "mresdict.h"

#define PYCBC_REFCNT_ASSERT pycbc_assert

void pycbc_fetch_error(PyObject *err[3]);
void pycbc_store_error(PyObject *err[3]);
lcb_STATUS pycbc_report_err(int res, const char *generic_errmsg, const char* FILE, int LINE);
#define PYCBC_REPORT_ERR(RES, MSG) pycbc_report_err(RES,MSG, __FILE__, __LINE__);

void *malloc_and_log(const char *file,
                     const char *func,
                     int line,
                     size_t quant,
                     size_t size,
                     const char *type_name);
void *calloc_and_log(const char *file,
                     const char *func,
                     int line,
                     size_t quant,
                     size_t size,
                     const char *type_name);

#define PYCBC_STASH_EXCEPTION(OP)                           \
    {                                                       \
        PyObject *pycbc_err[3] = {0};                       \
        pycbc_store_error(pycbc_err);                       \
        {                                                   \
            OP;                                             \
        }                                                   \
        if (pycbc_err[0] || pycbc_err[1] || pycbc_err[2]) { \
            pycbc_fetch_error(pycbc_err);                   \
        }                                                   \
    };

/**
 * See http://docs.python.org/2/c-api/arg.html for an explanation of this
 * definition.
 */
#ifdef PY_SSIZE_T_CLEAN
typedef Py_ssize_t pycbc_strlen_t;
#else
typedef int pycbc_strlen_t;
#endif

#define PYCBC_MODULE_NAME "_libcouchbase"
#define PYCBC_FQNAME PYCBC_PACKAGE_NAME "." PYCBC_MODULE_NAME

#define PYCBC_TCNAME_ENCODE_KEY "encode_key"
#define PYCBC_TCNAME_ENCODE_VALUE "encode_value"
#define PYCBC_TCNAME_DECODE_KEY "decode_key"
#define PYCBC_TCNAME_DECODE_VALUE "decode_value"

/**
 * Python 2.x and Python 3.x have different ideas of what a basic string
 * and int types are. These blocks help us sort things out if we just want a
 * "plain" integer or string
 */
#if PY_MAJOR_VERSION == 3
#define PYCBC_POBJ_HEAD_INIT(t) { PyObject_HEAD_INIT(t) },

/**
 * The IntFrom* macros get us a 'default' integer type from a long, etc.
 * Implemented (if not a simple macro) in numutil.c
 */
#define pycbc_IntFromL PyLong_FromLong
#define pycbc_IntFromUL PyLong_FromUnsignedLong
#define pycbc_IntFromULL PyLong_FromUnsignedLongLong

/**
 * The IntAs* convert the integer type (long, int) into something we want
 */
#define pycbc_IntAsULL PyLong_AsUnsignedLongLong
#define pycbc_IntAsLL PyLong_AsLongLong
#define pycbc_IntAsUL PyLong_AsUnsignedLong
#define pycbc_IntAsL PyLong_AsLong

/**
 * The SimpleString macros generate strings for us. The 'Z' variant takes a
 * NUL-terminated string, while the 'N' variant accepts a length specifier
 */
#define pycbc_SimpleStringZ(c) PyUnicode_FromString(c)
#define pycbc_SimpleStringN(c, n) PyUnicode_FromStringAndSize(c, n)


#else

/**
 * This defines the PyObject head for our types
 */
#define PYCBC_POBJ_HEAD_INIT(t) PyObject_HEAD_INIT(t)

/**
 * See above block for explanation of these macros
 */
#define pycbc_IntFromL PyInt_FromLong
#define pycbc_IntFromUL PyLong_FromUnsignedLong
#define pycbc_IntFromULL PyLong_FromUnsignedLongLong
#define pycbc_SimpleStringZ(c) PyString_FromString(c)
#define pycbc_SimpleStringN(c, n) PyString_FromStringAndSize(c, n)

unsigned PY_LONG_LONG pycbc_IntAsULL(PyObject *o);
PY_LONG_LONG pycbc_IntAsLL(PyObject *o);
long pycbc_IntAsL(PyObject *o);
unsigned long pycbc_IntAsUL(PyObject *o);

#endif

const char *pycbc_cstr(PyObject *object);
const char *pycbc_cstrn(PyObject *object, Py_ssize_t *length);

#define PYCBC_CSTR(X) pycbc_cstr(X)
#define PYCBC_CSTRN(X, n) pycbc_cstrn((X), (Py_ssize_t *)(n))

PyObject* pycbc_replace_str(PyObject** string, const char* pat, const char* replace);
PyObject *pycbc_none_or_value(PyObject *maybe_value);

/**
 * Fetches a valid TTL from the object
 * @param obj an object to be parsed as the TTL
 * @param ttl a pointer to the TTL itself
 * @param nonzero whether to allow a value of 0 for the TTL
 * @return 0 on success, nonzero on error.
 */
int pycbc_get_ttl(PyObject *obj, unsigned long *ttl, int nonzero);

/**
 * Fetches a valid 32 bit integer from the object. The object must be a long
 * or int.
 * @param obj the object containing the number
 * @param out a pointer to a 32 bit integer to be populated
 * @return 0 on success, -1 on failure. On failure, the error indicator is also
 * set
 */
int pycbc_get_u32(PyObject *obj, lcb_uint32_t *out);

/**
 * Converts the object into an PyInt (2.x only) or PyLong (2.x or 3.x)
 */
PyObject *pycbc_maybe_convert_to_int(PyObject *o);

/**
 * Gives us a C buffer from a Python string.
 * @param orig the original object containg a string thing. This is something
 * we can convert into a byte buffer
 *
 * @param buf out, the C buffer, out, set to the new buffer
 * @param nbuf, out, the length of the new buffer
 * @param newkey, out, the new PyObject, which will back the buffer.
 * This should not be DECREF'd until the @c buf is no longer needed
 */
int pycbc_BufFromString(PyObject *orig,
                        char **buf,
                        Py_ssize_t *nbuf,
                        PyObject **newkey);


/**
 * These constants are used internally to figure out the high level
 * operation being performed.
 *
 * Note that not all operations are defined here; it is only those operations
 * where a single C function can handle multiple entry points.
 */
enum {
    PYCBC_CMD_GET = 500,
    PYCBC_CMD_LOCK,
    PYCBC_CMD_TOUCH,
    PYCBC_CMD_GAT,
    PYCBC_CMD_COUNTER,
    PYCBC_CMD_DELETE,
    PYCBC_CMD_UNLOCK,
    PYCBC_CMD_GETREPLICA,
    /** "Extended" get replica, provides for more options */
    PYCBC_CMD_GETREPLICA_INDEX,
    PYCBC_CMD_GETREPLICA_ALL,
    PYCBC_CMD_ENDURE
};

/**
 * Various exception types to be thrown
 */
enum {
    /** Argument Error. User passed the wrong arguments */
    PYCBC_EXC_ARGUMENTS,

    /** Couldn't encode/decode something */
    PYCBC_EXC_ENCODING,

    /** Operational error returned from LCB */
    PYCBC_EXC_LCBERR,

    /** Internal error. There's something wrong with our code */
    PYCBC_EXC_INTERNAL,

    /** HTTP Error */
    PYCBC_EXC_HTTP,

    /** ObjectThreadError */
    PYCBC_EXC_THREADING,

    /** Object destroyed before it could connect */
    PYCBC_EXC_DESTROYED,

    /** Illegal operation in pipeline context */
    PYCBC_EXC_PIPELINE
};

/* Argument options */
enum {
    /** Entry point is a single key variant */
    PYCBC_ARGOPT_SINGLE = 0x1,

    /** Entry point is a multi key variant */
    PYCBC_ARGOPT_MULTI = 0x2,

    PYCBC_ARGOPT_SUBDOC = 0x04,

    PYCBC_ARGOPT_SDMULTI = 0x08
};

/**
 * Format flags
 */
enum {
    PYCBC_FMT_LEGACY_JSON = 0x00,
    PYCBC_FMT_LEGACY_PICKLE = 0x01,
    PYCBC_FMT_LEGACY_BYTES = 0x02,
    PYCBC_FMT_LEGACY_UTF8 = 0x04,
    PYCBC_FMT_LEGACY_MASK = 0x07,

    PYCBC_FMT_COMMON_PICKLE = (0x01U << 24),
    PYCBC_FMT_COMMON_JSON = (0x02U << 24),
    PYCBC_FMT_COMMON_BYTES = (0x03U << 24),
    PYCBC_FMT_COMMON_UTF8 = (0x04U << 24),
    PYCBC_FMT_COMMON_MASK = (0xFFU << 24),

    PYCBC_FMT_JSON = PYCBC_FMT_LEGACY_JSON|PYCBC_FMT_COMMON_JSON,
    PYCBC_FMT_PICKLE = PYCBC_FMT_LEGACY_PICKLE|PYCBC_FMT_COMMON_PICKLE,
    PYCBC_FMT_BYTES = PYCBC_FMT_LEGACY_BYTES|PYCBC_FMT_COMMON_BYTES,
    PYCBC_FMT_UTF8 = PYCBC_FMT_LEGACY_UTF8|PYCBC_FMT_COMMON_UTF8
};

typedef enum {
    PYCBC_LOCKMODE_NONE = 0,
    PYCBC_LOCKMODE_EXC = 1,
    PYCBC_LOCKMODE_WAIT = 2,
    PYCBC_LOCKMODE_MAX
} pycbc_lockmode_t;

enum {
    PYCBC_CONN_F_WARNEXPLICIT = 1 << 0,
    PYCBC_CONN_F_USEITEMRESULT = 1 << 1,
    PYCBC_CONN_F_CLOSED = 1 << 2,

    /**
     * For use with (but not limited to) Twisted.
     *
     * Deliver results asynchronously. This means:
     * 1) Don't call lcb_wait()
     * 2) Return an AsyncContainer (i.e. a MultiResult)
     * 3) Invoke the MultiResult (AsyncContainer)'s callback as needed
     */
    PYCBC_CONN_F_ASYNC = 1 << 3,

    /** Whether this instance has been connected */
    PYCBC_CONN_F_CONNECTED = 1 << 4,

    /** Schedule destruction of iops and lcb instance for later */
    PYCBC_CONN_F_ASYNC_DTOR = 1 << 5
};


#ifndef PYCBC_DUR_DISABLED
#define PYCBC_DUR_ENABLED
#endif

#ifdef PYCBC_DUR_ENABLED
#    define PYCBC_DUR_INIT(ERR, CMD, TYPE, DUR)                            \
        if ((DUR).durability_level) {                                      \
            PYCBC_DEBUG_LOG("Setting sync durability level %d",            \
                            (DUR).durability_level)                        \
            ERR = lcb_cmd##TYPE##_durability(CMD, (DUR).durability_level); \
            assert(!((DUR).persist_to || (DUR).replicate_to));             \
        }                                                                  \
        if ((DUR).persist_to || (DUR).replicate_to) {                      \
            PYCBC_DEBUG_LOG(                                               \
                    "Setting client durability level persist_to=%d, "      \
                    "replicate_to=%d",                                     \
                    (DUR).persist_to,                                      \
                    (DUR).replicate_to)                                    \
            ERR = lcb_cmd##TYPE##_durability_observe(                      \
                    CMD, (DUR).persist_to, (DUR).replicate_to);            \
        }
#else
#    define PYCBC_DUR_INIT(ERR, CMD, TYPE, DUR)
#endif

typedef struct {
    char persist_to;
    char replicate_to;
    pycbc_DURABILITY_LEVEL durability_level;
} pycbc_dur_params;

void pycbc_dict_add_text_kv(PyObject *dict, const char *key, const char *value);

struct pycbc_Tracer;

#define PYCBC_TRACING


extern pycbc_strn pycbc_invalid_strn;

extern const char PYCBC_UNKNOWN[];
static char *const PYCBC_DEBUG_INFO_STR = "debug_info";
#define sizeof_array(X) sizeof(X) / sizeof(X[0])

#define PYCBC_STRN_FREE(BUF)                            \
    PYCBC_DEBUG_LOG("Freeing string buffer %.*s at %p", \
                    (int)(BUF).content.length,          \
                    (BUF).content.buffer,               \
                    (BUF).content.buffer)               \
    pycbc_strn_free(BUF);

#define PYCBC_DUMMY(...)

#define CMDSCOPE_GENERIC_FAIL(PREFIX, UC, LC) \
    fail = 1;                                 \
    goto GT_##PREFIX##_##UC##_ERR;

#define CMDSCOPE_GENERIC_DONE(PREFIX, UC, LC) goto GT_##PREFIX##_##UC##_DONE;

#define CMDSCOPE_GENERIC_ALL_PREFIX(                                           \
        PREFIX, UC, LC, INITIALIZER, DESTRUCTOR, CMDS, ...)                    \
    INITIALIZER(UC, LC, CMDS, __VA_ARGS__);                                    \
    PYCBC_DEBUG_LOG("Called initialzer for %s, %s, with args %s",              \
                    #UC,                                                       \
                    #LC,                                                       \
                    #__VA_ARGS__)                                              \
    goto SKIP_##PREFIX##_##UC##_FAIL;                                          \
    goto GT_##PREFIX##_##UC##_DONE;                                            \
    GT_##PREFIX##_##UC##_DONE : PYCBC_DEBUG_LOG("Cleanup up %s %s", #UC, #LC)( \
                                        void)(DESTRUCTOR(UC, LC, CMDS));       \
    goto GT_DONE;                                                              \
    goto GT_##PREFIX##_##UC##_ERR;\
    GT_##PREFIX##_##UC##_ERR : (void)(DESTRUCTOR(UC, LC, CMDS));               \
    goto GT_ERR;                                                               \
                                                                               \
    SKIP_##PREFIX##_##UC##_FAIL                                                \
        : for (int finished = 0, fail = 0; !(finished) && !fail;               \
               (finished = (1 + DESTRUCTOR(UC, LC, CMDS))))

#define CMDSCOPE_GENERIC_ALL(UC, LC, INITIALIZER, DESTRUCTOR, CMDS, ...) \
    CMDSCOPE_GENERIC_ALL_PREFIX(                                         \
            , UC, LC, INITIALIZER, DESTRUCTOR, CMDS, __VA_ARGS__)

#define pycbc_verb_postfix(POSTFIX, VERB, INSTANCE, COOKIE, CMD) \
    pycbc_logging_monad_verb(__FILE__,                           \
                             __FUNCTION__,                       \
                             __LINE__,                           \
                             INSTANCE,                           \
                             COOKIE,                             \
                             CMD,                                \
                             #CMD,                               \
                             #VERB,                              \
                             lcb_##VERB##POSTFIX(INSTANCE, COOKIE, CMD))

lcb_STATUS pycbc_logging_monad_verb(const char *FILE,
                                    const char *FUNC,
                                    int LINE,
                                    lcb_INSTANCE *instance,
                                    void *COOKIE,
                                    void *CMD,
                                    const char *CMDNAME,
                                    const char *VERB,
                                    lcb_STATUS result);

#define IMPL_DECL(...)
#define DECL_IMPL(...) __VA_ARGS__
#define DECL_DECL(...) DECL_IMPL(__VA_ARGS__);
#define IMPL_IMPL(...) __VA_ARGS__

#define CMDSCOPE_SDCMD_CREATE_V4(TYPE, LC, CMD, ...) \
    TYPE *CMD = NULL;                                \
    lcb_##LC##_create(&(CMD), __VA_ARGS__);

#define CMDSCOPE_SDCMD_DESTROY_RAW_V4(TYPE, LC, CMD, ...) \
    lcb_##LC##_destroy(CMD)

#define CMDSCOPE_CREATECMD_RAW_V4(UC, LC, CMD, ...) \
    lcb_CMD##UC *CMD = NULL;                        \
    lcb_cmd##LC##_create(&CMD)

#define CMDSCOPE_CREATECMD_V4(UC, LC, CMD, ...) \
    lcb_CMD##UC *CMD = NULL;                    \
    lcb_cmd##LC##_create(&CMD, __VA_ARGS__)

#define CMDSCOPE_DESTROYCMD_V4(UC, LC, CMD, ...) lcb_cmd##LC##_destroy(CMD)

#define CMDSCOPE_DESTROYCMD_RAW_V4(UC, LC, CMD, ...) lcb_cmd##LC##_destroy(CMD)

#define CMDSCOPE_NG_V4(UC, LC)                       \
    CMDSCOPE_GENERIC_ALL(UC,                         \
                         LC,                         \
                         CMDSCOPE_CREATECMD_RAW_V4,  \
                         CMDSCOPE_DESTROYCMD_RAW_V4, \
                         cmd)
#define CMDSCOPE_NG(UC, LC) \
    CMDSCOPE_GENERIC_ALL(   \
            UC, LC, CMDSCOPE_CREATECMD_RAW, CMDSCOPE_DESTROYCMD_RAW, cmd)

#define CMDSCOPE_NG_PARAMS(UC, LC, ...) \
    CMDSCOPE_GENERIC_ALL(               \
            UC, LC, CMDSCOPE_CREATECMD, CMDSCOPE_DESTROYCMD, cmd, __VA_ARGS__)
#define CMDSCOPE_NG_GENERIC_PARAMS(PREFIX, TYPE, LC, CMDNAME, ...) \
    CMDSCOPE_GENERIC_ALL_PREFIX(PREFIX,                            \
                                TYPE,                              \
                                LC,                                \
                                CMDSCOPE_SDCMD_CREATE_V4,          \
                                CMDSCOPE_SDCMD_DESTROY_RAW_V4,     \
                                CMDNAME,                           \
                                __VA_ARGS__)


typedef struct {
    PyObject_HEAD

    /** LCB instance */
    lcb_INSTANCE *instance;
    /** Tracer **/
    struct pycbc_Tracer *tracer;
    PyObject *parent_tracer;
    /** Transcoder object */
    PyObject *tc;

    /** Default format, PyInt */
    PyObject *dfl_fmt;

    /** Callback to be invoked when connected */
    PyObject *conncb;

    /**
     * Callback to be invoked upon destruction. Because we can fall out
     * of scope in middle of an LCB function, this is required.
     *
     * The dtorcb is first called when the refcount of the connection
     */
    PyObject *dtorcb;

    /**
     * Test hook for reacting to durability/persistence settings from within
     * mutator functions
     */
    PyObject *dur_testhook;


    /** String bucket */
    PyObject *bucket;

    /** Bucket type */
    PyObject *btype;

    /** Pipeline MultiResult container */
    PyObject *pipeline_queue;

    /** If using a custom IOPS, this contains it */
    PyObject *iopswrap;

    /** Thread state. Used to lock/unlock the GIL */
    PyThreadState *thrstate;

    PyThread_type_lock lock;
    unsigned int lockmode;

    /** Whether to not raise any exceptions */
    unsigned int quiet;

    /** Whether GIL handling is in effect */
    unsigned int unlock_gil;

    /** Don't decode anything */
    unsigned int data_passthrough;

    /** whether __init__ has already been called */
    unsigned char init_called;

    /** How many operations are waiting for a reply */
    Py_ssize_t nremaining;

    unsigned int flags;

    pycbc_dur_params dur_global;
    unsigned long dur_timeout;

} pycbc_Bucket;

/**
 * Collection structures
 */

/** Text coordinates for collections */
typedef struct {
    pycbc_strn_unmanaged collection;
    pycbc_strn_unmanaged scope;
} pycbc_Collection_coords;

/** Collection class **/

typedef struct pycbc_Collection pycbc_Collection_t;

struct pycbc_Collection {
    PyObject_HEAD pycbc_Bucket *bucket;
    pycbc_Collection_coords collection;
};

/**
 * Server-provided IDs/handles for collections
 */

typedef struct {
    lcb_U64 manifest_id;
    lcb_U32 collection_id;
} pycbc_coll_res_success_t;

typedef struct {
    pycbc_coll_res_success_t value;
    lcb_STATUS err;
} pycbc_coll_res_t;

typedef struct {
    pycbc_coll_res_t result;
    pycbc_Collection_t *coll;
} pycbc_coll_context;

int pycbc_collection_init_from_fn_args(pycbc_Collection_t *self,
                                       pycbc_Bucket *bucket,
                                       PyObject *kwargs);
pycbc_Collection_t pycbc_Collection_as_value(pycbc_Bucket *self,
                                             PyObject *kwargs);
void pycbc_Collection_free_unmanaged_contents(
        const pycbc_Collection_t *collection);
#define PYCBC_COLLECTION_XARGS(X) X("collection", &collection, "O")

#ifdef PYCBC_DEBUG
lcb_STATUS pycbc_log_coll(const char *TYPE,
                          void *CMD,
                          const char *SCOPE,
                          size_t NSCOPE,
                          const char *COLLECTION,
                          size_t NCOLLECTION,
                          lcb_STATUS RC);

#    define PYCBC_DO_COLL_LOGGING_IF_APPLICABLE(               \
            TYPE, CMD, SCOPE, NSCOPE, COLLECTION, NCOLLECTION) \
        pycbc_log_coll(                                        \
                #TYPE,                                         \
                CMD,                                           \
                SCOPE,                                         \
                NSCOPE,                                        \
                COLLECTION,                                    \
                NCOLLECTION,                                   \
                PYCBC_DO_COLL(                                 \
                        TYPE, CMD, SCOPE, NSCOPE, COLLECTION, NCOLLECTION))
#else
#    define PYCBC_DO_COLL_LOGGING_IF_APPLICABLE(               \
            TYPE, CMD, SCOPE, NSCOPE, COLLECTION, NCOLLECTION) \
        PYCBC_DO_COLL(TYPE, CMD, SCOPE, NSCOPE, COLLECTION, NCOLLECTION)
#endif

#define PYCBC_DO_COLL_IF_APPLICABLE(                                     \
        TYPE, CMD, SCOPE, NSCOPE, COLLECTION, NCOLLECTION)               \
    ((NSCOPE && SCOPE) || (COLLECTION && NCOLLECTION))                   \
            ? PYCBC_DO_COLL_LOGGING_IF_APPLICABLE(                       \
                      TYPE, CMD, SCOPE, NSCOPE, COLLECTION, NCOLLECTION) \
            : LCB_SUCCESS

#define PYCBC_CMD_COLLECTION(TYPE, CMD, COLLECTION)             \
    PYCBC_DO_COLL_IF_APPLICABLE(                                \
            TYPE,                                               \
            CMD,                                                \
            (COLLECTION)->collection.scope.content.buffer,      \
            (COLLECTION)->collection.scope.content.length,      \
            (COLLECTION)->collection.collection.content.buffer, \
            (COLLECTION)->collection.collection.content.length)

#define PYCBC_CMD_PROXY(UC, LC, SUBJECT, IMPL_TYPE)                            \
    DECL_##IMPL_TYPE(lcb_STATUS pycbc_##LC(                                    \
            SUBJECT##_##ARG subject, void *cookie, lcb_CMD##UC *cmd))          \
            IMPL_##IMPL_TYPE({                                                 \
                lcb_STATUS rc = SUBJECT##_##SET_COLL(UC, LC, cmd, subject);    \
                return rc ? rc                                                 \
                          : pycbc_verb(                                        \
                                    LC, SUBJECT##_##GETINSTANCE, cookie, cmd); \
            };)
#define PYCBC_X_VERBS(X, COLLECTION, NOCOLLECTION, IMPL_TYPE) \
    X(COUNTER, counter, COLLECTION, IMPL_TYPE)                \
    X(GET, get, COLLECTION, IMPL_TYPE)                        \
    X(TOUCH, touch, COLLECTION, IMPL_TYPE)                    \
    X(UNLOCK, unlock, COLLECTION, IMPL_TYPE)                  \
    X(REMOVE, remove, COLLECTION, IMPL_TYPE)                  \
    X(STORE, store, COLLECTION, IMPL_TYPE)                    \
    X(HTTP, http, NOCOLLECTION, IMPL_TYPE)                    \
    X(PING, ping, NOCOLLECTION, IMPL_TYPE)                    \
    X(SUBDOC, subdoc, COLLECTION, IMPL_TYPE)

#define COLLECTION_ARG pycbc_Collection_t *
#define NOCOLLECTION_ARG lcb_INSTANCE *
#define COLLECTION_GETINSTANCE subject->bucket->instance
#define NOCOLLECTION_GETINSTANCE subject
#define COLLECTION_SET_COLL(UC, LC, CMD, SUBJECT) \
    PYCBC_CMD_COLLECTION(LC, CMD, SUBJECT)
#define NOCOLLECTION_SET_COLL(UC, LC, CMD, SUBJECT) LCB_SUCCESS
PYCBC_X_VERBS(PYCBC_CMD_PROXY, COLLECTION, NOCOLLECTION, DECL);

void *pycbc_capsule_value_or_null(PyObject *capsule, const char *capsule_name);

typedef struct pycbc_Tracer {
    PyObject_HEAD
    lcbtrace_TRACER *tracer;
} pycbc_Tracer_t;

typedef struct {
    PyObject_HEAD
    lcbtrace_SPAN *span;
} pycbc_Span_t;

typedef struct pycbc_context_children_t {
    pycbc_stack_context_handle value;
    struct pycbc_context_children_t *next;
} pycbc_context_children;

typedef struct pycbc_stack_context_decl {
    int is_stub;
    pycbc_Tracer_t* tracer;
    lcbtrace_SPAN* span;
    pycbc_stack_context_handle parent;
    size_t ref_count;
#ifdef PYCBC_REF_ACCOUNTING
#ifdef PYCBC_INLINE_ACC
    pycbc_stack_context_handle next;
    pycbc_stack_context_handle first_child;
#else
    pycbc_context_children *acc_node;
    pycbc_context_children *children;
#endif
#endif
#ifdef PYCBC_TABBED_CONTEXTS
    size_t depth;
#endif
} pycbc_stack_context;

pycbc_strn pycbc_get_string_tag_basic(lcbtrace_SPAN *span, const char *tagname);
PyObject *pycbc_Context_capsule(pycbc_stack_context_handle context);
void pycbc_Context_capsule_destructor(PyObject *context_capsule);
void *pycbc_Context_capsule_value(PyObject *context_capsule);

#define PYCBC_RES_CONTEXT(MRES) (MRES) ? (MRES)->tracing_context : NULL

typedef struct pycbc_Result pycbc_Result_t;
typedef struct pycbc_MultiResult_st pycbc_MultiResult;
void pycbc_set_dict_kv_object(PyObject *dict,
                              PyObject *key,
                              const char *value_str);

void pycbc_set_kv_ull(PyObject *dict,
                      PyObject *keystr,
                      lcb_uint64_t parenti_id);
void pycbc_set_kv_ull_str(PyObject *dict,
                          const char *keystr,
                          lcb_uint64_t parenti_id);

int pycbc_is_async_or_pipeline(const pycbc_Bucket *self);

pycbc_stack_context_handle pycbc_Result_start_context(
        pycbc_stack_context_handle parent_context,
        PyObject *hkey,
        const char *component,
        char *operation,
        lcbtrace_REF_TYPE ref_type);
void pycbc_Result_propagate_context(pycbc_Result_t *res,
                                    pycbc_stack_context_handle parent_context,
                                    pycbc_Bucket *bucket);
void pycbc_MultiResult_init_context(pycbc_MultiResult *self,
                                    PyObject *curkey,
                                    pycbc_stack_context_handle context,
                                    pycbc_Bucket *bucket);
pycbc_stack_context_handle pycbc_MultiResult_extract_context(
        pycbc_MultiResult *self, PyObject *hkey, pycbc_Result_t **res);
#define PYCBC_MULTIRESULT_EXTRACT_CONTEXT(MRES, KEY, RES) \
    pycbc_MultiResult_extract_context(MRES, KEY, RES)
pycbc_stack_context_handle pycbc_Result_extract_context(
        const pycbc_Result_t *res);
#define PYCBC_RESULT_EXTRACT_CONTEXT(RESULT) \
    pycbc_Result_extract_context(RESULT)

pycbc_stack_context_handle pycbc_Context_init(
        pycbc_Tracer_t *py_tracer,
        const char *operation,
        lcb_uint64_t now,
        pycbc_stack_context_handle ref_context,
        lcbtrace_REF_TYPE ref_type,
        const char *component);

pycbc_stack_context_handle pycbc_Context_init_debug(
        const char *FILE,
        int LINE,
        const char *FUNC,
        pycbc_Tracer_t *py_tracer,
        const char *operation,
        lcb_uint64_t now,
        pycbc_stack_context_handle ref_context,
        lcbtrace_REF_TYPE ref_type,
        const char *component);
#define PYCBC_CONTEXT_INIT(                                          \
        py_tracer, operation, now, ref_context, ref_type, component) \
    pycbc_Context_init_debug(__FILE__,                               \
                             __LINE__,                               \
                             __FUNCTION_NAME__,                      \
                             py_tracer,                              \
                             operation,                              \
                             now,                                    \
                             ref_context,                            \
                             ref_type,                               \
                             component)

pycbc_stack_context_handle pycbc_Context_check(
        pycbc_stack_context_handle context,
        const char *file,
        const char *func,
        int line);

#ifdef PYCBC_DEBUG
#define PYCBC_CHECK_CONTEXT(CONTEXT) \
    pycbc_Context_check(CONTEXT, __FILE__, __FUNCTION_NAME__, __LINE__)
#else
#define PYCBC_CHECK_CONTEXT(CONTEXT) \
    pycbc_Context_check(CONTEXT, __FILE__, "N/A", __LINE__)
#endif

pycbc_stack_context_handle pycbc_Context_deref(
        pycbc_stack_context_handle context,
        int should_be_final,
        int account_for_children,
        pycbc_stack_context_handle from_context);
pycbc_stack_context_handle pycbc_Context_deref_debug(
        const char *file,
        const char *func,
        int line,
        pycbc_stack_context_handle context,
        int should_be_final,
        int dealloc_children,
        pycbc_stack_context_handle from_context);

#ifdef PYCBC_DEBUG
#define PYCBC_CONTEXT_DEREF_FROM_CONTEXT(                         \
        CONTEXT, SHOULD_BE_FINAL, DEALLOC_CHILDREN, FROM_CONTEXT) \
    pycbc_Context_deref_debug(__FILE__,                           \
                              __FUNCTION_NAME__,                  \
                              __LINE__,                           \
                              CONTEXT,                            \
                              SHOULD_BE_FINAL,                    \
                              DEALLOC_CHILDREN,                   \
                              FROM_CONTEXT);
#else
#define PYCBC_CONTEXT_DEREF_FROM_CONTEXT(                         \
        CONTEXT, SHOULD_BE_FINAL, DEALLOC_CHILDREN, FROM_CONTEXT) \
    pycbc_Context_deref(      CONTEXT,                            \
                              SHOULD_BE_FINAL,                    \
                              DEALLOC_CHILDREN,                   \
                              FROM_CONTEXT);

#endif

#ifdef PYCBC_DEBUG
#define PYCBC_CONTEXT_DEREF(CONTEXT, SHOULD_BE_FINAL) \
    pycbc_Context_deref_debug(__FILE__,               \
                              __FUNCTION_NAME__,      \
                              __LINE__,               \
                              CONTEXT,                \
                              SHOULD_BE_FINAL,        \
                              1,                      \
                              NULL)
#else
#define PYCBC_CONTEXT_DEREF(CONTEXT, SHOULD_BE_FINAL) \
    pycbc_Context_deref(CONTEXT, SHOULD_BE_FINAL, 1, NULL)
#endif

size_t pycbc_Context_get_ref_count(pycbc_stack_context_handle context);
size_t pycbc_Context_get_ref_count_debug(const char *FILE,
                                         const char *FUNC,
                                         int line,
                                         pycbc_stack_context_handle context);
#ifdef PYCBC_DEBUG
#define PYCBC_CONTEXT_GET_REF_COUNT(CONTEXT) \
    pycbc_Context_get_ref_count_debug(       \
            __FILE__, __FUNCTION_NAME__, __LINE__, CONTEXT)
#else
#define PYCBC_CONTEXT_GET_REF_COUNT(CONTEXT) \
    pycbc_Context_get_ref_count(CONTEXT)
#endif

void pycbc_ref_context(pycbc_stack_context_handle parent_context);
#define PYCBC_REF_CONTEXT(CONTEXT)                                     \
    PYCBC_DEBUG_LOG_CONTEXT(                                           \
            CONTEXT,                                                   \
            "starting reffing context %p cv %llu",                     \
            CONTEXT,                                                   \
            (long long unsigned)pycbc_Context_get_ref_count(CONTEXT)); \
    pycbc_ref_context(CONTEXT);                                        \
    PYCBC_DEBUG_LOG_CONTEXT(                                           \
            CONTEXT,                                                   \
            "finished reffing context %p to get %llu",                 \
            CONTEXT,                                                   \
            (long long unsigned)pycbc_Context_get_ref_count(CONTEXT));

pycbc_stack_context_handle pycbc_Tracer_start_span(
        pycbc_Tracer_t *py_tracer,
        PyObject *kwargs,
        const char *operation,
        lcb_uint64_t now,
        pycbc_stack_context_handle *context,
        lcbtrace_REF_TYPE ref_type,
        const char *component);
pycbc_stack_context_handle pycbc_Tracer_start_span_debug(
        const char *FILE,
        int LINE,
        const char *FUNCTION,
        pycbc_Tracer_t *py_tracer,
        PyObject *kwargs,
        const char *operation,
        lcb_uint64_t now,
        pycbc_stack_context_handle *context,
        lcbtrace_REF_TYPE ref_type,
        const char *component);

#ifdef PYCBC_DEBUG
#define PYCBC_TRACER_START_SPAN(...) \
    pycbc_Tracer_start_span_debug(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#else
#define PYCBC_TRACER_START_SPAN(...) pycbc_Tracer_start_span(__VA_ARGS__)
#endif

void pycbc_Tracer_propagate(pycbc_Tracer_t *tracer);
void pycbc_Tracer_set_child(pycbc_Tracer_t *pTracer, lcbtrace_TRACER *pTRACER);

#define PYCBC_TRACE_GET_STACK_CONTEXT_TOPLEVEL(KWARGS, CATEGORY, TRACER, NAME) \
    PYCBC_TRACER_START_SPAN(                                                   \
            TRACER, KWARGS, CATEGORY, 0, NULL, LCBTRACE_REF_NONE, NAME)


#define PYCBC_TRACECMD_PURE(TYPE, CMD, CONTEXT)                    \
    {                                                              \
        if (PYCBC_CHECK_CONTEXT(CONTEXT)) {                        \
            PYCBC_LOG_KEY(CMD, key)                                \
            PYCBC_CMD_SET_TRACESPAN(TYPE, (CMD), (CONTEXT)->span); \
        } else {                                                   \
            PYCBC_EXCEPTION_LOG_NOCLEAR;                           \
        }                                                          \
    }

#define PYCBC_TRACECMD_SCOPED_GENERIC(RV,           \
                                      SCOPE,        \
                                      COMMAND,      \
                                      INSTANCE,     \
                                      CMD,          \
                                      HANDLE,       \
                                      CONTEXT,      \
                                      SPAN_OPERAND, \
                                      OPERAND,      \
                                      ...)          \
    SPAN_OPERAND(SCOPE, INSTANCE, CMD, HANDLE, CONTEXT);\
    RV = OPERAND(SCOPE, INSTANCE, CMD, HANDLE, CONTEXT, COMMAND, __VA_ARGS__);

#    define PYCBC_TRACECMD_SCOPED_NULL(                     \
            RV, SCOPE, INSTANCE, CMD, HANDLE, CONTEXT, ...) \
        PYCBC_TRACECMD_SCOPED_GENERIC(RV,                   \
                                      SCOPE,                \
                                      ,                     \
                                      INSTANCE,             \
                                      CMD,                  \
                                      HANDLE,               \
                                      CONTEXT,              \
                                      GENERIC_SPAN_OPERAND, \
                                      GENERIC_NULL_OPERAND, \
                                      __VA_ARGS__)
#    define PYCBC_TRACECMD_TYPED(TYPE, CMD, CONTEXT, MRES, CURKEY, BUCKET) \
        PYCBC_TRACECMD_PURE(TYPE, CMD, CONTEXT);                           \
        pycbc_MultiResult_init_context(MRES, CURKEY, CONTEXT, BUCKET);

#    define PYCBC_TRACE_POP_CONTEXT(CONTEXT) PYCBC_CONTEXT_DEREF((CONTEXT), 1);

pycbc_stack_context_handle pycbc_wrap_setup(const char *CATEGORY,
                                            const char *NAME,
                                            pycbc_Tracer_t *TRACER,
                                            const char *STRINGNAME,
                                            PyObject *KWARGS);

void pycbc_wrap_teardown(pycbc_stack_context_handle sub_context,
                         pycbc_Bucket *self,
                         const char *NAME,
                         PyObject **RV);

#    define PYCBC_TRACE_WRAP_TOPLEVEL_WITHNAME(                        \
            RV, CATEGORY, NAME, TRACER, STRINGNAME, ...)               \
        {                                                              \
            pycbc_stack_context_handle sub_context = pycbc_wrap_setup( \
                    CATEGORY, #NAME, TRACER, STRINGNAME, kwargs);      \
            RV = NAME(__VA_ARGS__, sub_context);                       \
            pycbc_wrap_teardown(sub_context, self, #NAME, &RV);        \
        }

typedef struct pycbc_common_vars pycbc_common_vars_t;
int pycbc_wrap_and_pop(pycbc_stack_context_handle *contextptr,
                       int noterv,
                       int result,
                       pycbc_common_vars_t *cv);
pycbc_stack_context_handle pycbc_logging_monad(const char *FILE,
                        int LINE,
                        const char *FUNC,
                        const char *NAME,
                        pycbc_stack_context_handle context);

int pycbc_wrap_and_pop_debug(const char *FILE,
                             int LINE,
                             const char *FUNC,
                             const char *NAME,
                             pycbc_stack_context_handle *contextptr,
                             int noterv,
                             int result,
                             pycbc_common_vars_t *cv);

#define PYCBC_WRAP_AND_POP(CONTEXTPTR, RESULT, NAME, NOTERV, CV) \
    pycbc_wrap_and_pop_debug(__FILE__,                           \
                             __LINE__,                           \
                             __FUNCTION__,                       \
                             NAME,                               \
                             CONTEXTPTR,                         \
                             NOTERV,                             \
                             (RESULT),                           \
                             CV)

pycbc_stack_context_handle pycbc_explicit_named_setup(
        const char *FILE,
        int LINE,
        const char *FUNCTION,
        pycbc_stack_context_handle *CONTEXTPTR,
        const char *COMPONENTNAME,
        const char *CATEGORY,
        PyObject *KWARGS,
        pycbc_Tracer_t *self);

#define PYCBC_EXPLICIT_NAMED_SETUP(                        \
        CONTEXTPTR, COMPONENTNAME, CATEGORY, KWARGS, SELF) \
    pycbc_explicit_named_setup(__FILE__,                   \
                               __LINE__,                   \
                               __FUNCTION__,               \
                               CONTEXTPTR,                 \
                               COMPONENTNAME,              \
                               CATEGORY,                   \
                               KWARGS,                     \
                               SELF)

#define PYCBC_TRACE_WRAP_EXPLICIT_NAMED(CONTEXTPTR,                      \
                                        NAME,                            \
                                        COMPONENTNAME,                   \
                                        CATEGORY,                        \
                                        KWARGS,                          \
                                        NOTERV,                          \
                                        CV,                              \
                                        SELF,                            \
                                        ...)                             \
    PYCBC_WRAP_AND_POP(CONTEXTPTR,                                       \
                       NAME(__VA_ARGS__,                                 \
                            PYCBC_EXPLICIT_NAMED_SETUP(CONTEXTPTR,       \
                                                       COMPONENTNAME,    \
                                                       CATEGORY,         \
                                                       KWARGS,           \
                                                       (SELF)->tracer)), \
                       COMPONENTNAME,                                    \
                       NOTERV,                                           \
                       CV)

#define PYCBC_TRACE_WRAP_EXPLICIT_NAMED_VOID(                             \
        CONTEXTPTR, NAME, COMPONENTNAME, CATEGORY, KWARGS, CV, SELF, ...) \
    {                                                                     \
        NAME(__VA_ARGS__,                                                 \
             PYCBC_EXPLICIT_NAMED_SETUP(CONTEXTPTR,                       \
                                        COMPONENTNAME,                    \
                                        CATEGORY,                         \
                                        KWARGS,                           \
                                        SELF->tracer));                   \
        (void)PYCBC_WRAP_AND_POP(CONTEXTPTR, 0, COMPONENTNAME, 0, CV);    \
    }

#define PYCBC_TRACE_WRAP_NOTERV(NAME, KWARGS, NOTERV, CV, CONTEXT, SELF, ...) \
    PYCBC_TRACE_WRAP_EXPLICIT_NAMED(CONTEXT,                                  \
                                    NAME,                                     \
                                    #NAME,                                    \
                                    NAME##_category(),                        \
                                    KWARGS,                                   \
                                    NOTERV,                                   \
                                    CV,                                       \
                                    SELF,                                     \
                                    __VA_ARGS__)

#define PYCBC_TRACE_WRAP(NAME, KWARGS, ...) \
    PYCBC_TRACE_WRAP_NOTERV(NAME, KWARGS, 0, NULL, &context, self, __VA_ARGS__)

#define PYCBC_TRACE_WRAP_VOID(NAME, KWARGS, CONTEXT, SELF, ...) \
    PYCBC_TRACE_WRAP_EXPLICIT_NAMED_VOID(CONTEXT,               \
                                         NAME,                  \
                                         #NAME,                 \
                                         NAME##_category(),     \
                                         KWARGS,                \
                                         NULL,                  \
                                         SELF,                  \
                                         __VA_ARGS__)

#define PYCBC_TRACE_WRAP_TOPLEVEL(RV, CATEGORY, NAME, TRACER, ...) \
    PYCBC_TRACE_WRAP_TOPLEVEL_WITHNAME(                            \
            RV, CATEGORY, NAME, TRACER, #NAME, __VA_ARGS__)



#define TRACED_FUNCTION(CATEGORY,QUALIFIERS,RTYPE,NAME,...)\
    const char* NAME##_category(void){ return CATEGORY; }\
    QUALIFIERS RTYPE NAME(__VA_ARGS__, pycbc_stack_context_handle context)

#define TRACED_FUNCTION_DECL(QUALIFIERS,RTYPE,NAME,...)\
    const char* NAME##_category(void);\
    QUALIFIERS RTYPE NAME(__VA_ARGS__, pycbc_stack_context_handle context);

#define TRACED_FUNCTION_WRAPPER(name, CATEGORY, CLASS)                    \
    PyObject *pycbc_##CLASS##_##name##_real(                              \
            pycbc_##CLASS *self,                                          \
            PyObject *args,                                               \
            PyObject *kwargs,                                             \
            pycbc_stack_context_handle context);                          \
    PyObject *pycbc_##CLASS##_##name(                                     \
            pycbc_##CLASS *self, PyObject *args, PyObject *kwargs)        \
    {                                                                     \
        PyObject *result;                                                 \
        PYCBC_TRACE_WRAP_TOPLEVEL_WITHNAME(result,                        \
                                           CATEGORY,                      \
                                           pycbc_##CLASS##_##name##_real, \
                                           self->tracer,                  \
                                           #CLASS "." #name,              \
                                           self,                          \
                                           args,                          \
                                           kwargs);                       \
        return result;                                                    \
    }                                                                     \
    PyObject *pycbc_##CLASS##_##name##_real(                              \
            pycbc_##CLASS *self,                                          \
            PyObject *args,                                               \
            PyObject *kwargs,                                             \
            pycbc_stack_context_handle context)

/*****************
 * Result Objects.
 *****************
 *
 * These objects are returned to indicate the status/value of operations.
 * The following defines a 'base' class and several 'subclasses'.
 *
 * See result.c and opresult.c
 */

#define TRACING_DATA                            \
    pycbc_stack_context_handle tracing_context; \
    char is_tracing_stub;                       \
    PyObject *tracing_output;

#define pycbc_Result_HEAD \
    PyObject_HEAD \
    lcb_STATUS rc; \
    PyObject *key; \
    TRACING_DATA

#define pycbc_OpResult_HEAD \
    pycbc_Result_HEAD \
    lcb_uint64_t cas; \
    PyObject *mutinfo;

typedef struct pycbc_Result {
    pycbc_Result_HEAD
} pycbc_Result;

typedef struct {
    pycbc_OpResult_HEAD
} pycbc_OperationResult;


#define pycbc_ValResult_HEAD \
    pycbc_OpResult_HEAD \
    PyObject *value; \
    lcb_uint32_t flags;

typedef struct {
    pycbc_ValResult_HEAD
} pycbc_ValueResult;

/**
 * Item or 'Document' object
 */
typedef struct {
    pycbc_ValResult_HEAD
    PyObject* vdict;
} pycbc_Item;

typedef struct {
    pycbc_OpResult_HEAD
    /* List of results. (value,errcode) */
    PyObject *results;

    /* Original list of specs passed. We can cache this later on if access
     * by element is required. */
    PyObject *specs;
} pycbc__SDResult;

enum {
    PYCBC_HTTP_HVIEW = 1,
    PYCBC_HTTP_HRAW,
    PYCBC_HTTP_HN1QL,
    PYCBC_HTTP_HFTS,
    PYCBC_HTTP_HNONE
};


enum {
    /** 'quiet' boolean set */
    PYCBC_MRES_F_QUIET      = 1 << 0,

    /** We're using a user-created Item; Don't create our own results */
    PYCBC_MRES_F_ITEMS      = 1 << 1,

    /** Items are already allocated and present within the dictionary. */
    PYCBC_MRES_F_UALLOCED   = 1 << 2,

    /** For GET (and possibly others), force FMT_BYTES */
    PYCBC_MRES_F_FORCEBYTES = 1 << 3,

    /** The commands have durability requirements */
    PYCBC_MRES_F_DURABILITY = 1 << 4,

    /** The command is an async subclass. Do we need this? */
    PYCBC_MRES_F_ASYNC = 1 << 5,

    /** This result is from a call to one of the single-item APIs */
    PYCBC_MRES_F_SINGLE = 1 << 6,

    /* Hint to dispatch to the view callback functions */
    PYCBC_MRES_F_VIEWS = 1 << 7
};
/**
 * Contextual info for enhanced error logging
 */
typedef struct {
    const char *FILE;
    const char *FUNC;
    int LINE;
} pycbc_debug_info;

int pycbc_debug_info_is_valid(pycbc_debug_info *info);

typedef PyObject pycbc_enhanced_err_info;

/**
 * Object containing the result of a 'Multi' operation. It's the same as a
 * normal dict, except we add an 'all_ok' field, so a user doesn't need to
 * skim through all the pairs to determine if something failed.
 *
 * See multiresult.c
 */
struct pycbc_MultiResult_st {
    PYCBC_MULTIRESULT_BASE;

    /** parent Connection object */
    pycbc_Bucket *parent;

    /**
     * A list of fatal exceptions, i.e. ones not resulting from a bad
     * LCB error code
     */
    PyObject *exceptions;

    /** A failed LCB operation, if any */
    PyObject *errop;

    pycbc_dur_params dur;

    /** Quick-check value to see if everything went well */
    int all_ok;

    /** Options for 'MultiResult' */
    int mropts;

    pycbc_enhanced_err_info *err_info;
};

typedef struct {
    pycbc_MultiResult base;

    /* How many operations do we have remaining */
    unsigned int nops;

    /* Object for the callback */
    PyObject *callback;

    /* Object to be invoked with errors */
    PyObject *errback;
} pycbc_AsyncResult;


/**
 * This structure is passed to our exception throwing function, it's
 * usually wrapped by one of the macros below
 */
struct pycbc_exception_params {
    /** C Source file at which the error was thrown (populated by macro */
    const char *file;

    /** C Source line, as above */
    int line;

    /** LCB Error code, if any */
    lcb_STATUS err;

    /** Error message, if any */
    const char *msg;

    /** Key at which the error occurred. Not always present */
    PyObject *key;

    /** Single result which triggered the error, if present */
    PyObject *result;

    /**
     * A MultiResult object. This contains other operations which may
     * or may not have failed. This allows a user to check the status
     * of multi operations in which one of the keys resulted in an
     * exception
     */
    PyObject *all_results;

    /**
     * Extra info which caused the error. This is usually some kind of
     * bad parameter.
     */
    PyObject *objextra;

    /**
     * Enhanced error info if required.
     */
    pycbc_enhanced_err_info *err_info;
};

/**
 * Initializes a pycbc_exception_params to contain the proper
 * source context info
 */
#define PYCBC_EXC_STATIC_INIT { __FILE__, __LINE__ }

/**
 * Argument object, used for passing more information to the
 * multi functions. This isn't documented API yest.
 */
typedef struct {
    PyDictObject dict;
    int dummy; /* avoid sizing issues */
} pycbc_ArgumentObject;


/**
 * Object used as the 'value' for observe responses
 */
typedef struct {
    PyObject_HEAD
    unsigned int flags;
    int from_master;
    unsigned PY_LONG_LONG cas;
} pycbc_ObserveInfo;

/**
 * Flags to use for each type to indicate which subfields are relevant to
 * print out.
 */
enum {
    PYCBC_RESFLD_RC     = 1 << 0,
    PYCBC_RESFLD_CAS    = 1 << 1,
    PYCBC_RESFLD_KEY    = 1 << 2,
    PYCBC_RESFLD_FLAGS  = 1 << 3,
    PYCBC_RESFLD_HTCODE = 1 << 4,
    PYCBC_RESFLD_VALUE  = 1 << 5,
    PYCBC_RESFLD_URL    = 1 << 6
};

#define PYCBC_RESULT_BASEFLDS (PYCBC_RESFLD_RC)
#define PYCBC_OPRESULT_BASEFLDS \
    (PYCBC_RESULT_BASEFLDS| \
            PYCBC_RESFLD_CAS| \
            PYCBC_RESFLD_KEY)

#define PYCBC_VALRESULT_BASEFLDS (PYCBC_OPRESULT_BASEFLDS| \
        PYCBC_RESFLD_VALUE| \
        PYCBC_RESFLD_FLAGS)

#define PYCBC_HTRESULT_BASEFLDS \
    (       PYCBC_RESULT_BASEFLDS   | \
            PYCBC_RESFLD_HTCODE     | \
            PYCBC_RESFLD_URL        | \
            PYCBC_RESFLD_VALUE)

#define PYCBC_RESPROPS_NAME "_fldprops"
/**
 * Wrapper around PyType_Ready which also injects the common flags properties
 */
int pycbc_ResultType_ready(PyTypeObject *p, int flags);

/**
 * Types used for tracing
 */
#define PYCBC_TRACING_TYPES(X)\
    X(Tracer, "The Tracer Object")

#define PYCBC_CRYPTO_TYPES(X)                         \
    X(CryptoProvider,                                 \
      "A Cryptography Provider for Field Encryption", \
      pycbc_CryptoProvideType_extra_init(ptr))        \
    X(NamedCryptoProvider, "A Named Cryptography Provider for Field Encryption")

#define PYCBC_COLLECTION_TYPES(X) \
    X(Collection,                 \
      "A Couchbase Collection",   \
      pycbc_CryptoProvideType_extra_init(ptr))
#define PYCBC_AUTODEF_TYPES(X) \
    PYCBC_CRYPTO_TYPES(X)      \
    PYCBC_TRACING_TYPES(X)     \
    PYCBC_COLLECTION_TYPES(X)

/**
 * Extern PyTypeObject declaraions.
 */

/* multiresult.c */
extern PyTypeObject pycbc_MultiResultType;
extern PyTypeObject pycbc_AsyncResultType;

/* result.c */
extern PyTypeObject pycbc_ResultType;

/* opresult.c */
extern PyTypeObject pycbc_OperationResultType;
extern PyTypeObject pycbc_ValueResultType;
extern PyTypeObject pycbc_HttpResultType;
extern PyTypeObject pycbc_ItemType;
extern PyTypeObject pycbc__SDResultType;

/* views.c */
extern PyTypeObject pycbc_ViewResultType;

/* ext.c */
#define PYCBC_EXTERN(X, DOC, ...) extern PyTypeObject pycbc_##X##Type;

PYCBC_AUTODEF_TYPES(PYCBC_EXTERN);
#undef PYCBC_EXTERN
/**
 * Result type check macros
 */
#define PYCBC_VALRES_CHECK(o) \
        PyObject_IsInstance(o, &pycbc_ValueResultType)

#define PYCBC_OPRES_CHECK(o) \
    PyObject_IsInstance(o, (PyObject*)&pycbc_OperationResultType)

extern PyTypeObject pycbc_ArgumentType;

/**
 * XXX: This isn't used.
 */
extern PyObject *pycbc_ExceptionType;

/**
 * X-macro to define the helpers we pass from _bootstrap.py along to
 * the module's '_init_helpers' function. We use an xmacro here because
 * the parameters may change and the argument handling is rather complex.
 * See below (in the pycbc_helpers structure) and in ext.c for more usages.
 */
#define PYCBC_XHELPERS(X) \
    X(result_reprfunc) \
    X(fmt_utf8_flags) \
    X(fmt_bytes_flags) \
    X(fmt_json_flags) \
    X(fmt_pickle_flags) \
    X(pickle_encode) \
    X(pickle_decode) \
    X(json_encode) \
    X(json_decode) \
    X(lcb_errno_map) \
    X(misc_errno_map) \
    X(default_exception) \
    X(obsinfo_reprfunc) \
    X(itmcoll_base_type) \
    X(itmopts_dict_type) \
    X(itmopts_seq_type) \
    X(fmt_auto) \
    X(view_path_helper) \
    X(sd_result_type) \
    X(sd_multival_type)

#define PYCBC_XHELPERS_STRS(X) \
    X(tcname_encode_key, PYCBC_TCNAME_ENCODE_KEY) \
    X(tcname_encode_value, PYCBC_TCNAME_ENCODE_VALUE) \
    X(tcname_decode_key, PYCBC_TCNAME_DECODE_KEY) \
    X(tcname_decode_value, PYCBC_TCNAME_DECODE_VALUE) \
    X(ioname_modevent, "update_event") \
    X(ioname_modtimer, "update_timer") \
    X(ioname_startwatch, "start_watching") \
    X(ioname_stopwatch, "stop_watching") \
    X(ioname_mkevent, "io_event_factory") \
    X(ioname_mktimer, "timer_event_factory") \
    X(vkey_id,        "id") \
    X(vkey_key,       "key") \
    X(vkey_value,     "value") \
    X(vkey_geo,       "geometry") \
    X(vkey_docresp,   "__DOCRESULT__")

/**
 * Definition of global helpers. This is only instantiated once as
 * pycbc_helpers.
 */
struct pycbc_helpers_ST {
    #define X(n) PyObject *n;
    PYCBC_XHELPERS(X)
    #undef X

    #define X(n, s) PyObject *n;
    PYCBC_XHELPERS_STRS(X)
    #undef X
};

/**
 * We use this one a lot. This is defined in ext.c
 */
extern struct pycbc_helpers_ST pycbc_helpers;

/**
 * Threading macros
 */
#define PYCBC_USE_THREADS

#ifdef PYCBC_USE_THREADS
#define PYCBC_CONN_THR_BEGIN(conn) \
    if ((conn)->unlock_gil) { \
        pycbc_assert((conn)->thrstate == NULL); \
        (conn)->thrstate = PyEval_SaveThread(); \
    }

#define PYCBC_CONN_THR_END(conn) \
    if ((conn)->unlock_gil) { \
        pycbc_assert((conn)->thrstate); \
        PyEval_RestoreThread((conn)->thrstate); \
        (conn)->thrstate = NULL; \
    }

#else
#define PYCBC_CONN_THR_BEGIN(X)
#define PYCBC_CONN_THR_END(X)
#endif

/*******************************
 * Type Initialization Functions
 *******************************
 *
 * These functions are called once from the extension's import method.
 * See ext.c
 *
 * They basically initialize the corresponding Python type so that
 * we can use them further on.
 */

/** Initializes the constants, constants. */
void pycbc_init_pyconstants(PyObject *module);
PyObject *pycbc_lcb_errstr(lcb_t instance, lcb_STATUS err);
PyObject *pycbc_print_constants(PyObject *mod, PyObject *args);

int pycbc_ResultType_init(PyObject **ptr);
int pycbc_BucketType_init(PyObject **ptr);
int pycbc_MultiResultType_init(PyObject **ptr);
int pycbc_ValueResultType_init(PyObject **ptr);
int pycbc_OperationResultType_init(PyObject **ptr);
int pycbc_SDResultType_init(PyObject **ptr);
int pycbc_HttpResultType_init(PyObject **ptr);
int pycbc_TranscoderType_init(PyObject **ptr);
int pycbc_ObserveInfoType_init(PyObject **ptr);
int pycbc_ItemType_init(PyObject **ptr);
int pycbc_EventType_init(PyObject **ptr);
int pycbc_TimerEventType_init(PyObject **ptr);
int pycbc_IOEventType_init(PyObject **ptr);
int pycbc_AsyncResultType_init(PyObject **ptr);
int pycbc_IOPSWrapperType_init(PyObject **ptr);
int pycbc_ViewResultType_init(PyObject **ptr);
int pycbc_CollectionType_init(PyObject **ptr);

#define PYCBC_TYPE_INIT_DECL(TYPENAME, TYPE_DOC, ...) \
    int pycbc_##TYPENAME##Type_init(PyObject **ptr);

PYCBC_AUTODEF_TYPES(PYCBC_TYPE_INIT_DECL)

#undef PYCBC_TYPE_INIT_DECL
/**
 * Calls the type's constructor with no arguments:
 */

#define PYCBC_TYPE_CTOR_1_args(t)               PyObject_CallFunction((PyObject*)t, 0)
#define PYCBC_TYPE_CTOR_2_args(t, args)         PyObject_CallFunction((PyObject*)t, "O", args)
#define PYCBC_TYPE_CTOR_3_args(t, args, kwargs) PyObject_CallFunction((PyObject*)t, "OO", args, kwargs)

#define GET_4TH_ARG(arg1, arg2, arg3, arg4, ...) arg4
#define PYCBC_TYPE_CTOR_CHOOSER(...) \
    GET_4TH_ARG(__VA_ARGS__, PYCBC_TYPE_CTOR_3_args, \
                PYCBC_TYPE_CTOR_2_args, PYCBC_TYPE_CTOR_1_args, )

#define PYCBC_TYPE_CTOR(...) PYCBC_TYPE_CTOR_CHOOSER(__VA_ARGS__)(__VA_ARGS__)

/**
 * Allocators for result functions. See callbacks.c:get_common
 */
PyObject *pycbc_result_new(pycbc_Bucket *parent);
PyObject *pycbc_multiresult_new(pycbc_Bucket *parent);
pycbc_ValueResult *pycbc_valresult_new(pycbc_Bucket *parent);
pycbc_OperationResult *pycbc_opresult_new(pycbc_Bucket *parent);
pycbc_Item *pycbc_item_new(pycbc_Bucket *parent);
pycbc__SDResult *pycbc_sdresult_new(pycbc_Bucket *parent, PyObject *specs);

/* Add a result to a list of multi results. Specify the index */
void pycbc_sdresult_addresult(pycbc__SDResult *obj, size_t ii, PyObject *item);

/**
 * Simple function, here because it's defined in result.c but needed in
 * opresult.c
 */
void pycbc_Result_dealloc(pycbc_Result *self);

/**
 * Traps the current exception and adds it to the current MultiResult
 * context.
 * @param mres The MultiResult object.
 *
 * This calls pycbc_exc_mktuple(), so the constrains there apply to this
 * function as well.
 */
void pycbc_multiresult_adderr(pycbc_MultiResult* mres);

/**
 * Raise an exception from a multi result. This will raise an exception if:
 * 1) There is a 'fatal' error in the 'exceptions' list
 * 2) There is an 'operr'. 'operr' can be a failed LCB code (if no_raise_enoent
 * is on, this is not present if the failed code was LCB_KEY_ENOENT)
 */
int pycbc_multiresult_maybe_raise(pycbc_MultiResult *self);

/**
 * Return the effective user-facing value from this MultiResult object.
 * This should only be called if 'maybe_raise' returns false.
 * @param self the object
 * @return a new reference to the final result, or NULL on error.
 */
PyObject* pycbc_multiresult_get_result(pycbc_MultiResult *self);

/**
 * Invokes a callback when an operation has been completed. This will either
 * invoke the operation's "error callback" or the operation's "result callback"
 * depending on the state.
 */
void pycbc_asyncresult_invoke(pycbc_AsyncResult *mres,
                              pycbc_enhanced_err_info *err_info);

/**
 * Initialize the callbacks for the lcb_t
 */
void pycbc_callbacks_init(lcb_t instance);
void pycbc_http_callbacks_init(lcb_t instance);
void pycbc_views_callbacks_init(lcb_t instance);

/**
 * "Real" exception handler.
 * @param mode one of the PYCBC_EXC_* constants
 * @param p a struct of exception parameters
 */
void pycbc_exc_wrap_REAL(int mode, struct pycbc_exception_params *p);

/**
 * Get the appropriate Couchbase exception object.
 * @param mode one of the PYCBC_EXC_* constants
 * @param err the libcouchbase error, if any
 * @return a borrowed reference to the appropriate exception class
 */
PyObject* pycbc_exc_map(int mode, lcb_STATUS err);

/**
 * Creates a simple exception with a given message. The exception
 * is not thrown.
 */
PyObject* pycbc_exc_message(int mode, lcb_STATUS err, const char *msg);

/**
 * Gets the error classifier categories (as a set of bit flags) for a given
 * error code.
 */
PyObject* pycbc_exc_get_categories(PyObject *self, PyObject *arg);
/**
 * Throws an exception. If an exception is pending, it is caught and wrapped,
 * delivered into the CouchbaseError's 'inner_cause' field
 *
 * @param e_mode one of the PYCBC_EXC_* constants
 * @param e_err the LCB error code (use 0 if none)
 * @param e_msg a string message, if any
 * @param e_key the key during the handling of which the error occurred
 * @param e_objextra the problematic object which actually caused the errror
 */
#define PYCBC_EXC_WRAP_EX_FILE_LINE(e_mode, e_err, e_msg, e_key, e_objextra, e_err_info, e_file, e_line) \
    {                                                                          \
        PYCBC_DEBUG_LOG("Raising exception at %s, %d", e_file, e_line)     \
        struct pycbc_exception_params __pycbc_ep = {0};                        \
        __pycbc_ep.file = e_file;                                              \
        __pycbc_ep.line = e_line;                                              \
        __pycbc_ep.err = e_err;                                                \
        __pycbc_ep.msg = e_msg;                                                \
        __pycbc_ep.key = e_key;                                                \
        __pycbc_ep.objextra = e_objextra;                                      \
        __pycbc_ep.err_info = e_err_info;                                      \
        Py_XINCREF(e_err_info);                                                \
        pycbc_exc_wrap_REAL(e_mode, &__pycbc_ep);                              \
    }

#define PYCBC_EXC_WRAP_EX(e_mode, e_err, e_msg, e_key, e_objextra, e_err_info) \
    PYCBC_EXC_WRAP_EX_FILE_LINE(e_mode, e_err, e_msg, e_key, e_objextra, e_err_info, __FILE__, __LINE__)

#define PYCBC_EXC_WRAP(mode, err, msg) \
    PYCBC_EXC_WRAP_EX(mode, err, msg, NULL, NULL, NULL)

#define PYCBC_EXC_WRAP_OBJ(mode, err, msg, obj) \
    PYCBC_EXC_WRAP_EX(mode, err, msg, NULL, obj, NULL)

#define PYCBC_EXC_WRAP_KEY(mode, err, msg, key) \
    PYCBC_EXC_WRAP_EX(mode, err, msg, key, NULL, NULL)

#define PYCBC_EXC_WRAP_KEY_ERR_INFO(mode, err, msg, key, err_info) \
    PYCBC_EXC_WRAP_EX(mode, err, msg, key, NULL, err_info)

#define PYCBC_EXC_WRAP_VALUE PYCBC_EXC_WRAP_KEY

int pycbc_handle_assert(const char *msg, const char* file, int line);

/**
 * Creates a tuple of (class, object, traceback), similar to what would be
 * returned from sys.exc_info()
 * @return The error tuple.
 *
 * Calling this function will also clear the error
 * state. This must be called only if PyErr_Occurred() is true.
 */
PyObject *pycbc_exc_mktuple(void);

/**
 * This macro can be used as an 'if' structure. It returns false if the
 * condition fails and try otherwise
 */
#define pycbc_assert(e) ((e) ? 1 : pycbc_handle_assert(#e, __FILE__, __LINE__))

/**
 * EXCTHROW macros. These provide error messages for common stages.
 */
#define PYCBC_EXCTHROW_WAIT(err) PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, \
       "There was a problem while trying to send/receive " \
       "your request over the network. This may be a result of a " \
       "bad network or a misconfigured client or server")

#define PYCBC_EXCTHROW_SCHED(err) PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, \
        "There was a problem scheduling your request, or determining " \
        "the appropriate server or vBucket for the key(s) requested. "\
        "This may also be a bug in the SDK if there are no network issues")

#define PYCBC_EXCTHROW_ARGS() PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, \
                                           "Bad/insufficient arguments provided")

#define PYCBC_EXCTHROW_EMPTYKEY() PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, \
        "Empty key (i.e. '', empty string) passed")

/**
 * Encodes a key into a buffer.
 * @param conn the connection object
 * @param key. in-out. Input should be the Python key. Output should be the
 * new python object which contains the underlying buffer for the key.
 * @param buf a pointer to a buffer pointer
 * @param nbuf pointer to the length of the buffer
 *
 * The buf parameter will likely be tied to the key parameter, so be sure not
 * to decrement its refcount until buf is no longer needed
 *
 * @return
 * 0 on success, nonzero on error
 */
int pycbc_tc_encode_key(pycbc_Bucket *conn, PyObject *src, pycbc_pybuffer *dst);

/**
 * Decodes a key buffer into a python object.
 * @param conn the connection object
 * @param key the key buffer
 * @param nkey the size of the key
 * @param pobj a pointer to a PyObject*, will be set with a newly-created python
 * object which represents the converted key
 *
 * @return
 * 0 on success, nonzero on error
 */
int pycbc_tc_decode_key(pycbc_Bucket *conn, const void *key, size_t nkey,
                        PyObject **pobj);

/**
 * Encode a value with flags
 * @param value. in-out. Input should be the Python value, Output should be the
 * new python object which contains the converted value.
 * @param flag_v. Python object representing 'flags'. This is used for efficiency
 * in order to pass a pythonized version of the flags
 * @param buf a pointer to a buffer, likely tied to 'buf'
 * @param nbuf pointer to buffer length
 * @param flags pointer to a flags variable, will be set with the appropriate
 * flags
 */
int pycbc_tc_encode_value(pycbc_Bucket *conn, PyObject *srcbuf, PyObject *flag_v,
                          pycbc_pybuffer *dstbuf, lcb_U32 *dstflags);

/**
 * Decode a value with flags
 * @param conn the connection object
 * @param value as received from the server
 * @param nvalue length of value
 * @param flags flags as received from the server
 * @param pobj the pythonized value
 */
int pycbc_tc_decode_value(pycbc_Bucket *conn, const void *value, size_t nvalue,
                          lcb_U32 flags, PyObject **pobj);

/**
 * Like encode_value, but only uses built-in encoders
 */
int pycbc_tc_simple_encode(PyObject *src, pycbc_pybuffer *dst, lcb_U32 flags);

/**
 * Like decode_value, but only uses built-in decoders
 */
int pycbc_tc_simple_decode(PyObject **vp, const char *buf, size_t nbuf,
                           lcb_U32 flags);

/**
 * Automatically determine the format for the object.
 */
PyObject *
pycbc_tc_determine_format(PyObject *value);

PyObject *
pycbc_iowrap_new(pycbc_Bucket *conn, PyObject *pyio);

lcb_io_opt_t
pycbc_iowrap_getiops(PyObject *iowrap);

/**
 * Event callback handling
 */
void pycbc_invoke_connected_event(pycbc_Bucket *conn, lcb_STATUS err);

/**
 * Schedule the dtor event
 */
void pycbc_schedule_dtor_event(pycbc_Bucket *self);

/**
 * Pipeline handlers
 */
PyObject* pycbc_Bucket__start_pipeline(pycbc_Bucket *);
PyObject* pycbc_Bucket__end_pipeline(pycbc_Bucket *);

/**
 * Control methods
 */
PyObject* pycbc_Bucket__cntl(pycbc_Bucket *, PyObject *, PyObject *);
PyObject* pycbc_Bucket__vbmap(pycbc_Bucket *, PyObject *);
PyObject* pycbc_Bucket__cntlstr(pycbc_Bucket *conn, PyObject *args, PyObject *kw);

/**
 * Health-check methods
 */
PyObject *pycbc_Bucket__ping(pycbc_Bucket *self,
                             PyObject *args,
                             PyObject *kwargs);

PyObject *pycbc_Bucket__diagnostics(pycbc_Bucket *self,
                                    PyObject *args,
                                    PyObject *kwargs);

/**
 * Encryption Provider
 */
typedef struct {
    PyObject_HEAD lcbcrypto_PROVIDER *lcb_provider;
} pycbc_CryptoProvider;

typedef struct {
    PyObject_HEAD pycbc_CryptoProvider *orig_py_provider;
    lcbcrypto_PROVIDER *lcb_provider;
    PyObject *name;
} pycbc_NamedCryptoProvider;

#define PP_FOR_EACH_CRYPTO_EXCEPTION(X, ...)                               \
    X(PYCBC_CRYPTO_PROVIDER_NOT_FOUND, = LCB_MAX_ERROR),                   \
            X(PYCBC_CRYPTO_PROVIDER_ALIAS_NULL),                           \
            X(PYCBC_CRYPTO_PROVIDER_MISSING_PUBLIC_KEY),                   \
            X(PYCBC_CRYPTO_PROVIDER_MISSING_SIGNING_KEY),                  \
            X(PYCBC_CRYPTO_PROVIDER_MISSING_PRIVATE_KEY),                  \
            X(PYCBC_CRYPTO_PROVIDER_SIGNING_FAILED),                       \
            X(PYCBC_CRYPTO_PROVIDER_ENCRYPT_FAILED),                       \
            X(PYCBC_CRYPTO_PROVIDER_DECRYPT_FAILED),                       \
            X(PYCBC_CRYPTO_PROVIDER_KEY_SIZE_EXCEPTION),                   \
            X(PYCBC_CRYPTO_CONFIG_ERROR), X(PYCBC_CRYPTO_EXECUTION_ERROR), \
            X(PYCBC_CRYPTO_ERROR)

typedef enum {
#define PYCBC_CRYPTO_X(NAME, ...) NAME __VA_ARGS__
#define COMMA ,
    PP_FOR_EACH_CRYPTO_EXCEPTION(PYCBC_CRYPTO_X),
    PYCBC_CRYPTO_PROVIDER_ERROR_MAX
#undef COMMA
#undef PYCBC_CRYPTO_X
} pycbc_crypto_err;

PyObject *pycbc_gen_crypto_exception_map(void);

/**
 * Flag to check if logging is enabled for the library via Python's logging
 */
extern PyObject* pycbc_log_handler;
extern struct lcb_logprocs_st pycbc_lcb_logprocs;

/**
 * Dummy tuple/keywords, used for PyArg_ParseTupleAndKeywordArgs, which dies
 * if one of the arguments is NULL, so these contain empty tuples and dicts,
 * respectively.
 */
extern PyObject *pycbc_DummyTuple;
extern PyObject *pycbc_DummyKeywords;

#endif /* PYCBC_H_ */
