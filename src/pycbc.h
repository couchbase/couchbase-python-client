#ifndef PYCBC_H_
#define PYCBC_H_
#undef NDEBUG

#include <Python.h>
#include <libcouchbase/couchbase.h>


#ifdef PY_SSIZE_T_CLEAN
typedef Py_ssize_t pycbc_strlen_t;
#else
typedef int pycbc_strlen_t;
#endif

#define PYCBC_PACKAGE_NAME "couchbase"
#define PYCBC_MODULE_NAME "_libcouchbase"
#define PYCBC_FQNAME PYCBC_PACKAGE_NAME "." PYCBC_MODULE_NAME


#if PY_MAJOR_VERSION == 3
#define PYCBC_POBJ_HEAD_INIT(t) { PyObject_HEAD_INIT(t) },

#define pycbc_IntFromL PyLong_FromLong
#define pycbc_IntFromUL PyLong_FromUnsignedLong
#define pycbc_IntFromULL PyLong_FromUnsignedLongLong
#define pycbc_IntAsULL PyLong_AsUnsignedLongLong
#define pycbc_IntAsLL PyLong_AsLongLong
#define pycbc_IntAsUL PyLong_AsUnsignedLong
#define pycbc_IntAsL PyLong_AsLong
#define pycbc_SimpleStringZ(c) PyUnicode_FromString(c)
#define pycbc_SimpleStringN(c, n) PyUnicode_FromStringAndSize(c, n)


#else
#define PYCBC_POBJ_HEAD_INIT(t) PyObject_HEAD_INIT(t)


#define pycbc_IntFromL PyInt_FromLong
#define pycbc_IntFromUL PyLong_FromUnsignedLong
#define pycbc_IntFromULL PyLong_FromUnsignedLong
#define pycbc_SimpleStringZ(c) PyString_FromString(c)
#define pycbc_SimpleStringN(c, n) PyString_FromStringAndSize(c, n)

unsigned PY_LONG_LONG pycbc_IntAsULL(PyObject *o);
PY_LONG_LONG pycbc_IntAsLL(PyObject *o);
long pycbc_IntAsL(PyObject *o);
unsigned long pycbc_IntAsUL(PyObject *o);


#endif

PyObject *pycbc_maybe_convert_to_int(PyObject *o);
int pycbc_BufFromString(PyObject *orig,
                        char **buf,
                        Py_ssize_t *nbuf,
                        PyObject **newkey);


/**
 * 'GET' operations:
 */
enum {
    PYCBC_CMD_GET = 500,
    PYCBC_CMD_LOCK,
    PYCBC_CMD_TOUCH,
    PYCBC_CMD_GAT,
    PYCBC_CMD_INCR,
    PYCBC_CMD_DECR,
    PYCBC_CMD_ARITH,
    PYCBC_CMD_DELETE,
    PYCBC_CMD_UNLOCK
};

/**
 * Various exception types to be thrown
 */
enum {
    PYCBC_EXC_ARGUMENTS,
    PYCBC_EXC_ENCODING,
    PYCBC_EXC_LCBERR
};

/* Argument options */
enum {
    PYCBC_ARGOPT_SINGLE = 0x1,
    PYCBC_ARGOPT_MULTI = 0x2
};

/**
 * Format flags
 */
enum {
    PYCBC_FMT_JSON = 0x0,
    PYCBC_FMT_PICKLE = 0x1,
    PYCBC_FMT_BYTES = 0x2,

    PYCBC_FMT_UTF8 = 0x4,

    PYCBC_FMT_MASK = 0x7
};

typedef struct {
    PyObject_HEAD

    /** LCB instance */
    lcb_t instance;

    /** Transcoder object */
    PyObject *tc;

    /** Default format, PyInt */
    PyObject *dfl_fmt;

    /** Connection Errors */
    PyObject *errors;

    /** Thread state */
    PyThreadState *thrstate;

    /** Whether to not raise any exceptions */
    unsigned int quiet;

    unsigned int unlock_gil;

    unsigned int data_passthrough;

    /** whether __init__ has already been called */
    unsigned char init_called;

    unsigned int flags;

} pycbc_ConnectionObject;
/**
 * Result object to be used with callbacks
 */
typedef struct {
    PyObject_HEAD
    lcb_uint64_t cas;
    lcb_uint32_t flags;
    lcb_error_t rc;

    /** Actual value */
    PyObject *value;

    /** Raw Value */
    PyObject *raw_value;

    /** Key */
    PyObject *key;

} pycbc_ResultObject;

/**
 * Object containing the result of a 'Multi' operation. It's the same as a
 * normal dict, except we add an 'all_ok' field, so a user doesn't need to
 * skim through all the pairs to determine if something failed.
 */
typedef struct {
    PyDictObject dict;
    pycbc_ConnectionObject *parent;
    PyObject *exceptions;
    PyObject *errop;
    int all_ok;
    int no_raise_enoent;
} pycbc_MultiResultObject;

struct pycbc_exception_params {
    const char *file;
    int line;
    lcb_error_t err;
    const char *msg;
    PyObject *key;
    PyObject *result;
    PyObject *all_results;
    PyObject *objextra;
};

#define PYCBC_EXC_STATIC_INIT { __FILE__, __LINE__ }


typedef struct {
    PyDictObject dict;
    int dummy; /* avoid sizing issues */
} pycbc_ArgumentObject;

extern PyTypeObject pycbc_MultiResultType;
extern PyTypeObject pycbc_ResultType;
extern PyTypeObject pycbc_ArgumentType;
extern PyObject *pycbc_ExceptionType;

#define PYCBC_XHELPERS(X) \
    X(result_reprfunc) \
    X(fmt_utf8_flags) \
    X(fmt_bytes_flags) \
    X(pickle_encode) \
    X(pickle_decode) \
    X(json_encode) \
    X(json_decode) \
    X(lcb_errno_map) \
    X(misc_errno_map) \
    X(default_exception)

struct pycbc_helpers_ST {
    #define X(n) PyObject *n;
    PYCBC_XHELPERS(X)
    #undef X
};

extern struct pycbc_helpers_ST pycbc_helpers;

/**
 * Threading macros
 */

#define PYCBC_USE_THREADS

#ifdef PYCBC_USE_THREADS
#define PYCBC_CONN_THR_BEGIN(conn) \
    if ((conn)->unlock_gil) { \
        assert((conn)->thrstate == NULL); \
        (conn)->thrstate = PyEval_SaveThread(); \
    }

#define PYCBC_CONN_THR_END(conn) \
    if ((conn)->unlock_gil) { \
        assert((conn)->thrstate); \
        PyEval_RestoreThread((conn)->thrstate); \
        (conn)->thrstate = NULL; \
    }

#else
#define PYCBC_CONN_THR_BEGIN(X)
#define PYCBC_CONN_THR_END(X)
#endif

/**
 * Initializes the constants
 */
void pycbc_init_pyconstants(PyObject *module);
PyObject *pycbc_lcb_errstr(lcb_t instance, lcb_error_t err);

int pycbc_ResultType_init(PyObject **ptr);
int pycbc_ConnectionType_init(PyObject **ptr);
int pycbc_MultiResultType_init(PyObject **ptr);
int pycbc_ArgumentType_init(PyObject **ptr);



PyObject *pycbc_result_new(pycbc_ConnectionObject *parent);

PyObject *pycbc_multiresult_new(pycbc_ConnectionObject *parent);
int pycbc_multiresult_maybe_raise(pycbc_MultiResultObject *self);

void pycbc_callbacks_init(lcb_t instance);


void pycbc_exc_wrap_REAL(int mode, struct pycbc_exception_params *p);

#define PYCBC_EXC_WRAP_EX(e_mode, e_err, e_msg, e_key, e_objextra) { \
    struct pycbc_exception_params __pycbc_ep = {0}; \
    __pycbc_ep.file = __FILE__; \
    __pycbc_ep.line = __LINE__; \
    __pycbc_ep.err = e_err; \
    __pycbc_ep.msg = e_msg; \
    __pycbc_ep.key = e_key; \
    __pycbc_ep.objextra = e_objextra; \
    pycbc_exc_wrap_REAL(e_mode, &__pycbc_ep); \
}

#define PYCBC_EXC_WRAP(mode, err, msg) \
        PYCBC_EXC_WRAP_EX(mode, err, msg, NULL, NULL)

#define PYCBC_EXC_WRAP_OBJ(mode, err, msg, obj) \
    PYCBC_EXC_WRAP_EX(mode, err, msg, NULL, obj)

#define PYCBC_EXC_WRAP_KEY(mode, err, msg, key) \
    PYCBC_EXC_WRAP_EX(mode, err, msg, key, NULL)

#define PYCBC_EXC_WRAP_VALUE PYCBC_EXC_WRAP_KEY

#define pycbc_exc_wrap_value PYCBC_EXC_WRAP_KEY

#define pycbc_exc_wrap_bytes(mode, err, msg, bytes, nbytes) \
    PYCBC_EXC_WRAP(mode, err, msg)




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
int pycbc_tc_encode_key(pycbc_ConnectionObject *conn,
                         PyObject **key,
                         void **buf,
                         size_t *nbuf);

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
int pycbc_tc_decode_key(pycbc_ConnectionObject *conn,
                         const void *key,
                         size_t nkey,
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
int pycbc_tc_encode_value(pycbc_ConnectionObject *conn,
                           PyObject **value,
                           PyObject *flag_v,
                           void **buf,
                           size_t *nbuf,
                           lcb_uint32_t *flags);

/**
 * Decode a value with flags
 * @param conn the connection object
 * @param value as received from the server
 * @param nvalue length of value
 * @param flags flags as received from the server
 * @param pobj the pythonized value
 */
int pycbc_tc_decode_value(pycbc_ConnectionObject *conn,
                           const void *value,
                           size_t nvalue,
                           lcb_uint32_t flags,
                           PyObject **pobj);

#endif /* PYCBC_H_ */
