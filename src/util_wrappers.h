//
// Created by Ellis Breen on 2019-06-28.
//

#ifndef COUCHBASE_PYTHON_CLIENT_UTIL_WRAPPERS_H
#define COUCHBASE_PYTHON_CLIENT_UTIL_WRAPPERS_H

#ifndef __FUNCTION_NAME__
#    if defined(_WIN32) || defined(WIN32) // WINDOWS
#        define __FUNCTION_NAME__ __FUNCTION__
#    else //*NIX
#        define __FUNCTION_NAME__ __func__
#    endif
#endif

typedef struct pycbc_stack_context_decl *pycbc_stack_context_handle;

#define PYCBC_TABBED_CONTEXTS
#ifndef PYCBC_DEBUG
#    ifdef PYCBC_DEBUG_ENABLE
#        define PYCBC_DEBUG
#    endif
#endif

#ifdef PYCBC_DEBUG
void pycbc_debug_log_prefix(const char *FILE,
                            const char *FUNC,
                            int LINE,
                            pycbc_stack_context_handle CONTEXT);
void pycbc_debug_log(const char *FILE,
                     const char *FUNC,
                     int LINE,
                     pycbc_stack_context_handle CONTEXT,
                     const char *format,
                     ...);
#    define PYCBC_DEBUG_LOG_PREFIX(FILE, FUNC, LINE, CONTEXT) \
        pycbc_debug_log_prefix(FILE, FUNC, LINE, CONTEXT);
#    define PYCBC_DEBUG_LOG_CONTEXT_FULL(FILE, FUNC, LINE, CONTEXT, ...) \
        pycbc_debug_log(FILE, FUNC, LINE, CONTEXT, __VA_ARGS__);
void pycbc_debug_log_prefix_nocontext(const char *FILE,
                                      const char *FUNC,
                                      int LINE);
void pycbc_debug_log_nocontext(
        const char *FILE, const char *FUNC, int LINE, const char *format, ...);
#    define PYCBC_DEBUG_LOG_NOCONTEXT_FULL(FILE, FUNC, LINE, ...) \
        pycbc_debug_log_nocontext(FILE, FUNC, LINE, __VA_ARGS__);
#    define PYCBC_DEBUG_LOG_PREFIX_NOCONTEXT(FILE, FUNC, LINE) \
        pycbc_debug_log_prefix_nocontext(FILE, FUNC, LINE);
#else
#    define PYCBC_DEBUG_LOG_NOCONTEXT_FULL(FILE, FUNC, LINE, CONTEXT, ...)
#    define PYCBC_DEBUG_LOG_CONTEXT_FULL(FILE, FUNC, LINE, CONTEXT, FORMAT, ...)
#    define PYCBC_DEBUG_LOG_PREFIX(FILE, FUNC, LINE, CONTEXT)
#    define PYCBC_DEBUG_LOG_PREFIX_NOCONTEXT(FILE, FUNC, LINE)
#endif

#ifdef PYCBC_DEBUG
#    define PYCBC_DEBUG_LOG_RAW(...) fprintf(stderr, __VA_ARGS__);
void pycbc_print_pyformat(const char *format, ...);
void pycbc_exception_log(const char *file,
                         const char *func,
                         int line,
                         int clear);
#    define PYCBC_DEBUG_PYFORMAT_FILE_FUNC_LINE_CONTEXT(  \
            FILE, FUNC, LINE, CONTEXT, ...)               \
        PYCBC_DEBUG_LOG_PREFIX(FILE, FUNC, LINE, CONTEXT) \
        pycbc_print_pyformat(__VA_ARGS__);                \
        fprintf(stderr, "\n");
#    define PYCBC_DEBUG_PYFORMAT_FILE_FUNC_LINE_NOCONTEXT( \
            FILE, FUNC, LINE, ...)                         \
        PYCBC_DEBUG_LOG_PREFIX_NOCONTEXT(FILE, FUNC, LINE) \
        pycbc_print_pyformat(__VA_ARGS__);                 \
        fprintf(stderr, "\n");

#    define PYCBC_DEBUG_PYFORMAT_FILE_FUNC_AND_LINE(FILE, FUNC, LINE, ...) \
        PYCBC_DEBUG_PYFORMAT_FILE_FUNC_LINE_NOCONTEXT(                     \
                FILE, FUNC, LINE, __VA_ARGS__)
#    define PYCBC_DEBUG_PYFORMAT_CONTEXT(CONTEXT, ...) \
        PYCBC_DEBUG_PYFORMAT_FILE_FUNC_LINE_CONTEXT(   \
                __FILE__, __FUNCTION_NAME__, __LINE__, CONTEXT, __VA_ARGS__)
#    define PYCBC_DEBUG_PYFORMAT(...)            \
        PYCBC_DEBUG_PYFORMAT_FILE_FUNC_AND_LINE( \
                __FILE__, __FUNCTION_NAME__, __LINE__, __VA_ARGS__)
#    define PYCBC_EXCEPTION_LOG_NOCLEAR \
        pycbc_exception_log(__FILE__, __FUNCTION_NAME__, __LINE__, 0);
#    define PYCBC_EXCEPTION_LOG \
        pycbc_exception_log(__FILE__, __FUNCTION_NAME__, __LINE__, 1);
#    define PYCBC_DEBUG_FLUSH fflush(stderr);
#else
#    define PYCBC_DEBUG_LOG_RAW(...)
#    define PYCBC_DEBUG_PYFORMAT_CONTEXT(CONTEXT, FORMAT, ...)
#    define PYCBC_DEBUG_PYFORMAT(...)
#    define PYCBC_EXCEPTION_LOG_NOCLEAR
#    define PYCBC_EXCEPTION_LOG PyErr_Clear();
#    define PYCBC_DEBUG_FLUSH
#endif

#define PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_POSTFIX( \
        FILE, FUNC, LINE, CONTEXT, ...)                  \
    PYCBC_DEBUG_LOG_CONTEXT_FULL(FILE, FUNC, LINE, CONTEXT, __VA_ARGS__)

#define PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_POSTFIX_NOCONTEXT( \
        FILE, FUNC, LINE, ...)                                     \
    PYCBC_DEBUG_LOG_NOCONTEXT_FULL(FILE, FUNC, LINE, __VA_ARGS__)

#define PYCBC_DEBUG_LOG_WITH_FILE_FUNC_LINE_CONTEXT_NEWLINE( \
        FILE, FUNC, LINE, CONTEXT, ...)                      \
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_POSTFIX(         \
            FILE, FUNC, LINE, CONTEXT, __VA_ARGS__)
#define PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE(FILE, FUNC, LINE, ...) \
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_POSTFIX_NOCONTEXT(                 \
            FILE, FUNC, LINE, __VA_ARGS__)
#define PYCBC_DEBUG_LOG(...)                         \
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_AND_LINE_NEWLINE( \
            __FILE__, __FUNCTION_NAME__, __LINE__, __VA_ARGS__)
#define PYCBC_DEBUG_LOG_CONTEXT(CONTEXT, ...)            \
    PYCBC_DEBUG_LOG_WITH_FILE_FUNC_LINE_CONTEXT_NEWLINE( \
            __FILE__, __FUNCTION_NAME__, __LINE__, CONTEXT, __VA_ARGS__)

#ifdef PYCBC_DEBUG
#    define PYCBC_MALLOC(X) \
        malloc_and_log(__FILE__, __FUNCTION_NAME__, __LINE__, 1, X, #X)
#    define PYCBC_MALLOC_TYPED(X, Y) \
        malloc_and_log(__FILE__, __FUNCTION_NAME__, __LINE__, X, sizeof(Y), #Y)
#    define PYCBC_CALLOC(X, Y) \
        calloc_and_log(__FILE__, __FUNCTION_NAME__, __LINE__, X, Y, "unknown")
#    define PYCBC_CALLOC_TYPED(X, Y) \
        calloc_and_log(__FILE__, __FUNCTION_NAME__, __LINE__, X, sizeof(Y), #Y)
#    define PYCBC_FREE(X)                     \
        if (X) {                              \
            PYCBC_DEBUG_LOG("freeing %p", X); \
        }                                     \
        free(X);

#    define LOG_REFOP(Y, OP)                                       \
        {                                                          \
            pycbc_assert((Y) && Py_REFCNT(Y) > 0);                 \
            PYCBC_DEBUG_PYFORMAT(                                  \
                    "%p has count of %ld: *** %R ***: OP: %s",     \
                    Y,                                             \
                    (long)(((PyObject *)(Y)) ? Py_REFCNT(Y) : 0),  \
                    ((PyObject *)(Y) ? (PyObject *)(Y) : Py_None), \
                    #OP);                                          \
            Py_##OP((PyObject *)Y);                                \
        }
#    define LOG_REFOPX(Y, OP)                                      \
        {                                                          \
            pycbc_assert(!(Y) || Py_REFCNT(Y) > 0);                \
            PYCBC_DEBUG_PYFORMAT(                                  \
                    "%p has count of %ld: *** %R ***: OP: %s",     \
                    Y,                                             \
                    (long)(((PyObject *)(Y)) ? Py_REFCNT(Y) : 0),  \
                    ((PyObject *)(Y) ? (PyObject *)(Y) : Py_None), \
                    #OP);                                          \
            Py_##X##OP((PyObject *)(Y));                           \
        }

#else
#    define PYCBC_FREE(X) free(X)
#    define PYCBC_MALLOC(X) malloc(X)
#    define PYCBC_MALLOC_TYPED(X, Y) malloc((X) * sizeof(Y))
#    define PYCBC_CALLOC(X, Y) calloc(X, Y)
#    define PYCBC_CALLOC_TYPED(X, Y) calloc(X, sizeof(Y))
#    define LOG_REFOP(Y, OP) Py_##OP(Y)
#    define LOG_REFOPX(Y, OP) Py_X##OP(Y)
#endif
#define PYCBC_DECREF(X) LOG_REFOP(X, DECREF)
#define PYCBC_XDECREF(X) LOG_REFOPX(X, DECREF)
#define PYCBC_INCREF(X) LOG_REFOP(X, INCREF)
#define PYCBC_XINCREF(X) LOG_REFOPX(X, INCREF)

#include <stddef.h>
typedef struct pycbc_stack_context_decl *pycbc_stack_context_handle;
typedef struct {
    const void *v;
    size_t n;
} pycbc_generic_array;
typedef struct {
    char *buffer;
    size_t length;
} pycbc_strn_base;
typedef struct {
    const char *buffer;
    size_t length;
} pycbc_strn_base_const;
typedef pycbc_strn_base pycbc_strn;
typedef struct {
    pycbc_strn_base content;
} pycbc_strn_unmanaged;
const char *pycbc_strn_buf(pycbc_strn buf);

int pycbc_strn_valid(pycbc_strn buf);

size_t pycbc_strn_len(pycbc_strn_base_const buf);

char *pycbc_strn_buf_psz(pycbc_strn_unmanaged buf);

void pycbc_strn_free(pycbc_strn_unmanaged buf);

pycbc_generic_array pycbc_strn_base_const_array(pycbc_strn_base_const orig);

#define IMPL_DECL(...)
#define DECL_IMPL(...) __VA_ARGS__
#define DECL_DECL(...) DECL_IMPL(__VA_ARGS__);
#define IMPL_IMPL(...) __VA_ARGS__

#endif // COUCHBASE_PYTHON_CLIENT_UTIL_WRAPPERS_H
