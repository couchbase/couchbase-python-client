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
#include "pycbc_http.h"
#define CB_THREADS

#ifdef CB_THREADS

static void
cb_thr_end(pycbc_Bucket *self)
{
    PYCBC_CONN_THR_END(self);
    Py_INCREF((PyObject *)self);
}

static void
cb_thr_begin(pycbc_Bucket *self)
{
    if (self && self->tracer) {
        PYCBC_DEBUG_LOG("propagating tracer from %p, %p", self, self->tracer);
        pycbc_Tracer_propagate(self->tracer);
    }
    if (Py_REFCNT(self) > 1) {
        Py_DECREF(self);
        PYCBC_CONN_THR_BEGIN(self);
        return;
    }

    pycbc_assert(self->unlock_gil == 0);
    Py_DECREF(self);
}

#define CB_THR_END                 \
    PYCBC_DEBUG_LOG("cb_thr_end"); \
    cb_thr_end
#define CB_THR_BEGIN                 \
    PYCBC_DEBUG_LOG("cb_thr_begin"); \
    cb_thr_begin
#else
#define CB_THR_END(x)
#define CB_THR_BEGIN(x)
#endif


enum {
    RESTYPE_BASE = 1 << 0,
    RESTYPE_VALUE = 1 << 1,
    RESTYPE_OPERATION = 1 << 2,

    /* Extra flag indicating it's ok if it already exists */
    RESTYPE_EXISTS_OK = 1 << 3,

    /* Don't modify "remaining" count */
    RESTYPE_VARCOUNT = 1 << 4,

    /* return an array (whose value you can indicate
     * with one of the flags above) */
    RESTYPE_ARRAY = 1 << 5
};

/* Returns true if an error has been added... */

static int maybe_push_operr(pycbc_MultiResult *mres,
                            pycbc_Result *res,
                            lcb_STATUS err,
                            int check_enoent,
                            pycbc_debug_info debug_info)
{
    pycbc_stack_context_handle parent_context =
            res ? (res->tracing_context ? res->tracing_context->parent : NULL)
                : NULL;
    PYCBC_DEBUG_LOG_CONTEXT(parent_context, "maybe_push_operr")
    if (err == LCB_SUCCESS || mres->errop) {
        return 0;
    }
    if (parent_context) {
        PYCBC_DEBUG_LOG_CONTEXT(parent_context, "maybe_push_operr")
        pycbc_Result_propagate_context(
                res, res->tracing_context, mres ? mres->parent : NULL);
    }
    if (check_enoent && (mres->mropts & PYCBC_MRES_F_QUIET) &&
        (err == LCB_ERR_DOCUMENT_NOT_FOUND ||
         err == LCB_ERR_SUBDOC_PATH_NOT_FOUND)) {
        return 0;
    }

    mres->errop = (PyObject *)res;
    if (pycbc_debug_info_is_valid(&debug_info)) {
        PyObject *py_debug_info = NULL;
        if (!res->tracing_output) {
            res->tracing_output = PyDict_New();
        }
        {
            py_debug_info = PyDict_GetItemString(res->tracing_output,
                                                 PYCBC_DEBUG_INFO_STR);
            if (!py_debug_info) {
                py_debug_info = PyDict_New();
                PyDict_SetItemString(res->tracing_output,
                                     PYCBC_DEBUG_INFO_STR,
                                     py_debug_info);
            }
            pycbc_dict_add_text_kv(py_debug_info, "FILE", debug_info.FILE);
            pycbc_dict_add_text_kv(py_debug_info, "FUNC", debug_info.FUNC);
            pycbc_set_kv_ull_str(
                    py_debug_info, "LINE", (lcb_uint64_t)debug_info.LINE);
            PYCBC_DECREF(py_debug_info);
        };
    }
    Py_INCREF(mres->errop);
    return 1;
}

pycbc_debug_info pycbc_build_debug_info(const char *FILE,
                                        const char *FUNC,
                                        int line)
{
    return (pycbc_debug_info){FILE, FUNC, line};
}

#define PYCBC_BUILD_DEBUG_INFO \
    pycbc_build_debug_info(__FILE__, __FUNCTION_NAME__, __LINE__)
#define MAYBE_PUSH_OPERR(mres, res, err, check_enoent) \
    maybe_push_operr(mres, res, err, check_enoent, PYCBC_BUILD_DEBUG_INFO)

static void operation_completed3(pycbc_Bucket *self,
                                 pycbc_MultiResult *mres,
                                 pycbc_enhanced_err_info *err_info)
{
    pycbc_assert(self->nremaining);
    --self->nremaining;
    if (mres) {
        mres->err_info = err_info;
        Py_XINCREF(err_info);
    }
    if ((self->flags & PYCBC_CONN_F_ASYNC) == 0) {
        if (!self->nremaining) {
            lcb_breakout(self->instance);
        }
        return;
    }

    if (mres) {
        pycbc_AsyncResult *ares;
        ares = (pycbc_AsyncResult *)mres;
        if (--ares->nops) {
            return;
        }
        pycbc_asyncresult_invoke(ares, err_info);
    }
}

void pycbc_enhanced_err_register_entry(PyObject **dict,
                                       const char *key,
                                       const char *value)
{
    if (!value) {
        return;
    }
    if (!*dict) {
        *dict = PyDict_New();
    }
    pycbc_dict_add_text_kv(*dict, key, value);
}

void pycbc_convert_kv_error_context(const lcb_KEY_VALUE_ERROR_CONTEXT* ctx,
                                    pycbc_enhanced_err_info** err_info,
                                    const char* extended_context,
                                    const char* extended_ref) {
    PyObject* err_context = NULL;
    if (!*err_info) {
        *err_info = PyDict_New();
        err_context = PyDict_New();
        PyDict_SetItemString(*err_info, "error_context", err_context);
    }
    if (ctx) {
        uint16_t status_code;
        uint32_t opaque;
        uint64_t cas;
        const char* val;
        size_t len;

        lcb_errctx_kv_status_code(ctx, &status_code);
        lcb_errctx_kv_cas(ctx, &cas);
        lcb_errctx_kv_opaque(ctx, &opaque);
        pycbc_set_kv_ull_str(err_context, "status_code", (lcb_uint64_t)status_code);
        pycbc_set_kv_ull_str(err_context, "opaque", (lcb_uint64_t)opaque);
        pycbc_set_kv_ull_str(err_context, "cas", cas);
        lcb_errctx_kv_key(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "key", val, len);
        lcb_errctx_kv_bucket(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "bucket", val, len);
        lcb_errctx_kv_collection(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "collection", val, len);
        lcb_errctx_kv_scope(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "scope", val, len);
        lcb_errctx_kv_context(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "context", val, len);
        lcb_errctx_kv_ref(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "ref", val, len);
        lcb_errctx_kv_endpoint(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "endpoint", val, len);
        pycbc_dict_add_text_kv(err_context, "type", "KVErrorContext");
    }
    if (extended_context) {
        pycbc_dict_add_text_kv(err_context, "extended_context", extended_context);
    }
    if (extended_ref) {
        pycbc_dict_add_text_kv(err_context, "extended_ref", extended_ref);
    }
    Py_DECREF(err_context);
}

pycbc_enhanced_err_info* get_operation_err_info(const lcb_RESPBASE* respbase,
                                                lcb_CALLBACK_TYPE cbtype) {

    /* get the extended error context and ref, if any */
    const char *extended_ref = lcb_resp_get_error_ref(cbtype, respbase);
    const char *extended_context = lcb_resp_get_error_context(cbtype, respbase);
    /* To get the error_context, we need to cast to appropriate resp type
       and call appropriate respXXX_error_context function
    */
    pycbc_enhanced_err_info* info = NULL;
    const lcb_KEY_VALUE_ERROR_CONTEXT* ctx = NULL;
    lcb_STATUS rc;
    switch(cbtype) {
        case LCB_CALLBACK_GET:
            rc = lcb_respget_error_context((const lcb_RESPGET*)respbase, &ctx);
            break;
        case LCB_CALLBACK_STORE:
            rc = lcb_respstore_error_context((const lcb_RESPSTORE*)respbase, &ctx);
            break;
        case LCB_CALLBACK_UNLOCK:
            rc = lcb_respunlock_error_context((const lcb_RESPUNLOCK*)respbase, &ctx);
            break;
        case LCB_CALLBACK_TOUCH:
            rc = lcb_resptouch_error_context((const lcb_RESPTOUCH*)respbase, &ctx);
            break;
        case LCB_CALLBACK_GETREPLICA:
            rc = lcb_respgetreplica_error_context((const lcb_RESPGETREPLICA*)respbase, &ctx);
            break;
        case LCB_CALLBACK_COUNTER:
            rc = lcb_respcounter_error_context((const lcb_RESPCOUNTER*)respbase, &ctx);
            break;
        case LCB_CALLBACK_SDLOOKUP:
        case LCB_CALLBACK_SDMUTATE:
            rc = lcb_respsubdoc_error_context((const lcb_RESPSUBDOC*)respbase, &ctx);
            break;
        case LCB_CALLBACK_DEFAULT:
            rc = LCB_ERR_SDK_INTERNAL;
            break;
        case LCB_CALLBACK_REMOVE:
            rc = lcb_respremove_error_context((const lcb_RESPREMOVE*)respbase, &ctx);
            break;
        case LCB_CALLBACK_EXISTS:
            rc = lcb_respexists_error_context((const lcb_RESPEXISTS*)respbase, &ctx);
            break;

        case LCB_CALLBACK_HTTP:  // handled in pycbc_add_error_context in pycbc_http.c
        case LCB_CALLBACK_STATS:
        case LCB_CALLBACK_VERSIONS:
        case LCB_CALLBACK_VERBOSITY:
        case LCB_CALLBACK_OBSERVE:
        case LCB_CALLBACK_ENDURE:
        case LCB_CALLBACK_CBFLUSH:
        case LCB_CALLBACK_OBSEQNO:
        case LCB_CALLBACK_STOREDUR:
        case LCB_CALLBACK_NOOP:
        case LCB_CALLBACK_PING:
        case LCB_CALLBACK_DIAG:
        case LCB_CALLBACK_COLLECTIONS_GET_MANIFEST:
        case LCB_CALLBACK_GETCID:
        case LCB_CALLBACK__MAX:
        default:
            goto SKIP;
    }
    if (LCB_SUCCESS == rc) {
        if (ctx) {
            pycbc_convert_kv_error_context(ctx, &info, extended_context, extended_ref);
        }
    }
    SKIP:
    return info;
}

static pycbc_enhanced_err_info *pycbc_enhanced_err_info_store(
        const lcb_RESPBASE *respbase, int cbtype, pycbc_debug_info *info)
{
    pycbc_enhanced_err_info *err_info = get_operation_err_info(respbase, cbtype);
    if (info) {
         char LINEBUF[100] = {0};

        pycbc_enhanced_err_register_entry(&err_info, "FILE", info->FILE);
        pycbc_enhanced_err_register_entry(&err_info, "FUNC", info->FUNC);
        snprintf(LINEBUF, 100, "%d", info->LINE);
        pycbc_enhanced_err_register_entry(&err_info, "LINE", LINEBUF);
    }
    return err_info;
}

static void operation_completed_with_err_info(pycbc_Bucket *self,
                                              pycbc_MultiResult *mres,
                                              int cbtype,
                                              const lcb_RESPBASE *resp,
                                              pycbc_Result *res)
{
    pycbc_enhanced_err_info *err_info =
            pycbc_enhanced_err_info_store(resp, cbtype, NULL);
    pycbc_stack_context_handle context = PYCBC_RESULT_EXTRACT_CONTEXT(res);
    PYCBC_DEBUG_LOG("Completed context %p with %p, nremaining is %d",
                    context,
                    res ? (PyObject *)res : NULL,
                    self ? self->nremaining : 0)
    PYCBC_CONTEXT_DEREF(context, 0);
    operation_completed3(self, mres, err_info);
    Py_XDECREF(err_info);
}

/**
 * Call this function for each callback. Note that even if this function
 * returns nonzero, CB_THR_BEGIN() must still be called, and the `conn`
 * and `mres` out parameters are considered valid
 * @param resp base response object
 * @param[out] conn the bucket object
 * @param[out] res the result object for the individual operation
 * @param restype What type should `res` be if it needs to be created
 * @param[out] mres the context for the current operation
 * @return 0 if operation processing may proceed, nonzero if operation
 * processing has completed. In both cases the `conn` and `mres` paramters
 * are valid, however.
 */

typedef struct {
    lcb_CALLBACK_TYPE cbtype;
    pycbc_strn_base_const key;
    pycbc_MultiResult *mres;
    lcb_STATUS rc;
    uint64_t cas;
} response_handler;

#define PYCBC_X_FOR_EACH_OP(X, NOKEY, STATSOPS, GETOP, COUNTERSOPS, SDOPS) \
    X(STORE, store)                                                        \
    X(REMOVE, remove)                                                      \
    X(UNLOCK, unlock)                                                      \
    X(TOUCH, touch)                                                        \
    GETOP(GET, get)                                                        \
    X(GETREPLICA, getreplica)                                              \
    COUNTERSOPS(COUNTER, counter)                                          \
    STATSOPS(STATS, stats)                                                 \
    NOKEY(PING, ping)                                                      \
    NOKEY(DIAG, diag)                                                      \
    OBSERVEOPS(X)                                                          \
    ENDUREOPS(X)                                                           \
    X(HTTP, http)                                                          \
    SDOPS(SUBDOC, subdoc)                                                  \
    X(EXISTS, exists)

int pycbc_extract_respdata(const lcb_RESPBASE *resp,
                           pycbc_MultiResult *const *mres,
                           response_handler *handler)
{
    lcb_STATUS result = LCB_SUCCESS;
    switch (handler->cbtype) {
#define PYCBC_UPDATE_ALL_GENERIC(UC, LC, RESPPOSTFIX, FNPOSTFIX)               \
    lcb_resp##FNPOSTFIX##_key((const lcb_RESP##RESPPOSTFIX *)resp,             \
                              &(handler->key.buffer),                          \
                              &(handler->key.length));                         \
    handler->rc = lcb_resp##FNPOSTFIX##_status((lcb_RESP##RESPPOSTFIX *)resp); \
    lcb_resp##FNPOSTFIX##_cookie((const lcb_RESP##RESPPOSTFIX *)resp,          \
                                 (void **)mres);                               \
    lcb_resp##FNPOSTFIX##_cas((const lcb_RESP##RESPPOSTFIX *)resp,             \
                              (uint64_t *)&(handler->cas));                    \
    break;
#define PYCBC_UPDATE_ALL(UC, LC) \
    case LCB_CALLBACK_##UC:      \
        PYCBC_UPDATE_ALL_GENERIC(UC, LC, UC, LC)
#define PYCBC_UPDATE_SDOPS(UC, LC) \
    case LCB_CALLBACK_SDMUTATE:    \
    case LCB_CALLBACK_SDLOOKUP:    \
        PYCBC_UPDATE_ALL_GENERIC(UC, LC, SUBDOC, LC)
#define PYCBC_UPDATE_ALL_NOKEYORCAS(UC, LC)                               \
    case LCB_CALLBACK_##UC:                                               \
        handler->rc = lcb_resp##LC##_status((lcb_RESP##UC *)resp);        \
        lcb_resp##LC##_cookie((const lcb_RESP##UC *)resp, (void **)mres); \
        break;
#define PYCBC_UPDATE_ALL_NO_KEY_OR_COOKIE_OR_CAS(UC, LC)                  \
    case LCB_CALLBACK_##UC:                                               \
        lcb_resp##LC##_cookie((const lcb_RESP##UC *)resp, (void **)mres); \
        break;
#ifdef PYCBC_GEN_OPS
        PYCBC_X_FOR_EACH_OP(PYCBC_UPDATE_ALL,
                            PYCBC_UPDATE_ALL_NOKEYORCAS,
                            PYCBC_UPDATE_ALL_NO_KEY_OR_COOKIE_OR_CAS,
                            PYCBC_UPDATE_ALL,
                            PYCBC_UPDATE_ALL,
                            PYCBC_UPDATE_SDOPS)
#else
    case LCB_CALLBACK_STORE:
        lcb_respstore_key((const lcb_RESPSTORE *)resp,
                          &(handler->key.buffer),
                          &(handler->key.length));
        handler->rc = lcb_respstore_status((lcb_RESPSTORE *)resp);
        lcb_respstore_cookie((const lcb_RESPSTORE *)resp, (void **)mres);
        lcb_respstore_cas((const lcb_RESPSTORE *)resp,
                          (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_REMOVE:
        lcb_respremove_key((const lcb_RESPREMOVE *)resp,
                           &(handler->key.buffer),
                           &(handler->key.length));
        handler->rc = lcb_respremove_status((lcb_RESPREMOVE *)resp);
        lcb_respremove_cookie((const lcb_RESPREMOVE *)resp, (void **)mres);
        lcb_respremove_cas((const lcb_RESPREMOVE *)resp,
                           (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_UNLOCK:
        lcb_respunlock_key((const lcb_RESPUNLOCK *)resp,
                           &(handler->key.buffer),
                           &(handler->key.length));
        handler->rc = lcb_respunlock_status((lcb_RESPUNLOCK *)resp);
        lcb_respunlock_cookie((const lcb_RESPUNLOCK *)resp, (void **)mres);
        lcb_respunlock_cas((const lcb_RESPUNLOCK *)resp,
                           (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_EXISTS:
        lcb_respexists_key((const lcb_RESPEXISTS *)resp,
                           &(handler->key.buffer),
                           &(handler->key.length));
        handler->rc = lcb_respexists_status((lcb_RESPEXISTS *)resp);
        lcb_respexists_cookie((const lcb_RESPEXISTS *)resp, (void **)mres);
        lcb_respexists_cas((const lcb_RESPEXISTS *)resp,
                           (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_TOUCH:
        lcb_resptouch_key((const lcb_RESPTOUCH *)resp,
                          &(handler->key.buffer),
                          &(handler->key.length));
        handler->rc = lcb_resptouch_status((lcb_RESPTOUCH *)resp);
        lcb_resptouch_cookie((const lcb_RESPTOUCH *)resp, (void **)mres);
        lcb_resptouch_cas((const lcb_RESPTOUCH *)resp,
                          (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_GET:
        lcb_respget_key((const lcb_RESPGET *)resp,
                        &(handler->key.buffer),
                        &(handler->key.length));
        handler->rc = lcb_respget_status((lcb_RESPGET *)resp);
        lcb_respget_cookie((const lcb_RESPGET *)resp, (void **)mres);
        lcb_respget_cas((const lcb_RESPGET *)resp, (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_GETREPLICA:
        lcb_respgetreplica_key((const lcb_RESPGETREPLICA *)resp,
                               &(handler->key.buffer),
                               &(handler->key.length));
        handler->rc = lcb_respgetreplica_status((lcb_RESPGETREPLICA *)resp);
        lcb_respgetreplica_cookie((const lcb_RESPGETREPLICA *)resp,
                                  (void **)mres);
        lcb_respgetreplica_cas((const lcb_RESPGETREPLICA *)resp,
                               (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_COUNTER:
        lcb_respcounter_key((const lcb_RESPCOUNTER *)resp,
                            &(handler->key.buffer),
                            &(handler->key.length));
        handler->rc = lcb_respcounter_status((lcb_RESPCOUNTER *)resp);
        lcb_respcounter_cookie((const lcb_RESPCOUNTER *)resp, (void **)mres);
        lcb_respcounter_cas((const lcb_RESPCOUNTER *)resp,
                            (uint64_t *)&(handler->cas));
        break;
    case LCB_CALLBACK_STATS:;
        break;
    case LCB_CALLBACK_PING:
        handler->rc = lcb_respping_status((lcb_RESPPING *)resp);
        lcb_respping_cookie((const lcb_RESPPING *)resp, (void **)mres);
        break;
    case LCB_CALLBACK_DIAG:
        handler->rc = lcb_respdiag_status((lcb_RESPDIAG *)resp);
        lcb_respdiag_cookie((const lcb_RESPDIAG *)resp, (void **)mres);
        break;
    case LCB_CALLBACK_HTTP:;
        handler->rc = lcb_resphttp_status((lcb_RESPHTTP *)resp);
        lcb_resphttp_cookie((const lcb_RESPHTTP *)resp, (void **)mres);
        ;
        break;
    case LCB_CALLBACK_SDMUTATE:
    case LCB_CALLBACK_SDLOOKUP:
        lcb_respsubdoc_key((const lcb_RESPSUBDOC *)resp,
                           &(handler->key.buffer),
                           &(handler->key.length));
        handler->rc = lcb_respsubdoc_status((lcb_RESPSUBDOC *)resp);
        lcb_respsubdoc_cookie((const lcb_RESPSUBDOC *)resp, (void **)mres);
        lcb_respsubdoc_cas((const lcb_RESPSUBDOC *)resp,
                           (uint64_t *)&(handler->cas));
        break;
#endif
        // none of these appear to be necessary for our purposes, this is just to satisfy the compiler
        case LCB_CALLBACK_DEFAULT:
            break;
        case LCB_CALLBACK_VERSIONS:
            break;
        case LCB_CALLBACK_VERBOSITY:
            break;
        case LCB_CALLBACK_OBSERVE:
            break;
        case LCB_CALLBACK_ENDURE:
            break;
        case LCB_CALLBACK_CBFLUSH:
            break;
        case LCB_CALLBACK_OBSEQNO:
            break;
        case LCB_CALLBACK_STOREDUR:
            break;
        case LCB_CALLBACK_NOOP:
            break;
        case LCB_CALLBACK_COLLECTIONS_GET_MANIFEST:
            break;
        case LCB_CALLBACK_GETCID:
            break;
        case LCB_CALLBACK__MAX:
            break;
    }
    return result;
}

static int get_common_objects(const lcb_RESPBASE *resp,
                              pycbc_Bucket **conn,
                              pycbc_Result **res,
                              int restype,
                              pycbc_MultiResult **mres,
                              response_handler *handler)

{
    PyObject *hkey;
    PyObject *mrdict;
    int rv;
    PyObject *pycbc_err[3] = {0};
    pycbc_stack_context_handle parent_context = NULL;
    pycbc_stack_context_handle decoding_context = NULL;
    pycbc_extract_respdata(resp, mres, handler);

    pycbc_assert(pycbc_multiresult_check(*mres));
    *conn = (*mres)->parent;

    CB_THR_END(*conn);

    rv = pycbc_tc_decode_key(
            *conn, handler->key.buffer, handler->key.length, &hkey);

    if (rv < 0) {
        pycbc_multiresult_adderr((pycbc_MultiResult *)*mres);
        return -1;
    }
    pycbc_store_error(pycbc_err);
    {
        mrdict = pycbc_multiresult_dict(*mres);
        *res = (pycbc_Result*)PyDict_GetItem(mrdict, hkey);
        if (restype & RESTYPE_ARRAY) {
            if (!*res || !PyList_Check((PyObject*)*res)) {
                /* we need to create the empty array */
                PyObject* list = PyList_New(0);
                PYCBC_DEBUG_LOG("creating new array for %.*s", handler->key.length, handler->key.buffer);
                PyDict_SetItem(mrdict, hkey, list);
                PYCBC_DECREF(list);
            }
            /* no matter what, *res needs to be NULL now */
            *res = NULL;
        }


        PYCBC_INCREF(hkey);
        parent_context = PYCBC_MULTIRESULT_EXTRACT_CONTEXT(
                (pycbc_MultiResult *)*mres, hkey, res);
        if (parent_context) {
            decoding_context =
                    pycbc_Result_start_context(parent_context,
                                               hkey,
                                               "get_common_objects",
                                               LCBTRACE_OP_RESPONSE_DECODING,
                                               LCBTRACE_REF_FOLLOWS_FROM);
        };
        if (*res) {
            int exists_ok = (restype & RESTYPE_EXISTS_OK) ||
                            ((*mres)->mropts & PYCBC_MRES_F_UALLOCED);

            if (!exists_ok) {
                if ((*conn)->flags & PYCBC_CONN_F_WARNEXPLICIT) {
                    PyErr_WarnExplicit(PyExc_RuntimeWarning,
                                       "Found duplicate key",
                                       __FILE__,
                                       __LINE__,
                                       PYCBC_PACKAGE_NAME "._libcouchbase",
                                       NULL);

                } else {
                    PyErr_WarnEx(
                            PyExc_RuntimeWarning, "Found duplicate key", 1);
                }
                /**
                 * We need to destroy the existing object and re-create it.
                 */
                PyDict_DelItem(mrdict, hkey);
                *res = NULL;

            } else {
                Py_XDECREF(hkey);
            }
        }

        if (*res == NULL) {
            /* Now, get/set the result object */
            if ((*mres)->mropts & PYCBC_MRES_F_ITEMS) {
                PYCBC_DEBUG_LOG("Item creation");
                *res = (pycbc_Result *)pycbc_item_new(*conn);
            } else if (restype & RESTYPE_BASE) {
                PYCBC_DEBUG_LOG("Result creation");
                *res = (pycbc_Result *)pycbc_result_new(*conn);

            } else if (restype & RESTYPE_OPERATION) {
                PYCBC_DEBUG_LOG("Opresult creation");
                *res = (pycbc_Result *)pycbc_opresult_new(*conn);

            } else if (restype & RESTYPE_VALUE) {
                PYCBC_DEBUG_LOG("Valresult creation");
                *res = (pycbc_Result *)pycbc_valresult_new(*conn);
            } else {
                *res = (pycbc_Result *)pycbc_result_new(*conn);
                if ((*conn)->nremaining) {
                    --(*conn)->nremaining;
                }
            }
            if (*res) {
                if (restype & RESTYPE_ARRAY) {
                    /* we actually need to put *res into the array, not directly into
                     * the dict, so...*/
                     PyObject* list = PyDict_GetItem(mrdict, hkey);
                     if (!list) {
                        pycbc_multiresult_adderr((pycbc_MultiResult *)*mres);
                        return -1;
                     }
                     PyList_Append(list, (PyObject *)*res);
                } else {
                    PyDict_SetItem(mrdict, hkey, (PyObject *)*res);
                }
                (*res)->key = hkey;
                PYCBC_DECREF(*res);

            } else {
                abort();
            }
        }
        if (res && *res) {
            pycbc_Result_propagate_context(*res, parent_context, *conn);
        }
        PYCBC_CONTEXT_DEREF(decoding_context, 1);
        if (parent_context && parent_context->is_stub) {
            PYCBC_CONTEXT_DEREF(parent_context, 0);
        }
        if (handler->rc && res && *res) {
            (*res)->rc = handler->rc;
        }

        if (handler->rc != LCB_SUCCESS) {
            (*mres)->all_ok = 0;
        }
    }
#define PYCBC_RESTORE_PRE_CONTEXT_ERROR
    if (pycbc_err[0] || pycbc_err[1] || pycbc_err[2]) {
#ifdef PYCBC_RESTORE_PRE_CONTEXT_ERROR
        pycbc_fetch_error(pycbc_err);
#else

        PYCBC_XDECREF(pycbc_err[0]);
        PYCBC_XDECREF(pycbc_err[1]);
        PYCBC_XDECREF(pycbc_err[2]);
#endif
    }
    return 0;
}

static void
invoke_endure_test_notification(pycbc_Bucket *self, pycbc_Result *resp)
{
    PyObject *ret;
    PyObject *argtuple = Py_BuildValue("(O)", resp);
    ret = PyObject_CallObject(self->dur_testhook, argtuple);
    pycbc_assert(ret);

    Py_XDECREF(ret);
    Py_XDECREF(argtuple);
}
static void
dur_chain2(pycbc_Bucket *conn,
           pycbc_MultiResult *mres,
           pycbc_OperationResult *res, int cbtype, const lcb_RESPBASE *resp)
{
    int is_delete = cbtype == LCB_CALLBACK_REMOVE;
    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "durability chain callback")
                            response_handler handler={.cbtype=cbtype,.mres=mres};
    pycbc_extract_respdata(resp,&mres,&handler);
    res->rc = handler.rc;
    if(res->rc == LCB_SUCCESS) {
#ifdef PYCBC_MUTATION_TOKENS_ENABLED

        const lcb_MUTATION_TOKEN *mutinfo = lcb_resp_get_mutation_token(cbtype,resp);
        Py_XDECREF(res->mutinfo);

        if (mutinfo && lcb_mutation_token_is_valid(mutinfo)) {
            // Create the mutation token tuple: (vb,uuid,seqno)
            res->mutinfo = Py_BuildValue("HKKO",
                                         pycbc_mutation_token_vbid(mutinfo),
                                         pycbc_mutation_token_uuid(mutinfo),
                                         pycbc_mutation_token_seqno(mutinfo),
                                         conn->bucket);
            PYCBC_DEBUG_PYFORMAT_CONTEXT(res ? res->tracing_context : NULL, "Got mutinfo %R",res->mutinfo)
            PYCBC_EXCEPTION_LOG_NOCLEAR
        } else {
            Py_INCREF(Py_None);
            res->mutinfo = Py_None;
        }

#else
        /*
        Until mlcb_resp_get_mutation_token is back in libcouchbase includes, do
            this:*/
        Py_INCREF(Py_None);
        res->mutinfo = Py_None;
#endif

        res->cas = handler.cas;
    }

    /** For remove, we check quiet */
    MAYBE_PUSH_OPERR(mres, (pycbc_Result *)res, res->rc, is_delete ? 1 : 0);

    if ((mres->mropts & PYCBC_MRES_F_DURABILITY) == 0 || res->rc != LCB_SUCCESS) {
        operation_completed_with_err_info(
                conn, mres, cbtype, resp, (pycbc_Result *)res);
        CB_THR_BEGIN(conn);
        return;
    }

    if (conn->dur_testhook && conn->dur_testhook != Py_None) {
        invoke_endure_test_notification(conn, (pycbc_Result *)res);
    }

    if (lcb_respstore_observe_attached((const lcb_RESPSTORE*)resp)) {
        uint16_t npersisted, nreplicated;
        lcb_respstore_observe_num_persisted((const lcb_RESPSTORE*)resp, &npersisted);
        lcb_respstore_observe_num_replicated((const lcb_RESPSTORE*)resp, &nreplicated);
        if (res->rc == LCB_SUCCESS) {
            PYCBC_DEBUG_LOG("Stored. Persisted(%u). Replicated(%u)", npersisted, nreplicated);
        } else {
            int store_ok;
            lcb_respstore_observe_stored((const lcb_RESPSTORE*)resp, &store_ok);
            if (store_ok) {
                PYCBC_DEBUG_LOG("Store OK, but durability failed. Persisted(%u). Replicated(%u)", npersisted, nreplicated);
            } else {
                PYCBC_DEBUG_LOG("%s", "Store failed");
            }
        }
    }

    operation_completed_with_err_info(conn, mres, cbtype, resp, (pycbc_Result *)res);
    CB_THR_BEGIN(conn);
}

/**
 * Common handler for durability
 */
static void
durability_chain_common(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    pycbc_Bucket *conn;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    int restype = RESTYPE_VARCOUNT;
    response_handler dur_handler = {.cbtype = cbtype};
    PYCBC_DEBUG_LOG("Durability chain callback")
    if (cbtype == LCB_CALLBACK_COUNTER) {
        restype |= RESTYPE_VALUE;
    } else {
        restype |= RESTYPE_OPERATION;
    }

    if (get_common_objects(resp,
                           &conn,
                           (pycbc_Result **)&res,
                           restype,
                           &mres,
                           &dur_handler) != 0) {
        operation_completed_with_err_info(
                conn, mres, cbtype, resp, (pycbc_Result *)res);
        PYCBC_DEBUG_LOG("Durability chain returning")

        CB_THR_BEGIN(conn);
        return;
    }
    PYCBC_DEBUG_LOG_CONTEXT(res ? res->tracing_context : NULL,
                            "durability_chain_common")
    dur_chain2(conn, mres, res, cbtype, resp);
}

static void
value_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    int rv;
    pycbc_Bucket *conn = NULL;
    pycbc_ValueResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    response_handler handler = {.cbtype = cbtype};
    PYCBC_DEBUG_LOG("Value callback");
    int restype = RESTYPE_VALUE;
    if (cbtype == LCB_CALLBACK_GETREPLICA) {
        restype |= RESTYPE_EXISTS_OK | RESTYPE_ARRAY;
    }
    int is_final = 1;
    rv = get_common_objects(
            resp, &conn, (pycbc_Result **)&res, restype, &mres, &handler);

    if (rv < 0) {
        goto GT_DONE;
    }
    PYCBC_DEBUG_LOG_CONTEXT(PYCBC_RES_CONTEXT(res), "Value callback continues")

    /* for only getreplica, we need to check is_final.  And we need to do that before
     * the GT_DONE.  So, though this isn't super clean, lets do it here.
     */
    if (cbtype == LCB_CALLBACK_GETREPLICA) {
        const lcb_RESPGETREPLICA *gresp = (const lcb_RESPGETREPLICA *)resp;
        is_final = lcb_respgetreplica_is_final(gresp);
    }
    if (handler.rc == LCB_SUCCESS) {
        res->cas = handler.cas;
    } else {
        MAYBE_PUSH_OPERR(mres,
                         (pycbc_Result *)res,
                         handler.rc,
                         cbtype != LCB_CALLBACK_COUNTER);
        goto GT_DONE;
    }

    if (cbtype == LCB_CALLBACK_GETREPLICA) {
        const lcb_RESPGETREPLICA *gresp = (const lcb_RESPGETREPLICA *)resp;
        lcb_U32 eflags;
        lcb_respgetreplica_flags(gresp, &res->flags);
        if (mres->mropts & PYCBC_MRES_F_FORCEBYTES) {
            eflags = PYCBC_FMT_BYTES;
        } else {
            lcb_respgetreplica_flags(gresp, &eflags);
        }

        if (res->value) {
            Py_DECREF(res->value);
            res->value = NULL;
        }
        {
            const char *value = NULL;
            size_t nvalue = 0;
            lcb_respgetreplica_value(gresp, &value, &nvalue);
            rv = pycbc_tc_decode_value(
                    mres->parent, value, nvalue, eflags, &res->value);
        }
        if (rv < 0) {
            pycbc_multiresult_adderr(mres);
        }
    } else if (cbtype == LCB_CALLBACK_GET) {
        const lcb_RESPGET *gresp = (const lcb_RESPGET *)resp;
        lcb_U32 eflags;
        lcb_respget_flags(gresp, &res->flags);
        if (mres->mropts & PYCBC_MRES_F_FORCEBYTES) {
            eflags = PYCBC_FMT_BYTES;
        } else {
            lcb_respget_flags(gresp, &eflags);
        }

        if (res->value) {
            Py_DECREF(res->value);
            res->value = NULL;
        }
        {
            const char *value = NULL;
            size_t nvalue = 0;
            lcb_respget_value(gresp, &value, &nvalue);
            rv = pycbc_tc_decode_value(
                    mres->parent, value, nvalue, eflags, &res->value);
        }
        if (rv < 0) {
            pycbc_multiresult_adderr(mres);
        }
    } else if (cbtype == LCB_CALLBACK_COUNTER) {
        const lcb_RESPCOUNTER *cresp = (const lcb_RESPCOUNTER *)resp;
        uint64_t value = 0;

        lcb_respcounter_value(cresp, &value);
        res->value = pycbc_IntFromULL(value);
    }
    GT_DONE:
        if(!(mres->mropts & PYCBC_MRES_F_MULTI) || is_final) {
            operation_completed_with_err_info(
                    conn, mres, cbtype, resp, (pycbc_Result *)res);
        }
        CB_THR_BEGIN(conn);
        (void)instance;
}

/*
 * Add the sub-document error to the result list
 */

static void
mk_sd_error(pycbc__SDResult *res,
    pycbc_MultiResult *mres, lcb_STATUS rc, size_t ix)
{
    PyObject *spec = PyTuple_GET_ITEM(res->specs, ix);
    PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_LCBERR, rc, "Subcommand failure", spec);
    pycbc_multiresult_adderr(mres);
}

static PyObject *mk_sd_tuple(const pycbc_SDENTRY *ent)
{
    PyObject *val = NULL;
    PyObject *ret;
    pycbc_strn_base_const ent_strn = pycbc_respsubdoc_value(ent);
    if (pycbc_respsubdoc_status(ent) == LCB_SUCCESS && ent_strn.length != 0) {
        int rv = pycbc_tc_simple_decode(
                &val, ent_strn.buffer, ent_strn.length, PYCBC_FMT_JSON);
        if (rv != 0) {
            return NULL;
        }
    }

    if (val == NULL) {
        val = Py_None;
        Py_INCREF(Py_None);
    }

    ret = Py_BuildValue("(iO)", pycbc_respsubdoc_status(ent), val);
    Py_DECREF(val);
    return ret;

}

static void
subdoc_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb)
{
    int rv;
    pycbc_Bucket *conn;
    pycbc__SDResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    pycbc_SDENTRY cur;
    response_handler handler = {.cbtype = cbtype};
    size_t vii = 0, oix = 0;
    const lcb_RESPSUBDOC *resp = (const lcb_RESPSUBDOC *)rb;
    PYCBC_DEBUG_LOG("Subdoc callback")
    rv = get_common_objects(rb,
                            &conn,
                            (pycbc_Result **)&res,
                            RESTYPE_EXISTS_OK,
                            &mres,
                            &handler);
    if (rv < 0) {
        goto GT_ERROR;
    }

    PYCBC_DEBUG_LOG_CONTEXT(PYCBC_RES_CONTEXT(res), "Subdoc callback continues")
    if (handler.rc == LCB_SUCCESS || handler.rc == LCB_ERR_SUBDOC_PATH_NOT_FOUND)  {
        res->cas = handler.cas;
    } else {
        MAYBE_PUSH_OPERR(mres, (pycbc_Result *)res, handler.rc, 0);
        goto GT_ERROR;
    }

    while ((pycbc_sdresult_next(resp, &cur, &vii))) {
        size_t cur_index;
        lcb_STATUS rc;
        PyObject *cur_tuple = mk_sd_tuple(&cur);

        if (cbtype == LCB_CALLBACK_SDMUTATE) {
            cur_index = cur.index;
        } else {
            cur_index = oix++;
        }

        if (cur_tuple == NULL) {
            pycbc_multiresult_adderr(mres);
            goto GT_ERROR;
        }

        rc = pycbc_respsubdoc_status(&cur);
        if (rc != LCB_SUCCESS) {
            if (cbtype == LCB_CALLBACK_SDMUTATE) {
                mk_sd_error(res, mres, rc, cur_index);
            } else if (rc != LCB_ERR_SUBDOC_PATH_NOT_FOUND) {
                mk_sd_error(res, mres, rc, cur_index);
            }
        }

        pycbc_sdresult_addresult(res, cur_index, cur_tuple);
        Py_DECREF(cur_tuple);
    }
    if (handler.rc == LCB_SUCCESS) {
        dur_chain2(conn, mres, (pycbc_OperationResult*)res, cbtype, (const lcb_RESPBASE*)resp);
        return;
    }

    GT_ERROR:
        operation_completed_with_err_info(
                conn, mres, cbtype, rb, (pycbc_Result *)res);
        CB_THR_BEGIN(conn);
        (void)instance;
}

static void
keyop_simple_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp)
{
    int rv;
    int optflags = RESTYPE_OPERATION;
    pycbc_Bucket *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    response_handler handler = {.cbtype = cbtype};
    if (cbtype == LCB_CALLBACK_ENDURE) {
        optflags |= RESTYPE_EXISTS_OK;
    }
    PYCBC_DEBUG_LOG("Keyop callback")
    rv = get_common_objects(
            resp, &conn, (pycbc_Result **)&res, optflags, &mres, &handler);
    PYCBC_DEBUG_LOG_CONTEXT(PYCBC_RES_CONTEXT(res), "Keyop callback continues")

    if (rv == 0) {
        res->rc = handler.rc;
        MAYBE_PUSH_OPERR(mres, (pycbc_Result *)res, handler.rc, 0);
    }
    if (handler.cas) {
        res->cas = handler.cas;
    }

    operation_completed_with_err_info(
            conn, mres, cbtype, resp, (pycbc_Result *)res);
    CB_THR_BEGIN(conn);
    (void)instance;

}

static void
observe_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *resp_base)
{
    int rv;
    pycbc_ObserveInfo *oi;
    pycbc_Bucket *conn;
    pycbc_ValueResult *vres = NULL;
    pycbc_MultiResult *mres = NULL;
    const pycbc_RESPOBSERVE *oresp = (const pycbc_RESPOBSERVE *)resp_base;
    (void)oresp;
    response_handler handler = {.cbtype = cbtype};
    lcb_uint64_t flags = 0;
    PYCBC_DEBUG_LOG("observe callback")
    if (!lcb_respobserve_flags(oresp, flags) && (flags & LCB_RESP_F_FINAL)) {
        (void)lcb_respobserve_cookie(oresp, &mres);
        operation_completed_with_err_info(
                mres->parent, mres, cbtype, resp_base, (pycbc_Result *)vres);
        return;
    }

    rv = get_common_objects(
            resp_base,
            &conn,
            (pycbc_Result **)&vres,
            RESTYPE_VALUE | RESTYPE_EXISTS_OK | RESTYPE_VARCOUNT,
            &mres,
            &handler);

    if (rv < 0) {
        goto GT_DONE;
    }

    PYCBC_DEBUG_LOG_CONTEXT(PYCBC_RES_CONTEXT(vres),
                            "observe callback continues")

    if (handler.rc != LCB_SUCCESS) {
        MAYBE_PUSH_OPERR(mres, (pycbc_Result *)vres, handler.rc, 0);
        goto GT_DONE;
    }

    if (!vres->value) {
        vres->value = PyList_New(0);
    }

    oi = pycbc_observeinfo_new(conn);
    if (oi == NULL) {
        pycbc_multiresult_adderr(mres);
        goto GT_DONE;
    }
    (void)lcb_respobserve_cas(oresp, &oi->cas);
    (void)lcb_respobserve_is_master(oresp, &oi->from_master);
    oi->flags = lcb_respobserve_status(oresp);
    PyList_Append(vres->value, (PyObject*)oi);
    Py_DECREF(oi);

    GT_DONE:
    CB_THR_BEGIN(conn);
    (void)instance; (void)cbtype;
}

static int
start_global_callback(lcb_t instance, pycbc_Bucket **selfptr)
{
    *selfptr = (pycbc_Bucket *)lcb_get_cookie(instance);
    if (!*selfptr) {
        return 0;
    }
    PYCBC_DEBUG_LOG("start of bootstrap callback on bucket %p", selfptr);
    CB_THR_END(*selfptr);
    Py_INCREF((PyObject *)*selfptr);
    return 1;
}

static void
end_global_callback(lcb_t instance, pycbc_Bucket *self)
{
    Py_DECREF((PyObject *)(self));

    self = (pycbc_Bucket *)lcb_get_cookie(instance);
    if (self) {
        CB_THR_BEGIN(self);
    }
    PYCBC_DEBUG_LOG("end of bootstrap callback on bucket %p", self);
}

static void
bootstrap_callback(lcb_t instance, lcb_STATUS err)
{
    pycbc_Bucket *self;

    if (!start_global_callback(instance, &self)) {
        return;
    }
    PYCBC_DEBUG_LOG("bootstrap callback on bucket %p", self);
    pycbc_invoke_connected_event(self, err);
    end_global_callback(instance, self);
}

#define LCB_PING_FOR_ALL_TYPES(X) \
    X(KV, kv)                     \
    X(VIEWS, views)               \
    X(QUERY, query)               \
    X(SEARCH, search)             \
    X(ANALYTICS, analytics)

const char *get_type_s(lcb_PING_SERVICE type)
{
    switch (type) {
        LCB_PING_FOR_ALL_TYPES(LCB_PING_GET_TYPE_S)
    case LCB_PING_SERVICE__MAX:
        pycbc_assert(type != LCB_PING_SERVICE__MAX);
    default:
        break;
    }
    return "Unknown type";
}

#undef LCB_PING_GET_TYPE_S
#undef LCB_PING_FOR_ALL_TYPES

pycbc_strn_base_const pycbc_strn_base_const_from_psz(const char *buf)
{
    return (pycbc_strn_base_const){.buffer = buf,
                                   .length = buf ? strlen(buf) : 0};
}

static void exists_callback(lcb_t instance,
                            int cbtype,
                            const lcb_RESPBASE * resp_base) {

    int rv;
    int optflags = RESTYPE_OPERATION;
    pycbc_Bucket *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    response_handler handler = {.cbtype = cbtype};

    rv = get_common_objects(
            resp_base, &conn, (pycbc_Result **)&res, optflags, &mres, &handler);
    if (rv < 0) {
        goto DONE;
    }

    PYCBC_DEBUG_LOG_CONTEXT(PYCBC_RES_CONTEXT(res), "Exists callback continues")

    if (handler.rc != LCB_SUCCESS) {
        MAYBE_PUSH_OPERR(mres, (pycbc_Result *)res, handler.rc, 0);
        goto DONE;
    }
    int exists = lcb_respexists_is_found((const lcb_RESPEXISTS*)resp_base);
    PYCBC_DEBUG_LOG("is_found returns %d", exists);
    /* we just use the existance of the cas as an indication, in python.  Note a deleted
     * doc will return a cas too - hence the explicit is_found call */
    if (handler.cas && exists) {
        res->cas = handler.cas;
    } else {
        res->cas = 0;
    }

    DONE:
    operation_completed_with_err_info(conn, mres, cbtype, resp_base, (pycbc_Result *)res);
    CB_THR_BEGIN(conn);
    (void)instance;
}

static void ping_callback(lcb_t instance,
                          int cbtype,
                          const lcb_RESPBASE *resp_base)
{
    pycbc_Bucket *parent;
    const lcb_RESPPING *resp = (const lcb_RESPPING *)resp_base;

    pycbc_MultiResult *mres = NULL;
    PyObject *resultdict = NULL;

    lcb_respping_cookie(resp, (void **)&mres);
    resultdict = pycbc_multiresult_dict(mres);
    parent = mres->parent;
    CB_THR_END(parent);
    if (lcb_respping_status(resp) != LCB_SUCCESS) {
        if (mres->errop == NULL) {
            pycbc_Result *res = (pycbc_Result *)pycbc_result_new(parent);
            res->rc = lcb_respping_status(resp);
            res->key = Py_None;
            Py_INCREF(res->key);
            MAYBE_PUSH_OPERR(mres, res, lcb_respping_status(resp), 0);
        }
    }

    {
        PyObject *struct_services_dict = PyDict_New();

        lcb_SIZE ii;
        for (ii = 0; ii < lcb_respping_result_size(resp); ii++) {
            lcb_PING_SERVICE svc = LCB_PING_SERVICE__MAX;
            lcb_respping_result_service(resp, ii, &svc);
            const char *type_s = get_type_s(svc);
            PyObject *struct_server_list =
                    PyDict_GetItemString(struct_services_dict, type_s);
            if (!struct_server_list) {
                struct_server_list = PyList_New(0);
                PyDict_SetItemString(
                        struct_services_dict, type_s, struct_server_list);
                Py_DECREF(struct_server_list);
            }
            {
                PyObject *mrdict = PyDict_New();
                PyList_Append(struct_server_list, mrdict);

                pycbc_assert(lcb_respping_result_status(resp, ii) !=
                             LCB_PING_STATUS__MAX);
                pycbc_dict_add_text_kv(
                        mrdict,
                        "details",
                        lcb_strerror_long(
                                (lcb_STATUS)lcb_respping_result_status(resp,
                                                                       ii)));
                {
                    pycbc_strn_base_const server_name;
                    lcb_respping_result_remote(
                            resp, ii, &server_name.buffer, &server_name.length);
                    pycbc_dict_add_text_kv_strn(
                            mrdict,
                            pycbc_strn_base_const_from_psz("server"),
                            server_name);
                }
                PyDict_SetItemString(
                        mrdict,
                        "status",
                        PyLong_FromLong(
                                (long)lcb_respping_result_status(resp, ii)));
                {
                    uint64_t latency = 0;
                    lcb_respping_result_latency(resp, ii, &latency);
                    PyDict_SetItemString(
                            mrdict,
                            "latency",
                            PyLong_FromUnsignedLong((unsigned long)latency));
                    Py_DECREF(mrdict);
                }
            }
        }
        PyDict_SetItemString(
                resultdict, "services_struct", struct_services_dict);
        Py_DECREF(struct_services_dict);
    }
    {
        pycbc_strn_base_const json;
        lcb_respping_value(resp, &json.buffer, &json.length);
        if (json.length) {
            pycbc_dict_add_text_kv_strn(
                    resultdict,
                    pycbc_strn_base_const_from_psz("services_json"),
                    json);
        }
    }
#ifdef PYCBC_V3_DEPRECATED
    if (resp->rflags & LCB_RESP_F_FINAL) {
        /* Note this can happen in both success and error cases!*/
        operation_completed_with_err_info(
                parent, mres, cbtype, resp_base, NULL);
    }
#endif
    CB_THR_BEGIN(parent);
}


static void diag_callback(lcb_t instance,
                          int cbtype,
                          const lcb_RESPBASE *resp_base)
{
    pycbc_Bucket *parent;
    const lcb_RESPDIAG *resp = (const lcb_RESPDIAG *)resp_base;
    pycbc_MultiResult *mres = NULL;
    pycbc_Result *res = NULL;
    PyObject *resultdict = NULL;

    lcb_respdiag_cookie(resp, (void **)&mres);
    resultdict = pycbc_multiresult_dict(mres);
    parent = mres->parent;
    CB_THR_END(parent);
    if (lcb_respdiag_status(resp) != LCB_SUCCESS) {
        if (mres->errop == NULL) {
            res = (pycbc_Result *)pycbc_result_new(parent);
            res->rc = lcb_respdiag_status(resp);
            res->key = Py_None;
            Py_INCREF(res->key);
            MAYBE_PUSH_OPERR(mres, res, lcb_respdiag_status(resp), 0);
        }
    }
    {
        pycbc_strn_base_const json;

        lcb_respdiag_value(resp, &json.buffer, &json.length);
        if (json.length) {
            pycbc_dict_add_text_kv_strn(
                    resultdict,
                    pycbc_strn_base_const_from_psz("health_json"),
                    json);
        }
    }
#ifdef PYCBC_V3_DEPRECATED
    if (resp->rflags & LCB_RESP_F_FINAL) {
        /* Note this can happen in both success and error cases!*/
        operation_completed_with_err_info(parent, mres, cbtype, resp_base, res);
    }
#endif
    CB_THR_BEGIN(parent);
}

void pycbc_generic_cb(lcb_t instance,
                      int cbtype,
                      const lcb_RESPBASE *rb,
                      const char *NAME)
{
    const lcb_RESPCOUNTER *resp = (const lcb_RESPCOUNTER *)rb;
    int rv;
    int optflags = RESTYPE_OPERATION;
    pycbc_Bucket *conn = NULL;
    pycbc_OperationResult *res = NULL;
    pycbc_MultiResult *mres = NULL;
    response_handler handler = {.cbtype = cbtype};
    PYCBC_DEBUG_LOG("%s callback", NAME)
    rv = get_common_objects((const lcb_RESPBASE *)resp,
                            &conn,
                            (pycbc_Result **)&res,
                            optflags,
                            &mres,
                            &handler);
    PYCBC_DEBUG_LOG_CONTEXT(
            PYCBC_RES_CONTEXT(res), "%s callback continues", NAME)

    if (rv == 0) {
        res->rc = lcb_respcounter_status(resp);
        MAYBE_PUSH_OPERR(mres, (pycbc_Result *)res, res->rc, 0);
    }

    operation_completed_with_err_info(conn,
                                      mres,
                                      cbtype,
                                      (const lcb_RESPBASE *)resp,
                                      (pycbc_Result *)res);
    CB_THR_BEGIN(conn);
    (void)instance;
}

#define PYCBC_CALLBACK_GENERIC(NAME)                                   \
    void NAME##_cb(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) \
    {                                                                  \
        pycbc_generic_cb(instance, cbtype, rb, #NAME);                 \
    }

#ifdef PYCBC_EXTRA_CALLBACK_WRAPPERS
#define PYCBC_FOR_EACH_GEN_CALLBACK(X) \
    X(LCB_CALLBACK_VERSIONS)           \
    X(LCB_CALLBACK_VERBOSITY)          \
    X(LCB_CALLBACK_FLUSH)              \
    X(LCB_CALLBACK_CBFLUSH)            \
    X(LCB_CALLBACK_OBSEQNO)            \
    X(LCB_CALLBACK_STOREDUR)           \
    X(LCB_CALLBACK_COUNTER)

PYCBC_FOR_EACH_GEN_CALLBACK(PYCBC_CALLBACK_GENERIC)
#endif


void
pycbc_callbacks_init(lcb_t instance)
{
    lcb_install_callback(instance, LCB_CALLBACK_STORE, durability_chain_common);
    lcb_install_callback(instance, LCB_CALLBACK_REMOVE, durability_chain_common);
    lcb_install_callback(instance, LCB_CALLBACK_UNLOCK, keyop_simple_callback);
    lcb_install_callback(instance, LCB_CALLBACK_TOUCH, keyop_simple_callback);
    lcb_install_callback(instance, LCB_CALLBACK_ENDURE, keyop_simple_callback);
    lcb_install_callback(instance, LCB_CALLBACK_GET, value_callback);
    lcb_install_callback(instance, LCB_CALLBACK_GETREPLICA, value_callback);
    lcb_install_callback(instance, LCB_CALLBACK_COUNTER, value_callback);
    lcb_install_callback(instance, LCB_CALLBACK_OBSERVE, observe_callback);
    // Comment out until stats are back in lcb lcb_install_callback(instance,
    // LCB_CALLBACK_STATS, stats_callback);
    lcb_install_callback(instance, LCB_CALLBACK_PING, ping_callback);
    lcb_install_callback(instance, LCB_CALLBACK_DIAG, diag_callback);
    lcb_install_callback(instance, LCB_CALLBACK_EXISTS, exists_callback);
#ifdef PYCBC_EXTRA_CALLBACK_WRAPPERS
#define X(NAME) lcb_install_callback3(instance, NAME, NAME##_cb);
    PYCBC_FOR_EACH_GEN_CALLBACK(X)
#undef X
#endif
    /* Subdoc */
    lcb_install_callback(instance, LCB_CALLBACK_SDLOOKUP, subdoc_callback);
    lcb_install_callback(instance, LCB_CALLBACK_SDMUTATE, subdoc_callback);

    lcb_set_bootstrap_callback(instance, bootstrap_callback);

    pycbc_http_callbacks_init(instance);
}
