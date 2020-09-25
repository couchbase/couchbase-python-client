#include "pycbc.h"
#include <libcouchbase/ixmgmt.h>
#include "oputil.h"
#include "pycbc_http.h"

void pycbc_extract_unlock_bucket(const pycbc_MultiResult *mres,
                                 pycbc_Bucket **bucket,
                                 pycbc_ViewResult **vres)
{
    if (mres) {
        (*vres) = (pycbc_ViewResult *)PyDict_GetItem((PyObject *)mres, Py_None);
        (*bucket) = mres->parent;
        if (*bucket) {
#ifdef PYCBC_NO_REENTRANT_THREADS
            PYCBC_CONN_THR_END((*bucket));
#else
            if ((*bucket)->thrstate) {
                PyEval_RestoreThread((*bucket)->thrstate);
                (*bucket)->thrstate = NULL;
            }
#endif
        }
    }
}

void pycbc_get_headers_status(const lcb_RESPHTTP *htresp,
                              const char *const **hdrs,
                              short *htcode)
{
    if (htresp) {
        lcb_resphttp_headers(htresp, hdrs);
        (*htcode) = lcb_resphttp_status(htresp);
    }
}

void pycbc_add_row_or_data(pycbc_MultiResult *mres,
                           pycbc_ViewResult *vres,
                           const char *rows,
                           size_t row_count,
                           int is_final)
{
    if (is_final) {
        pycbc_httpresult_add_data(mres, &vres->base, rows, row_count);
    } else {
        /* Like views, try to decode the row and invoke the callback;
         * if we can */
        /* Assume success! */
        pycbc_viewresult_addrow(vres, mres, rows, row_count);
    }
}

/* note that the analytics and query error contexts are identical -- could
   make a clever macro solution to DRY this up, but then I'd never be able to
   understand it again.  So, no. Maybe later.
   */
void convert_analytics_error_context(const lcb_ANALYTICS_ERROR_CONTEXT* ctx,
                                     pycbc_MultiResult *mres,
                                     const char* extended_context,
                                     const char* extended_ref) {

    pycbc_enhanced_err_info* err_info = PyDict_New();
    PyObject* err_context = PyDict_New();
    PyDict_SetItemString(err_info, "error_context", err_context);
    if (ctx) {
        uint32_t uint32_val;
        const char* val;
        size_t len;

        lcb_errctx_analytics_first_error_code(ctx, &uint32_val);
        pycbc_set_kv_ull_str(err_context, "first_error_code", (lcb_uint64_t)uint32_val);
        lcb_errctx_analytics_http_response_code(ctx, &uint32_val);
        pycbc_set_kv_ull_str(err_context, "http_response_code", (lcb_uint64_t)uint32_val);
        lcb_errctx_analytics_first_error_message(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "first_error_message", val, len);
        lcb_errctx_analytics_statement(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "statement", val, len);
        lcb_errctx_analytics_client_context_id(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "client_context_id", val, len);
        lcb_errctx_analytics_query_params(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "query_params", val, len);
        lcb_errctx_analytics_http_response_body(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "http_response_body", val, len);
        lcb_errctx_analytics_endpoint(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "endpoint", val, len);
        pycbc_dict_add_text_kv(err_context, "type", "AnalyticsErrorContext");
    }
    if (extended_context) {
        pycbc_dict_add_text_kv(err_context, "extended_context", extended_context);
    }
    if (extended_ref) {
        pycbc_dict_add_text_kv(err_context, "extended_ref", extended_ref);
    }
    mres->err_info = err_info;
    Py_DECREF(err_context);
}
void convert_query_error_context(const lcb_QUERY_ERROR_CONTEXT* ctx,
                                 pycbc_MultiResult *mres,
                                 const char* extended_context,
                                 const char* extended_ref) {
    pycbc_enhanced_err_info* err_info = PyDict_New();
    PyObject* err_context = PyDict_New();
    PyDict_SetItemString(err_info, "error_context", err_context);
    if (ctx) {
        uint32_t uint32_val;
        const char* val;
        size_t len;

        lcb_errctx_query_first_error_code(ctx, &uint32_val);
        pycbc_set_kv_ull_str(err_context, "first_error_code", (lcb_uint64_t)uint32_val);
        lcb_errctx_query_http_response_code(ctx, &uint32_val);
        pycbc_set_kv_ull_str(err_context, "http_response_code", (lcb_uint64_t)uint32_val);
        lcb_errctx_query_first_error_message(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "first_error_message", val, len);
        lcb_errctx_query_statement(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "statement", val, len);
        lcb_errctx_query_client_context_id(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "client_context_id", val, len);
        lcb_errctx_query_query_params(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "query_params", val, len);
        lcb_errctx_query_http_response_body(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "http_response_body", val, len);
        lcb_errctx_query_endpoint(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "endpoint", val, len);
        pycbc_dict_add_text_kv(err_context, "type", "QueryErrorContext");
    }
    if (extended_context) {
        pycbc_dict_add_text_kv(err_context, "extended_context", extended_context);
    }
    if (extended_ref) {
        pycbc_dict_add_text_kv(err_context, "extended_ref", extended_ref);
    }
    mres->err_info = err_info;
    Py_DECREF(err_context);
}

/* Same here -- this is practically identical to the query one below, but no
   fancy macros here, yet.
*/
void pycbc_add_analytics_error_context(const lcb_RESPANALYTICS* resp,
                                       pycbc_MultiResult* mres) {
    /* get the extended error context and ref, if any */
    const char *extended_ref = lcb_resp_get_error_ref(LCB_CALLBACK_ANALYTICS, (lcb_RESPBASE*)resp);
    const char *extended_context = lcb_resp_get_error_context(LCB_CALLBACK_ANALYTICS, (lcb_RESPBASE*)resp);
    const lcb_ANALYTICS_ERROR_CONTEXT* ctx;
    if (LCB_SUCCESS == lcb_respanalytics_error_context(resp, &ctx)) {
        if (ctx) {
            convert_analytics_error_context(ctx, mres, extended_context, extended_ref);
        }
    }
}

void pycbc_add_query_error_context(const lcb_RESPQUERY* resp,
                                   pycbc_MultiResult* mres) {
    /* get the extended error context and ref, if any */
    const char *extended_ref = lcb_resp_get_error_ref(LCB_CALLBACK_QUERY, (lcb_RESPBASE*)resp);
    const char *extended_context = lcb_resp_get_error_context(LCB_CALLBACK_QUERY, (lcb_RESPBASE*)resp);
    const lcb_QUERY_ERROR_CONTEXT* ctx;
    if (LCB_SUCCESS == lcb_respquery_error_context(resp, &ctx)) {
        if (ctx) {
            convert_query_error_context(ctx, mres, extended_context, extended_ref);
        }
    }
}
#define PYCBC_QUERY_CALLBACK(UC, LC)                                      \
    static void LC##_row_callback(                                        \
            lcb_t instance, int ign, const lcb_RESP##UC *respbase)        \
    {                                                                     \
        pycbc_MultiResult *mres = NULL;                                   \
        pycbc_Bucket *bucket = NULL;                                      \
        pycbc_ViewResult *vres = NULL;                                    \
        const char *const *hdrs = NULL;                                   \
        short htcode = 0;                                                 \
        const lcb_RESPHTTP *htresp = NULL;                                \
        const lcb_RESP##UC *resp = (const lcb_RESP##UC *)respbase;        \
        lcb_resp##LC##_cookie(resp, (void **)&mres);                      \
        pycbc_extract_unlock_bucket(mres, &bucket, &vres);                \
        lcb_resp##LC##_http_response(resp, &htresp);                      \
        pycbc_get_headers_status(htresp, &hdrs, &htcode);                 \
        if (vres) {                                                       \
            const char *rows = NULL;                                      \
            size_t row_count = 0;                                         \
            int is_final = lcb_resp##LC##_is_final(resp);                 \
            lcb_resp##LC##_row(resp, &rows, &row_count);                  \
            pycbc_add_row_or_data(mres, vres, rows, row_count, is_final); \
            pycbc_viewresult_step(                                        \
                    vres, mres, bucket, lcb_resp##LC##_is_final(resp));   \
        }                                                                 \
        if (lcb_resp##LC##_is_final(resp)) {                              \
            if (vres) {                                                   \
                pycbc_httpresult_complete(&vres->base,                    \
                                          mres,                           \
                                          lcb_resp##LC##_status(resp),    \
                                          htcode,                         \
                                          hdrs);                          \
            }                                                             \
        } else {                                                          \
            PYCBC_CONN_THR_BEGIN(bucket);                                 \
        }                                                                 \
    }

#ifdef PYCBC_QUERY_GEN
PYCBC_QUERY_CALLBACK(ANALYTICS, analytics)
PYCBC_QUERY_CALLBACK(QUERY, query)
#else

static void analytics_row_callback(lcb_t instance, int ign, const lcb_RESPANALYTICS *respbase) {
    pycbc_MultiResult *mres = ((void *) 0);
    pycbc_Bucket *bucket = ((void *) 0);
    pycbc_ViewResult *vres = ((void *)0);
    const char *const *hdrs = ((void *) 0);
    short htcode = 0;
    const lcb_RESPHTTP *htresp = ((void *) 0);
    const lcb_RESPANALYTICS *resp = (const lcb_RESPANALYTICS *) respbase;
    lcb_respanalytics_cookie(resp, (void **) &mres);
    pycbc_extract_unlock_bucket(mres, &bucket, &vres);
    lcb_respanalytics_http_response(resp, &htresp);
    pycbc_get_headers_status(htresp, &hdrs, &htcode);
    if (vres) {
        const char *rows = ((void *) 0);
        size_t row_count = 0;
        int is_final = lcb_respanalytics_is_final(resp);
        lcb_respanalytics_row(resp, &rows, &row_count);
        pycbc_add_row_or_data(mres, vres, rows, row_count, is_final);
        pycbc_viewresult_step(
                vres, mres, bucket, lcb_respanalytics_is_final(resp));
    }
    if (lcb_respanalytics_is_final(resp)) {
        if (vres) {
            pycbc_add_analytics_error_context(resp, mres);
            pycbc_httpresult_complete(&vres->base,
                                      mres,
                                      lcb_respanalytics_status(resp),
                                      htcode,
                                      hdrs);
        }
    }
    else {
        PYCBC_CONN_THR_BEGIN(bucket);
    }
}

static void query_row_callback(lcb_t instance,
                               int ign,
                               const lcb_RESPQUERY *respbase)
{
    pycbc_MultiResult *mres = ((void *) 0);
    pycbc_Bucket *bucket = ((void *) 0);
    pycbc_ViewResult *vres = ((void *)0);
    const char *const *hdrs = ((void *) 0);
    short htcode = 0;
    const lcb_RESPHTTP *htresp = ((void *) 0);
    const lcb_RESPQUERY *resp = (const lcb_RESPQUERY *)respbase;
    lcb_respquery_cookie(resp, (void **)&mres);
    pycbc_extract_unlock_bucket(mres, &bucket, &vres);
    lcb_respquery_http_response(resp, &htresp);
    pycbc_get_headers_status(htresp, &hdrs, &htcode);
    if (vres) {
        const char *rows = ((void *) 0);
        size_t row_count = 0;
        int is_final = lcb_respquery_is_final(resp);
        lcb_respquery_row(resp, &rows, &row_count);
        pycbc_add_row_or_data(mres, vres, rows, row_count, is_final);
        pycbc_viewresult_step(vres, mres, bucket, is_final);
    }
    if (lcb_respquery_is_final(resp)) {
        if (vres) {
            pycbc_add_query_error_context(resp, mres);
            pycbc_httpresult_complete(&vres->base,
                                      mres,
                                      lcb_respquery_status(resp),
                                      htcode,
                                      hdrs);
        }
    } else {
        PYCBC_CONN_THR_BEGIN(bucket);
    }
}
#endif

#define PYCBC_ADHOC(CMD, PREPARED)  \
    lcb_cmdquery_adhoc(cmd, !(PREPARED));

#define PYCBC_FLEX(CMD, IS_FLEX) lcb_cmdquery_flex_index(cmd, IS_FLEX);
#define PYCBC_QUERY_MULTIAUTH(CMD, IS_XBUCKET)                      \
    {                                                               \
        lcb_STATUS ma_status =                                      \
                is_xbucket ? pycbc_cmdquery_multiauth(cmd, 1) : rc; \
        if (ma_status) {                                            \
            PYCBC_DEBUG_LOG_CONTEXT(context,                        \
                                    "Couldn't set multiauth: %s",   \
                                    lcb_strerror_short(ma_status))  \
        }                                                           \
    }

#define PYCBC_HANDLE_QUERY(UC, LC, ADHOC, MULTIAUTH, FLEX)           \
    lcb_STATUS pycbc_handle_##LC(const pycbc_Bucket *self,           \
                                 const char *params,                 \
                                 unsigned int nparams,               \
                                 int is_prepared,                    \
                                 int is_xbucket,                     \
                                 pycbc_MultiResult *mres,            \
                                 pycbc_ViewResult *vres,             \
                                 lcb_uint32_t timeout,               \
                                 int flex_index,                     \
                                 pycbc_stack_context_handle context) \
    {                                                                \
        lcb_STATUS rc = LCB_SUCCESS;                                 \
        {                                                            \
            CMDSCOPE_NG(UC, LC)                                      \
            {                                                        \
                lcb_cmd##LC##_callback(cmd, LC##_row_callback);      \
                lcb_cmd##LC##_payload(cmd, params, nparams);         \
                lcb_cmd##LC##_handle(cmd, &vres->base.u.LC);         \
                if (timeout) {                                       \
                    lcb_cmd##LC##_timeout(cmd, timeout);             \
                }                                                    \
                ADHOC(cmd, is_prepared)                              \
                MULTIAUTH(CMD, is_xbucket)                           \
                FLEX(CMD, flex_index)                                \
                PYCBC_TRACECMD_SCOPED_NULL(rc,                       \
                                           LC,                       \
                                           self->instance,           \
                                           cmd,                      \
                                           vres->base.u.LC,          \
                                           context,                  \
                                           mres,                     \
                                           cmd)                      \
            }                                                        \
        }                                                            \
    GT_ERR:                                                          \
    GT_DONE:                                                         \
        return rc;                                                   \
    }

typedef lcb_STATUS (*pycbc_query_handler)(const pycbc_Bucket *self,
                                          const char *params,
                                          unsigned int nparams,
                                          int is_prepared,
                                          int is_xbucket,
                                          pycbc_MultiResult *mres,
                                          pycbc_ViewResult *vres,
                                          lcb_uint32_t timeout,
                                          int flex_index,
                                          pycbc_stack_context_handle);
#undef PYCBC_QUERY_GEN
#ifdef PYCBC_QUERY_GEN
PYCBC_HANDLE_QUERY(ANALYTICS, analytics, PYCBC_DUMMY, PYCBC_DUMMY, PYCBC_DUMMY);
PYCBC_HANDLE_QUERY(
        QUERY, query, PYCBC_ADHOC, PYCBC_QUERY_MULTIAUTH, PYCBC_FLEX);

#else
lcb_STATUS pycbc_handle_analytics(const pycbc_Bucket *self,
                                  const char *params,
                                  unsigned int nparams,
                                  int is_prepared,
                                  int is_xbucket,
                                  pycbc_MultiResult *mres,
                                  pycbc_ViewResult *vres,
                                  lcb_uint32_t timeout,
                                  int flex_index,
                                  pycbc_stack_context_handle context)
{
    (void)is_prepared;
    (void)is_xbucket;
    lcb_STATUS rc = LCB_SUCCESS;
    {
        CMDSCOPE_NG(ANALYTICS, analytics)
        {
            lcb_cmdanalytics_callback(cmd, analytics_row_callback);
            lcb_cmdanalytics_payload(cmd, params, nparams);
            lcb_cmdanalytics_handle(cmd, &(vres->base.u.analytics));
            if (timeout) {
                lcb_cmdanalytics_timeout(cmd, timeout);
            }

            PYCBC_TRACECMD_SCOPED_NULL(rc,
                                       analytics,
                                       self->instance,
                                       cmd,
                                       vres->base.u.analytics,
                                       context,
                                       mres,
                                       cmd)
        }
    }
GT_ERR:
GT_DONE:
    return rc;
};

lcb_STATUS pycbc_handle_query(const pycbc_Bucket *self,
                              const char *params,
                              unsigned int nparams,
                              int is_prepared,
                              int is_xbucket,
                              pycbc_MultiResult *mres,
                              pycbc_ViewResult *vres,
                              lcb_uint32_t timeout,
                              int flex_index,
                              pycbc_stack_context_handle context)
{
    lcb_STATUS rc = LCB_SUCCESS;
    {
        CMDSCOPE_NG(QUERY, query) {
            lcb_cmdquery_callback(cmd, query_row_callback);
            lcb_cmdquery_payload(cmd, params, nparams);
            lcb_cmdquery_handle(cmd, &(vres->base.u.query));
            if (timeout) {
                lcb_cmdquery_timeout(cmd, timeout);
            }
            lcb_cmdquery_adhoc(cmd, !(is_prepared));
            {
                lcb_STATUS ma_status =
                        is_xbucket ? pycbc_cmdquery_multiauth(cmd, 1) : rc;
                if (ma_status) {
                }
            }
            lcb_cmdquery_flex_index(cmd, flex_index);
            PYCBC_TRACECMD_SCOPED_NULL(rc,
                                       query,
                                       self->instance,
                                       cmd,
                                       vres->base.u.query,
                                       context,
                                       mres,
                                       cmd)
       }
    }

GT_ERR:
GT_DONE:
    return rc;
};

#endif

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                PyObject *,
                query_common,
                pycbc_Bucket *self,
                const char *params,
                unsigned nparams,
                int is_prepared,
                int is_xbucket,
                int is_analytics,
                PyObject *timeout_O,
                int flex_index)
{
    PyObject *ret = NULL;
    pycbc_MultiResult *mres = NULL;
    pycbc_ViewResult *vres = NULL;
    lcb_STATUS rc = LCB_SUCCESS;
    unsigned long timeout = 0;

    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }

    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE,
                       0,
                       "N1QL queries cannot be executed in "
                       "pipeline context");
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = pycbc_propagate_view_result(context);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = is_analytics ? PYCBC_HTTP_HANALYTICS : PYCBC_HTTP_HQUERY;

    if (pycbc_get_duration(timeout_O, &timeout, 1))
    {
        goto GT_DONE;
    }

    static pycbc_query_handler handlers[] = {pycbc_handle_query,
                                             pycbc_handle_analytics};

    rc = (handlers[is_analytics])(self,
                                  params,
                                  nparams,
                                  is_prepared,
                                  is_xbucket,
                                  mres,
                                  vres,
                                  timeout,
                                  flex_index,
                                  context);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't schedule n1ql query");
        goto GT_DONE;
    }

    ret = (PyObject *)mres;
    mres = NULL;

GT_DONE:
    Py_XDECREF(mres);
    pycbc_oputil_conn_unlock(self);
    return ret;
}

PyObject *
pycbc_Bucket__n1ql_query(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    const char *params = NULL;
    pycbc_strlen_t nparams = 0;
    int prepared = 0, cross_bucket = 0;
    PyObject *result = NULL;
    PyObject* timeout_O = NULL;
    int flex_index = 0;
    static char *kwlist[] = {
            "params", "prepare", "cross_bucket", "timeout", "flex_index", NULL};
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "s#|iiOi",
                                     kwlist,
                                     &params,
                                     &nparams,
                                     &prepared,
                                     &cross_bucket,
                                     &timeout_O,
                                     &flex_index)) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }
    PYCBC_TRACE_WRAP_TOPLEVEL(result,
                              LCBTRACE_OP_REQUEST_ENCODING,
                              query_common,
                              self->tracer,
                              self,
                              params,
                              nparams,
                              prepared,
                              cross_bucket,
                              0,
                              timeout_O,
                              flex_index);
    return result;
}

PyObject *pycbc_Bucket__cbas_query(pycbc_Bucket *self,
                                   PyObject *args,
                                   PyObject *kwargs)
{
    const char *params = NULL;
    pycbc_strlen_t nparams = 0;
    static char *kwlist[] = {"params", "timeout", NULL};
    PyObject *result = NULL;
    PyObject* timeout_O = NULL;
    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "s#|O", kwlist, &params, &nparams, &timeout_O)) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }
    {
        PYCBC_TRACE_WRAP_TOPLEVEL(result,
                                  LCBTRACE_OP_REQUEST_ENCODING,
                                  query_common,
                                  self->tracer,
                                  self,
                                  params,
                                  nparams,
                                  0,
                                  0,
                                  1,
                                  timeout_O,
                                  0);
    }
    return result;
}
