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
        pycbc_viewresult_step(vres, mres, bucket, lcb_respquery_is_final(resp));
    }
    if (lcb_respquery_is_final(resp)) {
        if (vres) {
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
    if (PREPARED) {                 \
        lcb_cmdquery_adhoc(cmd, 1); \
    }
#define PYCBC_HOST(CMD, HOST)               \
    if (HOST) {                             \
        pycbc_cmdanalytics_host(CMD, HOST); \
    }
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

#define PYCBC_HANDLE_QUERY(UC, LC, ADHOC, HOST, MULTIAUTH)      \
    lcb_STATUS pycbc_handle_##LC(const pycbc_Bucket *self,      \
                                 const char *params,            \
                                 unsigned int nparams,          \
                                 const char *host,              \
                                 int is_prepared,               \
                                 int is_xbucket,                \
                                 pycbc_MultiResult *mres,       \
                                 pycbc_ViewResult *vres)        \
    {                                                           \
        lcb_STATUS rc = LCB_SUCCESS;                            \
        {                                                       \
            CMDSCOPE_NG(UC, LC)                                 \
            {                                                   \
                lcb_cmd##LC##_callback(cmd, LC##_row_callback); \
                lcb_cmd##LC##_query(cmd, params, nparams);      \
                lcb_cmd##LC##_handle(cmd, &vres->base.u.LC);    \
                ADHOC(cmd, is_prepared)                         \
                HOST(cmd, host)                                 \
                MULTIAUTH(CMD, is_xbucket)                      \
                PYCBC_TRACECMD_SCOPED_NULL(rc,                  \
                                           LC,                  \
                                           self->instance,      \
                                           cmd,                 \
                                           vres->base.u.LC,     \
                                           context,             \
                                           mres,                \
                                           cmd)                \
            }                                                   \
        }                                                       \
    GT_DONE:                                                    \
        return rc;                                              \
    }

typedef lcb_STATUS (*pycbc_query_handler)(const pycbc_Bucket *self,
                                          const char *params,
                                          unsigned int nparams,
                                          const char *host,
                                          int is_prepared,
                                          int is_xbucket,
                                          pycbc_MultiResult *mres,
                                          pycbc_ViewResult *vres,
                                          pycbc_stack_context_handle);
#undef PYCBC_QUERY_GEN
#ifdef PYCBC_QUERY_GEN
PYCBC_HANDLE_QUERY(ANALYTICS, analytics, PYCBC_DUMMY, PYCBC_HOST, PYCBC_DUMMY);
PYCBC_HANDLE_QUERY(
        QUERY, query, PYCBC_ADHOC, PYCBC_DUMMY, PYCBC_QUERY_MULTIAUTH);

#else
lcb_STATUS pycbc_handle_analytics(const pycbc_Bucket *self,
                                  const char *params,
                                  unsigned int nparams,
                                  const char *host,
                                  int is_prepared,
                                  int is_xbucket,
                                  pycbc_MultiResult *mres,
                                  pycbc_ViewResult *vres,
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
            if (host) {
                pycbc_cmdanalytics_host(cmd, host);
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
                              const char *host,
                              int is_prepared,
                              int is_xbucket,
                              pycbc_MultiResult *mres,
                              pycbc_ViewResult *vres,
                              pycbc_stack_context_handle context)
{
    (void)host;
    lcb_STATUS rc = LCB_SUCCESS;
    {
        CMDSCOPE_NG(QUERY, query)
        {
            lcb_cmdquery_callback(cmd, query_row_callback);
            lcb_cmdquery_payload(cmd, params, nparams);
            lcb_cmdquery_handle(cmd, &(vres->base.u.query));
            if (is_prepared) {
                lcb_cmdquery_adhoc(cmd, 1);
            }
            {
                lcb_STATUS ma_status =
                        is_xbucket ? pycbc_cmdquery_multiauth(cmd, 1) : rc;
                if (ma_status) {
                }
            }
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
                const char *host,
                int is_prepared,
                int is_xbucket,
                int is_analytics)
{
    PyObject *ret = NULL;
    pycbc_MultiResult *mres;
    pycbc_ViewResult *vres;
    lcb_STATUS rc = LCB_SUCCESS;
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

    static pycbc_query_handler handlers[] = {pycbc_handle_query,
                                             pycbc_handle_analytics};
    Py_INCREF(vres);
    rc = (handlers[is_analytics])(
            self, params, nparams, host, is_prepared, is_xbucket, mres, vres, context);
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
    static char *kwlist[] = { "params", "prepare", "cross_bucket", NULL };
    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "s#|ii", kwlist, &params,
        &nparams, &prepared, &cross_bucket)) {

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
                              NULL,
                              prepared,
                              cross_bucket,
                              0);
    return result;
}

PyObject *pycbc_Bucket__cbas_query(pycbc_Bucket *self,
                                   PyObject *args,
                                   PyObject *kwargs)
{
    PyObject *pyhost = Py_None;
    const char *params = NULL;
    pycbc_strlen_t nparams = 0;
    static char *kwlist[] = {"params", "host", NULL};
    PyObject *result = NULL;
    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "s#O", kwlist, &params, &nparams, &pyhost)) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }
    {
        const char *host = NULL;
        if (PyObject_IsTrue(pyhost)) {
            host = pycbc_cstr(pyhost);
            if (!host) {
                PYCBC_EXCTHROW_ARGS();
            }
        }
        PYCBC_TRACE_WRAP_TOPLEVEL(result,
                                  LCBTRACE_OP_REQUEST_ENCODING,
                                  query_common,
                                  self->tracer,
                                  self,
                                  params,
                                  nparams,
                                  host,
                                  0,
                                  0,
                                  1);
    }
    return result;
}
