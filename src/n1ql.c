#include <libcouchbase/ixmgmt.h>
#include "oputil.h"
#include "pycbc.h"
#include "pycbc_http.h"

void pycbc_extract_unlock_bucket(const pycbc_MultiResult *mres,
                                 pycbc_Bucket **bucket,
                                 pycbc_ViewResult **vres)
{
    (*vres) = (pycbc_ViewResult *)PyDict_GetItem((PyObject *)mres, Py_None);
    (*bucket) = mres->parent;
    PYCBC_CONN_THR_END((*bucket));
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
        pycbc_ViewResult *vres;                                           \
        const char *const *hdrs = NULL;                                   \
        short htcode = 0;                                                 \
        const lcb_RESPHTTP *htresp = NULL;                                \
        const lcb_RESP##UC *resp = (const lcb_RESP##UC *)respbase;        \
        lcb_resp##LC##_cookie(resp, (void **)&mres);                      \
        pycbc_extract_unlock_bucket(mres, &bucket, &vres);                \
        lcb_resp##LC##_http_response(resp, &htresp);                      \
        pycbc_get_headers_status(htresp, &hdrs, &htcode);                 \
        {                                                                 \
            const char *rows = NULL;                                      \
            size_t row_count = 0;                                         \
            int is_final = lcb_resp##LC##_is_final(resp);                 \
            lcb_resp##LC##_row(resp, &rows, &row_count);                  \
            pycbc_add_row_or_data(mres, vres, rows, row_count, is_final); \
        }                                                                 \
        pycbc_viewresult_step(                                            \
                vres, mres, bucket, lcb_resp##LC##_is_final(resp));       \
                                                                          \
        if (lcb_resp##LC##_is_final(resp)) {                              \
            pycbc_httpresult_complete(&vres->base,                        \
                                      mres,                               \
                                      lcb_resp##LC##_status(resp),        \
                                      htcode,                             \
                                      hdrs);                              \
        } else {                                                          \
            PYCBC_CONN_THR_BEGIN(bucket);                                 \
        }                                                                 \
    }

#define PYCBC_QUERY_GEN
#ifdef PYCBC_QUERY_GEN
PYCBC_QUERY_CALLBACK(ANALYTICS, analytics)
PYCBC_QUERY_CALLBACK(N1QL, n1ql)
#else
static void analytics_row_callback(lcb_t instance, int ign, const lcb_RESPANALYTICS *respbase) {
    pycbc_MultiResult *mres = ((void *) 0);
    pycbc_Bucket *bucket = ((void *) 0);
    pycbc_ViewResult *vres;
    const char *const *hdrs = ((void *) 0);
    short htcode = 0;
    const lcb_RESPHTTP *htresp = ((void *) 0);
    const lcb_RESPANALYTICS *resp = (const lcb_RESPANALYTICS *) respbase;
    lcb_respanalytics_cookie(resp, (void **) &mres);
    pycbc_extract_unlock_bucket(mres, &bucket, &vres);
    lcb_respanalytics_http_response(resp, &htresp);
    pycbc_get_headers_status(htresp, &hdrs, &htcode);
    {
        const char *rows = ((void *) 0);
        size_t row_count = 0;
        int is_final = lcb_respanalytics_is_final(resp);
        lcb_respanalytics_row(resp, &rows, &row_count);
        pycbc_add_row_or_data(mres, vres, rows, row_count, is_final);
    }
    pycbc_viewresult_step(vres, mres, bucket, lcb_respanalytics_is_final(resp));
    if (lcb_respanalytics_is_final(resp)) {
        pycbc_httpresult_complete(&vres->base, mres, lcb_respanalytics_status(resp), htcode, hdrs);
    }
    else {
        PYCBC_CONN_THR_BEGIN(bucket);
    }
}

static void n1ql_row_callback(lcb_t instance, int ign, const lcb_RESPN1QL *respbase) {
    pycbc_MultiResult *mres = ((void *) 0);
    pycbc_Bucket *bucket = ((void *) 0);
    pycbc_ViewResult *vres;
    const char *const *hdrs = ((void *) 0);
    short htcode = 0;
    const lcb_RESPHTTP *htresp = ((void *) 0);
    const lcb_RESPN1QL *resp = (const lcb_RESPN1QL *) respbase;
    lcb_respn1ql_cookie(resp, (void **) &mres);
    pycbc_extract_unlock_bucket(mres, &bucket, &vres);
    lcb_respn1ql_http_response(resp, &htresp);
    pycbc_get_headers_status(htresp, &hdrs, &htcode);
    {
        const char *rows = ((void *) 0);
        size_t row_count = 0;
        int is_final = lcb_respn1ql_is_final(resp);
        lcb_respn1ql_row(resp, &rows, &row_count);
        pycbc_add_row_or_data(mres, vres, rows, row_count, is_final);
    }
    pycbc_viewresult_step(vres, mres, bucket, lcb_respn1ql_is_final(resp));
    if (lcb_respn1ql_is_final(resp)) {
        pycbc_httpresult_complete(&vres->base, mres, lcb_respn1ql_status(resp), htcode, hdrs);
    }
    else {
        PYCBC_CONN_THR_BEGIN(bucket);
    }
}
#endif

#define PYCBC_ADHOC(CMD, PREPARED) \
    if (PREPARED) {                \
        lcb_cmdn1ql_adhoc(cmd, 1); \
    }
#define PYCBC_HOST(CMD, HOST)               \
    if (HOST) {                             \
        pycbc_cmdanalytics_host(CMD, HOST); \
    }
#define PYCBC_N1QL_MULTIAUTH(CMD, IS_XBUCKET)                                \
    {                                                                        \
        lcb_STATUS ma_status =                                               \
                is_xbucket ? pycbc_cmdn1ql_multiauth(cmd, 1) : rc;           \
        if (ma_status) {                                                     \
            PYCBC_DEBUG_LOG_CONTEXT(context,                                 \
                                    "Couldn't set multiauth: %s",            \
                                    lcb_strerror(self->instance, ma_status)) \
        }                                                                    \
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
PYCBC_HANDLE_QUERY(N1QL, n1ql, PYCBC_ADHOC, PYCBC_DUMMY, PYCBC_N1QL_MULTIAUTH);

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
            lcb_cmdanalytics_query(cmd, params, nparams);
            lcb_cmdanalytics_handle(cmd, &vres->base.u.analytics);
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

lcb_STATUS
pycbc_handle_n1ql(const pycbc_Bucket *self, const char *params, unsigned int nparams, const char *host, int is_prepared,
                  int is_xbucket, pycbc_MultiResult *mres, pycbc_ViewResult *vres, pycbc_stack_context_handle context)
{
    (void)host;
    lcb_STATUS rc = LCB_SUCCESS;
    {
        CMDSCOPE_NG(N1QL, n1ql)
        {
            lcb_cmdn1ql_callback(cmd, n1ql_row_callback);
            lcb_cmdn1ql_query(cmd, params, nparams);
            lcb_cmdn1ql_handle(cmd, &vres->base.u.n1ql);
            if (is_prepared) {
                lcb_cmdn1ql_adhoc(cmd, 1);
            }
            {
                lcb_STATUS ma_status =
                        is_xbucket ? pycbc_cmdn1ql_multiauth(cmd, 1) : rc;
                if (ma_status) {
                }
            }
            PYCBC_TRACECMD_SCOPED_NULL(rc,
                                       n1ql,
                                       self->instance,
                                       cmd,
                                       vres->base.u.n1ql,
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
                int is_xbucket)
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
    vres->base.htype = PYCBC_HTTP_HN1QL;

    static pycbc_query_handler handlers[] = {pycbc_handle_n1ql,
                                             pycbc_handle_analytics};
    rc = (handlers[host ? 1 : 0])(
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
                              cross_bucket);
    return result;
}

PyObject *
pycbc_Bucket__cbas_query(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    const char *host = NULL;
    const char *params = NULL;
    pycbc_strlen_t nparams = 0;
    static char *kwlist[] = { "params", "host", NULL };
    PyObject *result = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#s", kwlist,
        &params, &nparams, &host)) {
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
                              host,
                              0,
                              0);
    return result;
}
