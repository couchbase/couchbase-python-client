#include "oputil.h"
#include "pycbc.h"
#include "pycbc_http.h"

void convert_search_error_context(const lcb_SEARCH_ERROR_CONTEXT* ctx,
                                  pycbc_MultiResult* mres) {

    pycbc_enhanced_err_info* err_info = PyDict_New();
    PyObject* err_context = PyDict_New();
    PyDict_SetItemString(err_info, "error_context", err_context);
    if (ctx) {
        uint32_t uint32_val;
        const char* val;
        size_t len;

        lcb_errctx_search_http_response_code(ctx, &uint32_val);
        pycbc_set_kv_ull_str(err_context, "http_response_code", (lcb_uint64_t)uint32_val);
        lcb_errctx_search_error_message(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "error_message", val, len);
        lcb_errctx_search_index_name(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "index_name", val, len);
        lcb_errctx_search_query(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "query", val, len);
        lcb_errctx_search_params(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "params", val, len);
        lcb_errctx_search_http_response_body(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "http_response_body", val, len);
        lcb_errctx_search_endpoint(ctx, &val, &len);
        pycbc_dict_add_text_kv_strn2(err_context, "endpoint", val, len);
        pycbc_dict_add_text_kv(err_context, "type", "SearchErrorContext");
    }
    mres->err_info = err_info;
    Py_INCREF(err_info);
    Py_DECREF(err_context);
}

void pycbc_add_fts_error_context(const lcb_RESPSEARCH* resp,
                                 pycbc_MultiResult* mres) {
    const lcb_SEARCH_ERROR_CONTEXT* ctx;
    if (LCB_SUCCESS == lcb_respsearch_error_context(resp, &ctx)) {
        if (ctx) {
            convert_search_error_context(ctx, mres);
        }
    }
}

static void fts_row_callback(lcb_t instance,
                             int ign,
                             const lcb_RESPSEARCH *resp)
{
    pycbc_MultiResult *mres = NULL;
    pycbc_Bucket *bucket = NULL;
    pycbc_ViewResult *vres;
    const char *const *hdrs = NULL;
    short htcode = 0;
    lcb_respsearch_cookie(resp, (void **)&mres);
    bucket = mres->parent;
    // it is possible to get the callback even if we didn't lcb_wait.  So, check for that.
    // This is a hack, which we need because LCB now calls the callback even though the lcb_search
    // errored out.
    if (!bucket->thrstate && bucket->unlock_gil) {
        return;
    }
    PYCBC_CONN_THR_END(bucket);
    vres = (pycbc_ViewResult *)PyDict_GetItem((PyObject *)mres, Py_None);
    {
        const lcb_RESPHTTP *lcb_resphttp = NULL;
        lcb_respsearch_http_response(resp, &lcb_resphttp);
        if (lcb_resphttp) {
            lcb_resphttp_headers(lcb_resphttp, &hdrs);
            htcode = lcb_resphttp_status(lcb_resphttp);
        }
    }
    {
        pycbc_strn_base_const row = {0};
        lcb_respsearch_row(resp, &row.buffer, &row.length);
        if (lcb_respsearch_is_final(resp)) {
            pycbc_httpresult_add_data_strn(mres, &vres->base, row);
        } else {
            /* Like views, try to decode the row and invoke the callback; if we
             * can */
            /* Assume success! */
            pycbc_viewresult_addrow(vres, mres, row.buffer, row.length);
        }
    }
    pycbc_viewresult_step(vres, mres, bucket, lcb_respsearch_is_final(resp));
    if (lcb_respsearch_is_final(resp)) {
        pycbc_add_fts_error_context(resp, mres);
        pycbc_httpresult_complete(
                &vres->base, mres, lcb_respsearch_status(resp), htcode, hdrs);
    } else {
        PYCBC_CONN_THR_BEGIN(bucket);
    }
}


PyObject *
pycbc_Bucket__fts_query(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    int rv;
    PyObject *ret = NULL;
    pycbc_MultiResult *mres;
    pycbc_ViewResult *vres;
    lcb_STATUS rc = LCB_SUCCESS;
    pycbc_pybuffer buf = { 0 };
    PyObject *params_o = NULL;
    PyObject *external_span = NULL;
    PyObject *index = NULL;
    pycbc_stack_context_handle context = NULL;
    static char *kwlist[] = { "params", "span", "index", NULL };
    pycbc_Collection_t collection = pycbc_Collection_as_value(self, kwargs);
    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O|OO", kwlist, &params_o, &external_span, &index);
    if (!rv) {
        goto GT_FAIL;
    }

    create_outer_search_span(self->tracer, mres, external_span, PyUnicode_AsUTF8(index));

    lcbtrace_SPAN *encode_span = create_encode_search_span(self->tracer, mres);
    rv = pycbc_tc_simple_encode(params_o, &buf, PYCBC_FMT_UTF8);
    lcbtrace_span_finish(encode_span, LCBTRACE_NOW);
    if (rv != 0) {
        goto GT_FAIL;
    }

    if (-1 == pycbc_oputil_conn_lock(self)) {
        goto GT_FAIL;
    }
    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE,
                       0,
                       "Search queries cannot be executed in pipeline context");
    }

    vres = pycbc_propagate_view_result(context);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = PYCBC_HTTP_HSEARCH;
    {
        CMDSCOPE_NG(SEARCH, search)
        {
            lcb_cmdsearch_callback(cmd, fts_row_callback);
            lcb_cmdsearch_payload(cmd, buf.buffer, buf.length);
            lcb_cmdsearch_handle(cmd, &vres->base.u.search);
            lcb_cmdsearch_parent_span(cmd, mres->outer_span);

            PYCBC_TRACECMD_SCOPED_GENERIC(rc,
                                          search,
                                          query,
                                          self->instance,
                                          cmd,
                                          *cmd->handle,
                                          context,
                                          GENERIC_SPAN_OPERAND,
                                          GENERIC_NULL_OPERAND,
                                          mres,
                                          cmd);
        }
    }
    GT_ERR:
    PYCBC_PYBUF_RELEASE(&buf);

    if (rc != LCB_SUCCESS) {
        // we never sent the request, so the callbacks will not close outer span.  Do that here.
        lcbtrace_span_finish(mres->outer_span, LCBTRACE_NOW);
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't schedule fts query");
        goto GT_DONE;
    }

    ret = (PyObject *)mres;
    mres = NULL;

    GT_DONE:
    Py_XDECREF(mres);
    pycbc_oputil_conn_unlock(self);
    GT_FINAL:
        pycbc_Collection_free_unmanaged_contents(&collection);
        return ret;
    GT_FAIL:
        // we never sent the request, so the callbacks will not close outer span.  Do that here.
        lcbtrace_span_finish(mres->outer_span, LCBTRACE_NOW);
        ret = NULL;
        goto GT_FINAL;
}
