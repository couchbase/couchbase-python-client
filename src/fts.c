#include "oputil.h"
#include "pycbc_http.h"


static void fts_row_callback(lcb_t instance, int ign, const lcb_RESPFTS *resp)
{
    pycbc_MultiResult *mres = NULL;
    pycbc_Bucket *bucket = NULL;
    pycbc_ViewResult *vres;
    const char *const *hdrs = NULL;
    short htcode = 0;
    lcb_respfts_cookie(resp, (void **)&mres);
    bucket = mres->parent;
    PYCBC_CONN_THR_END(bucket);
    vres = (pycbc_ViewResult *)PyDict_GetItem((PyObject *)mres, Py_None);
    {
        const lcb_RESPHTTP *lcb_resphttp = NULL;
        lcb_respfts_http_response(resp, &lcb_resphttp);
        if (lcb_resphttp) {
            lcb_resphttp_headers(lcb_resphttp, &hdrs);
            htcode = lcb_resphttp_status(lcb_resphttp);
        }
    }
    {
        pycbc_strn_base_const row = {0};
        lcb_respfts_row(resp, &row.buffer, &row.length);
        if (lcb_respfts_is_final(resp)) {
            pycbc_httpresult_add_data_strn(mres, &vres->base, row);
        } else {
            /* Like views, try to decode the row and invoke the callback; if we
             * can */
            /* Assume success! */
            pycbc_viewresult_addrow(vres, mres, row.buffer, row.length);
        }
    }
    pycbc_viewresult_step(vres, mres, bucket, lcb_respfts_is_final(resp));
    if (lcb_respfts_is_final(resp)) {
        pycbc_httpresult_complete(
                &vres->base, mres, lcb_respfts_status(resp), htcode, hdrs);
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
    lcb_STATUS rc;
    pycbc_pybuffer buf = { 0 };
    PyObject *params_o = NULL;
    pycbc_stack_context_handle context = PYCBC_TRACE_GET_STACK_CONTEXT_TOPLEVEL(
            kwargs, LCBTRACE_OP_REQUEST_ENCODING, self->tracer, "fts_query");
    static char *kwlist[] = { "params", NULL };
    pycbc_Collection_t collection = pycbc_Collection_as_value(self, kwargs);
    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &params_o);

    if (!rv) {
        goto GT_FAIL;
    }

    if (pycbc_tc_simple_encode(params_o, &buf, PYCBC_FMT_UTF8) != 0) {
        goto GT_FAIL;
    }

    if (-1 == pycbc_oputil_conn_lock(self)) {
        goto GT_FAIL;
    }
    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "FTS queries cannot be executed in pipeline context");
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = pycbc_propagate_view_result(context);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = PYCBC_HTTP_HFTS;
    {
        CMDSCOPE_NG(FTS, fts)
        {
            lcb_cmdfts_callback(cmd, fts_row_callback);
            lcb_cmdfts_query(cmd, buf.buffer, buf.length);
            lcb_cmdfts_handle(cmd, &vres->base.u.fts);

            PYCBC_TRACECMD_SCOPED_GENERIC(rc,
                                          fts,
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
        ret = NULL;
        goto GT_FINAL;
}
