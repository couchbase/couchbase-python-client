#include "pycbc.h"
#include "oputil.h"
#include "structmember.h"
#include <libcouchbase/cbft.h>

static void
fts_row_callback(lcb_t instance, int ign, const lcb_RESPFTS *resp)
{
    pycbc_MultiResult *mres = (pycbc_MultiResult *)resp->cookie;
    pycbc_Bucket *bucket = mres->parent;
    pycbc_ViewResult *vres;
    const char * const * hdrs = NULL;
    short htcode = 0;

    PYCBC_CONN_THR_END(bucket);
    vres = (pycbc_ViewResult *)PyDict_GetItem((PyObject*)mres, Py_None);

    if (resp->htresp) {
        hdrs = resp->htresp->headers;
        htcode = resp->htresp->htstatus;
    }

    if (resp->rflags & LCB_RESP_F_FINAL) {
        pycbc_httpresult_add_data(mres, &vres->base, resp->row, resp->nrow);
    } else {
        /* Like views, try to decode the row and invoke the callback; if we can */
        /* Assume success! */
        pycbc_viewresult_addrow(vres, mres, resp->row, resp->nrow);
    }

    pycbc_viewresult_step(vres, mres, bucket, resp->rflags & LCB_RESP_F_FINAL);

    if (resp->rflags & LCB_RESP_F_FINAL) {
        pycbc_httpresult_complete(&vres->base, mres, resp->rc, htcode, hdrs);
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
    lcb_error_t rc;
    lcb_CMDFTS cmd = { 0 };
    pycbc_pybuffer buf = { 0 };
    PyObject *params_o = NULL;

    static char *kwlist[] = { "params", NULL };
    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &params_o);

    if (!rv) {
        return NULL;
    }

    if (pycbc_tc_simple_encode(params_o, &buf, PYCBC_FMT_UTF8) != 0) {
        return NULL;
    }

    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }
    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "FTS queries cannot be executed in pipeline context");
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = (pycbc_ViewResult *)PYCBC_TYPE_CTOR(&pycbc_ViewResultType);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = PYCBC_HTTP_HFTS;

    cmd.callback = fts_row_callback;
    cmd.query = buf.buffer;
    cmd.nquery = buf.length;
    cmd.handle = &vres->base.u.fts;
    rc = lcb_fts_query(self->instance, mres, &cmd);
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
    return ret;
}
