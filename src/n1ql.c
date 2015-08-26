#include "pycbc.h"
#include "oputil.h"
#include "structmember.h"
#include <libcouchbase/n1ql.h>

static void
n1ql_row_callback(lcb_t instance, int ign, const lcb_RESPN1QL *resp)
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
pycbc_Bucket__n1ql_query(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    int rv;
    PyObject *ret = NULL;
    pycbc_MultiResult *mres;
    pycbc_ViewResult *vres;
    lcb_error_t rc;
    lcb_CMDN1QL cmd = { 0 };
    const char *params;
    pycbc_strlen_t nparams;
    int prepared = 0;

    static char *kwlist[] = { "params", "prepare", NULL };
    rv = PyArg_ParseTupleAndKeywords(
        args, kwargs, "s#|i", kwlist, &params, &nparams, &prepared);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }
    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }
    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "N1QL queries cannot be executed in "
                       "pipeline context");
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = (pycbc_ViewResult *)PYCBC_TYPE_CTOR(&pycbc_ViewResultType);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = PYCBC_HTTP_HN1QL;

    cmd.content_type = "application/json";
    cmd.callback = n1ql_row_callback;
    cmd.query = params;
    cmd.nquery = nparams;
    cmd.handle = &vres->base.u.nq;
    if (prepared) {
        cmd.cmdflags |= LCB_CMDN1QL_F_PREPCACHE;
    }
    rc = lcb_n1ql_query(self->instance, mres, &cmd);

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
