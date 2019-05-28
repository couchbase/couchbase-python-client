#include "oputil.h"
#include "pycbc.h"
#include "pycbc_http.h"
#include "structmember.h"

#ifdef LCB_N1XSPEC_F_PRIMARY
/* lcb callback for index management operations */
static void
mgmt_callback(lcb_t instance, int ign, const lcb_RESPN1XMGMT *resp)
{
    pycbc_MultiResult *mres = (pycbc_MultiResult *)resp->cookie;
    pycbc_Bucket *bucket = mres->parent;
    pycbc_ViewResult *vres;
    size_t ii;
    const char * const * hdrs = NULL;
    short htcode = 0;

    PYCBC_CONN_THR_END(bucket);
    vres = (pycbc_ViewResult *)PyDict_GetItem((PyObject*)mres, Py_None);
    for (ii = 0; ii < resp->nspecs; ++ii) {
        const lcb_N1XSPEC *spec = resp->specs[ii];
        pycbc_viewresult_addrow(vres, mres, spec->rawjson, spec->nrawjson);
    }

    pycbc_viewresult_step(vres, mres, bucket, 1);

    if (resp->inner) {
        const char *row = NULL;
        size_t row_count;
        lcb_respn1ql_row(resp->inner, &row, &row_count);
        pycbc_httpresult_add_data(mres, &vres->base, row, row_count);
        {
            const lcb_RESPHTTP *inner_http = NULL;
            lcb_respn1ql_http_response(resp->inner, &inner_http);
            if (inner_http) {
                lcb_resphttp_headers(inner_http, &hdrs);
                htcode = lcb_resphttp_status(inner_http);
            }
        }
    }
    pycbc_httpresult_complete(&vres->base, mres, resp->rc, htcode, NULL);
}

/* Handles simple single-index commands.
 * We simply need to pass in the raw JSON payload (the second argument) as the
 * rawjson/nrawjson fields for LCB
 */
PyObject *
pycbc_Bucket__ixmanage(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    int rv;
    PyObject *ret = NULL;
    pycbc_MultiResult *mres;
    pycbc_ViewResult *vres;
    lcb_STATUS rc=LCB_SUCCESS;
    unsigned cmdflags = 0;
    lcb_CMDN1XMGMT cmd = { { 0 } };
    const char *params;
    const char *action;
    pycbc_strlen_t nparams;
    lcb_STATUS (*action_fn)(lcb_t, const void *, const lcb_CMDN1XMGMT*);

    static char *kwlist[] = { "action", "index", "flags",  NULL };
    rv = PyArg_ParseTupleAndKeywords(args, kwargs,
        "ss#|I", kwlist, &action, &params, &nparams, &cmdflags);

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }
    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }
    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
            "index management operations executed in pipeline context");
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = (pycbc_ViewResult *)PYCBC_TYPE_CTOR(&pycbc_ViewResultType);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = PYCBC_HTTP_HNONE;

    cmd.callback = mgmt_callback;
    cmd.spec.flags = cmdflags;
    cmd.spec.rawjson = params;
    cmd.spec.nrawjson = nparams;
    if (!strcmp(action, "create")) {
        action_fn = lcb_n1x_create;
    } else if (!strcmp(action, "drop")) {
        action_fn = lcb_n1x_drop;
    } else if (!strcmp(action, "list")) {
        action_fn = lcb_n1x_list;
    } else if (!strcmp(action, "build")) {
        action_fn = lcb_n1x_startbuild;
    } else {
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "Bad action name!");
        goto GT_DONE;
    }

    rc = action_fn(self->instance, mres, &cmd);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(
            PYCBC_EXC_LCBERR, rc, "Couldn't schedule ixmgmt operation");
        goto GT_DONE;
    }

    ret = (PyObject *)mres;
    mres = NULL;

    GT_DONE:
    Py_XDECREF(mres);
    pycbc_oputil_conn_unlock(self);
    return ret;
}

/*
 * Handles 'watch'. This accepts multiple index definitons and is thus its
 * own function.
 */
PyObject *
pycbc_Bucket__ixwatch(pycbc_Bucket *self, PyObject *args, PyObject *kw)
{
    unsigned timeout = 0, interval = 0;
    PyObject *indexes;
    PyObject *ret = NULL;
    pycbc_pybuffer *bufs = NULL;
    pycbc_MultiResult *mres = NULL;
    pycbc_ViewResult *vres = NULL;
    lcb_CMDN1XWATCH cmd = { 0 };
    lcb_N1XSPEC **specs = NULL;
    int rv;
    size_t ii;
    Py_ssize_t nspecs;
    lcb_STATUS rc=LCB_SUCCESS;

    static char *kwlist[] = { "indexes", "timeout_us", "interval_us", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kw, "OII", kwlist,
        &indexes, &timeout, &interval)) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
            "Index management operations cannot be performed in a pipeline");
    }
    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = (pycbc_ViewResult *)PYCBC_TYPE_CTOR(&pycbc_ViewResultType);
    pycbc_httpresult_init(&vres->base, mres);
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;
    vres->base.htype = PYCBC_HTTP_HNONE;
    cmd.callback = mgmt_callback;
    cmd.interval = interval;
    cmd.timeout = timeout;

    nspecs = PySequence_Length(indexes);
    if (nspecs == -1) {
        goto GT_DONE;
    } else if (nspecs == 0) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "No indexes provided!");
        goto GT_DONE;
    }

    cmd.nspec = nspecs;
    specs = calloc(nspecs, sizeof *cmd.specs);
    cmd.specs = (const lcb_N1XSPEC * const *)specs;
    bufs = calloc(nspecs, sizeof *bufs);

    for (ii = 0; nspecs > 0 && ii < (size_t) nspecs; ++ii) {
        PyObject *index = PySequence_GetItem(indexes, ii);
        PyObject *strobj = NULL;
        if (index == NULL) {
            goto GT_DONE;
        }
        strobj = PyObject_Str(index);
        if (!strobj) {
            goto GT_DONE;
        }

        rv = pycbc_tc_simple_encode(strobj, bufs + ii, PYCBC_FMT_UTF8);
        Py_DECREF(strobj);

        if (rv != 0) {
            goto GT_DONE;
        }
        specs[ii] = calloc(1, sizeof *cmd.specs[ii]);
        specs[ii]->rawjson = bufs[ii].buffer;
        specs[ii]->nrawjson = bufs[ii].length;
    }

    rc = lcb_n1x_watchbuild(self->instance, mres, &cmd);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't schedule index watch");
        goto GT_DONE;
    }

    ret = (PyObject *)mres;
    mres = NULL;

    GT_DONE:
    Py_XDECREF(mres);
    pycbc_oputil_conn_unlock(self);
    for (ii = 0; ii < cmd.nspec; ++ii) {
        free(specs[ii]);
        PYCBC_PYBUF_RELEASE(bufs + ii);
    }
    free(specs);
    return ret;
}
#else
#warning "Index management operations not supported in this version of libcouchbase"
PyObject *
pycbc_Bucket__ixmanage(pycbc_Bucket *s, PyObject *a, PyObject *k)
{
    PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0,
        "Index management requires at least version 2.6.0 of libcouchbase. "
        "Please compile with a newer libcouchbase version");
    return NULL;
}
PyObject *
pycbc_Bucket__ixwatch(pycbc_Bucket *s, PyObject *a, PyObject *k)
{
    return pycbc_Bucket__ixmanage(s, a, k);
}
#endif
