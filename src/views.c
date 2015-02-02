#include "pycbc.h"
#include "oputil.h"
#include "structmember.h"

static int
should_call_async(const pycbc_ViewResult *vres, int flush_always)
{
    if (!flush_always) {
        return vres->rows_per_call > -1 &&
                PyList_GET_SIZE(vres->rows) > vres->rows_per_call;
    } else {
        return PyList_GET_SIZE(vres->rows);
    }
}

void
pycbc_viewresult_addrow(pycbc_ViewResult *vres, pycbc_MultiResult *mres,
                        const void *data, size_t n)
{
    PyObject *j;
    int rv;

    rv = pycbc_tc_simple_decode(&j, data, n, PYCBC_FMT_JSON);
    if (rv != 0) {
        pycbc_multiresult_adderr(mres);
        pycbc_tc_simple_decode(&j, data, n, PYCBC_FMT_BYTES);
    }

    PyList_Append(vres->rows, j);
    Py_DECREF(j);
}

void
pycbc_viewresult_step(pycbc_ViewResult *vres, pycbc_MultiResult *mres,
                      pycbc_Bucket *bucket, int force_callback)
{
    if ((bucket->flags & PYCBC_CONN_F_ASYNC) &&
            should_call_async(vres, force_callback)) {
        pycbc_AsyncResult *ares = (pycbc_AsyncResult*)mres;
        PyObject *args = PyTuple_Pack(1, mres);
        PyObject *result;

        pycbc_assert(ares->callback);

        result = PyObject_CallObject(ares->callback, args);
        Py_XDECREF(result);
        if (!result) {
            PyErr_Print();
        }

        Py_DECREF(args);

        Py_DECREF(vres->rows);
        vres->rows = PyList_New(0);
    }

    if (!bucket->nremaining) {
        lcb_breakout(bucket->instance);
    }
}


static void
row_callback(lcbex_vrow_ctx_t *rctx, const void *cookie,
             const lcbex_vrow_datum_t *row)
{
    pycbc_MultiResult *mres = (pycbc_MultiResult*)cookie;
    pycbc_Bucket *bucket = mres->parent;
    pycbc_ViewResult *vres = (pycbc_ViewResult *)
            PyDict_GetItem((PyObject*)mres, Py_None);

    if (row->type == LCBEX_VROW_ROW) {
        pycbc_viewresult_addrow(vres, mres, row->data, row->ndata);
    } else if (row->type == LCBEX_VROW_COMPLETE) {
        pycbc_httpresult_add_data(mres, &vres->base, row->data, row->ndata);
    } else if (row->type == LCBEX_VROW_ERROR) {
        if (!vres->has_parse_error) {
            PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR,
                           LCB_PROTOCOL_ERROR, "Couldn't parse row");
            vres->has_parse_error = 1;
        }
        pycbc_multiresult_adderr(mres);
        pycbc_httpresult_add_data(mres, &vres->base, row->data, row->ndata);
    }
    pycbc_viewresult_step(vres, mres, bucket,
                                 row->type != LCBEX_VROW_ROW);

    (void)rctx;
}

static void
views_data_callback(lcb_http_request_t req, lcb_t instance, const void *cookie,
                    lcb_error_t err, const lcb_http_resp_t *resp)
{
    pycbc_MultiResult *mres = (pycbc_MultiResult *)cookie;
    pycbc_Bucket *bucket = mres->parent;
    pycbc_ViewResult *vres;

    PYCBC_CONN_THR_END(bucket);

    vres = (pycbc_ViewResult *)PyDict_GetItem((PyObject*)mres, Py_None);

    if (!vres->has_parse_error) {
        lcbex_vrow_feed(vres->rctx, resp->v.v0.bytes, resp->v.v0.nbytes);
    } else {
        pycbc_httpresult_add_data(mres, &vres->base,
                                  resp->v.v0.bytes, resp->v.v0.nbytes);
    }
    PYCBC_CONN_THR_BEGIN(bucket);
}

void
pycbc_views_callbacks_init(lcb_t instance)
{
    lcb_set_http_data_callback(instance, views_data_callback);
}

typedef struct {
    const char *path;
    pycbc_strlen_t npath;
    const void *body;
    pycbc_strlen_t nbody;
    PyObject *bk;
} viewpath_st;

static int
get_viewpath_str(pycbc_Bucket *self, viewpath_st *vp,
                 PyObject *design, PyObject *view, PyObject *options)
{
    PyObject *args;

    if (!options) {
        options = Py_None;
    }

    args = PyTuple_Pack(3, design, view, options);
    vp->bk = PyObject_CallObject(pycbc_helpers.view_path_helper, args);
    Py_DECREF(args);

    if (!vp->bk) {
        return -1;
    } else {
        int rv = PyArg_ParseTuple(vp->bk, "s#s#", &vp->path, &vp->npath,
                                  &vp->body, &vp->nbody);
        if (!rv) {
            return -1;
        }
    }
    return 0;
}

PyObject *
pycbc_Bucket__view_request(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    int rv;
    PyObject *ret = NULL;
    pycbc_MultiResult *mres = NULL;
    pycbc_ViewResult *vres = NULL;
    lcb_http_cmd_t htcmd = { 0 };
    viewpath_st vp = { NULL };
    lcb_error_t rc;
    PyObject *design = NULL, *view = NULL, *options = NULL;
    int include_docs = 0;

    static char *kwlist[] = { "design", "view", "options",
            "include_docs", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Oi", kwlist,
                                     &design, &view, &options, &include_docs);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }
    if (-1 == pycbc_oputil_conn_lock(self)) {
        return NULL;
    }

    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "HTTP/View Requests cannot be executed in "
                       "pipeline context");
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = (pycbc_ViewResult *)PYCBC_TYPE_CTOR(&pycbc_ViewResultType);
    pycbc_httpresult_init(&vres->base, mres);

    rv = get_viewpath_str(self, &vp, design, view, options);
    if (rv != 0) {
        goto GT_DONE;
    }

    vres->rctx = lcbex_vrow_create();
    vres->rctx->callback = row_callback;
    vres->rctx->user_cookie = mres;
    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;

    htcmd.version = 1;
    htcmd.v.v1.chunked = 1;
    htcmd.v.v1.method = LCB_HTTP_METHOD_GET;
    htcmd.v.v1.path = vp.path;
    htcmd.v.v1.npath = vp.npath;

    if (vp.nbody) {
        htcmd.v.v1.method = LCB_HTTP_METHOD_POST;
        htcmd.v.v1.body = vp.body;
        htcmd.v.v1.nbody = vp.nbody;
        htcmd.v.v1.content_type = "application/json";
    }

    rc = lcb_make_http_request(self->instance, mres, LCB_HTTP_TYPE_VIEW,
                               &htcmd, &vres->base.htreq);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't schedule view");
        goto GT_DONE;
    }

    ret = (PyObject*)mres;
    mres = NULL; /* Avoid GT_DONE decref */

    GT_DONE:
    Py_XDECREF(mres);
    Py_XDECREF(vp.bk);
    pycbc_oputil_conn_unlock(self);
    return ret;
}

static PyObject *
ViewResult_fetch(pycbc_ViewResult *self, PyObject *args)
{
    PyObject *ret = NULL;
    pycbc_MultiResult *mres = NULL;
    pycbc_Bucket *bucket;
    int rv;

    rv = PyArg_ParseTuple(args, "O", &mres);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    bucket = mres->parent;

    if (bucket->flags & PYCBC_CONN_F_ASYNC) {
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "Cannot use fetch with async");
        return NULL;
    }

    if (-1 == pycbc_oputil_conn_lock(bucket)) {
        return NULL;
    }

    if (!self->base.done) {
        pycbc_oputil_wait_common(bucket);
    }

    if (pycbc_multiresult_maybe_raise(mres)) {
        goto GT_DONE;
    }

    ret = self->rows ? self->rows : PyList_New(0);
    self->rows = PyList_New(0);

    GT_DONE:
    pycbc_oputil_conn_unlock(bucket);
    return ret;
}

static void
ViewResult_dealloc(pycbc_ViewResult *vres)
{
    Py_CLEAR(vres->rows);
    if (vres->rctx) {
        lcbex_vrow_free(vres->rctx);
    }
    Py_TYPE(vres)->tp_base->tp_dealloc((PyObject*)vres);
}

PyTypeObject pycbc_ViewResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static struct PyMemberDef ViewResult_TABLE_members[] = {
        { "rows",
                T_OBJECT_EX, offsetof(pycbc_ViewResult, rows), READONLY,
                PyDoc_STR("Most recently fetched rows")
        },
        { "rows_per_call",
                T_LONG, offsetof(pycbc_ViewResult, rows_per_call), 0,
                PyDoc_STR("Rate limit callbacks to this many rows at a time")
        },
        { NULL }
};

static struct PyMethodDef ViewResult_TABLE_methods[] = {
        { "fetch", (PyCFunction)ViewResult_fetch, METH_VARARGS,
                PyDoc_STR("Call this to fetch items from the view")
        },
        { NULL }
};

int
pycbc_ViewResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ViewResultType;
    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "ViewResult";
    p->tp_doc = PyDoc_STR("Low level view result object");
    p->tp_new = PyType_GenericNew;
    p->tp_dealloc = (destructor)ViewResult_dealloc;
    p->tp_basicsize = sizeof(pycbc_ViewResult);
    p->tp_members = ViewResult_TABLE_members;
    p->tp_methods = ViewResult_TABLE_methods;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_base = &pycbc_HttpResultType;
    return pycbc_ResultType_ready(p, PYCBC_HTRESULT_BASEFLDS);
}
