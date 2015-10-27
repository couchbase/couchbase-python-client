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

static int
add_view_field(PyObject *dd, PyObject *k, const void *v, size_t n)
{
    PyObject *tmp;
    int rv;

    if (!n) {
        return 0;
    }

    rv = pycbc_tc_simple_decode(&tmp, v, n, PYCBC_FMT_JSON);
    if (rv != 0) {
        return rv;
    }

    PyDict_SetItem(dd, k, tmp);
    Py_XDECREF(tmp);
    return 0;
}

static int
parse_row_json(pycbc_Bucket *bucket, pycbc_ViewResult *vres,
               pycbc_MultiResult *mres, const lcb_RESPVIEWQUERY *resp)
{
    PyObject *dd = PyDict_New();
    PyObject *docid;
    int is_ok, rv = 0;

    if (resp->ndocid) {
        rv = pycbc_tc_decode_key(bucket, resp->docid, resp->ndocid, &docid);
        if (rv == -1) {
            goto GT_DONE;
        } else {
            PyDict_SetItem(dd, pycbc_helpers.vkey_id, docid);
            Py_XDECREF(docid);
        }
    }

    #define ADD_FIELD(helpname, fbase) \
    add_view_field(dd, pycbc_helpers.helpname, resp->fbase, resp->n##fbase)

    is_ok = ADD_FIELD(vkey_key, key) == 0 &&
            ADD_FIELD(vkey_value, value) == 0 &&
            ADD_FIELD(vkey_geo, geometry) == 0;

    #undef ADD_FIELD

    if (!is_ok) {
        rv = -1;
        goto GT_DONE;
    } else {
        PyList_Append(vres->rows, dd);
    }

    if (resp->docresp) {
        /* include_docs */
        const lcb_RESPGET *rg = resp->docresp;
        pycbc_ValueResult *docres = pycbc_valresult_new(bucket);

        docres->key = docid;
        Py_INCREF(docid);
        docres->rc = rg->rc;

        if (rg->rc == LCB_SUCCESS) {
            docres->cas = rg->cas;
            docres->flags = rg->itmflags;
            rv = pycbc_tc_decode_value(
                    bucket, rg->value, rg->nvalue, rg->itmflags, &docres->value);
            if (rv != 0) {
                pycbc_multiresult_adderr(mres);
            }
        }

        PyDict_SetItem(dd, pycbc_helpers.vkey_docresp, (PyObject*)docres);
        Py_DECREF(docres);
    }

    GT_DONE:
    Py_DECREF(dd);
    return rv;
}

static void
row_callback(lcb_t instance, int cbtype, const lcb_RESPVIEWQUERY *resp)
{
    pycbc_MultiResult *mres = (pycbc_MultiResult*)resp->cookie;
    pycbc_Bucket *bucket = mres->parent;
    const char * const * hdrs = NULL;
    short htcode = 0;
    pycbc_ViewResult *vres;

    if (resp->htresp != NULL) {
        hdrs = resp->htresp->headers;
        htcode = resp->htresp->htstatus;
    }

    PYCBC_CONN_THR_END(bucket);

    vres = (pycbc_ViewResult*)PyDict_GetItem((PyObject*)mres, Py_None);

    if (resp->rflags & LCB_RESP_F_FINAL) {
        pycbc_httpresult_add_data(mres, &vres->base, resp->value, resp->nvalue);
    } else if (resp->rc == LCB_SUCCESS) {
        if (parse_row_json(bucket, vres, mres, resp) != 0) {
            pycbc_multiresult_adderr(mres);
        }
    }

    pycbc_viewresult_step(vres, mres, bucket, resp->rflags & LCB_RESP_F_FINAL);

    if (resp->rflags & LCB_RESP_F_FINAL) {
        pycbc_httpresult_complete(&vres->base, mres, resp->rc, htcode, hdrs);
    } else {
        PYCBC_CONN_THR_BEGIN(bucket);
    }
    (void)instance; (void)cbtype;
}

void
pycbc_views_callbacks_init(lcb_t instance)
{
    (void)instance;
}

typedef struct {
    const char *optstr;
    pycbc_strlen_t noptstr;
    const void *body;
    pycbc_strlen_t nbody;
    PyObject *bk;
} viewpath_st;

static int
get_viewpath_str(pycbc_Bucket *self, viewpath_st *vp, PyObject *options)
{
    PyObject *args;

    if (!options) {
        options = Py_None;
    }

    args = PyTuple_Pack(1, options);
    vp->bk = PyObject_CallObject(pycbc_helpers.view_path_helper, args);
    Py_DECREF(args);

    if (!vp->bk) {
        return -1;
    } else {
        int rv = PyArg_ParseTuple(
                vp->bk, "s#s#", &vp->optstr, &vp->noptstr,
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
    lcb_CMDVIEWQUERY vcmd = { 0 };
    viewpath_st vp = { NULL };
    lcb_error_t rc;
    const char *view = NULL, *design = NULL;
    PyObject *options = NULL;
    int flags;

    static char *kwlist[] = { "design", "view", "options", "_flags", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "ss|Oi", kwlist,
                                     &design, &view, &options, &flags);
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
        goto GT_DONE;
    }

    mres = (pycbc_MultiResult *)pycbc_multiresult_new(self);
    vres = (pycbc_ViewResult *)PYCBC_TYPE_CTOR(&pycbc_ViewResultType);
    vres->base.htype = PYCBC_HTTP_HVIEW;

    pycbc_httpresult_init(&vres->base, mres);

    rv = get_viewpath_str(self, &vp, options);
    if (rv != 0) {
        goto GT_DONE;
    }

    vcmd.ddoc = design;
    vcmd.nddoc = strlen(design);
    vcmd.view = view;
    vcmd.nview = strlen(view);
    vcmd.optstr = vp.optstr;
    vcmd.noptstr = vp.noptstr;
    vcmd.postdata = vp.body;
    vcmd.npostdata = vp.nbody;
    vcmd.handle = &vres->base.u.vh;
    vcmd.callback = row_callback;
    vcmd.cmdflags = flags;

    vres->rows = PyList_New(0);
    vres->base.format = PYCBC_FMT_JSON;

    rc = lcb_view_query(self->instance, mres, &vcmd);

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
