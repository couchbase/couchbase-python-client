#include <libcouchbase/couchbase.h>
#include "oputil.h"
#include "pycbc.h"
#include "pycbc_http.h"
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

#define PYCBC_GETTERS(X)                           \
    X(view, VIEW, document, pycbc_RESPGET)         \
    X(view, VIEW, key, pycbc_strn_base_const)      \
    X(view, VIEW, geometry, pycbc_strn_base_const) \
    X(view, VIEW, row, pycbc_strn_base_const)
PYCBC_GETTERS(PYCBC_DUMMY);

#ifdef PYCBC_GETTER_GEN
PYCBC_GETTERS(PYCBC_RESP_GET)
#else
const lcb_RESPGET* pycbc_view_document(const lcb_RESPVIEW *ctx)
{
    const lcb_RESPGET* temp=NULL;
    lcb_respview_document(ctx, &temp);
    return temp;
};

pycbc_strn_base_const pycbc_view_key(const lcb_RESPVIEW *ctx)
{
    pycbc_strn_base_const temp;
    lcb_respview_key(ctx, &temp.buffer, &temp.length);
    return temp;
};

pycbc_strn_base_const pycbc_view_row(const lcb_RESPVIEW *ctx)
{
    pycbc_strn_base_const temp;
    lcb_respview_row(ctx, &temp.buffer, &temp.length);
    return temp;
};

#endif

static int add_view_field(PyObject *dd, PyObject *k, pycbc_generic_array array)
{
    PyObject *tmp;
    int rv;

    if (!array.n) {
        return 0;
    }

    rv = pycbc_tc_simple_decode(&tmp, array.v, array.n, PYCBC_FMT_JSON);
    if (rv != 0) {
        return rv;
    }

    PyDict_SetItem(dd, k, tmp);
    Py_XDECREF(tmp);
    return 0;
}

static int parse_row_json(pycbc_Bucket *bucket,
                          pycbc_ViewResult *vres,
                          pycbc_MultiResult *mres,
                          const lcb_RESPVIEW *resp)
{
    PyObject *dd = PyDict_New();
    PyObject *docid;
    int is_ok, rv = 0;

    const char *doc_id = NULL;
    size_t doc_id_len;
    lcb_respview_doc_id(resp, &doc_id, &doc_id_len);
    if (doc_id_len) {
        rv = pycbc_tc_decode_key(bucket, doc_id, doc_id_len, &docid);
        if (rv == -1) {
            goto GT_DONE;
        } else {
            PyDict_SetItem(dd, pycbc_helpers.vkey_id, docid);
            Py_XDECREF(docid);
        }
    }

#define ADD_FIELD(helpname, fbase)         \
    add_view_field(dd,                     \
                   pycbc_helpers.helpname, \
                   pycbc_strn_base_const_array(pycbc_view_##fbase(resp)))

#define VIEW_FIELD_OK(VKEY_POSTFIX, RHS) ADD_FIELD(vkey_##VKEY_POSTFIX, RHS)==0
    is_ok = VIEW_FIELDS_REDUCE(VIEW_FIELD_OK,&&);

#undef ADD_FIELD
    if (!is_ok) {
        rv = -1;
        goto GT_DONE;
    } else {
        PyList_Append(vres->rows, dd);
    }
    if (pycbc_view_document(resp)) {
        /* include_docs */
        const lcb_RESPGET *rg = pycbc_view_document(resp);
        pycbc_ValueResult *docres = pycbc_valresult_new(bucket);

        docres->key = docid;
        Py_INCREF(docid);
        docres->rc = lcb_respget_status(rg);

        if (docres->rc == LCB_SUCCESS) {
            rv = lcb_respget_cas(rg, &docres->cas);
            rv = lcb_respget_flags(rg, &docres->flags);
            {
                const char *val = NULL;
                size_t val_len;
                lcb_respget_value(rg, &val, &val_len);
                rv = pycbc_tc_decode_value(
                        bucket, val, val_len, docres->flags, &docres->value);
            }
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
static void row_callback(lcb_t instance, int cbtype, const lcb_RESPVIEW *resp)
{
    pycbc_MultiResult *mres;
    lcb_respview_cookie(resp, (void **)&mres);
    //= (pycbc_MultiResult*)resp->cookie;
    pycbc_Bucket *bucket = mres->parent;
    const char * const * hdrs = NULL;
    short htcode = 0;
    pycbc_ViewResult *vres;

    const lcb_RESPHTTP *htresp;
    if (!lcb_respview_http_response(resp, &htresp) && htresp != NULL) {
        lcb_resphttp_headers(htresp, &hdrs);
        htcode = lcb_resphttp_status(htresp);
    }

    PYCBC_CONN_THR_END(bucket);

    vres = (pycbc_ViewResult *)PyDict_GetItem((PyObject *)mres, Py_None);
    if (lcb_respview_is_final(resp)) {
        pycbc_strn_base_const resp_strn;
        lcb_respview_row(resp, &resp_strn.buffer, &resp_strn.length);
        pycbc_httpresult_add_data_strn(mres, &vres->base, resp_strn);
    } else if (lcb_respview_status(resp) == LCB_SUCCESS) {
        if (parse_row_json(bucket, vres, mres, resp) != 0) {
            pycbc_multiresult_adderr(mres);
        }
    }

    pycbc_viewresult_step(vres, mres, bucket, lcb_respview_is_final(resp));

    if (lcb_respview_is_final(resp)) {
        pycbc_httpresult_complete(
                &vres->base, mres, lcb_respview_status(resp), htcode, hdrs);
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
TRACED_FUNCTION_WRAPPER(_view_request, LCBTRACE_OP_REQUEST_ENCODING, Bucket)
{
    int rv;
    PyObject *ret = NULL;
    pycbc_MultiResult *mres = NULL;
    pycbc_ViewResult *vres = NULL;
    viewpath_st vp = { NULL };
    lcb_STATUS rc=LCB_SUCCESS;
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
    vres = pycbc_propagate_view_result(
            context);
    vres->base.htype = PYCBC_HTTP_HVIEW;

    pycbc_httpresult_init(&vres->base, mres);

    rv = get_viewpath_str(self, &vp, options);
    if (rv != 0) {
        goto GT_DONE;
    }
    CMDSCOPE_NG(VIEW, view)
    {
        lcb_CMDVIEW *vcmd = cmd;
        int is_spatial = flags & LCB_CMDVIEWQUERY_F_SPATIAL;
        rc = is_spatial ? pycbc_cmdview_spatial(vcmd, is_spatial) : LCB_SUCCESS;
        lcb_cmdview_create(&vcmd);
        lcb_cmdview_design_document(vcmd, design, strlen(design));
        lcb_cmdview_view_name(vcmd, view, strlen(view));
        lcb_cmdview_option_string(vcmd, vp.optstr, (size_t)vp.noptstr);
        lcb_cmdview_post_data(vcmd, vp.body, (size_t)vp.nbody);
        lcb_cmdview_handle(vcmd, &vres->base.u.vh);
        lcb_cmdview_callback(vcmd, row_callback);

        lcb_cmdview_include_docs(vcmd, flags & LCB_CMDVIEWQUERY_F_INCLUDE_DOCS);
        lcb_cmdview_no_row_parse(vcmd, flags & LCB_CMDVIEWQUERY_F_NOROWPARSE);
        if (rc) {
            CMDSCOPE_GENERIC_FAIL(, VIEW, view)
        }

        vres->rows = PyList_New(0);
        vres->base.format = PYCBC_FMT_JSON;

        PYCBC_TRACECMD_SCOPED_GENERIC(rc,
                                      view,
                                      query,
                                      self->instance,
                                      vcmd,
                                      *vcmd->handle,
                                      context,
                                      VIEW_SPAN_OPERAND,
                                      GENERIC_NULL_OPERAND,
                                      mres,
                                      vcmd);
    }
GT_ERR:
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

void *pycbc_ViewResult_get_context(const pycbc_ViewResult *self)
{
    return pycbc_Context_capsule_value(self->context_capsule);
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
        pycbc_oputil_wait_common(bucket, pycbc_ViewResult_get_context(self));
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

pycbc_ViewResult *pycbc_propagate_view_result(
        pycbc_stack_context_handle context)
{
    pycbc_ViewResult *vres;
    PyObject *kwargs = pycbc_DummyKeywords;
    if (PYCBC_CHECK_CONTEXT(context)) {
        kwargs = PyDict_New();
        PyDict_SetItemString(kwargs, "context", pycbc_Context_capsule(context));
    }
    vres = (pycbc_ViewResult *)PyObject_CallFunction(
            (PyObject *)&pycbc_ViewResultType, "OO", Py_None, kwargs);
    if (!vres) {
        PYCBC_DEBUG_LOG("null vres");
    }

    PYCBC_EXCEPTION_LOG_NOCLEAR;
    PYCBC_DEBUG_LOG("got vres: %p", vres);
    return vres;
}

static int
ViewResult__init__(PyObject *self_raw,
                   PyObject *args, PyObject *kwargs)
{
    pycbc_ViewResult *self = (pycbc_ViewResult *)self_raw;
    self->context_capsule =
            kwargs ? PyDict_GetItemString(kwargs, "context") : NULL;
    if (self->context_capsule) {
        PYCBC_DEBUG_LOG("Got parent context %p\n",
                        pycbc_ViewResult_get_context(self));
    }
    PYCBC_EXCEPTION_LOG_NOCLEAR;
    return 0;
}

static void
ViewResult_dealloc(pycbc_ViewResult *vres)
{
    Py_CLEAR(vres->rows);
    Py_XDECREF(vres->context_capsule);
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
    p->tp_init = ViewResult__init__;
    p->tp_dealloc = (destructor)ViewResult_dealloc;
    p->tp_basicsize = sizeof(pycbc_ViewResult);
    p->tp_members = ViewResult_TABLE_members;
    p->tp_methods = ViewResult_TABLE_methods;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_base = &pycbc_HttpResultType;
    return pycbc_ResultType_ready(p, PYCBC_HTRESULT_BASEFLDS);
}
