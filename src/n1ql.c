#include "pycbc.h"
#include "oputil.h"
#include "structmember.h"
#include <libcouchbase/n1ql.h>

typedef struct {
    PyObject_HEAD
    lcb_N1QLPARAMS *params;
} N1QLParams;

#define CHECK_RC(errmsg) \
    if (rc == LCB_SUCCESS) { \
        Py_RETURN_NONE; \
    } else { \
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, errmsg); \
        return NULL; \
    }

static PyObject *
N1qlParams_set_query(N1QLParams *self, PyObject *args, PyObject *kw)
{
    int rv, qtype = LCB_N1P_QUERY_STATEMENT;
    lcb_error_t rc;
    const char *qstr;
    static char *kwlist[] = { "query", "type", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kw, "s|i", kwlist, &qstr, &qtype);
    if (!rv) {
        return NULL;
    }
    rc = lcb_n1p_setquery(self->params, qstr, -1, qtype);
    CHECK_RC("Couldn't set query");
}

static PyObject *
N1qlParams_set_option(N1QLParams *self, PyObject *args)
{
    int rv;
    const char *key, *value;
    lcb_error_t rc;

    rv = PyArg_ParseTuple(args, "ss", &key, &value);
    if (!rv) {
        return NULL;
    }
    rc = lcb_n1p_setoptz(self->params, key, value);
    CHECK_RC("Couldn't set option");
}

static PyObject *
N1qlParams_set_namedarg(N1QLParams *self, PyObject *args)
{
    int rv;
    const char *key, *value;
    lcb_error_t rc;
    rv = PyArg_ParseTuple(args, "ss", &key, &value);
    if (!rv) {
        return NULL;
    }
    rc = lcb_n1p_namedparam(self->params, key, -1, value, -1);
    CHECK_RC("Couldn't set named parameter");
}

static PyObject *
N1qlParams_add_posarg(N1QLParams *self, PyObject *args)
{
    int rv;
    const char *value;
    lcb_error_t rc;
    rv = PyArg_ParseTuple(args, "s", &value);
    if (!rv) {
        return NULL;
    }
    rc = lcb_n1p_posparam(self->params, value, -1);
    CHECK_RC("Couldn't set positional parameter");
}

static PyObject *
N1qlParams_clear(N1QLParams *self)
{
    lcb_n1p_reset(self->params);
    Py_RETURN_NONE;
}

static PyMethodDef N1qlParams_TABLE_methods[] = {
        { "setquery", (PyCFunction)N1qlParams_set_query, METH_KEYWORDS },
        { "setopt", (PyCFunction)N1qlParams_set_option, METH_VARARGS },
        { "set_named_param", (PyCFunction)N1qlParams_set_namedarg, METH_VARARGS },
        { "add_pos_param", (PyCFunction)N1qlParams_add_posarg, METH_VARARGS },
        { "clear", (PyCFunction)N1qlParams_clear, METH_NOARGS },
        { NULL }
};

static int
N1qlParams__init__(N1QLParams *self)
{
    self->params = lcb_n1p_new();
    if (!self->params) {
        PyErr_SetNone(PyExc_MemoryError);
        return -1;
    }
    return 0;
}

static void
N1qlParams_dtor(N1QLParams *self)
{
    if (self->params) {
        lcb_n1p_free(self->params);
    }
}

static PyTypeObject N1qlParamsType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

int
pycbc_N1QLParamsType_init(PyObject **ptr)
{
    PyTypeObject *p = &N1qlParamsType;
    *ptr = (PyObject *)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "_N1qlParams";
    p->tp_new = PyType_GenericNew;
    p->tp_init = (initproc)N1qlParams__init__;
    p->tp_dealloc = (destructor)N1qlParams_dtor;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_doc = PyDoc_STR("Basic N1QL paramsets object (low level)");
    p->tp_basicsize = sizeof(N1QLParams);
    p->tp_methods = N1qlParams_TABLE_methods;
    return PyType_Ready(p);
}

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
    N1QLParams *params_O;
    const char *host = NULL;

    static char *kwlist[] = { "params", "_host", NULL };
    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O|s", kwlist,
                                     &params_O, &host);

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

    rc = lcb_n1p_mkcmd(params_O->params, &cmd);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't validate parameters");
        goto GT_DONE;
    }

    if (host && *host) {
        cmd.host = host;
    }
    cmd.callback = n1ql_row_callback;
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
