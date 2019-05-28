/**
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/
#include "oputil.h"
#include "structmember.h"

static struct PyMemberDef ObserveInfo_TABLE_members[] = {
        { "flags",
                T_UINT, offsetof(pycbc_ObserveInfo, flags),
                READONLY,
                PyDoc_STR("Server-side flags received from observe")
        },
        { "from_master",
                T_INT, offsetof(pycbc_ObserveInfo, from_master),
                READONLY, PyDoc_STR(
                        "Whether this response is from the "
                        "master node. This evaluates to False if this "
                        "status is from a replica")
        },
        { "cas",
                T_ULONGLONG, offsetof(pycbc_ObserveInfo, cas),
                READONLY,
                PyDoc_STR(
                        "CAS as it exists on the given node. "
                        "It is possible (though not likely) that different "
                        "nodes will have a different CAS value for a given "
                        "key. In this case, the actual CAS being used should "
                        "be the one from the *master* (use :attr:`from_master`)")
        },
        { NULL }
};

static PyTypeObject pycbc_ResultInfoType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static void
ObserveInfo_dealloc(pycbc_ObserveInfo *self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
ObserveInfo_repr(PyObject *self)
{
    return PyObject_CallFunction(pycbc_helpers.obsinfo_reprfunc, "O", self);
}

int
pycbc_ObserveInfoType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ResultInfoType;
    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }
    p->tp_name = "ObserveInfo";
    p->tp_doc = PyDoc_STR("Object containing information about "
            "a key's OBSERVED state");
    p->tp_new = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_ObserveInfo);
    p->tp_members = ObserveInfo_TABLE_members;
    p->tp_flags = Py_TPFLAGS_DEFAULT;
    p->tp_dealloc = (destructor)ObserveInfo_dealloc;
    p->tp_repr = ObserveInfo_repr;
    return PyType_Ready(p);
}

pycbc_ObserveInfo *
pycbc_observeinfo_new(pycbc_Bucket *parent)
{
    (void)parent;
    return (pycbc_ObserveInfo*)PyObject_CallFunction((PyObject*)&pycbc_ResultInfoType,
                                                     NULL, NULL);
}

static int
handle_single_observe(pycbc_Bucket *self, PyObject *curkey, int master_only,
    struct pycbc_common_vars *cv, pycbc_stack_context_handle context)
{
    int rv;
    pycbc_pybuffer keybuf = { NULL };
    lcb_CMDOBSERVE cmd = { 0 };
    lcb_STATUS err=LCB_SUCCESS;

    rv = pycbc_tc_encode_key(self, curkey, &keybuf);
    if (rv < 0) {
        return -1;
    }
    LCB_CMD_SET_KEY(&cmd, keybuf.buffer, keybuf.length);

    if (master_only) {
        cmd.cmdflags |= LCB_CMDOBSERVE_F_MASTER_ONLY;
    }
    PYCBC_TRACECMD_TYPED(observe, &cmd, context, cv->mres, curkey, self);
    err = cv->mctx->addcmd(cv->mctx, (lcb_CMDBASE*)&cmd);
    if (err == LCB_SUCCESS) {
        rv = 0;
    } else {
        PYCBC_EXCTHROW_SCHED(err);
        rv = -1;
    }

    PYCBC_PYBUF_RELEASE(&keybuf);
    return rv;
}

static PyObject *
observe_common(pycbc_Bucket *self, PyObject *args, PyObject *kwargs, int argopts, pycbc_stack_context_handle context)
{
    int rv;
    int ii;
    Py_ssize_t ncmds;
    PyObject *kobj = NULL;
    pycbc_seqtype_t seqtype;
    int master_only = 0;
    PyObject *master_only_O = NULL;

    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = { "keys", "master_only", NULL };
    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist,
        &kobj, &master_only_O);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(kobj, 1, &ncmds, &seqtype);
        if (rv < 0) {
            return NULL;
        }
    } else {
        ncmds = 1;
    }

    master_only = master_only_O && PyObject_IsTrue(master_only_O);

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, 0);
    if (rv < 0) {
        return NULL;
    }

    cv.mctx = lcb_observe3_ctxnew(self->instance);
    if (cv.mctx == NULL) {
        PYCBC_EXCTHROW_SCHED(LCB_CLIENT_ENOMEM);
        goto GT_DONE;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        Py_ssize_t dictpos;
        PyObject *curseq, *iter = NULL;
        curseq = pycbc_oputil_iter_prepare(seqtype, kobj, &iter, &dictpos);

        if (!curseq) {
            goto GT_DONE;
        }

        for (ii = 0; ii < ncmds; ii++) {
            PyObject *curkey = NULL, *curvalue = NULL;

            rv = pycbc_oputil_sequence_next(seqtype, curseq, &dictpos, ii,
                &curkey, &curvalue, context);
            if (rv < 0) {
                goto GT_ITER_DONE;
            }

            rv = handle_single_observe(self, curkey, master_only, &cv, context);

            GT_ITER_DONE:
            Py_XDECREF(curkey);
            Py_XDECREF(curvalue);

            if (rv < 0) {
                goto GT_DONE;
            }
        }

    } else {
        rv = handle_single_observe(self, kobj, master_only, &cv, context);

        if (rv < 0) {
            goto GT_DONE;
        }
    }

    cv.is_seqcmd = 1;
    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }

    GT_DONE:

    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

PyObject *
pycbc_Bucket_observe(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    PyObject* result;
    PYCBC_TRACE_WRAP_TOPLEVEL(result,LCBTRACE_OP_REQUEST_ENCODING, observe_common, self->tracer, self, args, kwargs, PYCBC_ARGOPT_SINGLE);
    return result;
}

PyObject *
pycbc_Bucket_observe_multi(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    PyObject* result;
    PYCBC_TRACE_WRAP_TOPLEVEL(result,LCBTRACE_OP_REQUEST_ENCODING, observe_common, self->tracer, self, args, kwargs, PYCBC_ARGOPT_MULTI);
    return result;
}
