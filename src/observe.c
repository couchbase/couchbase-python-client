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
    p->tp_str = ObserveInfo_repr;
    return PyType_Ready(p);
}

pycbc_ObserveInfo *
pycbc_observeinfo_new(pycbc_Connection *parent)
{
    (void)parent;
    return (pycbc_ObserveInfo*)PyObject_CallFunction((PyObject*)&pycbc_ResultInfoType,
                                                     NULL, NULL);
}

static int
handle_single_observe(pycbc_Connection *self,
                      PyObject *curkey,
                      int ii,
                      struct pycbc_common_vars *cv)
{
    int rv;
    char *key;
    size_t nkey;
    lcb_observe_cmd_t *ocmd = cv->cmds.obs + ii;

    rv = pycbc_tc_encode_key(self, &curkey, (void**)&key, &nkey);
    if (rv < 0) {
        return -1;
    }

    cv->enckeys[ii] = curkey;
    ocmd->v.v0.key = key;
    ocmd->v.v0.nkey = nkey;

    cv->cmdlist.obs[ii] = ocmd;
    return 0;
}

static PyObject *
observe_common(pycbc_Connection *self,
               PyObject *args,
               PyObject *kwargs,
               int argopts)
{
    int rv;
    int ii;
    Py_ssize_t ncmds;
    PyObject *kobj = NULL;
    pycbc_seqtype_t seqtype;
    lcb_error_t err;

    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = { "keys", NULL };
    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O",
                                     kwlist,
                                     &kobj);
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

    rv = pycbc_common_vars_init(&cv,
                                self,
                                argopts,
                                ncmds, sizeof(lcb_observe_cmd_t),
                                0);
    if (rv < 0) {
        return NULL;
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

            rv = pycbc_oputil_sequence_next(seqtype,
                                            curseq,
                                            &dictpos,
                                            ii,
                                            &curkey,
                                            &curvalue);
            if (rv < 0) {
                goto GT_ITER_DONE;
            }

            rv = handle_single_observe(self, curkey, ii, &cv);

            GT_ITER_DONE:
            Py_XDECREF(curkey);
            Py_XDECREF(curvalue);

            if (rv < 0) {
                goto GT_DONE;
            }
        }

    } else {
        rv = handle_single_observe(self, kobj, 0, &cv);

        if (rv < 0) {
            goto GT_DONE;
        }
    }

    err = lcb_observe(self->instance, cv.mres, ncmds, cv.cmdlist.obs);
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

    GT_DONE:

    pycbc_common_vars_finalize(&cv);
    return cv.ret;
}

PyObject *
pycbc_Connection_observe(pycbc_Connection *self, PyObject *args, PyObject *kw)
{
    return observe_common(self, args, kw, PYCBC_ARGOPT_SINGLE);
}

PyObject *
pycbc_Connection_observe_multi(pycbc_Connection *self, PyObject *args, PyObject *kw)
{
    return observe_common(self, args, kw, PYCBC_ARGOPT_MULTI);
}
