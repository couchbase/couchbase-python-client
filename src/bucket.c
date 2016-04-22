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

#include "pycbc.h"
#include "structmember.h"
#include "oputil.h"
#include "iops.h"
#include <libcouchbase/vbucket.h>

PyObject *pycbc_DummyTuple;
PyObject *pycbc_DummyKeywords;

static int
Bucket__init__(pycbc_Bucket *self,
                   PyObject *args,
                   PyObject *kwargs);

static PyObject*
Bucket__connect(pycbc_Bucket *self);

static void
Bucket_dtor(pycbc_Bucket *self);

static PyTypeObject BucketType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyObject *
Bucket_get_format(pycbc_Bucket *self, void * unused)
{
    if (self->dfl_fmt) {
        Py_INCREF(self->dfl_fmt);
        return self->dfl_fmt;
    }

    (void)unused;
    Py_RETURN_NONE;
}

static int
Bucket_set_format(pycbc_Bucket *self, PyObject *value, void *unused)
{
    if (value != pycbc_helpers.fmt_auto) {
        if (!PyNumber_Check(value)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                           "Format must be a number");
            return -1;
        }

        if (Py_TYPE(value) == &PyBool_Type) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                           "Format must not be a boolean");
            return -1;
        }
    }

    Py_XDECREF(self->dfl_fmt);
    Py_INCREF(value);
    self->dfl_fmt = value;

    (void)unused;
    return 0;
}

static int
Bucket_set_transcoder(pycbc_Bucket *self, PyObject *value, void *unused)
{
    Py_XDECREF(self->tc);
    if (PyObject_IsTrue(value)) {
        self->tc = value;
        Py_INCREF(self->tc);
    } else {
        self->tc = NULL;
    }
    (void)unused;
    return 0;
}

static PyObject*
Bucket_get_transcoder(pycbc_Bucket *self, void *unused)
{
    if (self->tc) {
        Py_INCREF(self->tc);
        return self->tc;
    }
    Py_INCREF(Py_None);

    (void)unused;
    return Py_None;
}

static PyObject *
Bucket_server_nodes(pycbc_Bucket *self, void *unused)
{
    const char * const *cnodes;
    const char **curnode;
    PyObject *ret_list;
    cnodes = lcb_get_server_list(self->instance);

    if (!cnodes) {
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "Can't get server nodes");
        return NULL;
    }

    ret_list = PyList_New(0);
    if (!ret_list) {
        return NULL;
    }

    for (curnode = (const char**)cnodes; *curnode; curnode++) {
        PyObject *tmpstr = pycbc_SimpleStringZ(*curnode);
        PyList_Append(ret_list, tmpstr);
        Py_DECREF(tmpstr);
    }

    (void)unused;

    return ret_list;
}

static PyObject *
Bucket_get_configured_replica_count(pycbc_Bucket *self, void *unused)
{
    PyObject *iret = pycbc_IntFromUL(lcb_get_num_replicas(self->instance));

    (void)unused;
    return iret;
}

static PyObject *
Bucket_connected(pycbc_Bucket *self, void *unused)
{
    PyObject* ret = self->flags & PYCBC_CONN_F_CONNECTED ? Py_True : Py_False;

    if (ret == Py_False) {
        void *handle = NULL;
        lcb_error_t err;
        err = lcb_cntl(self->instance, LCB_CNTL_GET, LCB_CNTL_VBCONFIG, &handle);
        if (err == LCB_SUCCESS && handle != NULL) {
            self->flags |= PYCBC_CONN_F_CONNECTED;
            ret = Py_True;
        }
    }

    Py_INCREF(ret);

    (void)unused;

    return ret;
}

static PyObject *
Bucket__instance_pointer(pycbc_Bucket *self, void *unused)
{
    PyObject *ret;
    Py_uintptr_t ptri = (Py_uintptr_t)self->instance;
    ret = pycbc_IntFromULL(ptri);
    (void)unused;
    return ret;
}

static PyObject *
Bucket__add_creds(pycbc_Bucket *self, PyObject *args)
{
    char *arr[2] = { NULL };
    lcb_error_t rc;
    if (!PyArg_ParseTuple(args, "ss", &arr[0], &arr[1])) {
        return NULL;
    }
    rc = lcb_cntl(self->instance, LCB_CNTL_SET, LCB_CNTL_BUCKET_CRED, arr);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't add credentials");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
Bucket__thr_lockop(pycbc_Bucket *self, PyObject *arg)
{
    int rv;
    int is_unlock = 0;
    rv = PyArg_ParseTuple(arg, "i:is_unlock", &is_unlock);

    if (!rv) {
        return NULL;
    }

    if (!self->lockmode) {
        PYCBC_EXC_WRAP(PYCBC_EXC_THREADING, 0, "lockmode is LOCKMODE_NONE");
        return NULL;
    }

    if (is_unlock) {
        PyThread_release_lock(self->lock);
    } else {
        if (!PyThread_acquire_lock(self->lock, WAIT_LOCK)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_THREADING, 0, "Couldn't lock");
            return NULL;
        }
    }

    Py_RETURN_NONE;
}

static PyObject *
Bucket__close(pycbc_Bucket *self)
{
    lcb_error_t err;

    if (self->flags & PYCBC_CONN_F_CLOSED) {
        Py_RETURN_NONE;
    }

    self->flags |= PYCBC_CONN_F_CLOSED;

    lcb_destroy(self->instance);

    if (self->iopswrap) {
        Py_XDECREF(self->iopswrap);
        self->iopswrap = NULL;
    }

    err = lcb_create(&self->instance, NULL);
    pycbc_assert(err == LCB_SUCCESS);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR,
                       err,
                       "Internal error while closing object");
        return NULL;
    }

    Py_RETURN_NONE;
}

static void
timings_callback(lcb_t instance,
                 const void *cookie,
                 lcb_timeunit_t timeunit,
                 lcb_uint32_t min,
                 lcb_uint32_t max,
                 lcb_uint32_t total,
                 lcb_uint32_t maxtotal)
{
    PyObject *arr = (PyObject *)cookie;
    PyObject *dict;
    double divisor = 1.0;
    double d_min, d_max;

    if (timeunit == LCB_TIMEUNIT_NSEC) {
        divisor = 1000000;
    } else if (timeunit == LCB_TIMEUNIT_USEC) {
        divisor = 1000;
    } else if (timeunit == LCB_TIMEUNIT_MSEC) {
        divisor = 1;
    } else if (timeunit == LCB_TIMEUNIT_SEC) {
        divisor = 0.001;
    }

    d_min = (double)min / divisor;
    d_max = (double)max / divisor;

    dict = PyDict_New();
    PyList_Append(arr, dict);
    PyDict_SetItemString(dict, "min", PyFloat_FromDouble(d_min));
    PyDict_SetItemString(dict, "max", PyFloat_FromDouble(d_max));
    PyDict_SetItemString(dict, "count", pycbc_IntFromUL(total));

    (void)maxtotal;
    (void)instance;
}

static PyObject *
Bucket__start_timings(pycbc_Bucket *self)
{
    lcb_disable_timings(self->instance);
    lcb_enable_timings(self->instance);
    Py_RETURN_NONE;
}

static PyObject *
Bucket__clear_timings(pycbc_Bucket *self)
{
    lcb_disable_timings(self->instance);
    Py_RETURN_NONE;
}

static PyObject *
Bucket__get_timings(pycbc_Bucket *self)
{
    PyObject *ll = PyList_New(0);
    lcb_get_timings(self->instance, ll, timings_callback);
    return ll;
}

static PyObject *
Bucket__mutinfo(pycbc_Bucket *self)
{
    PyObject *ll = PyList_New(0);
    size_t ii, vbmax;
    lcbvb_CONFIG *cfg = NULL;
    lcb_error_t rc;

    rc = lcb_cntl(self->instance, LCB_CNTL_GET, LCB_CNTL_VBCONFIG, &cfg);
    if (rc != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, rc, "Couldn't get vBucket config");
        return NULL;
    }

    vbmax = vbucket_config_get_num_vbuckets(cfg);
    for (ii = 0; ii < vbmax; ++ii) {
        lcb_KEYBUF kb = { 0 };
        const lcb_MUTATION_TOKEN *mt;
        lcb_error_t rc = LCB_SUCCESS;
        PyObject *cur;

        kb.type = LCB_KV_VBID;
        kb.contig.nbytes = (size_t)ii;

        mt = lcb_get_mutation_token(self->instance, &kb, &rc);
        if (mt == NULL) {
            continue;
        }
        cur = Py_BuildValue("HKK", LCB_MUTATION_TOKEN_VB(mt),
            LCB_MUTATION_TOKEN_ID(mt), LCB_MUTATION_TOKEN_SEQ(mt));
        PyList_Append(ll, cur);
        Py_DECREF(cur);
    }

    return ll;
}


static PyGetSetDef Bucket_TABLE_getset[] = {
        { "default_format",
                (getter)Bucket_get_format,
                (setter)Bucket_set_format,
                PyDoc_STR("The default format to use for encoding values "
                "(passed to transcoder)")
        },
        { "server_nodes",
                (getter)Bucket_server_nodes,
                NULL,
                PyDoc_STR("Get a list of the current nodes in the cluster")
        },
        { "configured_replica_count",
                (getter)Bucket_get_configured_replica_count,
                NULL,
                PyDoc_STR("Get the number of configured replicas for the bucket")
        },

        { "transcoder",
                (getter)Bucket_get_transcoder,
                (setter)Bucket_set_transcoder,
                PyDoc_STR("The :class:`~couchbase.transcoder.Transcoder` "
                        "object being used.\n\n"
                        ""
                        "This is normally ``None`` unless a custom "
                        ":class:`couchbase.transcoder.Transcoder` "
                        "is being used\n")
        },

        { "connected",
                (getter)Bucket_connected,
                NULL,
                PyDoc_STR("Boolean read only property indicating whether\n"
                        "this instance has been connected.\n"
                        "\n"
                        "Note that this will still return true even if\n"
                        "it is subsequently closed via :meth:`_close`\n")
        },

        { "_instance_pointer",
                (getter)Bucket__instance_pointer,
                NULL,
                PyDoc_STR("Gets the C level pointer for the underlying C "
                         "handle")
        },

        { NULL }
};

static struct PyMemberDef Bucket_TABLE_members[] = {
        { "quiet", T_UINT, offsetof(pycbc_Bucket, quiet),
                0,
                PyDoc_STR("Whether to suppress errors when keys are not found "
                "(in :meth:`get` and :meth:`delete` operations).\n"
                "\n"
                "An error is still returned within the :class:`Result`\n"
                "object")
        },

        { "data_passthrough", T_UINT, offsetof(pycbc_Bucket, data_passthrough),
                0,
                PyDoc_STR("When this flag is set, values are always returned "
                        "as raw bytes\n")
        },

        { "unlock_gil", T_UINT, offsetof(pycbc_Bucket, unlock_gil),
                READONLY,
                PyDoc_STR("Whether GIL manipulation is enabeld for "
                "this connection object.\n"
                "\n"
                "This attribute can only be set from the constructor.\n")
        },

        { "bucket", T_OBJECT_EX, offsetof(pycbc_Bucket, bucket),
                READONLY,
                PyDoc_STR("Name of the bucket this object is connected to")
        },

        { "lockmode", T_INT, offsetof(pycbc_Bucket, lockmode),
                READONLY,
                PyDoc_STR("How access from multiple threads is handled.\n"
                        "See :ref:`multiple_threads` for more information\n")
        },

        { "_privflags", T_UINT, offsetof(pycbc_Bucket, flags),
                0,
                PyDoc_STR("Internal flags.")
        },

        { "_conncb", T_OBJECT_EX, offsetof(pycbc_Bucket, conncb),
                0,
                PyDoc_STR("Internal connection callback.")
        },

        { "_dtorcb", T_OBJECT_EX, offsetof(pycbc_Bucket, dtorcb),
                0,
                PyDoc_STR("Internal destruction callback")
        },

        { "_dur_persist_to", T_BYTE,
                offsetof(pycbc_Bucket, dur_global.persist_to),
                0,
                PyDoc_STR("Internal default persistence settings")
        },

        { "_dur_replicate_to", T_BYTE,
                offsetof(pycbc_Bucket, dur_global.replicate_to),
                0,
                PyDoc_STR("Internal default replication settings")
        },

        { "_dur_timeout", T_ULONG,
                offsetof(pycbc_Bucket, dur_timeout),
                0,
                PyDoc_STR("Internal ")
        },

        { "_dur_testhook", T_OBJECT_EX,
                offsetof(pycbc_Bucket, dur_testhook),
                0,
                PyDoc_STR("Internal hook for durability tests")
        },

        { NULL }
};

static PyMethodDef Bucket_TABLE_methods[] = {

#define OPFUNC(name, doc) \
{ #name, (PyCFunction)pycbc_Bucket_##name, METH_VARARGS|METH_KEYWORDS, \
    PyDoc_STR(doc) }

        /** Basic Operations */
        OPFUNC(upsert, "Unconditionally store a key in Couchbase"),
        OPFUNC(insert, "Add a key in Couchbase if it does not already exist"),
        OPFUNC(replace, "Replace an existing key in Couchbase"),
        OPFUNC(append, "Append to an existing value in Couchbase"),
        OPFUNC(prepend, "Prepend to an existing value in Couchbase"),
        OPFUNC(upsert_multi, NULL),
        OPFUNC(insert_multi, NULL),
        OPFUNC(replace_multi, NULL),
        OPFUNC(append_multi, NULL),
        OPFUNC(prepend_multi, NULL),

        OPFUNC(get, "Get a key from Couchbase"),
        OPFUNC(touch, "Update the expiration time of a key in Couchbase"),
        OPFUNC(lock, "Lock a key in Couchbase"),
        OPFUNC(get_multi, NULL),
        OPFUNC(touch_multi, NULL),
        OPFUNC(lock_multi, NULL),
        OPFUNC(_rget, NULL),
        OPFUNC(_rgetix, NULL),

        OPFUNC(mutate_in, "Perform mutations in document paths"),
        OPFUNC(lookup_in, "Perform lookups in document paths"),

        OPFUNC(remove, "Delete a key in Couchbase"),
        OPFUNC(unlock, "Unlock a previously-locked key in Couchbase"),
        OPFUNC(remove_multi, "Multi-key variant of delete"),
        OPFUNC(unlock_multi, "Multi-key variant of unlock"),

        OPFUNC(counter, "Modify a counter in Couchbase"),
        OPFUNC(counter_multi, "Multi-key variant of counter"),
        OPFUNC(_stats, "Get various server statistics"),

        OPFUNC(_http_request, "Internal routine for HTTP requests"),
        OPFUNC(_view_request, "Internal routine for view requests"),
        OPFUNC(_n1ql_query, "Internal routine for N1QL queries"),
        OPFUNC(_fts_query, "Internal routine for Fulltext queries"),

        OPFUNC(_ixmanage, "Internal routine for managing indexes"),
        OPFUNC(_ixwatch, "Internal routine for monitoring indexes"),

        OPFUNC(observe, "Get replication/persistence status for keys"),
        OPFUNC(observe_multi, "multi-key variant of observe"),

        OPFUNC(endure_multi, "Check durability requirements"),


#undef OPFUNC


        { "_thr_lockop",
                (PyCFunction)Bucket__thr_lockop,
                METH_VARARGS,
                PyDoc_STR("Unconditionally lock/unlock the connection object "
                        "if 'lockmode' has been set. For testing uses only")
        },

        { "_close",
                (PyCFunction)Bucket__close,
                METH_NOARGS,
                PyDoc_STR(
                "Close the instance's underlying socket resources\n"
                "\n"
                "Note that operations pending on the connection may\n"
                "fail.\n"
                "\n")
        },

        { "_connect",
                (PyCFunction)Bucket__connect,
                METH_NOARGS,
                PyDoc_STR(
                "Connect this instance. This is typically called by one of\n"
                "the wrapping constructors\n")
        },

        { "_pipeline_begin",
                (PyCFunction)pycbc_Bucket__start_pipeline,
                METH_NOARGS,
                PyDoc_STR("Enter pipeline mode. Internal use")
        },

        { "_pipeline_end",
                (PyCFunction)pycbc_Bucket__end_pipeline,
                METH_NOARGS,
                PyDoc_STR(
                "End pipeline mode and wait for operations to complete")
        },

        { "_start_timings",
                (PyCFunction)Bucket__start_timings,
                METH_NOARGS,
                PyDoc_STR("Start recording timings")
        },

        { "_get_timings",
                (PyCFunction)Bucket__get_timings,
                METH_NOARGS,
                PyDoc_STR("Get all timings since the last call to start_timings")
        },

        { "_stop_timings",
                (PyCFunction)Bucket__clear_timings,
                METH_NOARGS,
                PyDoc_STR("Clear and disable timings")
        },

        { "_cntl",
                (PyCFunction)pycbc_Bucket__cntl,
                METH_VARARGS|METH_KEYWORDS,
                NULL
        },

        { "_cntlstr", (PyCFunction)pycbc_Bucket__cntlstr,
                METH_VARARGS|METH_KEYWORDS,
                NULL
        },

        { "_vbmap",
                (PyCFunction)pycbc_Bucket__vbmap,
                METH_VARARGS,
                PyDoc_STR("Returns a tuple of (vbucket, server index) for a key")
        },

        { "_mutinfo",
                (PyCFunction)Bucket__mutinfo,
                METH_NOARGS,
                PyDoc_STR("Gets known mutation information")
        },
        { "_add_creds",
                (PyCFunction)Bucket__add_creds,
                METH_VARARGS,
                PyDoc_STR("Add additional user/pasword information")
        },

        { NULL, NULL, 0, NULL }
};

static int
Bucket__init__(pycbc_Bucket *self,
                       PyObject *args, PyObject *kwargs)
{
    int rv;
    int conntype = LCB_TYPE_BUCKET;

    lcb_error_t err;
    PyObject *unlock_gil_O = NULL;
    PyObject *iops_O = NULL;
    PyObject *dfl_fmt = NULL;
    PyObject *tc = NULL;

    struct lcb_create_st create_opts = { 0 };


    /**
     * This xmacro enumerates the constructor keywords, targets, and types.
     * This was converted into an xmacro to ease the process of adding or
     * removing various parameters.
     */
#define XCTOR_ARGS(X) \
    X("connection_string", &create_opts.v.v3.connstr, "z") \
    X("connstr", &create_opts.v.v3.connstr, "z") \
    X("username", &create_opts.v.v3.username, "z") \
    X("password", &create_opts.v.v3.passwd, "z") \
    X("quiet", &self->quiet, "I") \
    X("unlock_gil", &unlock_gil_O, "O") \
    X("transcoder", &tc, "O") \
    X("default_format", &dfl_fmt, "O") \
    X("lockmode", &self->lockmode, "i") \
    X("_flags", &self->flags, "I") \
    X("_conntype", &conntype, "i") \
    X("_iops", &iops_O, "O")

    static char *kwlist[] = {
        #define X(s, target, type) s,
            XCTOR_ARGS(X)
        #undef X
            NULL
    };

    #define X(s, target, type) type
    static char *argspec = "|" XCTOR_ARGS(X);
    #undef X

    if (self->init_called) {
        PyErr_SetString(PyExc_RuntimeError, "__init__ was already called");
        return -1;
    }

    self->init_called = 1;
    self->flags = 0;
    self->unlock_gil = 1;
    self->lockmode = PYCBC_LOCKMODE_EXC;

    #define X(s, target, type) target,
    rv = PyArg_ParseTupleAndKeywords(args, kwargs, argspec, kwlist,
        XCTOR_ARGS(X) NULL);
    #undef X

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return -1;
    }

    if (unlock_gil_O && PyObject_IsTrue(unlock_gil_O) == 0) {
        self->unlock_gil = 0;
    }

    create_opts.version = 3;
    create_opts.v.v3.type = conntype;

    if (iops_O && iops_O != Py_None) {
        self->iopswrap = pycbc_iowrap_new(self, iops_O);
        create_opts.v.v3.io = pycbc_iowrap_getiops(self->iopswrap);
        self->unlock_gil = 0;
    }

    if (dfl_fmt == Py_None || dfl_fmt == NULL) {
        /** Set to 0 if None or NULL */
        dfl_fmt = pycbc_IntFromL(PYCBC_FMT_JSON);

    } else {
        Py_INCREF(dfl_fmt); /* later decref */
    }

    rv = Bucket_set_format(self, dfl_fmt, NULL);
    Py_XDECREF(dfl_fmt);
    if (rv == -1) {
        return rv;
    }

    /** Set the transcoder */
    if (tc && Bucket_set_transcoder(self, tc, NULL) == -1) {
        return -1;
    }

#if defined(WITH_THREAD)
    if (!self->unlock_gil) {
        self->lockmode = PYCBC_LOCKMODE_NONE;
    }

    if (self->lockmode != PYCBC_LOCKMODE_NONE) {
        self->lock = PyThread_allocate_lock();
    }
#else
    self->unlock_gil = 0;
    self->lockmode = PYCBC_LOCKMODE_NONE;
#endif

    err = lcb_create(&self->instance, &create_opts);
    if (err != LCB_SUCCESS) {
        self->instance = NULL;
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err,
                       "Couldn't create instance. Either bad "
                       "credentials/hosts/bucket names were "
                       "passed, or there was an internal error in creating the "
                       "object");
        return -1;
    }

    if (pycbc_log_handler) {
        err = lcb_cntl(self->instance, LCB_CNTL_SET, LCB_CNTL_LOGGER,
                       &pycbc_lcb_logprocs);
        if (err != LCB_SUCCESS) {
            self->instance = NULL;
            PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't create log handler");
            return -1;
        }
    }

    pycbc_callbacks_init(self->instance);
    lcb_set_cookie(self->instance, self);
    {
        const char *bucketstr;
        err = lcb_cntl(self->instance, LCB_CNTL_GET, LCB_CNTL_BUCKETNAME, &bucketstr);
        if (err == LCB_SUCCESS && bucketstr != NULL) {
            self->bucket = pycbc_SimpleStringZ(bucketstr);
        }
    }
    return 0;
}

static PyObject*
Bucket__connect(pycbc_Bucket *self)
{
    lcb_error_t err;

    if (self->flags & PYCBC_CONN_F_CONNECTED) {
        Py_RETURN_NONE;
    }

    err = lcb_connect(self->instance);

    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err,
                       "Couldn't schedule connection. This might be a result of "
                       "an invalid hostname.");
        return NULL;
    }

    pycbc_oputil_wait_common(self);
    if ((self->flags & PYCBC_CONN_F_ASYNC) == 0) {
        err = lcb_get_bootstrap_status(self->instance);
        if (err != LCB_SUCCESS) {
            PYCBC_EXCTHROW_WAIT(err);
            return NULL;
        }
    }
    Py_RETURN_NONE;
}

static void
Bucket_dtor(pycbc_Bucket *self)
{
    if (self->flags & PYCBC_CONN_F_CLOSED) {
        lcb_destroy(self->instance);
        self->instance = NULL;
    }

    if (self->instance) {
        lcb_set_cookie(self->instance, NULL);
        pycbc_schedule_dtor_event(self);
    }

    Py_XDECREF(self->dtorcb);
    Py_XDECREF(self->dfl_fmt);
    Py_XDECREF(self->tc);
    Py_XDECREF(self->bucket);
    Py_XDECREF(self->conncb);
    Py_XDECREF(self->dur_testhook);
    Py_XDECREF(self->iopswrap);

    if (self->instance) {
        lcb_destroy(self->instance);
    }

#ifdef WITH_THREAD
    if (self->lock) {
        PyThread_free_lock(self->lock);
        self->lock = NULL;
    }
#endif

    Py_TYPE(self)->tp_free((PyObject*)self);
}

int
pycbc_BucketType_init(PyObject **ptr)
{
    PyTypeObject *p = &BucketType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Bucket";
    p->tp_new = PyType_GenericNew;
    p->tp_init = (initproc)Bucket__init__;
    p->tp_dealloc = (destructor)Bucket_dtor;

    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_doc = PyDoc_STR("The connection object");

    p->tp_basicsize = sizeof(pycbc_Bucket);

    p->tp_methods = Bucket_TABLE_methods;
    p->tp_members = Bucket_TABLE_members;
    p->tp_getset = Bucket_TABLE_getset;

    pycbc_DummyTuple = PyTuple_New(0);
    pycbc_DummyKeywords = PyDict_New();

    return PyType_Ready(p);
}
