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

PyObject *pycbc_DummyTuple;
PyObject *pycbc_DummyKeywords;

static int
Connection__init__(pycbc_Connection *self,
                   PyObject *args,
                   PyObject *kwargs);

static PyObject*
Connection__connect(pycbc_Connection *self);

static void
Connection_dtor(pycbc_Connection *self);

static int
set_timeout_common(pycbc_Connection *self, PyObject *value, int op)
{
    double newval;
    lcb_uint32_t usecs;
    lcb_error_t err;

    newval = PyFloat_AsDouble(value);
    if (newval == -1.0) {
        if (PyErr_Occurred()) {
            return -1;
        }
    }

    if (newval <= 0) {
        PyErr_SetString(PyExc_ValueError, "Timeout must not be 0");
        return -1;
    }

    usecs = (lcb_uint32_t)(newval * 1000000);
    err = lcb_cntl(self->instance, LCB_CNTL_SET, op, &usecs);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, err, "Couldn't set timeout");
        return -1;
    }
    return 0;
}

static PyObject *
get_timeout_common(pycbc_Connection *self, int op)
{
    lcb_uint32_t usecs;
    lcb_error_t err;
    err = lcb_cntl(self->instance, LCB_CNTL_GET, op, &usecs);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, err, "Couldn't get timeout");
        return NULL;
    }
    return PyFloat_FromDouble((double)usecs / 1000000);
}

#define DECL_TMO_ACC(name, op) \
    static int \
    Connection_set_##name(pycbc_Connection *self, PyObject *val, void *unused) { \
        (void)unused; \
        return set_timeout_common(self, val, op); \
    } \
    static PyObject * Connection_get_##name(pycbc_Connection *self, void *unused) { \
        (void)unused; \
        return get_timeout_common(self, op); \
    }

DECL_TMO_ACC(timeout, 0x00)
DECL_TMO_ACC(views_timeout, 0x01)


static PyTypeObject ConnectionType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyObject *
Connection_get_format(pycbc_Connection *self, void * unused)
{
    if (self->dfl_fmt) {
        Py_INCREF(self->dfl_fmt);
        return self->dfl_fmt;
    }

    (void)unused;
    Py_RETURN_NONE;
}

static int
Connection_set_format(pycbc_Connection *self, PyObject *value, void *unused)
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
Connection_set_transcoder(pycbc_Connection *self,
                          PyObject *value,
                          void *unused)
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
Connection_get_transcoder(pycbc_Connection *self, void *unused)
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
Connection_server_nodes(pycbc_Connection *self, void *unused)
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
Connection_get_configured_replica_count(pycbc_Connection *self, void *unused)
{
    PyObject *iret = pycbc_IntFromUL(lcb_get_num_replicas(self->instance));

    (void)unused;
    return iret;
}

static PyObject *
Connection_connected(pycbc_Connection *self, void *unused)
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
Connection__instance_pointer(pycbc_Connection *self, void *unused)
{
    PyObject *ret;
    Py_uintptr_t ptri = (Py_uintptr_t)self->instance;
    ret = pycbc_IntFromULL(ptri);
    (void)unused;
    return ret;
}

static PyObject *
Connection_lcb_version(pycbc_Connection *self)
{
    const char *verstr;
    lcb_uint32_t vernum;
    PyObject *ret;

    verstr = lcb_get_version(&vernum);
    ret = Py_BuildValue("(s,k)", verstr, vernum);

    (void)self;

    return ret;
}

static PyObject *
Connection__thr_lockop(pycbc_Connection *self, PyObject *arg)
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
Connection__close(pycbc_Connection *self)
{
    lcb_error_t err;

    if (self->flags & PYCBC_CONN_F_CLOSED) {
        Py_RETURN_NONE;
    }

    self->flags |= PYCBC_CONN_F_CLOSED;

    lcb_destroy(self->instance);
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
Connection__start_timings(pycbc_Connection *self)
{
    lcb_disable_timings(self->instance);
    lcb_enable_timings(self->instance);
    Py_RETURN_NONE;
}

static PyObject *
Connection__clear_timings(pycbc_Connection *self)
{
    lcb_disable_timings(self->instance);
    Py_RETURN_NONE;
}

static PyObject *
Connection__get_timings(pycbc_Connection *self)
{
    PyObject *ll = PyList_New(0);
    lcb_get_timings(self->instance, ll, timings_callback);
    return ll;
}


static PyGetSetDef Connection_TABLE_getset[] = {
        { "timeout",
                (getter)Connection_get_timeout,
                (setter)Connection_set_timeout,
                PyDoc_STR("The timeout value for operations, in seconds")
        },

        { "views_timeout",
                (getter)Connection_get_views_timeout,
                (setter)Connection_set_views_timeout,
                PyDoc_STR("Set the timeout for views requests, in seconds")
        },

        { "default_format",
                (getter)Connection_get_format,
                (setter)Connection_set_format,
                PyDoc_STR("The default format to use for encoding values "
                "(passed to transcoder)")
        },
        { "server_nodes",
                (getter)Connection_server_nodes,
                NULL,
                PyDoc_STR("Get a list of the current nodes in the cluster")
        },
        { "configured_replica_count",
                (getter)Connection_get_configured_replica_count,
                NULL,
                PyDoc_STR("Get the number of configured replicas for the bucket")
        },

        { "transcoder",
                (getter)Connection_get_transcoder,
                (setter)Connection_set_transcoder,
                PyDoc_STR("The :class:`~couchbase.transcoder.Transcoder` "
                        "object being used.\n\n"
                        ""
                        "This is normally ``None`` unless a custom "
                        ":class:`couchbase.transcoder.Transcoder` "
                        "is being used\n")
        },

        { "connected",
                (getter)Connection_connected,
                NULL,
                PyDoc_STR("Boolean read only property indicating whether\n"
                        "this instance has been connected.\n"
                        "\n"
                        "Note that this will still return true even if\n"
                        "it is subsequently closed via :meth:`_close`\n")
        },

        { "_instance_pointer",
                (getter)Connection__instance_pointer,
                NULL,
                PyDoc_STR("Gets the C level pointer for the underlying C "
                         "handle")
        },

        { NULL }
};

static struct PyMemberDef Connection_TABLE_members[] = {
        { "_errors", T_OBJECT_EX, offsetof(pycbc_Connection, errors),
                READONLY,
                PyDoc_STR("List of connection errors")
        },

        { "quiet", T_UINT, offsetof(pycbc_Connection, quiet),
                0,
                PyDoc_STR("Whether to suppress errors when keys are not found "
                "(in :meth:`get` and :meth:`delete` operations).\n"
                "\n"
                "An error is still returned within the :class:`Result`\n"
                "object")
        },

        { "data_passthrough", T_UINT, offsetof(pycbc_Connection, data_passthrough),
                0,
                PyDoc_STR("When this flag is set, values are always returned "
                        "as raw bytes\n")
        },

        { "unlock_gil", T_UINT, offsetof(pycbc_Connection, unlock_gil),
                READONLY,
                PyDoc_STR("Whether GIL manipulation is enabeld for "
                "this connection object.\n"
                "\n"
                "This attribute can only be set from the constructor.\n")
        },

        { "bucket", T_OBJECT_EX, offsetof(pycbc_Connection, bucket),
                READONLY,
                PyDoc_STR("Name of the bucket this object is connected to")
        },

        { "lockmode", T_INT, offsetof(pycbc_Connection, lockmode),
                READONLY,
                PyDoc_STR("How access from multiple threads is handled.\n"
                        "See :ref:`multiple_threads` for more information\n")
        },

        { "_privflags", T_UINT, offsetof(pycbc_Connection, flags),
                0,
                PyDoc_STR("Internal flags.")
        },

        { "_conncb", T_OBJECT_EX, offsetof(pycbc_Connection, conncb),
                0,
                PyDoc_STR("Internal connection callback.")
        },

        { "_dtorcb", T_OBJECT_EX, offsetof(pycbc_Connection, dtorcb),
                0,
                PyDoc_STR("Internal destruction callback")
        },

        { "_dur_persist_to", T_BYTE,
                offsetof(pycbc_Connection, dur_global.persist_to),
                0,
                PyDoc_STR("Internal default persistence settings")
        },

        { "_dur_replicate_to", T_BYTE,
                offsetof(pycbc_Connection, dur_global.replicate_to),
                0,
                PyDoc_STR("Internal default replication settings")
        },

        { "_dur_timeout", T_ULONG,
                offsetof(pycbc_Connection, dur_timeout),
                0,
                PyDoc_STR("Internal ")
        },

        { "_dur_testhook", T_OBJECT_EX,
                offsetof(pycbc_Connection, dur_testhook),
                0,
                PyDoc_STR("Internal hook for durability tests")
        },

        { NULL }
};

static PyMethodDef Connection_TABLE_methods[] = {

#define OPFUNC(name, doc) \
{ #name, (PyCFunction)pycbc_Connection_##name, METH_VARARGS|METH_KEYWORDS, \
    PyDoc_STR(doc) }

        /** Basic Operations */
        OPFUNC(set, "Unconditionally store a key in Couchbase"),
        OPFUNC(add, "Add a key in Couchbase if it does not already exist"),
        OPFUNC(replace, "Replace an existing key in Couchbase"),
        OPFUNC(append, "Append to an existing value in Couchbase"),
        OPFUNC(prepend, "Prepend to an existing value in Couchbase"),
        OPFUNC(set_multi, NULL),
        OPFUNC(add_multi, NULL),
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

        OPFUNC(delete, "Delete a key in Couchbase"),
        OPFUNC(unlock, "Unlock a previously-locked key in Couchbase"),
        OPFUNC(delete_multi, "Multi-key variant of delete"),
        OPFUNC(unlock_multi, "Multi-key variant of unlock"),

        OPFUNC(arithmetic, "Modify a counter in Couchbase"),
        OPFUNC(incr, "Increment a counter in Couchbase"),
        OPFUNC(decr, "Decrement a counter in Couchbase"),
        OPFUNC(arithmetic_multi, NULL),
        OPFUNC(incr_multi, NULL),
        OPFUNC(decr_multi, NULL),
        OPFUNC(_stats, "Get various server statistics"),

        OPFUNC(_http_request, "Internal routine for HTTP requests"),

        OPFUNC(observe, "Get replication/persistence status for keys"),
        OPFUNC(observe_multi, "multi-key variant of observe"),

        OPFUNC(endure_multi, "Check durability requirements"),


#undef OPFUNC

        { "lcb_version",
                (PyCFunction)Connection_lcb_version,
                METH_NOARGS|METH_STATIC,
                PyDoc_STR(
                "Get `libcouchbase` version information\n"
                "\n"
                ":return: a tuple of ``(version_string, version_number)``\n"
                "  corresponding to the underlying libcouchbase version\n"

                "Show the versions ::\n" \
                "   \n"
                "   verstr, vernum = Connection.lcb_version()\n"
                "   print('0x{0:x}'.format(vernum))\n"
                "   # 0x020005\n"
                "   \n"
                "   print(verstr)\n"
                "   # 2.0.5\n"
                "\n"
                "\n")
        },

        { "_thr_lockop",
                (PyCFunction)Connection__thr_lockop,
                METH_VARARGS,
                PyDoc_STR("Unconditionally lock/unlock the connection object "
                        "if 'lockmode' has been set. For testing uses only")
        },

        { "_close",
                (PyCFunction)Connection__close,
                METH_NOARGS,
                PyDoc_STR(
                "Close the instance's underlying socket resources\n"
                "\n"
                "Note that operations pending on the connection may\n"
                "fail.\n"
                "\n")
        },

        { "_connect",
                (PyCFunction)Connection__connect,
                METH_NOARGS,
                PyDoc_STR(
                "Connect this instance. This is typically called by one of\n"
                "the wrapping constructors\n")
        },

        { "_pipeline_begin",
                (PyCFunction)pycbc_Connection__start_pipeline,
                METH_NOARGS,
                PyDoc_STR("Enter pipeline mode. Internal use")
        },

        { "_pipeline_end",
                (PyCFunction)pycbc_Connection__end_pipeline,
                METH_NOARGS,
                PyDoc_STR(
                "End pipeline mode and wait for operations to complete")
        },

        { "_start_timings",
                (PyCFunction)Connection__start_timings,
                METH_NOARGS,
                PyDoc_STR("Start recording timings")
        },

        { "_get_timings",
                (PyCFunction)Connection__get_timings,
                METH_NOARGS,
                PyDoc_STR("Get all timings since the last call to start_timings")
        },

        { "_stop_timings",
                (PyCFunction)Connection__clear_timings,
                METH_NOARGS,
                PyDoc_STR("Clear and disable timings")
        },

        { "_cntl",
                (PyCFunction)pycbc_Connection__cntl,
                METH_VARARGS|METH_KEYWORDS,
                NULL
        },

        { "_vbmap",
                (PyCFunction)pycbc_Connection__vbmap,
                METH_VARARGS,
                PyDoc_STR("Returns a tuple of (vbucket, server index) for a key")
        },

        { NULL, NULL, 0, NULL }
};

static lcb_error_t
set_config_cache(pycbc_Connection *self, const char *filename,
                 const struct lcb_create_st *cropts)
{
#ifndef LCB_CNTL_CONFIGCACHE
#define LCB_CNTL_CONFIGCACHE 0x21
#endif
    lcb_error_t err;

    err = lcb_cntl(self->instance, LCB_CNTL_SET, LCB_CNTL_CONFIGCACHE,
                   (void *)filename);

    if (err == LCB_SUCCESS) {
        return err;

    } else if (err == LCB_NOT_SUPPORTED) {
        /**
         * Otherwise we need to use the older 'compat' structure which is
         * deprecated and broken in newer versions of libcouchbase.
         */
        struct lcb_cached_config_st cached_config;
        memset(&cached_config, 0, sizeof cached_config);
        cached_config.cachefile = filename;
        memcpy(&cached_config.createopt, cropts, sizeof(*cropts));

        /** Destroy the existing instance first */
        lcb_destroy(self->instance);
        self->instance = NULL;
        err = lcb_create_compat(LCB_CACHED_CONFIG, &cached_config,
                                &self->instance, NULL);
        if (err != LCB_SUCCESS) {
            self->instance = NULL;
        }

    } else {
        /** Other error from lcb_cntl() */
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err,
                       "Could not assign configuration cache");
    }
    return err;
}

static int
Connection__init__(pycbc_Connection *self,
                       PyObject *args, PyObject *kwargs)
{
    int rv;
    int conntype = LCB_TYPE_BUCKET;
    lcb_error_t err;
    char *conncache = NULL;
    char *config_cache = NULL;
    PyObject *unlock_gil_O = NULL;
    PyObject *iops_O = NULL;
    PyObject *timeout = NULL;
    PyObject *dfl_fmt = NULL;
    PyObject *tc = NULL;

    struct lcb_create_st create_opts = { 0 };


    /**
     * This xmacro enumerates the constructor keywords, targets, and types.
     * This was converted into an xmacro to ease the process of adding or
     * removing various parameters.
     */
#define XCTOR_ARGS(X) \
    X("_errors", &self->errors, "O") \
    X("_flags", &self->flags, "I") \
    X("bucket", &create_opts.v.v1.bucket, "z") \
    X("username", &create_opts.v.v1.user, "z") \
    X("password", &create_opts.v.v1.passwd, "z") \
    X("host", &create_opts.v.v1.host, "z") \
    X("conncache", &conncache, "z") \
    X("config_cache", &config_cache, "z") \
    X("quiet", &self->quiet, "I") \
    X("unlock_gil", &unlock_gil_O, "O") \
    X("transcoder", &tc, "O") \
    X("timeout", &timeout, "O") \
    X("default_format", &dfl_fmt, "O") \
    X("lockmode", &self->lockmode, "i") \
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
    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     argspec,
                                     kwlist,
                                     XCTOR_ARGS(X) NULL);
    #undef X

    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return -1;
    }

    if (unlock_gil_O && PyObject_IsTrue(unlock_gil_O) == 0) {
        self->unlock_gil = 0;
    }

    if (create_opts.v.v1.bucket) {
        self->bucket = pycbc_SimpleStringZ(create_opts.v.v1.bucket);
    }

    create_opts.version = 1;
    create_opts.v.v1.type = conntype;

    if (iops_O && iops_O != Py_None) {
        self->iops = pycbc_iops_new(self, iops_O);
        create_opts.v.v1.io = self->iops;
        self->unlock_gil = 0;
    }

    Py_INCREF(self->errors);


    if (dfl_fmt == Py_None || dfl_fmt == NULL) {
        /** Set to 0 if None or NULL */
        dfl_fmt = pycbc_IntFromL(0);

    } else {
        Py_INCREF(dfl_fmt); /* later decref */
    }

    rv = Connection_set_format(self, dfl_fmt, NULL);
    Py_XDECREF(dfl_fmt);
    if (rv == -1) {
        return rv;
    }

    /** Set the transcoder */
    if (tc && Connection_set_transcoder(self, tc, NULL) == -1) {
        return -1;
    }

#if defined(WITH_THREAD) && !defined(PYPY_VERSION)
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
    if (config_cache) {
        conncache = config_cache;
    }

    if (conncache && conntype != LCB_TYPE_BUCKET) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Cannot use connection cache with "
                       "management connection");
        return -1;
    }

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

    if (conncache) {
        err = set_config_cache(self, conncache, &create_opts);
        if (err != LCB_SUCCESS) {
            return -1;
        }
    }


    pycbc_callbacks_init(self->instance);
    lcb_set_cookie(self->instance, self);

    if (timeout && timeout != Py_None) {
        if (Connection_set_timeout(self, timeout, NULL) == -1) {
            return -1;
        }
    }

    return 0;
}

static PyObject*
Connection__connect(pycbc_Connection *self)
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

    err = pycbc_oputil_wait_common(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_WAIT(err);
        return NULL;
    }

    Py_RETURN_NONE;
}

static void
Connection_dtor(pycbc_Connection *self)
{
    if (self->instance) {
        lcb_set_cookie(self->instance, NULL);
    }

    pycbc_schedule_dtor_event(self);

    Py_XDECREF(self->dtorcb);
    Py_XDECREF(self->dfl_fmt);
    Py_XDECREF(self->errors);
    Py_XDECREF(self->tc);
    Py_XDECREF(self->bucket);
    Py_XDECREF(self->conncb);
    Py_XDECREF(self->dur_testhook);

    if (self->instance) {
        lcb_destroy(self->instance);
    }

    if (self->iops) {
        pycbc_iops_free(self->iops);
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
pycbc_ConnectionType_init(PyObject **ptr)
{
    PyTypeObject *p = &ConnectionType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Connection";
    p->tp_new = PyType_GenericNew;
    p->tp_init = (initproc)Connection__init__;
    p->tp_dealloc = (destructor)Connection_dtor;

    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_doc = PyDoc_STR("The connection object");

    p->tp_basicsize = sizeof(pycbc_Connection);

    p->tp_methods = Connection_TABLE_methods;
    p->tp_members = Connection_TABLE_members;
    p->tp_getset = Connection_TABLE_getset;

    pycbc_DummyTuple = PyTuple_New(0);
    pycbc_DummyKeywords = PyDict_New();

    return PyType_Ready(p);
}
