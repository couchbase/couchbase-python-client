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

PyObject *pycbc_DummyTuple;
PyObject *pycbc_DummyKeywords;

static int
Connection__init__(pycbc_Connection *self,
                   PyObject *args,
                   PyObject *kwargs);

static void
Connection_dtor(pycbc_Connection *self);

static PyObject *
Connection_get_timeout(pycbc_Connection *self, void *);

static int
Connection_set_timeout(pycbc_Connection *self, PyObject *value, void *);

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
    if (!PyNumber_Check(value)) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Format must be a number");
        return -1;
    }

    if (Py_TYPE(value) == &PyBool_Type) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Format must not be a boolean");
        return -1;
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
    return iret;
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


static PyGetSetDef Connection_TABLE_getset[] = {
        { "timeout",
                (getter)Connection_get_timeout,
                (setter)Connection_set_timeout,
                PyDoc_STR("The timeout value for operations, in seconds")
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
                METH_VARARGS|METH_KEYWORDS|METH_STATIC,
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

        { NULL, NULL, 0, NULL }
};

static int
Connection_set_timeout(pycbc_Connection *self,
                       PyObject *other, void *unused)
{
    double newval;
    lcb_uint32_t usecs;
    newval = PyFloat_AsDouble(other);
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
    lcb_set_timeout(self->instance, usecs);

    (void)unused;
    return 0;
}

static PyObject *
Connection_get_timeout(pycbc_Connection *self, void *unused)
{
    lcb_uint32_t usecs = lcb_get_timeout(self->instance);

    (void)unused;
    return PyFloat_FromDouble((double)usecs / 1000000);
}


static int
Connection__init__(pycbc_Connection *self,
                       PyObject *args, PyObject *kwargs)
{
    int rv;
    int conntype = LCB_TYPE_BUCKET;
    lcb_error_t err;
    char *conncache = NULL;
    PyObject *unlock_gil_O = NULL;
    PyObject *timeout = NULL;
    PyObject *dfl_fmt = NULL;
    PyObject *tc = NULL;

    struct lcb_create_st create_opts = { 0 };
    struct lcb_cached_config_st cached_config = { { 0 } };


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
    X("quiet", &self->quiet, "I") \
    X("unlock_gil", &unlock_gil_O, "O") \
    X("transcoder", &tc, "O") \
    X("timeout", &timeout, "O") \
    X("default_format", &dfl_fmt, "O") \
    X("lockmode", &self->lockmode, "i") \
    X("_conntype", &conntype, "i") \

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

#ifdef WITH_THREAD
    if (!self->unlock_gil) {
        self->lockmode = PYCBC_LOCKMODE_NONE;
    }

    if (self->lockmode != PYCBC_LOCKMODE_NONE) {
        self->lock = PyThread_allocate_lock();
    }
#endif

    if (conncache) {
        if (conntype != LCB_TYPE_BUCKET) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                           "Cannot use connection cache with "
                           "management connection");
            return -1;
        }
        cached_config.cachefile = conncache;
        memcpy(&cached_config.createopt, &create_opts, sizeof(create_opts));
        err = lcb_create_compat(LCB_CACHED_CONFIG,
                                &cached_config,
                                &self->instance,
                                NULL);
    } else {
        err = lcb_create(&self->instance, &create_opts);
    }

    if (err != LCB_SUCCESS) {
        self->instance = NULL;
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err,
                       "Couldn't create instance. Either bad "
                       "credentials/hosts/bucket names were "
                       "passed, or there was an internal error in creating the "
                       "object");
        return -1;
    }

    pycbc_callbacks_init(self->instance);
    lcb_set_cookie(self->instance, self);

    if (timeout && timeout != Py_None) {
        if (Connection_set_timeout(self, timeout, NULL) == -1) {
            return -1;
        }
    }

    PYCBC_CONN_THR_BEGIN(self);
    err = lcb_connect(self->instance);
    PYCBC_CONN_THR_END(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err,
                       "Couldn't schedule connection. This might be a result of "
                       "an invalid hostname.");
        return -1;
    }


    err = pycbc_oputil_wait_common(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_WAIT(err);
        return -1;
    }

    return 0;
}

static void
Connection_dtor(pycbc_Connection *self)
{
    if (self->instance) {
        lcb_destroy(self->instance);
        self->instance = NULL;
    }

    Py_XDECREF(self->dfl_fmt);
    Py_XDECREF(self->errors);
    Py_XDECREF(self->tc);
    Py_XDECREF(self->bucket);

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
