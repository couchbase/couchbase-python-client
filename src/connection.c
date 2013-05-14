#include "pycbc.h"
#include "structmember.h"
#include "oputil.h"

PyObject *pycbc_DummyTuple;
PyObject *pycbc_DummyKeywords;

static int
Connection__init__(pycbc_ConnectionObject *self,
                   PyObject *args,
                   PyObject *kwargs);

static void
Connection_dtor(pycbc_ConnectionObject *self);

static PyObject *
Connection_get_timeout(pycbc_ConnectionObject *self, void *);

static int
Connection_set_timeout(pycbc_ConnectionObject *self, PyObject *value, void *);

static PyTypeObject ConnectionType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyObject *
Connection_get_format(pycbc_ConnectionObject *self, void * unused)
{
    if (self->dfl_fmt) {
        Py_INCREF(self->dfl_fmt);
        return self->dfl_fmt;
    }
    (void)unused;
    Py_RETURN_NONE;
}

static int
Connection_set_format(pycbc_ConnectionObject *self, PyObject *value, void *unused)
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

static PyObject *
Connection_server_nodes(pycbc_ConnectionObject *self, PyObject *value,
                        void *unused)
{
    const char * const *cnodes;
    const char **curnode;
    PyObject *ret_list;
    cnodes = lcb_get_server_list(self->instance);

    if (!cnodes) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Can't get server nodes");
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

    return ret_list;
}


static PyGetSetDef Connection_getset[] = {
        { "timeout",
                (getter)Connection_get_timeout,
                (setter)Connection_set_timeout,
                "The timeout value for operations, in seconds"
        },

        { "default_format",
                (getter)Connection_get_format,
                (setter)Connection_set_format,
                "The default format to use for encoding values "
                "(passed to transcoder)"
        },
        { "server_nodes",
                (getter)Connection_server_nodes,
                NULL,
                "Get a list of the current nodes in the cluster"
        },
        { NULL }
};

static struct PyMemberDef Connection_members[] = {
        { "transcoder", T_OBJECT_EX, offsetof(pycbc_ConnectionObject, tc),
                0,
                ":type transcoder: `couchbase.transcoder.Transcoder`\n"
        },

        { "_errors", T_OBJECT_EX, offsetof(pycbc_ConnectionObject, errors),
                READONLY,
                "List of connection errors"
        },

        { "quiet", T_UINT, offsetof(pycbc_ConnectionObject, quiet),
                0,
                "Whether to suppress errors when keys are not found (in \n"
                ":meth:`get` and :meth:`delete` operations).\n"
                "\n"
                "An error is still returned within the :class:`Result`\n"
                "object"
        },

        { "data_passthrough", T_UINT, offsetof(pycbc_ConnectionObject, data_passthrough),
                0,
                "When this flag is set, values are always returned as raw bytes\n"
                "\n"
        },

        { "unlock_gil", T_UINT, offsetof(pycbc_ConnectionObject, unlock_gil),
                READONLY,
                "Whether GIL manipulation is enabeld for this connection object.\n"
                "\n"
                "This attribute can only be set from the constructor.\n"
        },

        { NULL }
};

static PyMethodDef Connection_methods[] = {

#define OPFUNC(name, doc) \
{ #name, (PyCFunction)pycbc_Connection_##name, METH_VARARGS|METH_KEYWORDS, doc }

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

#undef OPFUNC

        { NULL, NULL, 0, NULL }
};

static int
Connection_set_timeout(pycbc_ConnectionObject *self,
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

    usecs = newval * 1000000;
    lcb_set_timeout(self->instance, usecs);

    (void)unused;
    return 0;
}

static PyObject *
Connection_get_timeout(pycbc_ConnectionObject *self, void *unused)
{
    lcb_uint32_t usecs = lcb_get_timeout(self->instance);

    (void)unused;
    return PyFloat_FromDouble((double)usecs / 1000000);
}


static int
Connection__init__(pycbc_ConnectionObject *self,
                       PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {
            /** Private  parameters */
            "_errors", "_flags",

            /** Public: Required */
            "bucket",

            /** Public: Optional */
            "username", "password", "host", "conncache",

            "quiet", "unlock_gil", "transcoder", "timeout",
            "default_format",
            NULL
    };

    int rv;
    lcb_error_t err;
    char *conncache = NULL;
    PyObject *unlock_gil_O = NULL;
    PyObject *timeout = NULL;

    struct lcb_create_st create_opts = { 0 };
    struct lcb_cached_config_st cached_config = { { 0 } };

    if (self->init_called) {
        PyErr_SetString(PyExc_RuntimeError, "__init__ was already called");
        return -1;
    }

    self->init_called = 1;
    self->flags = 0;

    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "OIs|"
                                     "zzzz"
                                     "I"
                                     "OOOO",
                                     kwlist,
                                     /* Required */
                                     &self->errors,
                                     &self->flags,
                                     &create_opts.v.v0.bucket,

                                     /* Optional, String */
                                     &create_opts.v.v0.user,
                                     &create_opts.v.v0.passwd,
                                     &create_opts.v.v0.host,
                                     &conncache,

                                     /** Optional, Boolean */
                                     &self->quiet,

                                     /** Optional, PyObject */
                                     &unlock_gil_O,
                                     &self->tc,
                                     &timeout,
                                     &self->dfl_fmt);
    if (!rv) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS,
                       0,
                       "Couldn't parse constructor arguments");
        return -1;
    }

    self->unlock_gil = (unlock_gil_O && PyObject_IsTrue(unlock_gil_O));

    Py_INCREF(self->errors);

    if (conncache) {
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
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't create instance");
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
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't schedule connection");
        return -1;
    }


    PYCBC_CONN_THR_BEGIN(self);
    err = lcb_wait(self->instance);
    PYCBC_CONN_THR_END(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "couldn't wait");
        return -1;
    }

    if (self->dfl_fmt) {
        if (self->dfl_fmt != Py_None) {
            if (!PyNumber_Check(self->dfl_fmt)) {
                PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Flags must be number or None");
                return -1;
            }
        } else {
            self->dfl_fmt = NULL;
        }
    }

    if (!self->dfl_fmt) {
        self->dfl_fmt = pycbc_IntFromL(0);

    } else {
        Py_INCREF(self->dfl_fmt);
    }

    if (self->dfl_fmt && self->dfl_fmt != Py_None) {
        Py_INCREF(self->dfl_fmt);
    }

    if (self->tc) {
        if (!PyObject_IsTrue(self->tc)) {
            self->tc = NULL;

        } else {
            Py_INCREF(self->tc);
        }
    }

    return 0;
}

static void
Connection_dtor(pycbc_ConnectionObject *self)
{
    if (self->instance) {
        lcb_destroy(self->instance);
        self->instance = NULL;
    }

    if (self->dfl_fmt) {
        Py_DECREF(self->dfl_fmt);
        self->dfl_fmt = NULL;
    }

    if (self->errors) {
        Py_DECREF(self->errors);
        self->errors = NULL;
    }

    Py_XDECREF(self->tc);
    Py_XDECREF(self->dfl_fmt);

    Py_TYPE(self)->tp_free((PyObject*)self);
}


int pycbc_ConnectionType_init(PyObject **ptr)
{
    *ptr = (PyObject*)&ConnectionType;

    if (ConnectionType.tp_name) {
        return 0;
    }

    ConnectionType.tp_name = "Connection";

    ConnectionType.tp_new = PyType_GenericNew;
    ConnectionType.tp_init = (initproc)Connection__init__;
    ConnectionType.tp_dealloc = (destructor)Connection_dtor;

    ConnectionType.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    ConnectionType.tp_doc = "The connection object";

    ConnectionType.tp_basicsize = sizeof(pycbc_ConnectionObject);

    ConnectionType.tp_methods = Connection_methods;
    ConnectionType.tp_members = Connection_members;
    ConnectionType.tp_getset = Connection_getset;

    pycbc_DummyTuple = PyTuple_New(0);
    pycbc_DummyKeywords = PyDict_New();

    return PyType_Ready(&ConnectionType);
}
