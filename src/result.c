#include "pycbc.h"
#include "structmember.h"

static PyObject *Result_success(pycbc_ResultObject *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong(self->rc == LCB_SUCCESS);
}

static PyObject *Result_repr(pycbc_ResultObject *self)
{
    return PyObject_CallFunction(pycbc_helpers.result_reprfunc, "O", self);
}

static PyObject *Result_value(pycbc_ResultObject *self, void *closure)
{
    (void)closure;
    if (!self->value) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    Py_INCREF(self->value);
    return self->value;
}

static PyObject *Result_errstr(pycbc_ResultObject *self, void *closure)
{
    (void)closure;
    return pycbc_lcb_errstr(NULL, self->rc);
}

static struct PyMemberDef Result_members[] = {
        { "cas",
                T_ULONGLONG, offsetof(pycbc_ResultObject, cas),
                READONLY, "The CAS of the operation. Returned for every operation"
        },
        { "flags",
                T_ULONG, offsetof(pycbc_ResultObject, flags),
                READONLY, "Flags with which the value were stored (if available)"
        },
        { "rc",
                T_INT, offsetof(pycbc_ResultObject, rc),
                READONLY, "libcouchbase error code"
        },
        { "key", T_OBJECT_EX, offsetof(pycbc_ResultObject, key),
                READONLY, "Key for the operation"
        },

        { NULL }
};

struct PyGetSetDef Result_getset[] = {
        { "success",
                (getter)Result_success,
                NULL,
                "Determine whether operation succeeded or not"
        },
        { "value",
                (getter)Result_value,
                NULL,
                "Value of the operation (if any)"
        },
        { "errstr",
                (getter)Result_errstr,
                NULL,
                "Returns a textual representation of the error"
        },
        { NULL }
};

PyTypeObject pycbc_ResultType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyMethodDef Result_methods[] = {
        { NULL }
};

static void Result_dealloc(pycbc_ResultObject *self)
{
    Py_XDECREF(self->value);
    Py_XDECREF(self->key);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

int pycbc_ResultType_init(PyObject **ptr)
{
    *ptr = (PyObject*)&pycbc_ResultType;

    if (pycbc_ResultType.tp_name) {
        return 0;
    }

    pycbc_ResultType.tp_name = "Result";
    pycbc_ResultType.tp_doc =
            "The standard return type for Couchbase operations.\n"
            "\n"
            "This is a lightweight object and may be subclassed by other\n"
            "operations which may required additional fields.";

    pycbc_ResultType.tp_new = PyType_GenericNew;
    pycbc_ResultType.tp_dealloc = (destructor)Result_dealloc;
    pycbc_ResultType.tp_basicsize = sizeof(pycbc_ResultObject);
    pycbc_ResultType.tp_members = Result_members;
    pycbc_ResultType.tp_methods = Result_methods;
    pycbc_ResultType.tp_getset = Result_getset;
    pycbc_ResultType.tp_repr = (reprfunc)Result_repr;
    pycbc_ResultType.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;

    return PyType_Ready(&pycbc_ResultType);
}

PyObject *pycbc_result_new(pycbc_ConnectionObject *parent)
{
    PyObject *obj = PyObject_CallFunction((PyObject*) &pycbc_ResultType,
                                          NULL,
                                          NULL);
    return obj;
}
