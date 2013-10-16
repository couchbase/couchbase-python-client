#include "pycbc.h"
#ifdef PYPY_VERSION

int
pycbc_PyPy_ByteArrayAsBytes(PyObject **bytesobj,
                            void **buf,
                            Py_ssize_t *plen)
{
    PyObject *s = PyObject_Str(*bytesobj);
    int rv;

    if (!s) {
        return -1;
    }

    rv = PyString_AsStringAndSize(s, (char **)buf, plen);
    Py_DECREF(*bytesobj);
    *bytesobj = s;
    return rv;
}

PyObject *
pycbc_multiresult_wrap(pycbc_MultiResult *self)
{
    PyObject *ret;
    PyObject *args = Py_BuildValue("(O,O)", self, pycbc_multiresult_dict(self));
    ret = PyObject_Call(pycbc_helpers.pypy_mres_factory, args, NULL);

    if (!ret) {
        abort();
        PyErr_PrintEx(0);
    }

    pycbc_assert(ret);
    Py_XDECREF(args);
    return ret;
}
#endif
