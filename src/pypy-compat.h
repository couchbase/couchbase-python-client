#ifndef PYCBC_PYPY_COMPAT_H
#define PYCBC_PYPY_COMPAT_H
#define PyByteArray_Check(x) PyObject_IsInstance(x, \
                                                 (PyObject *)&PyByteArray_Type)
#define PyByteArray_AS_STRING(x) NULL
#define PyByteArray_GET_SIZE(x) 0
#define PyErr_WarnExplicit(a,b,c,d,e,f)
#define PyUnicode_FromFormat(o, ...) pycbc_SimpleStringZ(o)


int
pycbc_PyPy_ByteArrayAsBytes(PyObject **bytesobj,
                            void **buf,
                            Py_ssize_t *plen);

struct pycbc_MultiResult_st;

PyObject *
pycbc_multiresult_wrap(struct pycbc_MultiResult_st *self);

#endif
