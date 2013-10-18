/**
 * Abstraction layer for interacting with the MultiResult dict.
 *
 * This may eventually lead to partial PyPy support
 */

#ifdef PYPY_VERSION
#define PYCBC_MULTIRESULT_BASE \
    PyObject_HEAD \
    PyObject *_dict_pointer

#define pycbc_multiresult_dict(mres) (mres)->_dict_pointer

#define pycbc_multiresult_set_base(o) o->tp_new = PyType_GenericNew

#define pycbc_multiresult_init_dict(mres, args, kwargs) \
    ( ((mres)->_dict_pointer = PyDict_New()) ? 0 : -1)

#define pycbc_multiresult_destroy_dict(mres)  { \
    Py_XDECREF((mres)->_dict_pointer); \
    Py_TYPE(self)->tp_free(self); \
}

#define pycbc_multiresult_check(opj) 1

#else

#define PYCBC_MULTIRESULT_BASE PyDictObject _dict_private

/**
 * Get a borrowed reference to the dictionary
 */
#define pycbc_multiresult_dict(mres) \
    (PyObject *)(&((mres)->_dict_private))

/**
 * Set the base class for the MultiResult
 */
#define pycbc_multiresult_set_base(tobj) (tobj)->tp_base = &PyDict_Type

/**
 * __init__ for the dictionary
 */
#define pycbc_multiresult_init_dict(iobj, args, kwargs) \
    (PyDict_Type.tp_init((PyObject *)iobj, args, kwargs))

/**
 * Destroy the dictionary
 */
#define pycbc_multiresult_destroy_dict(iobj) \
    PyDict_Type.tp_dealloc((PyObject *)iobj)

#define pycbc_multiresult_check(obj) \
    (Py_TYPE(obj) == &pycbc_MultiResultType || \
            Py_TYPE(obj) == &pycbc_AsyncResultType)

#endif
