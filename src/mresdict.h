/**
 * Abstraction layer for interacting with the MultiResult dict.
 */


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
