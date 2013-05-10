#include "pycbc.h"
#include "structmember.h"

/**
 * Simple argument type. We use this to distinguish between parameters and
 * dictionary values to be encoded.
 */


PyTypeObject pycbc_ArgumentType = {
    PYCBC_POBJ_HEAD_INIT(NULL)
    0
};


int pycbc_ArgumentType_init(PyObject **ptr)
{
    *ptr = (PyObject*)&pycbc_ArgumentType;
    if (pycbc_ArgumentType.tp_name) {
        return 0;
    }
    pycbc_ArgumentType.tp_base = &PyDict_Type;
    pycbc_ArgumentType.tp_name = "Arguments";
    pycbc_ArgumentType.tp_doc = "Simple dict subclass\n"
            "used for 'set' to differentiate between an actual dictionary\n"
            "value, and extended parameters";
    pycbc_ArgumentType.tp_basicsize = sizeof(pycbc_ArgumentObject);
    pycbc_ArgumentType.tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE;
    return PyType_Ready(&pycbc_ArgumentType);
}
