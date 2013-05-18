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

/**
 * Simple argument type. We use this to distinguish between parameters and
 * dictionary values to be encoded.
 */


PyTypeObject pycbc_ArgumentType = {
    PYCBC_POBJ_HEAD_INIT(NULL)
    0
};


int
pycbc_ArgumentType_init(PyObject **ptr)
{
    *ptr = (PyObject*)&pycbc_ArgumentType;

    if (pycbc_ArgumentType.tp_name) {
        return 0;
    }
    pycbc_ArgumentType.tp_base = &PyDict_Type;
    pycbc_ArgumentType.tp_name = "Arguments";
    pycbc_ArgumentType.tp_doc = PyDoc_STR("Simple dict subclass\n"
            "used for 'set' to differentiate between an actual dictionary\n"
            "value, and extended parameters");
    pycbc_ArgumentType.tp_basicsize = sizeof(pycbc_ArgumentObject);
    pycbc_ArgumentType.tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE;
    return PyType_Ready(&pycbc_ArgumentType);
}
