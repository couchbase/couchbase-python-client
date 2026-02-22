/*
 *   Copyright 2016-2026. Couchbase, Inc.
 *   All Rights Reserved.
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
 */

#pragma once

#include "Python.h"

namespace pycbc
{

/**
 * Register a Python type object with a module.
 * Handles PyType_Ready, INCREF, and PyModule_AddObject with proper error handling.
 *
 * @param module The Python module to add the type to
 * @param type The PyTypeObject to register
 * @param name The name to use when adding to the module
 * @return 0 on success, -1 on failure
 */
inline int
register_pytype(PyObject* module, PyTypeObject* type, const char* name)
{
  if (PyType_Ready(type) < 0) {
    return -1;
  }
  Py_INCREF(type);
  if (PyModule_AddObject(module, name, (PyObject*)type) < 0) {
    Py_DECREF(type);
    return -1;
  }
  return 0;
}

} // namespace pycbc
