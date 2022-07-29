/*
 *   Copyright 2016-2022. Couchbase, Inc.
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

#include "utils.hxx"

couchbase::core::utils::binary
PyObject_to_binary(PyObject* pyObj_value)
{
    char* buf;
    Py_ssize_t nbuf;
    if (PyBytes_AsStringAndSize(pyObj_value, &buf, &nbuf) == -1) {
        throw std::invalid_argument("Unable to determine bytes object from provided value.");
    }
    auto size = py_ssize_t_to_size_t(nbuf);
    return couchbase::core::utils::to_binary(reinterpret_cast<const char*>(buf), size);
}

PyObject*
binary_to_PyObject(couchbase::core::utils::binary value)
{
    auto buf = reinterpret_cast<const char*>(value.data());
    auto nbuf = size_t_to_py_ssize_t(value.size());
    return PyBytes_FromStringAndSize(buf, nbuf);
}

std::size_t
py_ssize_t_to_size_t(Py_ssize_t value)
{
    if (value < 0) {
        throw std::invalid_argument("Cannot convert provided Py_ssize_t value to size_t.");
    }

    return static_cast<std::size_t>(value);
}

Py_ssize_t
size_t_to_py_ssize_t(std::size_t value)
{
    if (value > INT_MAX) {
        throw std::invalid_argument("Cannot convert provided size_t value to Py_ssize_t.");
    }
    return static_cast<Py_ssize_t>(value);
}
