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

couchbase::replicate_to
PyObject_to_replicate_to(PyObject* pyObj_replicate_to)
{
    auto replicate_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_replicate_to));
    if (replicate_to == 0) {
        return couchbase::replicate_to::none;
    } else if (replicate_to == 1) {
        return couchbase::replicate_to::one;
    } else if (replicate_to == 2) {
        return couchbase::replicate_to::two;
    } else if (replicate_to == 3) {
        return couchbase::replicate_to::three;
    } else {
        return couchbase::replicate_to::none;
    }
}

couchbase::persist_to
PyObject_to_persist_to(PyObject* pyObj_persist_to)
{
    auto persist_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_persist_to));
    if (persist_to == 0) {
        return couchbase::persist_to::none;
    } else if (persist_to == 1) {
        return couchbase::persist_to::active;
    } else if (persist_to == 2) {
        return couchbase::persist_to::one;
    } else if (persist_to == 3) {
        return couchbase::persist_to::two;
    } else if (persist_to == 4) {
        return couchbase::persist_to::three;
    } else if (persist_to == 5) {
        return couchbase::persist_to::four;
    } else {
        return couchbase::persist_to::none;
    }
}

std::pair<couchbase::persist_to, couchbase::replicate_to>
PyObject_to_durability(PyObject* pyObj_durability)
{
    auto durability = std::make_pair(couchbase::persist_to::none, couchbase::replicate_to::none);
    PyObject* pyObj_persist_to = PyDict_GetItemString(pyObj_durability, "persist_to");
    if (pyObj_persist_to) {
        durability.first = PyObject_to_persist_to(pyObj_persist_to);
    }

    PyObject* pyObj_replicate_to = PyDict_GetItemString(pyObj_durability, "replicate_to");
    if (pyObj_replicate_to) {
        durability.second = PyObject_to_replicate_to(pyObj_replicate_to);
    }

    return durability;
}

couchbase::durability_level
PyObject_to_durability_level(PyObject* pyObj_durability_level)
{
    if (PyUnicode_Check(pyObj_durability_level)) {
        auto durability = std::string(PyUnicode_AsUTF8(pyObj_durability_level));
        if (durability.compare("majorityAndPersistActive") == 0) {
            return couchbase::durability_level::majority_and_persist_to_active;
        } else if (durability.compare("majority") == 0) {
            return couchbase::durability_level::majority;
        } else if (durability.compare("persistToMajority") == 0) {
            return couchbase::durability_level::persist_to_majority;
        } else if (durability.compare("none") == 0) {
            return couchbase::durability_level::none;
        } else {
            return couchbase::durability_level::none;
        }
    } else {
        auto durability = static_cast<uint8_t>(PyLong_AsLong(pyObj_durability_level));
        if (durability == 0) {
            return couchbase::durability_level::none;
        } else if (durability == 1) {
            return couchbase::durability_level::majority;
        } else if (durability == 2) {
            return couchbase::durability_level::majority_and_persist_to_active;
        } else if (durability == 3) {
            return couchbase::durability_level::persist_to_majority;
        } else {
            return couchbase::durability_level::none;
        }
    }
}
