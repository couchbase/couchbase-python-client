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

std::string
binary_to_string(couchbase::core::utils::binary value)
{
    auto json = couchbase::core::utils::json::parse_binary(value);
    return couchbase::core::utils::json::generate(json);
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

couchbase::core::operations::query_request
build_query_request(PyObject* pyObj_query_args)
{
    couchbase::core::operations::query_request req;
    PyObject* pyObj_statement = PyDict_GetItemString(pyObj_query_args, "statement");
    if (pyObj_statement != nullptr) {
        if (PyUnicode_Check(pyObj_statement)) {
            req.statement = std::string(PyUnicode_AsUTF8(pyObj_statement));
        } else {
            PyErr_SetString(PyExc_ValueError, "Query statement is not a string.");
            return {};
        }
    }

    PyObject* pyObj_adhoc = PyDict_GetItemString(pyObj_query_args, "adhoc");
    if (pyObj_adhoc != nullptr) {
        req.adhoc = pyObj_adhoc == Py_True ? true : false;
    }

    PyObject* pyObj_metrics = PyDict_GetItemString(pyObj_query_args, "metrics");
    if (pyObj_metrics != nullptr) {
        req.metrics = pyObj_metrics == Py_True ? true : false;
    }

    PyObject* pyObj_readonly = PyDict_GetItemString(pyObj_query_args, "readonly");
    if (pyObj_readonly != nullptr) {
        req.readonly = pyObj_readonly == Py_True ? true : false;
    }

    PyObject* pyObj_flex_index = PyDict_GetItemString(pyObj_query_args, "flex_index");
    if (pyObj_flex_index != nullptr) {
        req.flex_index = pyObj_flex_index == Py_True ? true : false;
    }

    PyObject* pyObj_preserve_expiry = PyDict_GetItemString(pyObj_query_args, "preserve_expiry");
    if (pyObj_preserve_expiry != nullptr) {
        req.preserve_expiry = pyObj_preserve_expiry == Py_True ? true : false;
    }

    PyObject* pyObj_max_parallelism = PyDict_GetItemString(pyObj_query_args, "max_parallelism");
    if (nullptr != pyObj_max_parallelism) {
        req.max_parallelism = PyLong_AsUnsignedLongLong(pyObj_max_parallelism);
    }

    PyObject* pyObj_scan_cap = PyDict_GetItemString(pyObj_query_args, "scan_cap");
    if (nullptr != pyObj_scan_cap) {
        req.scan_cap = PyLong_AsUnsignedLongLong(pyObj_scan_cap);
    }

    PyObject* pyObj_scan_wait = PyDict_GetItemString(pyObj_query_args, "scan_wait");
    if (nullptr != pyObj_scan_wait) {
        // comes in as microseconds
        req.scan_wait = std::chrono::milliseconds(PyLong_AsUnsignedLongLong(pyObj_scan_wait) / 1000ULL);
    }

    PyObject* pyObj_pipeline_batch = PyDict_GetItemString(pyObj_query_args, "pipeline_batch");
    if (nullptr != pyObj_pipeline_batch) {
        req.pipeline_batch = PyLong_AsUnsignedLongLong(pyObj_pipeline_batch);
    }

    PyObject* pyObj_pipeline_cap = PyDict_GetItemString(pyObj_query_args, "pipeline_cap");
    if (nullptr != pyObj_pipeline_cap) {
        req.pipeline_cap = PyLong_AsUnsignedLongLong(pyObj_pipeline_cap);
    }

    PyObject* pyObj_scan_consistency = PyDict_GetItemString(pyObj_query_args, "scan_consistency");
    if (pyObj_scan_consistency != nullptr) {
        if (PyUnicode_Check(pyObj_scan_consistency)) {
            req.scan_consistency =
              str_to_scan_consistency_type<couchbase::query_scan_consistency>(std::string(PyUnicode_AsUTF8(pyObj_scan_consistency)));
        } else {
            PyErr_SetString(PyExc_ValueError, "scan_consistency is not a string.");
        }
        if (PyErr_Occurred()) {
            return {};
        }
    }

    PyObject* pyObj_mutation_state = PyDict_GetItemString(pyObj_query_args, "mutation_state");
    if (pyObj_mutation_state != nullptr && PyList_Check(pyObj_mutation_state)) {
        req.mutation_state = get_mutation_state(pyObj_mutation_state);
    }

    PyObject* pyObj_query_context = PyDict_GetItemString(pyObj_query_args, "query_context");
    if (pyObj_query_context != nullptr) {
        if (PyUnicode_Check(pyObj_query_context)) {
            req.query_context = std::string(PyUnicode_AsUTF8(pyObj_query_context));
        } else {
            PyErr_SetString(PyExc_ValueError, "query_context is not a string.");
            return {};
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(pyObj_query_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        if (PyUnicode_Check(pyObj_client_context_id)) {
            req.client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        } else {
            PyErr_SetString(PyExc_ValueError, "client_context_id is not a string.");
            return {};
        }
    }

    PyObject* pyObj_timeout = PyDict_GetItemString(pyObj_query_args, "timeout");
    if (nullptr != pyObj_timeout) {
        // comes in as microseconds
        req.timeout = std::chrono::milliseconds(PyLong_AsUnsignedLongLong(pyObj_timeout) / 1000ULL);
    } else {
        req.timeout = couchbase::core::timeout_defaults::query_timeout;
    }

    PyObject* pyObj_profile_mode = PyDict_GetItemString(pyObj_query_args, "profile_mode");
    if (pyObj_profile_mode != nullptr) {
        if (PyUnicode_Check(pyObj_profile_mode)) {
            req.profile = str_to_profile_mode(std::string(PyUnicode_AsUTF8(pyObj_profile_mode)));
        } else {
            PyErr_SetString(PyExc_ValueError, "profile_mode is not a string.");
        }
        if (PyErr_Occurred()) {
            return {};
        }
    }

    PyObject* pyObj_send_to_node = PyDict_GetItemString(pyObj_query_args, "send_to_node");
    if (pyObj_send_to_node != nullptr) {
        if (PyUnicode_Check(pyObj_send_to_node)) {
            req.send_to_node = std::string(PyUnicode_AsUTF8(pyObj_send_to_node));
        } else {
            PyErr_SetString(PyExc_ValueError, "send_to_node is not a string.");
            return {};
        }
    }

    PyObject* pyObj_span = PyDict_GetItemString(pyObj_query_args, "span");
    if (pyObj_span != nullptr) {
        req.parent_span = std::make_shared<pycbc::request_span>(pyObj_span);
    }

    PyObject* pyObj_raw = PyDict_GetItemString(pyObj_query_args, "raw");
    std::map<std::string, couchbase::core::json_string, std::less<>> raw_options{};
    if (pyObj_raw && PyDict_Check(pyObj_raw)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            } else {
                PyErr_SetString(PyExc_ValueError, "Raw option key is not a string.  The raw option should be a dict[str, JSONString].");
                return {};
            }
            if (k.empty()) {
                PyErr_SetString(PyExc_ValueError, "Raw option key is empty!  The raw option should be a dict[str, JSONString].");
                return {};
            }

            if (PyBytes_Check(pyObj_value)) {
                try {
                    auto res = PyObject_to_binary(pyObj_value);
                    // this will crash b/c txns query_options expects a std::vector<std::byte>
                    // auto res = std::string(PyBytes_AsString(pyObj_value));
                    raw_options.emplace(k, couchbase::core::json_string{ std::move(res) });
                } catch (const std::exception& e) {
                    PyErr_SetString(PyExc_ValueError,
                                    "Unable to parse raw option value.  The raw option should be a dict[str, JSONString].");
                }
            } else {
                PyErr_SetString(PyExc_ValueError, "Raw option value not a string.  The raw option should be a dict[str, JSONString].");
                return {};
            }
        }
    }
    if (raw_options.size() > 0) {
        req.raw = raw_options;
    }

    PyObject* pyObj_positional_parameters = PyDict_GetItemString(pyObj_query_args, "positional_parameters");
    std::vector<couchbase::core::json_string> positional_parameters{};
    if (pyObj_positional_parameters && PyList_Check(pyObj_positional_parameters)) {
        size_t nargs = static_cast<size_t>(PyList_Size(pyObj_positional_parameters));
        size_t ii;
        for (ii = 0; ii < nargs; ++ii) {
            PyObject* pyOb_param = PyList_GetItem(pyObj_positional_parameters, ii);
            if (!pyOb_param) {
                PyErr_SetString(PyExc_ValueError, "Unable to parse positional parameter.");
                return {};
            }
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyOb_param);
            if (PyBytes_Check(pyOb_param)) {
                try {
                    auto res = PyObject_to_binary(pyOb_param);
                    positional_parameters.push_back(couchbase::core::json_string{ std::move(res) });
                } catch (const std::exception& e) {
                    PyErr_SetString(
                      PyExc_ValueError,
                      "Unable to parse positional paramter option value. Positional parameter options must all be json strings.");
                }
            } else {
                PyErr_SetString(PyExc_ValueError,
                                "Unable to parse positional parameter.  Positional parameter options must all be json strings.");
                return {};
            }
            Py_DECREF(pyOb_param);
            pyOb_param = nullptr;
        }
    }
    if (positional_parameters.size() > 0) {
        req.positional_parameters = positional_parameters;
    }

    PyObject* pyObj_named_parameters = PyDict_GetItemString(pyObj_query_args, "named_parameters");
    std::map<std::string, couchbase::core::json_string, std::less<>> named_parameters{};
    if (pyObj_named_parameters && PyDict_Check(pyObj_named_parameters)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_named_parameters, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            } else {
                PyErr_SetString(PyExc_ValueError,
                                "Named parameter key is not a string.  Named parameters should be a dict[str, JSONString].");
                return {};
            }
            if (k.empty()) {
                PyErr_SetString(PyExc_ValueError, "Named parameter key is empty. Named parameters should be a dict[str, JSONString].");
                return {};
            }
            if (PyBytes_Check(pyObj_value)) {
                try {
                    auto res = PyObject_to_binary(pyObj_value);
                    named_parameters.emplace(k, couchbase::core::json_string{ std::move(res) });
                } catch (const std::exception& e) {
                    PyErr_SetString(PyExc_ValueError,
                                    "Unable to parse named parameter option.  Named parameters should be a dict[str, JSONString].");
                }
            } else {
                PyErr_SetString(PyExc_ValueError,
                                "Named parameter value not a string.  Named parameters should be a dict[str, JSONString].");
                return {};
            }
        }
    }
    if (named_parameters.size() > 0) {
        req.named_parameters = named_parameters;
    }

    return req;
}

std::vector<couchbase::mutation_token>
get_mutation_state(PyObject* pyObj_mutation_state)
{
    std::vector<couchbase::mutation_token> mut_state{};
    size_t ntokens = static_cast<size_t>(PyList_GET_SIZE(pyObj_mutation_state));
    for (size_t ii = 0; ii < ntokens; ++ii) {

        PyObject* pyObj_mut_token = PyList_GetItem(pyObj_mutation_state, ii);
        PyObject* pyObj_bucket_name = PyDict_GetItemString(pyObj_mut_token, "bucket_name");
        auto bucket_name = std::string{ PyUnicode_AsUTF8(pyObj_bucket_name) };

        PyObject* pyObj_partition_uuid = PyDict_GetItemString(pyObj_mut_token, "partition_uuid");
        auto partition_uuid = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_partition_uuid));

        PyObject* pyObj_sequence_number = PyDict_GetItemString(pyObj_mut_token, "sequence_number");
        auto sequence_number = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_sequence_number));

        PyObject* pyObj_partition_id = PyDict_GetItemString(pyObj_mut_token, "partition_id");
        auto partition_id = static_cast<uint16_t>(PyLong_AsUnsignedLong(pyObj_partition_id));

        auto token = couchbase::mutation_token{ partition_uuid, sequence_number, partition_id, bucket_name };
        mut_state.emplace_back(token);
    }
    return mut_state;
}
