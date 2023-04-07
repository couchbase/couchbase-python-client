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

#include "search.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include "tracing.hxx"
#include "utils.hxx"
#include <core/search_highlight_style.hxx>
#include <core/search_scan_consistency.hxx>

PyObject*
get_result_row_fragments(std::map<std::string, std::vector<std::string>> fragments)
{
    PyObject* pyObj_row_fragments = PyDict_New();
    for (auto const& fragment : fragments) {

        PyObject* pyObj_fragments = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& f : fragment.second) {
            PyObject* pyObj_fragment = PyUnicode_FromString(f.c_str());
            if (-1 == PyList_Append(pyObj_fragments, pyObj_fragment)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_fragment);
        }
        if (-1 == PyDict_SetItemString(pyObj_row_fragments, fragment.first.c_str(), pyObj_fragments)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_fragments);
    }

    return pyObj_row_fragments;
}

PyObject*
get_result_row_locations(std::vector<couchbase::core::operations::search_response::search_location> locations)
{
    PyObject* pyObj_row_locations = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& location : locations) {
        PyObject* pyObj_row_location = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(location.field.c_str());
        if (-1 == PyDict_SetItemString(pyObj_row_location, "field", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(location.term.c_str());
        if (-1 == PyDict_SetItemString(pyObj_row_location, "term", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(location.position);
        if (-1 == PyDict_SetItemString(pyObj_row_location, "position", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(location.start_offset);
        if (-1 == PyDict_SetItemString(pyObj_row_location, "start", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(location.end_offset);
        if (-1 == PyDict_SetItemString(pyObj_row_location, "end", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (location.array_positions.has_value()) {
            PyObject* pyObj_array_positions = PyList_New(static_cast<Py_ssize_t>(0));
            for (auto const& array_position : location.array_positions.value()) {
                PyObject* pyObj_array_position = PyLong_FromUnsignedLongLong(array_position);
                if (-1 == PyList_Append(pyObj_array_positions, pyObj_array_position)) {
                    PyErr_Print();
                    PyErr_Clear();
                }
                Py_DECREF(pyObj_array_position);
            }

            if (-1 == PyDict_SetItemString(pyObj_row_location, "array_positions", pyObj_array_positions)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_array_positions);
        }

        if (-1 == PyList_Append(pyObj_row_locations, pyObj_row_location)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_row_location);
    }
    return pyObj_row_locations;
}

PyObject*
get_result_row(couchbase::core::operations::search_response::search_row row)
{
    PyObject* pyObj_row = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(row.index.c_str());
    if (-1 == PyDict_SetItemString(pyObj_row, "index", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(row.id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_row, "id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyFloat_FromDouble(row.score);
    if (-1 == PyDict_SetItemString(pyObj_row, "score", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (row.locations.size() > 0) {
        pyObj_tmp = get_result_row_locations(row.locations);
        if (-1 == PyDict_SetItemString(pyObj_row, "locations", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    if (row.fragments.size() > 0) {
        pyObj_tmp = get_result_row_fragments(row.fragments);
        if (-1 == PyDict_SetItemString(pyObj_row, "fragments", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyUnicode_FromString(row.fields.c_str());
    if (-1 == PyDict_SetItemString(pyObj_row, "fields", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(row.explanation.c_str());
    if (-1 == PyDict_SetItemString(pyObj_row, "explanation", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    return pyObj_row;
}

PyObject*
get_result_numeric_range_facets(
  std::vector<couchbase::core::operations::search_response::search_facet::numeric_range_facet> numeric_range_facets)
{
    PyObject* pyObj_numeric_range_facets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& numeric_range_facet : numeric_range_facets) {
        PyObject* pyObj_numeric_range_facet = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(numeric_range_facet.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_numeric_range_facet, "name", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(numeric_range_facet.count);
        if (-1 == PyDict_SetItemString(pyObj_numeric_range_facet, "count", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (std::holds_alternative<std::uint64_t>(numeric_range_facet.min)) {
            pyObj_tmp = PyLong_FromUnsignedLongLong(std::get<std::uint64_t>(numeric_range_facet.min));
            if (-1 == PyDict_SetItemString(pyObj_numeric_range_facet, "min", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        } else if (std::holds_alternative<double>(numeric_range_facet.min)) {
            pyObj_tmp = PyFloat_FromDouble(std::get<double>(numeric_range_facet.min));
            if (-1 == PyDict_SetItemString(pyObj_numeric_range_facet, "min", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (std::holds_alternative<std::uint64_t>(numeric_range_facet.max)) {
            pyObj_tmp = PyLong_FromUnsignedLongLong(std::get<std::uint64_t>(numeric_range_facet.max));
            if (-1 == PyDict_SetItemString(pyObj_numeric_range_facet, "max", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        } else if (std::holds_alternative<double>(numeric_range_facet.max)) {
            pyObj_tmp = PyFloat_FromDouble(std::get<double>(numeric_range_facet.max));
            if (-1 == PyDict_SetItemString(pyObj_numeric_range_facet, "max", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (-1 == PyList_Append(pyObj_numeric_range_facets, pyObj_numeric_range_facet)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_numeric_range_facet);
    }

    return pyObj_numeric_range_facets;
}

PyObject*
get_result_date_range_facets(std::vector<couchbase::core::operations::search_response::search_facet::date_range_facet> date_range_facets)
{
    PyObject* pyObj_date_range_facets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& date_range_facet : date_range_facets) {
        PyObject* pyObj_date_range_facet = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(date_range_facet.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_date_range_facet, "name", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(date_range_facet.count);
        if (-1 == PyDict_SetItemString(pyObj_date_range_facet, "count", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (date_range_facet.start.has_value()) {
            pyObj_tmp = PyUnicode_FromString(date_range_facet.start.value().c_str());
            if (-1 == PyDict_SetItemString(pyObj_date_range_facet, "start", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (date_range_facet.end.has_value()) {
            pyObj_tmp = PyUnicode_FromString(date_range_facet.end.value().c_str());
            if (-1 == PyDict_SetItemString(pyObj_date_range_facet, "end", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (-1 == PyList_Append(pyObj_date_range_facets, pyObj_date_range_facet)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_date_range_facet);
    }

    return pyObj_date_range_facets;
}

PyObject*
get_result_term_facets(std::vector<couchbase::core::operations::search_response::search_facet::term_facet> term_facets)
{
    PyObject* pyObj_term_facets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& term_facet : term_facets) {
        PyObject* pyObj_term_facet = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(term_facet.term.c_str());
        if (-1 == PyDict_SetItemString(pyObj_term_facet, "term", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(term_facet.count);
        if (-1 == PyDict_SetItemString(pyObj_term_facet, "count", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (-1 == PyList_Append(pyObj_term_facets, pyObj_term_facet)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_term_facet);
    }

    return pyObj_term_facets;
}

PyObject*
get_result_facets(std::vector<couchbase::core::operations::search_response::search_facet> facets)
{
    PyObject* pyObj_facets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& facet : facets) {
        PyObject* pyObj_facet = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(facet.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_facet, "name", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(facet.field.c_str());
        if (-1 == PyDict_SetItemString(pyObj_facet, "field", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(facet.total);
        if (-1 == PyDict_SetItemString(pyObj_facet, "total", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(facet.missing);
        if (-1 == PyDict_SetItemString(pyObj_facet, "missing", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(facet.other);
        if (-1 == PyDict_SetItemString(pyObj_facet, "other", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (facet.terms.size() > 0) {
            pyObj_tmp = get_result_term_facets(facet.terms);
            if (-1 == PyDict_SetItemString(pyObj_facet, "terms", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (facet.date_ranges.size() > 0) {
            pyObj_tmp = get_result_date_range_facets(facet.date_ranges);
            if (-1 == PyDict_SetItemString(pyObj_facet, "date_ranges", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (facet.numeric_ranges.size() > 0) {
            pyObj_tmp = get_result_numeric_range_facets(facet.numeric_ranges);
            if (-1 == PyDict_SetItemString(pyObj_facet, "numeric_ranges", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);
        }

        if (-1 == PyList_Append(pyObj_facets, pyObj_facet)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_facet);
    }

    return pyObj_facets;
}

PyObject*
get_result_metrics(couchbase::core::operations::search_response::search_metrics metrics)
{
    PyObject* pyObj_metrics = PyDict_New();
    std::chrono::duration<unsigned long long, std::nano> int_nsec = metrics.took;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
    if (-1 == PyDict_SetItemString(pyObj_metrics, "took", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.total_rows);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "total_rows", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyFloat_FromDouble(metrics.max_score);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "max_score", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.success_partition_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "success_partition_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.error_partition_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "error_partition_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    return pyObj_metrics;
}

PyObject*
get_result_metadata(couchbase::core::operations::search_response::search_meta_data metadata, bool include_metrics)
{
    PyObject* pyObj_metadata = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(metadata.client_context_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "client_context_id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (include_metrics) {
        PyObject* pyObject_metrics = get_result_metrics(metadata.metrics);
        if (-1 == PyDict_SetItemString(pyObj_metadata, "metrics", pyObject_metrics)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObject_metrics);
    }

    PyObject* pyObj_errors = PyDict_New();
    for (auto const& error : metadata.errors) {
        PyObject* pyObj_value = PyUnicode_FromString(error.second.c_str());
        if (-1 == PyDict_SetItemString(pyObj_errors, error.first.c_str(), pyObj_value)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_value);
    }
    if (-1 == PyDict_SetItemString(pyObj_metadata, "errors", pyObj_errors)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_errors);

    return pyObj_metadata;
}

result*
create_result_from_search_response(couchbase::core::operations::search_response resp, bool include_metrics)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_payload = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(resp.status.c_str());
    if (-1 == PyDict_SetItemString(pyObj_payload, "status", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(resp.error.c_str());
    if (-1 == PyDict_SetItemString(pyObj_payload, "error", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObject_metadata = get_result_metadata(resp.meta, include_metrics);
    if (-1 == PyDict_SetItemString(pyObj_payload, "metadata", pyObject_metadata)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObject_metadata);

    if (resp.facets.size() > 0) {
        pyObj_tmp = get_result_facets(resp.facets);
        if (-1 == PyDict_SetItemString(pyObj_payload, "facets", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    // if (resp.rows.size() > 0) {
    //     pyObj_tmp = get_result_rows(resp.rows);
    //     if (-1 == PyDict_SetItemString(pyObj_payload, "rows", pyObj_tmp)) {
    //         PyErr_Print();
    //         PyErr_Clear();
    //     }
    //     Py_DECREF(pyObj_tmp);
    // }

    if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_payload)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_payload);

    return res;
}

void
create_search_result(couchbase::core::operations::search_response resp,
                     std::shared_ptr<rows_queue<PyObject*>> rows,
                     PyObject* pyObj_callback,
                     PyObject* pyObj_errback,
                     bool include_metrics)
{
    auto set_exception = false;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();
    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing full text search operation.");
        // lets clear any errors
        PyErr_Clear();
        rows->put(pyObj_exc);
    } else {
        for (auto const& row : resp.rows) {
            PyObject* pyObj_row = get_result_row(row);
            rows->put(pyObj_row);
        }

        auto res = create_result_from_search_response(resp, include_metrics);
        if (res == nullptr || PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else {
            // None indicates done (i.e. raise StopIteration)
            Py_INCREF(Py_None);
            rows->put(Py_None);
            rows->put(reinterpret_cast<PyObject*>(res));
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Full text search operation error.");
        rows->put(pyObj_exc);
    }

    // This is for txcouchbase -- let it knows we're done w/ the FTS request
    if (pyObj_callback != nullptr) {
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Full text search complete callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

std::map<std::string, std::string>
get_facets(PyObject* pyObj_facets)
{
    std::map<std::string, std::string> facets{};
    if (pyObj_facets && PyDict_Check(pyObj_facets)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_facets, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_value));
                facets.emplace(k, res);
            }
        }
    }
    return facets;
}

std::map<std::string, couchbase::core::json_string>
get_raw_options(PyObject* pyObj_raw)
{
    std::map<std::string, couchbase::core::json_string> raw_options{};
    if (pyObj_raw && PyDict_Check(pyObj_raw)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_value));
                raw_options.emplace(k, couchbase::core::json_string{ std::move(res) });
            }
        }
    }
    return raw_options;
}

couchbase::core::operations::search_request
get_search_request(PyObject* op_args)
{
    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));

    PyObject* pyObj_query = PyDict_GetItemString(op_args, "query");
    auto query = std::string(PyUnicode_AsUTF8(pyObj_query));

    couchbase::core::operations::search_request req{ index_name, couchbase::core::json_string{ std::move(query) } };

    PyObject* pyObj_limit = PyDict_GetItemString(op_args, "limit");
    if (pyObj_limit != nullptr) {
        auto limit = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_limit));
        req.limit = limit;
    }

    PyObject* pyObj_skip = PyDict_GetItemString(op_args, "skip");
    if (pyObj_skip != nullptr) {
        auto skip = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_skip));
        req.skip = skip;
    }

    PyObject* pyObj_explain = PyDict_GetItemString(op_args, "explain");
    if (pyObj_explain != nullptr && pyObj_explain == Py_True) {
        req.explain = true;
    }

    PyObject* pyObj_disable_scoring = PyDict_GetItemString(op_args, "disable_scoring");
    if (pyObj_disable_scoring != nullptr && pyObj_disable_scoring == Py_True) {
        req.disable_scoring = true;
    }

    PyObject* pyObj_include_locations = PyDict_GetItemString(op_args, "include_locations");
    if (pyObj_include_locations != nullptr && pyObj_include_locations == Py_True) {
        req.include_locations = true;
    }

    PyObject* pyObj_highlight_style = PyDict_GetItemString(op_args, "highlight_style");
    if (pyObj_highlight_style != nullptr) {
        auto highlight_style = std::string(PyUnicode_AsUTF8(pyObj_highlight_style));
        if (highlight_style.compare("html") == 0) {
            req.highlight_style = couchbase::core::search_highlight_style::html;
        } else if (highlight_style.compare("ansi") == 0) {
            req.highlight_style = couchbase::core::search_highlight_style::ansi;
        }
    }

    PyObject* pyObj_highlight_fields = PyDict_GetItemString(op_args, "highlight_fields");
    if (pyObj_highlight_fields != nullptr && PyList_Check(pyObj_highlight_fields)) {
        size_t nfields = static_cast<size_t>(PyList_GET_SIZE(pyObj_highlight_fields));
        std::vector<std::string> fields{};
        size_t ii;
        for (ii = 0; ii < nfields; ++ii) {
            PyObject* pyObj_field = PyList_GetItem(pyObj_highlight_fields, ii);
            auto field = std::string(PyUnicode_AsUTF8(pyObj_field));
            fields.push_back(field);
        }

        if (fields.size() > 0) {
            req.highlight_fields = fields;
        }
    }

    PyObject* pyObj_fields = PyDict_GetItemString(op_args, "fields");
    if (pyObj_fields != nullptr && PyList_Check(pyObj_fields)) {
        size_t nfields = static_cast<size_t>(PyList_GET_SIZE(pyObj_fields));
        std::vector<std::string> fields{};
        size_t ii;
        for (ii = 0; ii < nfields; ++ii) {
            PyObject* pyObj_field = PyList_GetItem(pyObj_fields, ii);
            auto field = std::string(PyUnicode_AsUTF8(pyObj_field));
            fields.push_back(field);
        }

        if (fields.size() > 0) {
            req.fields = fields;
        }
    }

    PyObject* pyObj_collections = PyDict_GetItemString(op_args, "collections");
    if (pyObj_collections != nullptr && PyList_Check(pyObj_collections)) {
        size_t ncollections = static_cast<size_t>(PyList_GET_SIZE(pyObj_collections));
        std::vector<std::string> collections{};
        size_t ii;
        for (ii = 0; ii < ncollections; ++ii) {
            PyObject* pyObj_collection = PyList_GetItem(pyObj_collections, ii);
            auto collection = std::string(PyUnicode_AsUTF8(pyObj_collection));
            collections.push_back(collection);
        }

        if (collections.size() > 0) {
            req.collections = collections;
        }
    }

    PyObject* pyObj_scan_consistency = PyDict_GetItemString(op_args, "scan_consistency");
    if (pyObj_scan_consistency != nullptr) {
        auto scan_consistency = std::string(PyUnicode_AsUTF8(pyObj_scan_consistency));
        if (scan_consistency.compare("not_bounded") == 0) {
            req.scan_consistency = couchbase::core::search_scan_consistency::not_bounded;
        }
    }

    PyObject* pyObj_mutation_state = PyDict_GetItemString(op_args, "mutation_state");
    if (pyObj_mutation_state != nullptr && PyList_Check(pyObj_mutation_state)) {
        req.mutation_state = get_mutation_state(pyObj_mutation_state);
    }

    PyObject* pyObj_sort_specs = PyDict_GetItemString(op_args, "sort_specs");
    if (pyObj_sort_specs != nullptr && PyList_Check(pyObj_sort_specs)) {
        size_t nspecs = static_cast<size_t>(PyList_GET_SIZE(pyObj_sort_specs));
        std::vector<std::string> sort_specs{};
        size_t ii;
        for (ii = 0; ii < nspecs; ++ii) {
            PyObject* pyObj_spec = PyList_GetItem(pyObj_sort_specs, ii);
            auto spec = std::string(PyUnicode_AsUTF8(pyObj_spec));
            sort_specs.push_back(spec);
        }

        if (sort_specs.size() > 0) {
            req.sort_specs = sort_specs;
        }
    }

    PyObject* pyObj_facets = PyDict_GetItemString(op_args, "facets");
    if (pyObj_facets != nullptr) {
        auto facets = get_facets(pyObj_facets);
        if (facets.size() > 0) {
            req.facets = facets;
        }
    }

    PyObject* pyObj_raw = PyDict_GetItemString(op_args, "raw");
    if (pyObj_raw != nullptr) {
        auto raw_options = get_raw_options(pyObj_raw);
        if (raw_options.size() > 0) {
            req.raw = raw_options;
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::search_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        if (0 < timeout) {
            timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        }
    }
    req.timeout = timeout_ms;

    return req;
}

streamed_result*
handle_search_query([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    // optional
    PyObject* pyObj_op_args = nullptr;
    PyObject* pyObj_serializer = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_row_callback = nullptr;
    PyObject* pyObj_span = nullptr;

    static const char* kw_list[] = { "conn", "op_args", "serializer", "callback", "errback", "row_callback", "span", nullptr };

    const char* kw_format = "O!|OOOOOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &pyObj_op_args,
                                          &pyObj_serializer,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_row_callback,
                                          &pyObj_span);
    if (!ret) {
        PyErr_Print();
        PyErr_SetString(PyExc_ValueError, "Unable to parse arguments");
        return nullptr;
    }

    connection* conn = nullptr;
    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        PyErr_SetString(PyExc_ValueError, "passed null connection");
        return nullptr;
    }
    PyErr_Clear();

    auto req = get_search_request(pyObj_op_args);
    bool include_metrics = true;
    PyObject* pyObj_metrics = PyDict_GetItemString(pyObj_op_args, "metrics");
    if (pyObj_metrics != nullptr && pyObj_metrics == Py_False) {
        include_metrics = false;
    }
    if (nullptr != pyObj_span) {
        req.parent_span = std::make_shared<pycbc::request_span>(pyObj_span);
    }

    // timeout is always set either to default, or timeout provided in options
    streamed_result* streamed_res = create_streamed_result_obj(req.timeout.value());

    // TODO:  let the couchbase++ streaming stabilize a bit more...
    // req.row_callback = [rows = streamed_res->rows](std::string&& row) {
    //     PyGILState_STATE state = PyGILState_Ensure();
    //     PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
    //     rows->put(pyObj_row);
    //     PyGILState_Release(state);
    //     return couchbase::core::utils::json::stream_control::next_row;
    // };

    // we need the callback, errback, and logic to all stick around, so...
    // use XINCREF b/c they _could_ be NULL
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_callback);

    Py_BEGIN_ALLOW_THREADS conn->cluster_->execute(
      req, [rows = streamed_res->rows, pyObj_callback, pyObj_errback, include_metrics](couchbase::core::operations::search_response resp) {
          create_search_result(resp, rows, pyObj_callback, pyObj_errback, include_metrics);
      });
    Py_END_ALLOW_THREADS return streamed_res;
}
