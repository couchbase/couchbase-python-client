#pragma once

#include "client.hxx"
#include "result.hxx"
#include <couchbase/operations/document_query.hxx>

streamed_result*
handle_n1ql_query(PyObject* self, PyObject* args, PyObject* kwargs);

template<typename scan_consistency_type>
scan_consistency_type
str_to_scan_consistency_type(std::string consistency)
{
    if (consistency.compare("not_bounded") == 0) {
        return scan_consistency_type::not_bounded;
    }
    if (consistency.compare("request_plus") == 0) {
        return scan_consistency_type::request_plus;
    }

    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, "Invalid Scan Consistency type.");
    return {};
}

std::string
scan_consistency_type_to_string(couchbase::operations::query_request::scan_consistency_type consistency);

std::vector<couchbase::mutation_token>
get_mutation_state(PyObject* pyObj_mutation_state);
