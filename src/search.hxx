#pragma once

#include "client.hxx"
#include "result.hxx"
#include <couchbase/operations/document_search.hxx>

streamed_result*
handle_search_query(PyObject* self, PyObject* args, PyObject* kwargs);
