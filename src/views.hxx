#pragma once

#include "client.hxx"
#include "result.hxx"
#include <couchbase/operations/document_view.hxx>

streamed_result*
handle_view_query(PyObject* self, PyObject* args, PyObject* kwargs);
