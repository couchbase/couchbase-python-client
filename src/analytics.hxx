#pragma once

#include "client.hxx"
#include "result.hxx"
#include "n1ql.hxx"
#include <couchbase/operations/document_analytics.hxx>

streamed_result*
handle_analytics_query(PyObject* self, PyObject* args, PyObject* kwargs);
