#pragma once

#include "client.hxx"

#define CANNOT_PARSE_CONN_ARGS_MSG(msg) "Cannot " #msg " connection.  Unable to parse args/kwargs."
#define CANNOT_PARSE_BUCKET_ARGS_MSG(msg) "Cannot " #msg " bucket.  Unable to parse args/kwargs."

PyObject*
handle_create_connection(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_close_connection(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_open_or_close_bucket(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_conn_blocking_result(std::future<PyObject*>&& fut);
