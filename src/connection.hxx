#pragma once

#include "client.hxx"

PyObject*
handle_create_connection(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_close_connection(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_open_or_close_bucket(PyObject* self, PyObject* args, PyObject* kwargs);
