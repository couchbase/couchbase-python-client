/*
 *   Copyright 2016-2023. Couchbase, Inc.
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

#include "logger.hxx"

#include "exceptions.hxx"

static void
pycbc_logger_dealloc(pycbc_logger* self)
{
  Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject*
pycbc_logger__configure_logging_sink__(PyObject* self, PyObject* args, PyObject* kwargs)
{
  auto logger = reinterpret_cast<pycbc_logger*>(self);
  PyObject* pyObj_logger = nullptr;
  PyObject* pyObj_level = nullptr;
  const char* kw_list[] = { "logger", "level", nullptr };
  const char* kw_format = "OO";
  if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, kw_format, const_cast<char**>(kw_list), &pyObj_logger, &pyObj_level)) {
    pycbc_set_python_exception(PycbcError::InvalidArgument,
                               __FILE__,
                               __LINE__,
                               "Cannot set pycbc_logger sink.  Unable to parse args/kwargs.");
    return nullptr;
  }

  if (couchbase::core::logger::is_initialized()) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Cannot create logger.  Another logger has already been "
                               "initialized. Make sure the PYCBC_LOG_LEVEL env "
                               "variable is not set if using configure_logging.");
    return nullptr;
  }

  if (pyObj_logger != nullptr) {
    logger->logger_sink_ = std::make_shared<pycbc_logger_sink>(pyObj_logger);
  }

  couchbase::core::logger::configuration logger_settings;
  logger_settings.console = false;
  logger_settings.sink = logger->logger_sink_;
  auto level = convert_python_log_level(pyObj_level);
  logger_settings.log_level = level;
  couchbase::core::logger::create_file_logger(logger_settings);
  Py_RETURN_NONE;
}

PyObject*
pycbc_logger__create_logger__(PyObject* self, PyObject* args, PyObject* kwargs)
{
  auto logger = reinterpret_cast<pycbc_logger*>(self);
  char* log_level = nullptr;
  char* log_filename = nullptr;
  int enable_console = 0;
  const char* kw_list[] = { "level", "filename", "enable_console", nullptr };
  const char* kw_format = "s|si";
  if (!PyArg_ParseTupleAndKeywords(args,
                                   kwargs,
                                   kw_format,
                                   const_cast<char**>(kw_list),
                                   &log_level,
                                   &log_filename,
                                   &enable_console)) {
    pycbc_set_python_exception(PycbcError::InvalidArgument,
                               __FILE__,
                               __LINE__,
                               "Cannot create logger.  Unable to parse args/kwargs.");
    return nullptr;
  }

  if (couchbase::core::logger::is_initialized()) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation,
      __FILE__,
      __LINE__,
      "Cannot create logger.  Another logger has already been initialized.");
    return nullptr;
  }

  if (log_level == nullptr) {
    pycbc_set_python_exception(PycbcError::InvalidArgument,
                               __FILE__,
                               __LINE__,
                               "Cannot create logger.  Unable to determine log level.");
    return nullptr;
  }
  auto level = couchbase::core::logger::level_from_str(log_level);
  if (log_filename != nullptr) {
    couchbase::core::logger::configuration configuration{};
    configuration.filename = std::string{ log_filename };
    configuration.log_level = level;
    configuration.console = enable_console > 0;
    couchbase::core::logger::create_file_logger(configuration);
    logger->is_file_logger = true;
  } else {
    couchbase::core::logger::create_console_logger();
    couchbase::core::logger::set_log_levels(level);
    logger->is_console_logger = true;
  }
  Py_RETURN_NONE;
}

PyObject*
pycbc_logger__enable_protocol_logger__(PyObject* self, PyObject* args, PyObject* kwargs)
{
  char* filename = nullptr;
  const char* kw_list[] = { "filename", nullptr };
  const char* kw_format = "s";
  if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, kw_format, const_cast<char**>(kw_list), &filename)) {
    pycbc_set_python_exception(PycbcError::InvalidArgument,
                               __FILE__,
                               __LINE__,
                               "Cannot enable the protocol logger.  Unable to parse args/kwargs.");
    return nullptr;
  }
  couchbase::core::logger::configuration configuration{};
  configuration.filename = std::string{ filename };
  couchbase::core::logger::create_protocol_logger(configuration);
  Py_RETURN_NONE;
}

PyObject*
pycbc_logger__is_console_logger__(PyObject* self, PyObject* Py_UNUSED(ignored))
{
  auto logger = reinterpret_cast<pycbc_logger*>(self);
  if (logger->is_console_logger) {
    Py_INCREF(Py_True);
    return Py_True;
  } else {
    Py_INCREF(Py_False);
    return Py_False;
  }
}

PyObject*
pycbc_logger__is_file_logger__(PyObject* self, PyObject* Py_UNUSED(ignored))
{
  auto logger = reinterpret_cast<pycbc_logger*>(self);
  if (logger->is_file_logger) {
    Py_INCREF(Py_True);
    return Py_True;
  } else {
    Py_INCREF(Py_False);
    return Py_False;
  }
}

static PyMethodDef pycbc_logger_methods[] = {
  { "configure_logging_sink",
    (PyCFunction)pycbc_logger__configure_logging_sink__,
    METH_VARARGS | METH_KEYWORDS,
    PyDoc_STR("Configure logger's logging sink") },
  { "create_logger",
    (PyCFunction)pycbc_logger__create_logger__,
    METH_VARARGS | METH_KEYWORDS,
    PyDoc_STR("Create a C++ core logger") },
  { "enable_protocol_logger",
    (PyCFunction)pycbc_logger__enable_protocol_logger__,
    METH_VARARGS | METH_KEYWORDS,
    PyDoc_STR("Enables the protocol logger") },
  { "is_console_logger",
    (PyCFunction)pycbc_logger__is_console_logger__,
    METH_NOARGS,
    PyDoc_STR("Check if logger is console logger or not") },
  { "is_file_logger",
    (PyCFunction)pycbc_logger__is_file_logger__,
    METH_NOARGS,
    PyDoc_STR("Check if logger is file logger or not") },
  { NULL }
};

static PyObject*
pycbc_logger_new(PyTypeObject* type, PyObject*, PyObject*)
{
  pycbc_logger* self = reinterpret_cast<pycbc_logger*>(type->tp_alloc(type, 0));
  return reinterpret_cast<PyObject*>(self);
}

static PyTypeObject
init_pycbc_logger_type()
{
  PyTypeObject obj = {};
  obj.ob_base = PyVarObject_HEAD_INIT(NULL, 0) obj.tp_name = "pycbc_core.pycbc_logger";
  obj.tp_doc = PyDoc_STR("Python SDK Logger");
  obj.tp_basicsize = sizeof(pycbc_logger);
  obj.tp_itemsize = 0;
  obj.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  obj.tp_new = pycbc_logger_new;
  obj.tp_dealloc = (destructor)pycbc_logger_dealloc;
  obj.tp_methods = pycbc_logger_methods;
  return obj;
}

static PyTypeObject pycbc_logger_type = init_pycbc_logger_type();

size_t
convert_spdlog_level(spdlog::level::level_enum lvl)
{
  // TODO:  support trace level in the python logger
  switch (lvl) {
    case spdlog::level::level_enum::off:
      return 0;
    case spdlog::level::level_enum::trace:
      return 5;
    case spdlog::level::level_enum::debug:
      return 10;
    case spdlog::level::level_enum::info:
      return 20;
    case spdlog::level::level_enum::warn:
      return 30;
    case spdlog::level::level_enum::err:
      return 40;
    case spdlog::level::level_enum::critical:
      return 50;
    default:
      return 0;
  }
}

couchbase::core::logger::level
convert_python_log_level(PyObject* level)
{
  auto lvl = PyLong_AsSize_t(level);
  switch (lvl) {
    case 0:
      return couchbase::core::logger::level::off;
    case 5:
      return couchbase::core::logger::level::trace;
    case 10:
      return couchbase::core::logger::level::debug;
    case 20:
      return couchbase::core::logger::level::info;
    case 30:
      return couchbase::core::logger::level::warn;
    case 40:
      return couchbase::core::logger::level::err;
    case 50:
      return couchbase::core::logger::level::critical;
    default:
      return couchbase::core::logger::level::off;
  }
}

PyObject*
add_logger_objects(PyObject* pyObj_module)
{
  if (PyType_Ready(&pycbc_logger_type) < 0) {
    return nullptr;
  }
  Py_INCREF(&pycbc_logger_type);
  if (PyModule_AddObject(
        pyObj_module, "pycbc_logger", reinterpret_cast<PyObject*>(&pycbc_logger_type)) < 0) {
    Py_DECREF(&pycbc_logger_type);
    return nullptr;
  }
  return pyObj_module;
}
