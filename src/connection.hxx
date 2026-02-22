/*
 *   Copyright 2016-2026. Couchbase, Inc.
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

#pragma once

#include "Python.h"
#include "error_contexts.hxx"
#include "exceptions.hxx"
#include "operations_autogen.hxx"
#include "pytocbpp_defs.hxx"
#include "result.hxx"
#include "utils.hxx"
#include <asio/io_context.hpp>
#include <core/cluster.hxx>
#include <future>
#include <list>
#include <memory>
#include <thread>
#include <utility>

namespace pycbc
{

class Connection
{
public:
  Connection(int num_io_threads = 1);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  Connection(Connection&&) = delete;
  Connection& operator=(Connection&&) = delete;

  couchbase::core::cluster& cluster()
  {
    return cluster_;
  }

  bool is_connected() const
  {
    return connected_;
  }

  PyObject* connect(PyObject* kwargs);
  PyObject* close(PyObject* kwargs);
  PyObject* open_bucket(PyObject* kwargs);
  PyObject* close_bucket(PyObject* kwargs);
  PyObject* update_credentials(PyObject* kwargs);
  PyObject* diagnostics(PyObject* kwargs);
  PyObject* ping(PyObject* kwargs);
  PyObject* get_connection_info();
  PyObject* handle_range_scan_op(PyObject* kwargs);

  std::pair<std::optional<std::string>, std::optional<std::string>> get_cluster_labels();
  PyObject* get_cluster_labels_as_py_object();

  template<typename Request>
  PyObject* execute_kv_op(PyObject* kwargs);

  template<typename Request>
  PyObject* execute_mgmt_op(PyObject* kwargs);

  template<typename Request>
  PyObject* execute_streaming_op(PyObject* kwargs);

  // execute_multi_op is public so it can be called by handle_multi_op() in pycbc_connection.hxx
  template<typename Request>
  PyObject* execute_multi_op(PyObject* doc_list,
                             PyObject* op_args,
                             PyObject* per_key_args,
                             PyObject* bucket,
                             PyObject* scope,
                             PyObject* collection);

private:
  enum class connection_state_action {
    no_change,
    set_connected,
    set_disconnected
  };

  asio::io_context io_;
  couchbase::core::cluster cluster_;
  std::list<std::thread> io_threads_;

  bool connected_;

  void handle_connection_operation_callback(
    std::error_code ec,
    const char* operation,
    PyObject* pyObj_callback,
    PyObject* pyObj_errback,
    std::shared_ptr<std::promise<PyObject*>> barrier,
    connection_state_action state_action = connection_state_action::no_change);

  template<typename Response>
  void handle_cluster_operation_callback(const Response& resp,
                                         PyObject* pyObj_callback,
                                         PyObject* pyObj_errback,
                                         std::shared_ptr<std::promise<PyObject*>> barrier)
  {
    PyGILState_STATE state = PyGILState_Ensure();

    PyObject* pyObj_resp = cbpp_to_py<Response>(resp);
    PyObject* result = create_pycbc_result(pyObj_resp);
    Py_XDECREF(pyObj_resp);

    if (pyObj_callback != nullptr) {
      PyObject* ret = PyObject_CallFunctionObjArgs(pyObj_callback, result, nullptr);
      Py_XDECREF(ret);
      Py_XDECREF(result);
    } else if (barrier) {
      barrier->set_value(result);
    } else {
      Py_XDECREF(result);
    }
    Py_XDECREF(pyObj_callback);
    Py_XDECREF(pyObj_errback);
    PyGILState_Release(state);
  }

  template<typename Response>
  static std::pair<bool, PyObject*> process_streaming_kv_result(const Response& resp)
  {
    pycbc_streamed_result* streamed_res =
      create_pycbc_streamed_result(couchbase::core::timeout_defaults::key_value_durable_timeout);

    bool conversion_failed = false;
    for (const auto& entry : resp.entries) {
      PyObject* entry_dict = cbpp_to_py(entry);
      if (entry_dict == nullptr) {
        conversion_failed = true;
        break;
      }

      PyObject* result_obj = create_pycbc_result(entry_dict);
      Py_DECREF(entry_dict);

      if (result_obj == nullptr) {
        conversion_failed = true;
        break;
      }

      streamed_res->rows->put(result_obj);
    }

    if (conversion_failed) {
      Py_DECREF(streamed_res);
      PyObject* conversion_error =
        get_exception_as_object("Failed to convert entry", __FILE__, __LINE__);
      return { true, conversion_error };
    }

    Py_INCREF(Py_None);
    streamed_res->rows->put(Py_None);

    return { false, reinterpret_cast<PyObject*>(streamed_res) };
  }

  template<typename Response>
  static PyObject* build_stream_end_result_obj(const Response& resp)
  {
    PyObject* result_dict = PyDict_New();
    if (result_dict == nullptr) {
      return nullptr;
    }

    PyObject* metadata = cbpp_to_py(resp.meta);
    if (metadata) {
      PyDict_SetItemString(result_dict, "metadata", metadata);
      Py_DECREF(metadata);
    }

    // Add facets (search only)
    if constexpr (std::is_same_v<Response, couchbase::core::operations::search_response>) {
      PyObject* facets = cbpp_to_py(resp.facets);
      if (facets) {
        PyDict_SetItemString(result_dict, "facets", facets);
        Py_DECREF(facets);
      }
    }

    PyObject* result_obj = create_pycbc_result(result_dict);
    Py_DECREF(result_dict);
    return result_obj;
  }

  template<typename Request>
  void execute_op(
    const Request& req,
    PyObject* pyObj_callback,
    PyObject* pyObj_errback,
    std::shared_ptr<std::promise<PyObject*>> barrier = nullptr,
    std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span = nullptr)
  {
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.execute(
        req, [pyObj_callback, pyObj_errback, barrier, wrapper_span, this](response_type resp) {
          if (wrapper_span != nullptr) {
            wrapper_span->end();
            if (auto retries = get_cbpp_retries(resp.ctx); retries > 0 && wrapper_span) {
              wrapper_span->add_tag("retries", retries);
            }
          }
          PyGILState_STATE state = PyGILState_Ensure();
          PyObject* result = nullptr;

          PyObject* error =
            build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Operation failed");

          if (error) {
            result = error;
            add_core_span<pycbc_exception>(result, wrapper_span);
            if (pyObj_errback != nullptr) {
              PyObject* ret = PyObject_CallFunctionObjArgs(pyObj_errback, result, nullptr);
              Py_XDECREF(ret);
              Py_XDECREF(result);
            } else if (barrier) {
              barrier->set_value(result);
            } else {
              Py_XDECREF(result);
            }
          } else {
            if constexpr (is_streaming_kv_op<Request>::value) {
              auto [is_error, result_obj] = process_streaming_kv_result(resp);

              if (is_error) {
                result = result_obj;
                add_core_span<pycbc_exception>(result, wrapper_span);
                if (pyObj_errback != nullptr) {
                  PyObject* ret = PyObject_CallFunctionObjArgs(pyObj_errback, result, nullptr);
                  Py_XDECREF(ret);
                  Py_XDECREF(result);
                } else if (barrier) {
                  barrier->set_value(result);
                } else {
                  Py_XDECREF(result);
                }
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
                PyGILState_Release(state);
                return;
              }
              result = result_obj;
              add_core_span<pycbc_streamed_result>(result, wrapper_span);
            } else {
              result = cbpp_to_py(resp);
              add_core_span<pycbc_result>(result, wrapper_span);
            }

            if (pyObj_callback != nullptr) {
              PyObject* ret = PyObject_CallFunctionObjArgs(pyObj_callback, result, nullptr);
              Py_XDECREF(ret);
              Py_XDECREF(result);
            } else if (barrier) {
              barrier->set_value(result);
            } else {
              Py_XDECREF(result);
            }
          }
          Py_XDECREF(pyObj_callback);
          Py_XDECREF(pyObj_errback);
          PyGILState_Release(state);
        });
    }
    Py_END_ALLOW_THREADS
  }

  template<typename PyType>
  void add_core_span(
    PyObject* pyObj,
    const std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>& wrapper_span)
  {
    auto cluster_labels = wrapper_span != nullptr ? get_cluster_labels()
                                                  : std::make_pair(std::optional<std::string>{},
                                                                   std::optional<std::string>{});

    if (cluster_labels.first.has_value()) {
      wrapper_span->add_tag("cluster_name", cluster_labels.first.value());
    }
    if (cluster_labels.second.has_value()) {
      wrapper_span->add_tag("cluster_uuid", cluster_labels.second.value());
    }
    PyObject* pyObj_core_span = cbpp_wrapper_span_to_py(wrapper_span);
    if (pyObj_core_span != nullptr) {
      PyType* pyObj_ptr = reinterpret_cast<PyType*>(pyObj);
      PyObject* pyObj_old_core_span = pyObj_ptr->core_span;
      pyObj_ptr->core_span = pyObj_core_span;
      // might be nice to use Py_SETREF, but if we want the limited API it is not available
      Py_XDECREF(pyObj_old_core_span);
    }
  }

  template<typename Request>
  void add_cluster_labels(const Request& req)
  {
    if (req.parent_span != nullptr) {
      auto cluster_labels = get_cluster_labels();
      if (cluster_labels.first.has_value()) {
        req.parent_span->add_tag(couchbase::core::tracing::attributes::common::cluster_name,
                                 cluster_labels.first.value());
      }
      if (cluster_labels.second.has_value()) {
        req.parent_span->add_tag(couchbase::core::tracing::attributes::common::cluster_uuid,
                                 cluster_labels.second.value());
      }
    }
  }
};

template<typename Request>
PyObject*
Connection::execute_kv_op(PyObject* kwargs)
{
  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::string span_name;
  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span;
  extract_field(kwargs, "wrapper_span_name", span_name);
  if (!span_name.empty()) {
    wrapper_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>(span_name);
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    auto req = py_to_cbpp<Request>(kwargs, wrapper_span);
    if (PyErr_Occurred()) {
      Py_XDECREF(pyObj_callback);
      Py_XDECREF(pyObj_errback);
      if (barrier) {
        barrier->set_value(nullptr);
      }
      return nullptr;
    }

    // TODO(PYCBC-1746): Delete w/ removal of legacy tracing logic
    if (wrapper_span == nullptr) {
      add_cluster_labels(req);
    }

    execute_op(req, pyObj_callback, pyObj_errback, barrier, wrapper_span);
    if (barrier) {
      PyObject* result = nullptr;
      Py_BEGIN_ALLOW_THREADS result = fut.get();
      Py_END_ALLOW_THREADS return result;
    }
    Py_RETURN_NONE;
  } catch (const std::exception& e) {
    if (barrier) {
      barrier->set_value(nullptr);
    }
    PyErr_SetString(PyExc_RuntimeError, e.what());
    Py_XDECREF(pyObj_callback);
    Py_XDECREF(pyObj_errback);
    return nullptr;
  }
}

template<typename Request>
PyObject*
Connection::execute_multi_op(PyObject* doc_list,
                             PyObject* op_args,
                             PyObject* per_key_args,
                             PyObject* pyObj_bucket,
                             PyObject* pyObj_scope,
                             PyObject* pyObj_collection)
{
  if (!PyList_Check(doc_list)) {
    raise_invalid_argument("doc_list must be a list", __FILE__, __LINE__);
    return nullptr;
  }

  std::string span_name;
  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span;
  extract_field(op_args, "wrapper_span_name", span_name);

  size_t num_docs = static_cast<size_t>(PyList_Size(doc_list));
  std::vector<std::pair<std::string, std::shared_ptr<std::promise<PyObject*>>>> operations;
  operations.reserve(num_docs);

  PyObject* pyObj_multi_result = create_pycbc_result();
  pycbc_result* multi_result = reinterpret_cast<pycbc_result*>(pyObj_multi_result);

  // INCREF bucket/scope/collection PyObjects (already PyUnicode from caller) so we can reuse
  Py_INCREF(pyObj_bucket);
  Py_INCREF(pyObj_scope);
  Py_INCREF(pyObj_collection);

  for (size_t i = 0; i < num_docs; ++i) {
    PyObject* doc_item = PyList_GetItem(doc_list, i); // Borrowed ref

    std::string key;
    PyObject* value_bytes = nullptr;
    PyObject* flags = nullptr;

    if constexpr (is_mutation_op<Request>::value) {
      // Mutation ops expect (key, (value_bytes, flags)) tuple
      // NOTE: binary [ap|pre]pend will have FMT_BYTES as flags, but the flags are not used
      if (!PyTuple_Check(doc_item) || PyTuple_Size(doc_item) != 2) {
        raise_invalid_argument(
          "Mutation doc_item must be a (key, (value, flags)) tuple", __FILE__, __LINE__);
        Py_DECREF(pyObj_multi_result);
        return nullptr;
      }

      PyObject* pyObj_key = PyTuple_GetItem(doc_item, 0);   // Borrowed ref
      PyObject* value_tuple = PyTuple_GetItem(doc_item, 1); // Borrowed ref

      if (!PyUnicode_Check(pyObj_key)) {
        raise_invalid_argument("key must be a string", __FILE__, __LINE__);
        Py_DECREF(pyObj_multi_result);
        return nullptr;
      }

      if (!PyTuple_Check(value_tuple) || PyTuple_Size(value_tuple) != 2) {
        raise_invalid_argument("value must be a (bytes, flags) tuple", __FILE__, __LINE__);
        Py_DECREF(pyObj_multi_result);
        return nullptr;
      }

      key = std::string(PyUnicode_AsUTF8(pyObj_key));
      value_bytes = PyTuple_GetItem(value_tuple, 0); // Borrowed ref
      flags = PyTuple_GetItem(value_tuple, 1);       // Borrowed ref

    } else {
      // Read ops expect key as string
      if (!PyUnicode_Check(doc_item)) {
        raise_invalid_argument("read doc_item must be a string (key)", __FILE__, __LINE__);
        Py_DECREF(pyObj_multi_result);
        return nullptr;
      }

      key = std::string(PyUnicode_AsUTF8(doc_item));
    }

    PyObject* request_kwargs = PyDict_New();
    if (!request_kwargs) {
      Py_DECREF(pyObj_bucket);
      Py_DECREF(pyObj_scope);
      Py_DECREF(pyObj_collection);
      Py_DECREF(pyObj_multi_result);
      return nullptr;
    }

    PyObject* pyObj_doc_id = PyDict_New();
    if (!pyObj_doc_id) {
      Py_DECREF(pyObj_bucket);
      Py_DECREF(pyObj_scope);
      Py_DECREF(pyObj_collection);
      Py_DECREF(pyObj_multi_result);
      Py_DECREF(request_kwargs);
      return nullptr;
    }

    PyDict_SetItemString(pyObj_doc_id, "bucket", pyObj_bucket);
    PyDict_SetItemString(pyObj_doc_id, "scope", pyObj_scope);
    PyDict_SetItemString(pyObj_doc_id, "collection", pyObj_collection);
    PyObject* pyObj_key = PyUnicode_FromString(key.c_str());
    PyDict_SetItemString(pyObj_doc_id, "key", pyObj_key);
    Py_DECREF(pyObj_key);
    PyDict_SetItemString(request_kwargs, "id", pyObj_doc_id);
    Py_DECREF(pyObj_doc_id);

    if constexpr (is_mutation_op<Request>::value) {
      if (value_bytes == nullptr || flags == nullptr) {
        raise_invalid_argument("missing value or flags for mutation operation", __FILE__, __LINE__);
        Py_DECREF(request_kwargs);
        Py_DECREF(pyObj_multi_result);
        return nullptr;
      }
      PyDict_SetItemString(request_kwargs, "value", value_bytes);
      PyDict_SetItemString(request_kwargs, "flags", flags);
    }

    // Merge base options (op_args)
    PyDict_Update(request_kwargs, op_args);

    // Check for per-key overrides and merge
    PyObject* pyObj_key_str = PyUnicode_FromString(key.c_str());
    if (per_key_args != nullptr && PyDict_Check(per_key_args)) {
      if (PyDict_Contains(per_key_args, pyObj_key_str)) {
        PyObject* key_specific_args = PyDict_GetItem(per_key_args, pyObj_key_str); // Borrowed ref
        PyDict_Update(request_kwargs, key_specific_args); // Merge, overriding base args
      }
    }
    Py_DECREF(pyObj_key_str);

    if (!span_name.empty()) {
      wrapper_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>(span_name);
    }
    try {
      auto req = py_to_cbpp<Request>(request_kwargs, wrapper_span);
      if (PyErr_Occurred()) {
        if (wrapper_span != nullptr) {
          wrapper_span->end();
        }
        std::string err_msg = "Failed to create request for multi-op for key '" + key + "'";
        PyObject* pycbc_exc =
          build_pycbc_exception_from_python_exc(err_msg.c_str(), __FILE__, __LINE__);
        if (pycbc_exc != nullptr) {
          add_core_span<pycbc_exception>(pycbc_exc, wrapper_span);
          PyDict_SetItemString(multi_result->raw_result, key.c_str(), pycbc_exc);
          Py_DECREF(pycbc_exc);
        }
      } else {
        auto barrier = std::make_shared<std::promise<PyObject*>>();
        // multi-ops are only synchronous (e.g. callback/errback = nullptr)
        execute_op(req, nullptr, nullptr, barrier, wrapper_span);
        operations.emplace_back(key, barrier);
      }
    } catch (const std::exception& e) {
      if (wrapper_span != nullptr) {
        wrapper_span->end();
      }
      // Create a Python RuntimeError from the std::exception
      PyObject* exc_msg =
        PyUnicode_FromFormat("Failed to execute operation for key '%s': %s", key.c_str(), e.what());
      PyObject* exc_args = PyTuple_Pack(1, exc_msg);
      PyObject* runtime_error = PyObject_CallObject((PyObject*)&PyExc_RuntimeError, exc_args);
      Py_DECREF(exc_msg);
      Py_DECREF(exc_args);

      if (runtime_error) {
        // Set this new Python exception as the current error
        PyErr_SetObject((PyObject*)&PyExc_RuntimeError, runtime_error);
        // PyErr_SetObject INCREFs runtime_error, so we DECREF it.
        Py_DECREF(runtime_error);

        // Reuse the existing function to build the pycbc_exception with an inner_exception
        std::string err_msg = "Failed to execute operation for key '" + key + "'";
        PyObject* pycbc_exc =
          build_pycbc_exception_from_python_exc(err_msg.c_str(), __FILE__, __LINE__);
        if (pycbc_exc) {
          add_core_span<pycbc_exception>(pycbc_exc, wrapper_span);
          PyDict_SetItemString(multi_result->raw_result, key.c_str(), pycbc_exc);
          Py_DECREF(pycbc_exc);
        }
      }
    }
    Py_DECREF(request_kwargs);
  }

  // we INCREF'd these, don't forget to DECREF!
  Py_DECREF(pyObj_bucket);
  Py_DECREF(pyObj_scope);
  Py_DECREF(pyObj_collection);

  bool all_okay = true;
  for (auto& [key, barrier] : operations) {
    PyObject* op_result = nullptr;

    Py_BEGIN_ALLOW_THREADS op_result = barrier->get_future().get();
    Py_END_ALLOW_THREADS

      if (op_result != nullptr)
    {
      if (PyExceptionInstance_Check(op_result) ||
          PyObject_TypeCheck(op_result, &pycbc_exception_type)) {
        all_okay = false;
      }
      PyDict_SetItemString(multi_result->raw_result, key.c_str(), op_result);
      Py_DECREF(op_result);
    }
    else
    {
      // This is a bad state and ideally users never run into this situation.  If we do run into
      // this situation we try to preserve the operations that provided results and set others to
      // None.
      all_okay = false;
      // Try to give some sort of diagnostic info (also clear the error indicator)
      PyErr_Print();
      PyDict_SetItemString(multi_result->raw_result, key.c_str(), Py_None);
    }
  }
  PyDict_SetItemString(multi_result->raw_result, "all_okay", all_okay ? Py_True : Py_False);

  return pyObj_multi_result;
}

template<typename Request>
PyObject*
Connection::execute_streaming_op(PyObject* kwargs)
{
  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::string span_name;
  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span;
  extract_field(kwargs, "wrapper_span_name", span_name);
  if (!span_name.empty()) {
    wrapper_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>(span_name);
  }

  try {
    auto req = py_to_cbpp<Request>(kwargs, wrapper_span);
    if (PyErr_Occurred()) {
      Py_XDECREF(pyObj_callback);
      Py_XDECREF(pyObj_errback);
      return nullptr;
    }

    // TODO(PYCBC-1746): Delete w/ removal of legacy tracing logic
    if (wrapper_span == nullptr) {
      add_cluster_labels(req);
    }

    // streaming_timeout is always set either to default, or streaming_timeout provided in options
    auto streaming_timeout_ms = std::chrono::milliseconds::zero();
    auto streaming_timeout = get_default_timeout(req);
    extract_field<std::chrono::milliseconds>(kwargs, "streaming_timeout", streaming_timeout_ms);
    if (streaming_timeout_ms > std::chrono::milliseconds::zero()) {
      streaming_timeout = streaming_timeout_ms;
    }

    pycbc_streamed_result* streamed_res = create_pycbc_streamed_result(streaming_timeout);
    // Keep the streamed_result alive until the callback is done by INCREFing it here and DECREFing
    // in the callback
    Py_INCREF(streamed_res);

    // TODO: When C++ SDK supports row_callback, set it here:
    // req.row_callback = [rows = streamed_res->rows](std::string&& row) {
    //     PyGILState_STATE state = PyGILState_Ensure();
    //     PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
    //     rows->put(pyObj_row);
    //     PyGILState_Release(state);
    //     return couchbase::core::utils::json::stream_control::next_row;
    // };

    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.execute(
        req,
        // we pass the shared_ptr rows separately b/c we need t o allow the rows queue to survive
        // until streaming is complete
        [rows = streamed_res->rows,
         streamed_res,
         wrapper_span,
         callback = pyObj_callback,
         errback = pyObj_errback,
         this](response_type resp) {
          if (wrapper_span != nullptr) {
            wrapper_span->end();
            if (auto retries = get_cbpp_retries(resp.ctx); retries > 0 && wrapper_span) {
              wrapper_span->add_tag("retries", retries);
            }
          }
          PyGILState_STATE state = PyGILState_Ensure();
          add_core_span<pycbc_streamed_result>(reinterpret_cast<PyObject*>(streamed_res),
                                               wrapper_span);

          PyObject* error = build_exception_from_context(
            resp.ctx, __FILE__, __LINE__, "Streaming operation failed");
          if (error) {
            rows->put(error);
          } else {
            for (const auto& row : resp.rows) {
              PyObject* pyObj_row;
              if constexpr (std::is_same_v<response_type,
                                           couchbase::core::operations::search_response> ||
                            std::is_same_v<response_type,
                                           couchbase::core::operations::document_view_response>) {
                pyObj_row = cbpp_to_py(row);
              } else {
                // special case for query/analytics_query; we want bytes from str
                pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
              }
              rows->put(pyObj_row);
            }

            // Push sentinel to signal end of rows
            Py_INCREF(Py_None);
            rows->put(Py_None);

            PyObject* result_obj = build_stream_end_result_obj(resp);
            if (result_obj) {
              rows->put(result_obj);
            } else {
              PyObject* pycbc_exc = build_pycbc_exception_from_python_exc(
                "Failed to create stream end result.", __FILE__, __LINE__);
              if (pycbc_exc != nullptr) {
                rows->put(pycbc_exc);
              } else {
                Py_INCREF(Py_None);
                rows->put(Py_None);
              }
            }
          }

          // Special case for txcouchbase, we always call the callback to signal the query is "done"
          // The python side will determine if the query has rows or returned an error
          if (callback != nullptr) {
            PyObject* result = PyObject_CallFunction(callback, "O", Py_True);
            Py_XDECREF(result);
          }

          Py_DECREF(streamed_res);
          Py_XDECREF(callback);
          Py_XDECREF(errback);
          PyGILState_Release(state);
        });
    }
    Py_END_ALLOW_THREADS

      return reinterpret_cast<PyObject*>(streamed_res);

  } catch (const std::exception& e) {
    Py_XDECREF(pyObj_callback);
    Py_XDECREF(pyObj_errback);
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  }
}

template<typename Request>
PyObject*
Connection::execute_mgmt_op(PyObject* kwargs)
{
  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::string span_name;
  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span;
  extract_field(kwargs, "wrapper_span_name", span_name);
  if (!span_name.empty()) {
    wrapper_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>(span_name);
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    auto req = py_to_cbpp<Request>(kwargs, wrapper_span);
    if (PyErr_Occurred()) {
      Py_XDECREF(pyObj_callback);
      Py_XDECREF(pyObj_errback);
      if (barrier) {
        barrier->set_value(nullptr);
      }
      return nullptr;
    }

    // TODO(PYCBC-1746): Delete w/ removal of legacy tracing logic
    if (wrapper_span == nullptr) {
      add_cluster_labels(req);
    }

    execute_op(req, pyObj_callback, pyObj_errback, barrier, wrapper_span);
    if (barrier) {
      PyObject* result = nullptr;
      Py_BEGIN_ALLOW_THREADS result = fut.get();
      Py_END_ALLOW_THREADS return result;
    }
    Py_RETURN_NONE;
  } catch (const std::exception& e) {
    if (barrier) {
      barrier->set_value(nullptr);
    }
    PyErr_SetString(PyExc_RuntimeError, e.what());
    Py_XDECREF(pyObj_callback);
    Py_XDECREF(pyObj_errback);
    return nullptr;
  }
}

} // namespace pycbc
