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
#include "pycbc_kv_request.hxx"
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
  PyObject* execute_streaming_op(PyObject* kwargs);

  template<typename Request>
  PyObject* execute_mgmt_op(PyObject* kwargs);

  template<typename Request>
  PyObject* execute_kv_op(pycbc_kv_request* request);

  // execute_multi_op is public so it can be called by handle_multi_op() in pycbc_connection.hxx
  template<typename Request>
  PyObject* execute_multi_op(PyObject* arg);

  template<typename Request>
  typename Request::response_type dispatch_sync(Request&& req);

  template<typename Request, typename Response>
  PyObject* finalize_kv_result(
    Response resp,
    std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span,
    std::optional<std::chrono::system_clock::time_point> start_time);

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
    std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span = nullptr,
    std::optional<std::chrono::system_clock::time_point> start_time = std::nullopt)
  {
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.execute(
        req,
        [pyObj_callback, pyObj_errback, barrier, wrapper_span, start_time, this](
          response_type resp) {
          PyGILState_STATE state = PyGILState_Ensure();
          PyObject* result = finalize_kv_result<Request>(
            std::move(resp), std::move(wrapper_span), std::move(start_time));

          PyObject* target_handler =
            PyObject_TypeCheck(result, &pycbc_exception_type) ? pyObj_errback : pyObj_callback;

          if (target_handler != nullptr) {
            Py_XDECREF(PyObject_CallFunctionObjArgs(target_handler, result, nullptr));
          } else if (barrier != nullptr) {
            barrier->set_value(result);
            result = nullptr; // Reference transferred to barrier
          }

          Py_XDECREF(result);
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

  template<typename PyType>
  void maybe_add_start_and_end_time(
    PyObject* pyObj,
    std::optional<std::chrono::system_clock::time_point> start_time = std::nullopt,
    std::optional<std::chrono::system_clock::time_point> end_time = std::nullopt)
  {
    if (start_time.has_value() && end_time.has_value()) {
      PyType* pyObj_ptr = reinterpret_cast<PyType*>(pyObj);
      PyObject* pyObj_start_time =
        cbpp_to_py<std::chrono::system_clock::time_point>(start_time.value());
      if (pyObj_start_time) {

        PyObject* pyObj_old_start_time = pyObj_ptr->start_time;
        pyObj_ptr->start_time = pyObj_start_time;
        // might be nice to use Py_SETREF, but if we want the limited API it is not available
        Py_XDECREF(pyObj_old_start_time);
      }
      PyObject* pyObj_end_time =
        cbpp_to_py<std::chrono::system_clock::time_point>(end_time.value());
      if (pyObj_end_time) {
        PyObject* pyObj_old_end_time = pyObj_ptr->end_time;
        pyObj_ptr->end_time = pyObj_end_time;
        // might be nice to use Py_SETREF, but if we want the limited API it is not available
        Py_XDECREF(pyObj_old_end_time);
      }
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
typename Request::response_type
Connection::dispatch_sync(Request&& req)
{
  using Response = typename Request::response_type;
  auto barrier = std::make_shared<std::promise<Response>>();
  auto fut = barrier->get_future();
  Py_BEGIN_ALLOW_THREADS cluster_.execute(std::forward<Request>(req), [barrier](Response resp) {
    barrier->set_value(std::move(resp));
  });
  fut.wait();
  Py_END_ALLOW_THREADS return fut.get();
}

template<typename Request, typename Response>
PyObject*
Connection::finalize_kv_result(
  Response resp,
  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span,
  std::optional<std::chrono::system_clock::time_point> start_time)
{
  if (wrapper_span != nullptr) {
    wrapper_span->end();
    if (auto retries = get_cbpp_retries(resp.ctx); retries > 0 && wrapper_span) {
      wrapper_span->add_tag("retries", retries);
    }
  }

  PyObject* error = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Operation failed");
  if (error) {
    add_core_span<pycbc_exception>(error, wrapper_span);
    if (start_time.has_value()) {
      maybe_add_start_and_end_time<pycbc_exception>(
        error, start_time, std::chrono::system_clock::now());
    }
    return error;
  }

  PyObject* result = nullptr;
  if constexpr (is_streaming_kv_op<Request>::value) {
    auto [is_error, result_obj] = process_streaming_kv_result(resp);
    result = result_obj;
    if (is_error) {
      add_core_span<pycbc_exception>(result, wrapper_span);
      if (start_time.has_value()) {
        maybe_add_start_and_end_time<pycbc_exception>(
          result, start_time, std::chrono::system_clock::now());
      }
    } else {
      add_core_span<pycbc_streamed_result>(result, wrapper_span);
      if (start_time.has_value()) {
        maybe_add_start_and_end_time<pycbc_streamed_result>(
          result, start_time, std::chrono::system_clock::now());
      }
    }
  } else {
    result = cbpp_to_py(resp);
    add_core_span<pycbc_result>(result, wrapper_span);
    if (start_time.has_value()) {
      maybe_add_start_and_end_time<pycbc_result>(
        result, start_time, std::chrono::system_clock::now());
    }
  }
  return result;
}

template<typename Request>
PyObject*
Connection::execute_kv_op(pycbc_kv_request* request)
{
  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span;
  std::string span_name;
  extract_field(request->wrapper_span_name, span_name);
  if (!span_name.empty()) {
    wrapper_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>(span_name);
  }

  try {
    auto req = py_to_cbpp<Request>(request, wrapper_span);
    if (PyErr_Occurred()) {
      return nullptr;
    }

    // TODO(PYCBC-1746): Delete w/ removal of legacy tracing logic
    if (wrapper_span == nullptr) {
      add_cluster_labels(req);
    }

    std::optional<std::chrono::system_clock::time_point> start_time;
    if (request->with_metrics == Py_True) {
      start_time = std::chrono::system_clock::now();
    }

    if (request->callback == nullptr && request->errback == nullptr) {
      auto resp = dispatch_sync(std::move(req));
      return finalize_kv_result<Request>(
        std::move(resp), std::move(wrapper_span), std::move(start_time));
    } else {
      Py_INCREF(request->callback);
      Py_XINCREF(request->errback);
      execute_op(req, request->callback, request->errback, nullptr, wrapper_span, start_time);
      Py_RETURN_NONE;
    }
  } catch (const std::exception& e) {
    return raise_invalid_argument(e.what());
  }
}

template<typename Request>
PyObject*
Connection::execute_multi_op(PyObject* arg)
{
  using Response = typename Request::response_type;
  using Staging = typename kv_staging_trait<Request>::staging_type;

  size_t num_docs = static_cast<size_t>(PyList_Size(arg));
  std::vector<Staging> staging;
  staging.reserve(num_docs);

  PyObject* pyObj_multi_result = create_pycbc_result();
  pycbc_result* multi_result = reinterpret_cast<pycbc_result*>(pyObj_multi_result);

  for (size_t i = 0; i < num_docs; ++i) {
    PyObject* pyObj_binding = PyList_GetItem(arg, i); // Borrowed ref
    pycbc_kv_request* request = reinterpret_cast<pycbc_kv_request*>(pyObj_binding);
    std::string key_str = py_to_cbpp<std::string>(request->key);
    std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span;
    std::string span_name;
    extract_field(request->wrapper_span_name, span_name);
    if (!span_name.empty()) {
      wrapper_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>(span_name);
    }

    auto req = py_to_cbpp<Request>(request, wrapper_span);
    if (PyErr_Occurred()) {
      Py_DECREF(pyObj_multi_result);
      return nullptr;
    }

    // TODO(PYCBC-1746): Delete w/ removal of legacy tracing logic
    if (wrapper_span == nullptr) {
      add_cluster_labels(req);
    }

    std::optional<std::chrono::system_clock::time_point> start_time;
    if (request->with_metrics == Py_True) {
      start_time = std::chrono::system_clock::now();
    }

    auto barrier = std::make_shared<std::promise<Response>>();
    auto fut = barrier->get_future();

    staging.push_back({ std::move(req),
                        std::move(key_str),
                        std::move(wrapper_span),
                        start_time,
                        std::move(barrier),
                        std::move(fut) });
  }

  Py_BEGIN_ALLOW_THREADS for (auto& s : staging)
  {
    auto barrier = s.barrier;
    cluster_.execute(s.req, [barrier](Response resp) {
      barrier->set_value(std::move(resp));
    });
  }

  for (auto& s : staging) {
    s.fut.wait();
  }
  Py_END_ALLOW_THREADS

    bool all_okay = true;
  for (auto& s : staging) {
    PyObject* res =
      finalize_kv_result<Request>(s.fut.get(), std::move(s.wrapper_span), std::move(s.start_time));
    if (PyObject_TypeCheck(res, &pycbc_exception_type)) {
      all_okay = false;
    }
    PyDict_SetItemString(multi_result->raw_result, s.key_str.c_str(), res);
    Py_DECREF(res);
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
