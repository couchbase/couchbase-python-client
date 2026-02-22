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

#include "connection.hxx"
#include <core/agent_group.hxx>
#include <core/cluster_label_listener.hxx>
#include <core/cluster_options.hxx>
#include <core/origin.hxx>
#include <core/range_scan_orchestrator.hxx>
#include <core/timeout_defaults.hxx>
#include <core/utils/connection_string.hxx>

namespace pycbc
{

Connection::Connection(int num_io_threads)
  : io_()
  , cluster_(io_)
  , io_threads_()
  , connected_(false)
{
  for (int i = 0; i < num_io_threads; ++i) {
    io_threads_.emplace_back([this]() {
      try {
        io_.run();
      } catch (const std::exception& e) {
        CB_LOG_ERROR(e.what());
        throw;
      } catch (...) {
        CB_LOG_ERROR("Unknown exception");
        throw;
      }
    });
  }
}

Connection::~Connection()
{
  // lets automatically shutdown the cluster
  // NOTE: close() is idempotent w/in C++ core, so okay if user has called cluster.close()
  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  cluster_.close([barrier]() {
    barrier->set_value();
  });
  if (f.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    CB_LOG_WARNING("PYCBC: Cluster close timed out in destructor.");
  }

  io_.stop();

  for (auto& t : io_threads_) {
    if (t.get_id() == std::this_thread::get_id()) {
      // Cannot join from the same thread - detach instead to avoid deadlock
      CB_LOG_DEBUG("PYCBC: dealloc_conn called from IO thread, detaching instead of joining");
      t.detach();
    } else {
      t.join();
    }
  }
}

void
Connection::handle_connection_operation_callback(std::error_code ec,
                                                 const char* operation,
                                                 PyObject* pyObj_callback,
                                                 PyObject* pyObj_errback,
                                                 std::shared_ptr<std::promise<PyObject*>> barrier,
                                                 connection_state_action state_action)
{
  PyGILState_STATE state = PyGILState_Ensure();

  PyObject* result = nullptr;

  if (ec) {
    std::string msg = "Failed to ";
    msg += operation;
    result = build_exception(ec, __FILE__, __LINE__, msg.c_str());

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
    if (state_action == connection_state_action::set_connected) {
      connected_ = true;
    } else if (state_action == connection_state_action::set_disconnected) {
      connected_ = false;
    }

    Py_INCREF(Py_None);
    result = Py_None;

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
}

PyObject*
Connection::connect(PyObject* kwargs)
{
  PyObject* pyObj_connstr = PyDict_GetItemString(kwargs, "connstr");
  PyObject* pyObj_auth = PyDict_GetItemString(kwargs, "auth");
  PyObject* pyObj_options = PyDict_GetItemString(kwargs, "options");
  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    std::string conn_str = std::string(PyUnicode_AsUTF8(pyObj_connstr));
    auto connstr = couchbase::core::utils::parse_connection_string(conn_str);
    auto creds = py_to_cbpp<couchbase::core::cluster_credentials>(pyObj_auth);

    update_cluster_options_from_py(connstr.options, pyObj_options, pyObj_auth);
    couchbase::core::origin origin(creds, connstr);

    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.open(
        origin,
        [callback = pyObj_callback, errback = pyObj_errback, barrier, this](std::error_code ec) {
          handle_connection_operation_callback(
            ec, "connect", callback, errback, barrier, connection_state_action::set_connected);
        });
    }
    Py_END_ALLOW_THREADS

      if (barrier)
    {
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

PyObject*
Connection::close(PyObject* kwargs)
{
  PyObject* pyObj_callback = kwargs ? PyDict_GetItemString(kwargs, "callback") : nullptr;
  PyObject* pyObj_errback = kwargs ? PyDict_GetItemString(kwargs, "errback") : nullptr;

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.close([callback = pyObj_callback, errback = pyObj_errback, barrier, this]() {
        handle_connection_operation_callback(std::error_code{},
                                             "close",
                                             callback,
                                             errback,
                                             barrier,
                                             connection_state_action::set_disconnected);
      });
    }
    Py_END_ALLOW_THREADS

      if (barrier)
    {
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

PyObject*
Connection::open_bucket(PyObject* kwargs)
{
  PyObject* pyObj_bucket_name = PyDict_GetItemString(kwargs, "bucket_name");
  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    std::string bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));

    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.open_bucket(
        bucket_name,
        [callback = pyObj_callback, errback = pyObj_errback, barrier, this](std::error_code ec) {
          handle_connection_operation_callback(ec, "open_bucket", callback, errback, barrier);
        });
    }
    Py_END_ALLOW_THREADS

      if (barrier)
    {
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

PyObject*
Connection::close_bucket(PyObject* kwargs)
{
  PyObject* pyObj_bucket_name = PyDict_GetItemString(kwargs, "bucket_name");
  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    std::string bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));

    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.close_bucket(
        bucket_name,
        [callback = pyObj_callback, errback = pyObj_errback, barrier, this](std::error_code ec) {
          handle_connection_operation_callback(ec, "close_bucket", callback, errback, barrier);
        });
    }
    Py_END_ALLOW_THREADS

      if (barrier)
    {
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

PyObject*
Connection::update_credentials(PyObject* kwargs)
{
  PyObject* pyObj_auth = PyDict_GetItemString(kwargs, "auth");
  auto creds = py_to_cbpp<couchbase::core::cluster_credentials>(pyObj_auth);
  auto err = cluster_.update_credentials(std::move(creds));
  if (err.ec) {
    return build_exception(err.ec, __FILE__, __LINE__, err.message.c_str());
  }
  Py_RETURN_NONE;
}

PyObject*
Connection::get_connection_info()
{
  try {
    auto origin_result = cluster_.origin(); // returns pair<error_code, origin>
    if (origin_result.first) {
      PyObject* result = PyDict_New();
      if (!result) {
        return nullptr;
      }
      add_bool_field(result, "connected", connected_);
      return result;
    }

    const auto& origin = origin_result.second;
    auto creds = origin.credentials();
    auto opts = origin.options();
    return cluster_options_to_py(opts, creds);

  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  }
}

PyObject*
Connection::diagnostics(PyObject* kwargs)
{
  std::optional<std::string> report_id;
  extract_field<std::optional<std::string>>(kwargs, "report_id", report_id);

  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.diagnostics(report_id,
                           [pyObj_callback, pyObj_errback, barrier, this](
                             couchbase::core::diag::diagnostics_result resp) {
                             handle_cluster_operation_callback(
                               resp, pyObj_callback, pyObj_errback, barrier);
                           });
    }
    Py_END_ALLOW_THREADS

      if (barrier)
    {
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

PyObject*
Connection::ping(PyObject* kwargs)
{
  std::optional<std::string> report_id;
  extract_field<std::optional<std::string>>(kwargs, "report_id", report_id);
  std::optional<std::string> bucket_name;
  extract_field<std::optional<std::string>>(kwargs, "bucket_name", bucket_name);
  std::optional<std::chrono::milliseconds> timeout_ms;
  extract_field<std::optional<std::chrono::milliseconds>>(kwargs, "timeout", timeout_ms);
  std::set<couchbase::core::service_type> services;
  extract_field<std::set<couchbase::core::service_type>>(kwargs, "services", services);

  PyObject* pyObj_callback = PyDict_GetItemString(kwargs, "callback");
  PyObject* pyObj_errback = PyDict_GetItemString(kwargs, "errback");

  if (!validate_and_incref_callbacks(pyObj_callback, pyObj_errback)) {
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback && nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  try {
    Py_BEGIN_ALLOW_THREADS
    {
      cluster_.ping(
        report_id,
        bucket_name,
        services,
        timeout_ms,
        [pyObj_callback, pyObj_errback, barrier, this](couchbase::core::diag::ping_result resp) {
          handle_cluster_operation_callback(resp, pyObj_callback, pyObj_errback, barrier);
        });
    }
    Py_END_ALLOW_THREADS

      if (barrier)
    {
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

PyObject*
Connection::handle_range_scan_op(PyObject* kwargs)
{
  try {
    PyObject* pyObj_bucket = PyDict_GetItemString(kwargs, "bucket");
    PyObject* pyObj_scope = PyDict_GetItemString(kwargs, "scope");
    PyObject* pyObj_collection = PyDict_GetItemString(kwargs, "collection_name");

    if (!pyObj_bucket || !pyObj_scope || !pyObj_collection) {
      return raise_invalid_argument(
        "bucket, scope, and collection are required", __FILE__, __LINE__);
    }

    std::string bucket_name = PyUnicode_AsUTF8(pyObj_bucket);
    std::string scope_name = PyUnicode_AsUTF8(pyObj_scope);
    std::string collection_name = PyUnicode_AsUTF8(pyObj_collection);

    PyObject* pyObj_scan_type = PyDict_GetItemString(kwargs, "scan_type");
    if (!pyObj_scan_type) {
      return raise_invalid_argument("scan_type is required", __FILE__, __LINE__);
    }
    int py_scan_type = PyLong_AsLong(pyObj_scan_type);

    PyObject* pyObj_scan_config = PyDict_GetItemString(kwargs, "scan_config");
    if (!pyObj_scan_config || !PyDict_Check(pyObj_scan_config)) {
      return raise_invalid_argument("scan_config must be a dictionary", __FILE__, __LINE__);
    }

    PyObject* pyObj_orchestrator_opts = PyDict_GetItemString(kwargs, "orchestrator_options");
    if (nullptr != pyObj_orchestrator_opts && !PyDict_Check(pyObj_orchestrator_opts)) {
      return raise_invalid_argument(
        "orchestrator_options must be a dictionary", __FILE__, __LINE__);
    }

    auto barrier = std::make_shared<std::promise<
      std::pair<std::error_code, std::shared_ptr<couchbase::core::topology::configuration>>>>();
    auto f = barrier->get_future();
    cluster_.with_bucket_configuration(bucket_name,
                                       [barrier](std::error_code ec, auto config) mutable {
                                         barrier->set_value({ ec, std::move(config) });
                                       });
    auto [ec, bucket_config] = f.get();

    if (ec) {
      return build_exception(
        ec, __FILE__, __LINE__, "Failed to get bucket configuration for range scan");
    }
    if (!bucket_config->capabilities.supports_range_scan()) {
      return raise_feature_unavailable(
        "The server does not support key-value scan operations.", __FILE__, __LINE__);
    }
    if (!bucket_config->vbmap || bucket_config->vbmap->empty()) {
      return raise_unsuccessful_operation(
        "Cannot perform kv range scan operation.  Unable to get vbucket map.", __FILE__, __LINE__);
    }

    auto agent_group =
      couchbase::core::agent_group(io_, couchbase::core::agent_group_config{ { cluster_ } });
    agent_group.open_bucket(bucket_name);
    auto agent = agent_group.get_agent(bucket_name);

    if (!agent.has_value()) {
      return raise_unsuccessful_operation(
        "Cannot perform kv range scan operation.  Unable to get operation agent.",
        __FILE__,
        __LINE__);
    }

    auto orchestrator_options =
      py_to_cbpp<couchbase::core::range_scan_orchestrator_options>(pyObj_orchestrator_opts);

    std::variant<std::monostate,
                 couchbase::core::range_scan,
                 couchbase::core::prefix_scan,
                 couchbase::core::sampling_scan>
      scan_type{};
    switch (py_scan_type) {
      case 1: {
        scan_type = py_to_cbpp<couchbase::core::range_scan>(pyObj_scan_config);
        break;
      }
      case 2: {
        scan_type = py_to_cbpp<couchbase::core::prefix_scan>(pyObj_scan_config);
        break;
      }
      case 3: {
        scan_type = py_to_cbpp<couchbase::core::sampling_scan>(pyObj_scan_config);
        break;
      }
      default:
        return raise_invalid_argument("scan_type must be 1, 2, or 3", __FILE__, __LINE__);
    }

    auto orchestrator = couchbase::core::range_scan_orchestrator(io_,
                                                                 agent.value(),
                                                                 bucket_config->vbmap.value(),
                                                                 scope_name,
                                                                 collection_name,
                                                                 scan_type,
                                                                 orchestrator_options);
    auto scan_result = orchestrator.scan();
    if (!scan_result.has_value()) {
      return raise_unsuccessful_operation(
        "Cannot perform kv scan operation.  Unable to start scan operation.", __FILE__, __LINE__);
    }

    pycbc_scan_iterator* iter = create_pycbc_scan_iterator(std::move(scan_result.value()));
    return reinterpret_cast<PyObject*>(iter);
  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  }
}

std::pair<std::optional<std::string>, std::optional<std::string>>
Connection::get_cluster_labels()
{
  auto labels = std::make_pair(std::optional<std::string>{}, std::optional<std::string>{});
  auto cppLabels = cluster_.cluster_label_listener()->cluster_labels();
  if (cppLabels.cluster_name.has_value()) {
    labels.first = cppLabels.cluster_name.value();
  }
  if (cppLabels.cluster_uuid.has_value()) {
    labels.second = cppLabels.cluster_uuid.value();
  }
  return labels;
}

PyObject*
Connection::get_cluster_labels_as_py_object()
{
  PyObject* result = PyDict_New();
  if (!result) {
    return nullptr;
  }
  auto cluster_labels = get_cluster_labels();
  add_field<std::optional<std::string>>(result, "clusterName", cluster_labels.first);
  add_field<std::optional<std::string>>(result, "clusterUUID", cluster_labels.second);
  return result;
}

} // namespace pycbc
