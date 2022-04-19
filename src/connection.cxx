#include "connection.hxx"
#include "exceptions.hxx"
#include <couchbase/io/ip_protocol.hxx>

static void
dealloc_conn(PyObject* obj)
{
    auto conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(obj, "conn_"));
    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        conn->cluster_->close([barrier]() { barrier->set_value(); });
        f.get();
    }
    conn->io_.stop();
    for (auto& t : conn->io_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    LOG_INFO("{}: dealloc_conn completed", "PYCBC");
    // LOG_INFO_RAW("dealloc_conn completed");
    delete conn;
}

void
bucket_op_callback(std::error_code ec,
                   bool open,
                   PyObject* pyObj_callback,
                   PyObject* pyObj_errback,
                   std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    if (ec.value()) {
        // if(ctx.get_errback() == nullptr){
        if (pyObj_errback == nullptr) {
            std::string msg = "Error trying to ";
            msg.append((open ? "open" : "close") + std::string(" bucket."));
            auto pycbc_ex = PycbcException(msg, __FILE__, __LINE__, ec);
            barrier->set_exception(std::make_exception_ptr(pycbc_ex));
        } else {
            PyObject* exc = build_exception(ec);
            pyObj_func = pyObj_errback;
            // pyObj_func = ctx.get_errback();
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, exc);
        }
    } else {
        // if(ctx.get_callback() == nullptr){
        if (pyObj_callback == nullptr) {
            barrier->set_value(PyBool_FromLong(static_cast<long>(1)));
        } else {
            // pyObj_func = ctx.get_callback();
            pyObj_func = pyObj_callback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
        }
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            std::string msg;
            msg.append((open ? "Open" : "Close") + std::string(" bucket callback failed"));
            pycbc_set_python_exception(msg.c_str(), PycbcError::InternalSDKError, __FILE__, __LINE__);
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    LOG_INFO("{}: open/close bucket callback completed", "PYCBC");
    PyGILState_Release(state);
}

void
close_connection_callback(PyObject* pyObj_callback, PyObject* pyObj_errback, std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    // if(ctx.get_callback() == nullptr){
    if (pyObj_callback == nullptr) {
        barrier->set_value(PyBool_FromLong(static_cast<long>(1)));
    } else {
        // pyObj_func = ctx.get_callback();
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        LOG_INFO("{}: return from close conn callback.", "PYCBC");
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception("Close connection callback failed.", PycbcError::InternalSDKError, __FILE__, __LINE__);
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    LOG_INFO("{}: close conn callback completed", "PYCBC");

    PyGILState_Release(state);
}

void
create_connection_callback(PyObject* pyObj_conn,
                           std::error_code ec,
                           PyObject* pyObj_callback,
                           PyObject* pyObj_errback,
                           std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    if (ec.value()) {
        // if(ctx.get_errback() == nullptr){
        if (pyObj_errback == nullptr) {
            auto pycbc_ex = PycbcException("Error creating a connection.", __FILE__, __LINE__, ec);
            auto exc = std::make_exception_ptr(pycbc_ex);
            barrier->set_exception(exc);
        } else {
            PyObject* exc = build_exception(ec);
            // pyObj_func = ctx.get_errback();
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, exc);
        }
    } else {
        // if(ctx.get_callback() == nullptr){
        if (pyObj_callback == nullptr) {
            barrier->set_value(pyObj_conn);
        } else {
            // pyObj_func = ctx.get_callback();
            pyObj_func = pyObj_callback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_conn);
        }
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception("Create connection callback failed.", PycbcError::InternalSDKError, __FILE__, __LINE__);
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    Py_DECREF(pyObj_conn);
    LOG_INFO("{}: create conn callback completed", "PYCBC");
    PyGILState_Release(state);
}

couchbase::cluster_credentials
get_cluster_credentials(PyObject* pyObj_auth)
{
    couchbase::cluster_credentials auth{};
    PyObject* pyObj_username = PyDict_GetItemString(pyObj_auth, "username");
    if (pyObj_username != nullptr) {
        auto username = std::string(PyUnicode_AsUTF8(pyObj_username));
        auth.username = username;
    }

    PyObject* pyObj_password = PyDict_GetItemString(pyObj_auth, "password");
    if (pyObj_password != nullptr) {
        auto pw = std::string(PyUnicode_AsUTF8(pyObj_password));
        auth.password = pw;
    }

    PyObject* pyObj_cert_path = PyDict_GetItemString(pyObj_auth, "cert_path");
    if (pyObj_cert_path != nullptr) {
        auto cert_path = std::string(PyUnicode_AsUTF8(pyObj_cert_path));
        auth.certificate_path = cert_path;
    }

    PyObject* pyObj_key_path = PyDict_GetItemString(pyObj_auth, "key_path");
    if (pyObj_key_path != nullptr) {
        auto key_path = std::string(PyUnicode_AsUTF8(pyObj_key_path));
        auth.key_path = key_path;
    }

    return auth;
}

void
update_cluster_logging_meter_options(couchbase::cluster_options& options, PyObject* pyObj_emit_interval)
{
    couchbase::metrics::logging_meter_options logging_options{};
    bool has_logging_meter_options = false;

    if (pyObj_emit_interval != nullptr) {
        auto emit_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_emit_interval));
        auto emit_interval_ms = std::chrono::milliseconds(std::max(0ULL, emit_interval / 1000ULL));
        logging_options.emit_interval = emit_interval_ms;
        has_logging_meter_options = true;
    }

    if (has_logging_meter_options) {
        options.metrics_options = logging_options;
    }
}

void
update_cluster_tracing_options(couchbase::cluster_options& options, PyObject* pyObj_tracing_opts)
{
    couchbase::tracing::threshold_logging_options tracing_options{};
    bool has_tracing_options = false;

    PyObject* pyObj_kv_threshold = PyDict_GetItemString(pyObj_tracing_opts, "key_value_threshold");
    if (pyObj_kv_threshold != nullptr) {
        auto kv_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_kv_threshold));
        auto kv_threshold_ms = std::chrono::milliseconds(std::max(0ULL, kv_threshold / 1000ULL));
        tracing_options.key_value_threshold = kv_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_view_threshold = PyDict_GetItemString(pyObj_tracing_opts, "view_threshold");
    if (pyObj_view_threshold != nullptr) {
        auto view_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_view_threshold));
        auto view_threshold_ms = std::chrono::milliseconds(std::max(0ULL, view_threshold / 1000ULL));
        tracing_options.view_threshold = view_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_query_threshold = PyDict_GetItemString(pyObj_tracing_opts, "query_threshold");
    if (pyObj_query_threshold != nullptr) {
        auto query_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_query_threshold));
        auto query_threshold_ms = std::chrono::milliseconds(std::max(0ULL, query_threshold / 1000ULL));
        tracing_options.query_threshold = query_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_search_threshold = PyDict_GetItemString(pyObj_tracing_opts, "search_threshold");
    if (pyObj_search_threshold != nullptr) {
        auto search_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_search_threshold));
        auto search_threshold_ms = std::chrono::milliseconds(std::max(0ULL, search_threshold / 1000ULL));
        tracing_options.search_threshold = search_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_analytics_threshold = PyDict_GetItemString(pyObj_tracing_opts, "analytics_threshold");
    if (pyObj_analytics_threshold != nullptr) {
        auto analytics_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_analytics_threshold));
        auto analytics_threshold_ms = std::chrono::milliseconds(std::max(0ULL, analytics_threshold / 1000ULL));
        tracing_options.analytics_threshold = analytics_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_eventing_threshold = PyDict_GetItemString(pyObj_tracing_opts, "eventing_threshold");
    if (pyObj_eventing_threshold != nullptr) {
        auto eventing_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_eventing_threshold));
        auto eventing_threshold_ms = std::chrono::milliseconds(std::max(0ULL, eventing_threshold / 1000ULL));
        tracing_options.eventing_threshold = eventing_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_management_threshold = PyDict_GetItemString(pyObj_tracing_opts, "management_threshold");
    if (pyObj_management_threshold != nullptr) {
        auto management_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_management_threshold));
        auto management_threshold_ms = std::chrono::milliseconds(std::max(0ULL, management_threshold / 1000ULL));
        tracing_options.management_threshold = management_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_threshold_sample_size = PyDict_GetItemString(pyObj_tracing_opts, "threshold_sample_size");
    if (pyObj_threshold_sample_size != nullptr) {
        auto threshold_sample_size = static_cast<size_t>(PyLong_AsUnsignedLong(pyObj_threshold_sample_size));
        tracing_options.threshold_sample_size = threshold_sample_size;
        has_tracing_options = true;
    }

    PyObject* pyObj_threshold_emit_interval = PyDict_GetItemString(pyObj_tracing_opts, "threshold_emit_interval");
    if (pyObj_threshold_emit_interval != nullptr) {
        auto threshold_emit_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_threshold_emit_interval));
        auto threshold_emit_interval_ms = std::chrono::milliseconds(std::max(0ULL, threshold_emit_interval / 1000ULL));
        tracing_options.threshold_emit_interval = threshold_emit_interval_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_orphaned_emit_interval = PyDict_GetItemString(pyObj_tracing_opts, "orphaned_emit_interval");
    if (pyObj_orphaned_emit_interval != nullptr) {
        auto orphaned_emit_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_orphaned_emit_interval));
        auto orphaned_emit_interval_ms = std::chrono::milliseconds(std::max(0ULL, orphaned_emit_interval / 1000ULL));
        tracing_options.orphaned_emit_interval = orphaned_emit_interval_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_orphaned_sample_size = PyDict_GetItemString(pyObj_tracing_opts, "orphaned_sample_size");
    if (pyObj_orphaned_sample_size != nullptr) {
        auto orphaned_sample_size = static_cast<size_t>(PyLong_AsUnsignedLong(pyObj_orphaned_sample_size));
        tracing_options.orphaned_sample_size = orphaned_sample_size;
        has_tracing_options = true;
    }

    if (has_tracing_options) {
        options.tracing_options = tracing_options;
    }
}

void
update_cluster_timeout_options(couchbase::cluster_options& options, PyObject* pyObj_timeout_opts)
{
    PyObject* pyObj_bootstrap_timeout = PyDict_GetItemString(pyObj_timeout_opts, "bootstrap_timeout");
    if (pyObj_bootstrap_timeout != nullptr) {
        auto bootstrap_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_bootstrap_timeout));
        auto bootstrap_timeout_ms = std::chrono::milliseconds(std::max(0ULL, bootstrap_timeout / 1000ULL));
        options.bootstrap_timeout = bootstrap_timeout_ms;
    }

    PyObject* pyObj_resolve_timeout = PyDict_GetItemString(pyObj_timeout_opts, "resolve_timeout");
    if (pyObj_resolve_timeout != nullptr) {
        auto resolve_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_resolve_timeout));
        auto resolve_timeout_ms = std::chrono::milliseconds(std::max(0ULL, resolve_timeout / 1000ULL));
        options.resolve_timeout = resolve_timeout_ms;
    }

    PyObject* pyObj_connect_timeout = PyDict_GetItemString(pyObj_timeout_opts, "connect_timeout");
    if (pyObj_connect_timeout != nullptr) {
        auto connect_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_connect_timeout));
        auto connect_timeout_ms = std::chrono::milliseconds(std::max(0ULL, connect_timeout / 1000ULL));
        options.connect_timeout = connect_timeout_ms;
    }

    PyObject* pyObj_key_value_timeout = PyDict_GetItemString(pyObj_timeout_opts, "key_value_timeout");
    if (pyObj_key_value_timeout != nullptr) {
        auto key_value_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_key_value_timeout));
        auto key_value_timeout_ms = std::chrono::milliseconds(std::max(0ULL, key_value_timeout / 1000ULL));
        options.key_value_timeout = key_value_timeout_ms;
    }

    PyObject* pyObj_key_value_durable_timeout = PyDict_GetItemString(pyObj_timeout_opts, "key_value_durable_timeout");
    if (pyObj_key_value_durable_timeout != nullptr) {
        auto key_value_durable_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_key_value_durable_timeout));
        auto key_value_durable_timeout_ms = std::chrono::milliseconds(std::max(0ULL, key_value_durable_timeout / 1000ULL));
        options.key_value_durable_timeout = key_value_durable_timeout_ms;
    }

    PyObject* pyObj_view_timeout = PyDict_GetItemString(pyObj_timeout_opts, "view_timeout");
    if (pyObj_view_timeout != nullptr) {
        auto view_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_view_timeout));
        auto view_timeout_ms = std::chrono::milliseconds(std::max(0ULL, view_timeout / 1000ULL));
        options.view_timeout = view_timeout_ms;
    }

    PyObject* pyObj_query_timeout = PyDict_GetItemString(pyObj_timeout_opts, "query_timeout");
    if (pyObj_query_timeout != nullptr) {
        auto query_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_query_timeout));
        auto query_timeout_ms = std::chrono::milliseconds(std::max(0ULL, query_timeout / 1000ULL));
        options.query_timeout = query_timeout_ms;
    }

    PyObject* pyObj_analytics_timeout = PyDict_GetItemString(pyObj_timeout_opts, "analytics_timeout");
    if (pyObj_analytics_timeout != nullptr) {
        auto analytics_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_analytics_timeout));
        auto analytics_timeout_ms = std::chrono::milliseconds(std::max(0ULL, analytics_timeout / 1000ULL));
        options.analytics_timeout = analytics_timeout_ms;
    }

    PyObject* pyObj_search_timeout = PyDict_GetItemString(pyObj_timeout_opts, "search_timeout");
    if (pyObj_search_timeout != nullptr) {
        auto search_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_search_timeout));
        auto search_timeout_ms = std::chrono::milliseconds(std::max(0ULL, search_timeout / 1000ULL));
        options.search_timeout = search_timeout_ms;
    }

    PyObject* pyObj_management_timeout = PyDict_GetItemString(pyObj_timeout_opts, "management_timeout");
    if (pyObj_management_timeout != nullptr) {
        auto management_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_management_timeout));
        auto management_timeout_ms = std::chrono::milliseconds(std::max(0ULL, management_timeout / 1000ULL));
        options.management_timeout = management_timeout_ms;
    }

    PyObject* pyObj_dns_srv_timeout = PyDict_GetItemString(pyObj_timeout_opts, "dns_srv_timeout");
    if (pyObj_dns_srv_timeout != nullptr) {
        auto dns_srv_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_dns_srv_timeout));
        auto dns_srv_timeout_ms = std::chrono::milliseconds(std::max(0ULL, dns_srv_timeout / 1000ULL));
        options.dns_srv_timeout = dns_srv_timeout_ms;
    }

    PyObject* pyObj_idle_http_connection_timeout = PyDict_GetItemString(pyObj_timeout_opts, "idle_http_connection_timeout");
    if (pyObj_idle_http_connection_timeout != nullptr) {
        auto idle_http_connection_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_idle_http_connection_timeout));
        auto idle_http_connection_timeout_ms = std::chrono::milliseconds(std::max(0ULL, idle_http_connection_timeout / 1000ULL));
        options.idle_http_connection_timeout = idle_http_connection_timeout_ms;
    }

    PyObject* pyObj_config_idle_redial_timeout = PyDict_GetItemString(pyObj_timeout_opts, "config_idle_redial_timeout");
    if (pyObj_config_idle_redial_timeout != nullptr) {
        auto config_idle_redial_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_idle_redial_timeout));
        auto config_idle_redial_timeout_ms = std::chrono::milliseconds(std::max(0ULL, config_idle_redial_timeout / 1000ULL));
        options.config_idle_redial_timeout = config_idle_redial_timeout_ms;
    }
}

void
update_cluster_options(couchbase::cluster_options& options, PyObject* pyObj_options, PyObject* pyObj_auth)
{
    PyObject* pyObj_timeout_opts = PyDict_GetItemString(pyObj_options, "timeout_options");
    if (pyObj_timeout_opts != nullptr) {
        update_cluster_timeout_options(options, pyObj_timeout_opts);
    }

    PyObject* pyObj_tracing_opts = PyDict_GetItemString(pyObj_options, "tracing_options");
    if (pyObj_tracing_opts != nullptr) {
        update_cluster_tracing_options(options, pyObj_tracing_opts);
    }

    PyObject* pyObj_emit_interval = PyDict_GetItemString(pyObj_options, "emit_interval");
    if (pyObj_emit_interval != nullptr) {
        update_cluster_logging_meter_options(options, pyObj_emit_interval);
    }

    PyObject* pyObj_enable_tls = PyDict_GetItemString(pyObj_options, "enable_tls");
    if (pyObj_enable_tls != nullptr && pyObj_enable_tls == Py_True) {
        options.enable_tls = true;
    }

    PyObject* pyObj_trust_store_path = PyDict_GetItemString(pyObj_auth, "trust_store_path");
    if (pyObj_trust_store_path != nullptr) {
        auto trust_store_path = std::string(PyUnicode_AsUTF8(pyObj_trust_store_path));
        options.trust_certificate = trust_store_path;
    } else {
        pyObj_trust_store_path = PyDict_GetItemString(pyObj_options, "trust_store_path");
        if (pyObj_trust_store_path != nullptr) {
            auto trust_store_path = std::string(PyUnicode_AsUTF8(pyObj_trust_store_path));
            options.trust_certificate = trust_store_path;
        }
    }

    PyObject* pyObj_enable_mut_tokens = PyDict_GetItemString(pyObj_options, "enable_mutation_tokens");
    if (pyObj_enable_mut_tokens != nullptr && pyObj_enable_mut_tokens == Py_False) {
        options.enable_mutation_tokens = false;
    }

    PyObject* pyObj_enable_tcp_keep_alive = PyDict_GetItemString(pyObj_options, "enable_tcp_keep_alive");
    if (pyObj_enable_tcp_keep_alive != nullptr && pyObj_enable_tcp_keep_alive == Py_False) {
        options.enable_tcp_keep_alive = false;
    }

    PyObject* pyObj_force_ipv4 = PyDict_GetItemString(pyObj_options, "force_ipv4");
    if (pyObj_force_ipv4 != nullptr && pyObj_force_ipv4 == Py_True) {
        options.use_ip_protocol = couchbase::io::ip_protocol::force_ipv4;
    }

    PyObject* pyObj_enable_dns_srv = PyDict_GetItemString(pyObj_options, "enable_dns_srv");
    if (pyObj_enable_dns_srv != nullptr && pyObj_enable_dns_srv == Py_False) {
        options.enable_dns_srv = false;
    }

    PyObject* pyObj_show_queries = PyDict_GetItemString(pyObj_options, "show_queries");
    if (pyObj_show_queries != nullptr && pyObj_show_queries == Py_True) {
        options.enable_dns_srv = true;
    }

    PyObject* pyObj_enable_unordered_execution = PyDict_GetItemString(pyObj_options, "enable_unordered_execution");
    if (pyObj_enable_unordered_execution != nullptr && pyObj_enable_unordered_execution == Py_False) {
        options.enable_unordered_execution = false;
    }

    PyObject* pyObj_enable_clustermap_notification = PyDict_GetItemString(pyObj_options, "enable_clustermap_notification");
    if (pyObj_enable_clustermap_notification != nullptr && pyObj_enable_clustermap_notification == Py_False) {
        options.enable_clustermap_notification = false;
    }

    PyObject* pyObj_enable_compression = PyDict_GetItemString(pyObj_options, "enable_compression");
    if (pyObj_enable_compression != nullptr && pyObj_enable_compression == Py_False) {
        options.enable_compression = false;
    }

    PyObject* pyObj_enable_tracing = PyDict_GetItemString(pyObj_options, "enable_tracing");
    if (pyObj_enable_tracing != nullptr && pyObj_enable_tracing == Py_False) {
        options.enable_tracing = false;
    }

    PyObject* pyObj_enable_metrics = PyDict_GetItemString(pyObj_options, "enable_metrics");
    if (pyObj_enable_metrics != nullptr && pyObj_enable_metrics == Py_False) {
        options.enable_metrics = false;
    }

    PyObject* pyObj_network = PyDict_GetItemString(pyObj_options, "network");
    if (pyObj_network != nullptr) {
        auto network = std::string(PyUnicode_AsUTF8(pyObj_network));
        options.network = network;
    }

    PyObject* pyObj_tls_verify = PyDict_GetItemString(pyObj_options, "tls_verify");
    if (pyObj_tls_verify != nullptr) {
        auto tls_verify = std::string(PyUnicode_AsUTF8(pyObj_tls_verify));
        if (tls_verify.compare("none") == 0) {
            options.tls_verify = couchbase::tls_verify_mode::none;
        } else if (tls_verify.compare("peer") == 0) {
            options.tls_verify = couchbase::tls_verify_mode::peer;
        }
    }

    PyObject* pyObj_tcp_keep_alive_interval = PyDict_GetItemString(pyObj_options, "tcp_keep_alive_interval");
    if (pyObj_tcp_keep_alive_interval != nullptr) {
        auto tcp_keep_alive_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_tcp_keep_alive_interval));
        auto tcp_keep_alive_interval_ms = std::chrono::milliseconds(std::max(0ULL, tcp_keep_alive_interval / 1000ULL));
        options.tcp_keep_alive_interval = tcp_keep_alive_interval_ms;
    }

    PyObject* pyObj_config_poll_interval = PyDict_GetItemString(pyObj_options, "config_poll_interval");
    if (pyObj_config_poll_interval != nullptr) {
        auto config_poll_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_poll_interval));
        auto config_poll_interval_ms = std::chrono::milliseconds(std::max(0ULL, config_poll_interval / 1000ULL));
        options.config_poll_interval = config_poll_interval_ms;
    }

    PyObject* pyObj_config_poll_floor = PyDict_GetItemString(pyObj_options, "config_poll_floor");
    if (pyObj_config_poll_floor != nullptr) {
        auto config_poll_floor = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_poll_floor));
        auto config_poll_floor_ms = std::chrono::milliseconds(std::max(0ULL, config_poll_floor / 1000ULL));
        options.config_poll_floor = config_poll_floor_ms;
    }

    PyObject* pyObj_user_agent_extra = PyDict_GetItemString(pyObj_options, "user_agent_extra");
    if (pyObj_user_agent_extra != nullptr) {
        auto user_agent_extra = std::string(PyUnicode_AsUTF8(pyObj_user_agent_extra));
        options.user_agent_extra = user_agent_extra;
    }

    PyObject* pyObj_max_http_connections = PyDict_GetItemString(pyObj_options, "max_http_connections");
    if (pyObj_max_http_connections != nullptr) {
        auto max_http_connections = static_cast<size_t>(PyLong_AsUnsignedLong(pyObj_max_http_connections));
        options.max_http_connections = max_http_connections;
    }
}

PyObject*
handle_create_connection([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    char* conn_str = nullptr;
    PyObject* pyObj_auth = nullptr;
    PyObject* pyObj_options = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_result = nullptr;

    static const char* kw_list[] = { "", "auth", "options", "callback", "errback", nullptr };

    const char* kw_format = "s|OOOO";
    int ret = PyArg_ParseTupleAndKeywords(
      args, kwargs, kw_format, const_cast<char**>(kw_list), &conn_str, &pyObj_auth, &pyObj_options, &pyObj_callback, &pyObj_errback);

    if (!ret) {
        pycbc_set_python_exception(CANNOT_PARSE_CONN_ARGS_MSG("create"), PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }

    couchbase::utils::connection_string connection_str = couchbase::utils::parse_connection_string(conn_str);
    couchbase::cluster_credentials auth = get_cluster_credentials(pyObj_auth);
    update_cluster_options(connection_str.options, pyObj_options, pyObj_auth);

    PyObject* pyObj_num_io_threads = PyDict_GetItemString(pyObj_options, "num_io_threads");
    int num_io_threads = 1;
    if (pyObj_num_io_threads != nullptr) {
        num_io_threads = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_num_io_threads));
    }

    connection* const conn = new connection(num_io_threads);
    PyObject* pyObj_conn = PyCapsule_New(conn, "conn_", dealloc_conn);

    if (pyObj_conn == nullptr) {
        pycbc_set_python_exception(
          "Cannot create connection. Unable to create PyCapsule.", PycbcError::InternalSDKError, __FILE__, __LINE__);
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    Py_XINCREF(pyObj_conn);
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    {
        int callback_count = 0;
        Py_BEGIN_ALLOW_THREADS conn->cluster_->open(
          couchbase::origin(auth, connection_str),
          [pyObj_conn, pyObj_callback, pyObj_errback, callback_count, barrier](std::error_code ec) mutable {
              if (callback_count == 0) {
                  create_connection_callback(pyObj_conn, ec, pyObj_callback, pyObj_errback, barrier);
              }
              callback_count++;
          });
        Py_END_ALLOW_THREADS
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        return handle_conn_blocking_result(std::move(f));
        // PyObject* ret = nullptr;
        // std::string file;
        // int line;
        // std::error_code ec;
        // std::string msg;

        // Py_BEGIN_ALLOW_THREADS
        // try {
        //     ret = f.get();
        // }
        // catch (PycbcException e){
        //     msg = e.what();
        //     file = e.get_file();
        //     line = e.get_line();
        //     ec = e.get_error_code();
        // }
        // catch (const std::exception& e) {
        //     ec = PycbcError::InternalSDKError;
        //     msg = e.what();
        // }
        // Py_END_ALLOW_THREADS

        // std::string ec_category = std::string(ec.category().name());
        // if (!file.empty()){
        //     pycbc_set_python_exception(msg.c_str(), ec, file.c_str(), line);
        // }else if (ec_category.compare("pycbc") == 0){
        //     pycbc_set_python_exception(msg.c_str(), ec, __FILE__, __LINE__);
        // }
        // return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
handle_close_connection([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_conn = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_result = nullptr;

    static const char* kw_list[] = { "", "callback", "errback", nullptr };

    const char* kw_format = "O!|OO";
    int ret = PyArg_ParseTupleAndKeywords(
      args, kwargs, kw_format, const_cast<char**>(kw_list), &PyCapsule_Type, &pyObj_conn, &pyObj_callback, &pyObj_errback);

    if (!ret) {
        pycbc_set_python_exception(CANNOT_PARSE_CONN_ARGS_MSG("close"), PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }

    connection* conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(NULL_CONN_OBJECT, PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    {
        int callback_count = 0;
        Py_BEGIN_ALLOW_THREADS conn->cluster_->close([pyObj_callback, pyObj_errback, callback_count, barrier]() mutable {
            if (callback_count == 0) {
                close_connection_callback(pyObj_callback, pyObj_errback, barrier);
            }
            callback_count++;
        });
        Py_END_ALLOW_THREADS
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        return handle_conn_blocking_result(std::move(f));
        // PyObject* ret = nullptr;
        // std::string file;
        // int line;
        // std::error_code ec;
        // std::string msg;

        // Py_BEGIN_ALLOW_THREADS
        // try {
        //     ret = f.get();
        // }
        // catch (PycbcException e){
        //     msg = e.what();
        //     file = e.get_file();
        //     line = e.get_line();
        //     ec = e.get_error_code();
        // }
        // catch (const std::exception& e) {
        //     ec = PycbcError::InternalSDKError;
        //     msg = e.what();
        // }
        // Py_END_ALLOW_THREADS

        // std::string ec_category = std::string(ec.category().name());
        // if (!file.empty()){
        //     pycbc_set_python_exception(msg.c_str(), ec, file.c_str(), line);
        // }else if (ec_category.compare("pycbc") == 0){
        //     pycbc_set_python_exception(msg.c_str(), ec, __FILE__, __LINE__);
        // }
        // return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
handle_open_or_close_bucket([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    char* bucket_name = nullptr;
    PyObject* pyObj_conn = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_result = nullptr;

    int open = 1;

    static const char* kw_list[] = { "", "", "callback", "errback", "open_bucket", nullptr };

    // TODO:  something about passing in a boolean corrupts the param before it
    //      "O!s|OOp" would cause the errback PyObject to be corrupted -- no idea why???
    //      don't seem to have this issue in the KV ops...
    const char* kw_format = "O!s|OOi";

    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &bucket_name,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &open);

    if (!ret) {
        pycbc_set_python_exception(
          CANNOT_PARSE_BUCKET_ARGS_MSG(open == 1 ? "open" : "close"), PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }

    connection* conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(NULL_CONN_OBJECT, PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    {
        int callback_count = 0;
        Py_BEGIN_ALLOW_THREADS if (open)
        {
            conn->cluster_->open_bucket(bucket_name,
                                        [pyObj_callback, pyObj_errback, callback_count, open, barrier](std::error_code ec) mutable {
                                            // @TODO: should the c++ client execute this lambda more than once?
                                            if (callback_count == 0) {
                                                bucket_op_callback(ec, open, pyObj_callback, pyObj_errback, barrier);
                                            }
                                            callback_count++;
                                        });
        }
        else
        {
            conn->cluster_->close_bucket(bucket_name,
                                         [pyObj_callback, pyObj_errback, callback_count, open, barrier](std::error_code ec) mutable {
                                             if (callback_count == 0) {
                                                 bucket_op_callback(ec, open, pyObj_callback, pyObj_errback, barrier);
                                             }
                                             callback_count++;
                                         });
        }
        Py_END_ALLOW_THREADS
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        return handle_conn_blocking_result(std::move(f));
        // PyObject* ret = nullptr;
        // std::string file;
        // int line;
        // std::error_code ec;
        // std::string msg;

        // Py_BEGIN_ALLOW_THREADS
        // try {
        //     ret = f.get();
        // }
        // catch (PycbcException e){
        //     msg = e.what();
        //     file = e.get_file();
        //     line = e.get_line();
        //     ec = e.get_error_code();
        // }
        // catch (const std::exception& e) {
        //     ec = PycbcError::InternalSDKError;
        //     msg = e.what();
        // }
        // Py_END_ALLOW_THREADS

        // std::string ec_category = std::string(ec.category().name());
        // if (!file.empty()){
        //     pycbc_set_python_exception(msg.c_str(), ec, file.c_str(), line);
        // }else if (ec_category.compare("pycbc") == 0){
        //     pycbc_set_python_exception(msg.c_str(), ec, __FILE__, __LINE__);
        // }
        // return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
handle_conn_blocking_result(std::future<PyObject*>&& fut)
{
    PyObject* ret = nullptr;
    std::string file;
    int line;
    std::error_code ec;
    std::string msg;

    Py_BEGIN_ALLOW_THREADS
    try {
        ret = fut.get();
    } catch (PycbcException e) {
        msg = e.what();
        file = e.get_file();
        line = e.get_line();
        ec = e.get_error_code();
    } catch (const std::exception& e) {
        ec = PycbcError::InternalSDKError;
        msg = e.what();
    }
    Py_END_ALLOW_THREADS

      std::string ec_category = std::string(ec.category().name());
    if (!file.empty()) {
        pycbc_set_python_exception(msg.c_str(), ec, file.c_str(), line);
    } else if (ec_category.compare("pycbc") == 0) {
        pycbc_set_python_exception(msg.c_str(), ec, __FILE__, __LINE__);
    }
    return ret;
}
