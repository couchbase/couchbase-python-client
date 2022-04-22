#include "n1ql.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include <couchbase/query_scan_consistency.hxx>
#include <couchbase/query_profile_mode.hxx>

std::string
scan_consistency_type_to_string(couchbase::query_scan_consistency consistency)
{
    switch (consistency) {
        case couchbase::query_scan_consistency::not_bounded:
            return "not_bounded";
        case couchbase::query_scan_consistency::request_plus:
            return "request_plus";
    }
    // should not be able to reach here, since this is an enum class
    return "unknown";
}

couchbase::query_profile_mode
str_to_profile_mode(std::string profile_mode)
{
    if (profile_mode.compare("off") == 0) {
        return couchbase::query_profile_mode::off;
    }
    if (profile_mode.compare("phases") == 0) {
        return couchbase::query_profile_mode::phases;
    }
    if (profile_mode.compare("timings") == 0) {
        return couchbase::query_profile_mode::timings;
    }
    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, "Invalid Profile Mode.");
    return {};
}

std::vector<couchbase::mutation_token>
get_mutation_state(PyObject* pyObj_mutation_state)
{
    std::vector<couchbase::mutation_token> mut_state{};
    size_t ntokens = static_cast<size_t>(PySet_GET_SIZE(pyObj_mutation_state));
    for (size_t ii = 0; ii < ntokens; ++ii) {

        struct couchbase::mutation_token token = {};
        PyObject* pyObj_mut_token = PyList_GetItem(pyObj_mutation_state, ii);
        PyObject* pyObj_bucket_name = PyDict_GetItemString(pyObj_mut_token, "bucket_name");
        token.bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));

        PyObject* pyObj_partition_uuid = PyDict_GetItemString(pyObj_mut_token, "partition_uuid");
        token.partition_uuid = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_partition_uuid));

        PyObject* pyObj_sequence_number = PyDict_GetItemString(pyObj_mut_token, "sequence_number");
        token.sequence_number = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_sequence_number));

        PyObject* pyObj_partition_id = PyDict_GetItemString(pyObj_mut_token, "partition_id");
        token.partition_id = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_partition_id));

        mut_state.emplace_back(token);
    }
    return mut_state;
}

PyObject*
get_result_metrics(couchbase::operations::query_response::query_metrics metrics)
{
    PyObject* pyObj_metrics = PyDict_New();
    std::chrono::duration<unsigned long long, std::nano> int_nsec = metrics.elapsed_time;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
    if (-1 == PyDict_SetItemString(pyObj_metrics, "elapsed_time", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_nsec = metrics.execution_time;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
    if (-1 == PyDict_SetItemString(pyObj_metrics, "execution_time", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.result_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "result_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.result_size);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "result_size", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.sort_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "sort_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.mutation_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "mutation_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.error_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "error_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.warning_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "warning_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    return pyObj_metrics;
}

PyObject*
get_result_metadata(couchbase::operations::query_response::query_meta_data metadata, bool include_metrics)
{
    PyObject* pyObj_metadata = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(metadata.request_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "request_id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(metadata.client_context_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "client_context_id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(metadata.status.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "status", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (metadata.signature.has_value()) {
        pyObj_tmp = json_decode(metadata.signature.value().c_str(), metadata.signature.value().length());
        if (-1 == PyDict_SetItemString(pyObj_metadata, "signature", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }

    if (metadata.profile.has_value()) {
        pyObj_tmp = json_decode(metadata.profile.value().c_str(), metadata.profile.value().length());
        if (-1 == PyDict_SetItemString(pyObj_metadata, "profile", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }

    if (metadata.warnings.has_value()) {
        PyObject* pyObj_warnings = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& warning : metadata.warnings.value()) {
            PyObject* pyObj_warning = PyDict_New();

            pyObj_tmp = PyLong_FromLong(warning.code);
            if (-1 == PyDict_SetItemString(pyObj_warning, "code", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(warning.message.c_str());
            if (-1 == PyDict_SetItemString(pyObj_warning, "message", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            if (-1 == PyList_Append(pyObj_warnings, pyObj_warning)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_warning);
        }

        if (-1 == PyDict_SetItemString(pyObj_metadata, "warnings", pyObj_warnings)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_warnings);
    }

    if (metadata.errors.has_value()) {
        PyObject* pyObj_errors = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& error : metadata.errors.value()) {
            PyObject* pyObj_error = PyDict_New();

            pyObj_tmp = PyLong_FromLong(error.code);
            if (-1 == PyDict_SetItemString(pyObj_error, "code", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(error.message.c_str());
            if (-1 == PyDict_SetItemString(pyObj_error, "message", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            if (-1 == PyList_Append(pyObj_errors, pyObj_error)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_error);
        }

        if (-1 == PyDict_SetItemString(pyObj_metadata, "errors", pyObj_errors)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_errors);
    }

    if (include_metrics && metadata.metrics.has_value()) {
        PyObject* pyObject_metrics = get_result_metrics(metadata.metrics.value());

        if (-1 == PyDict_SetItemString(pyObj_metadata, "metrics", pyObject_metrics)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObject_metrics);
    }

    return pyObj_metadata;
}

result*
create_result_from_query_response(couchbase::operations::query_response resp, bool include_metrics)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_payload = PyDict_New();

    PyObject* pyObject_metadata = get_result_metadata(resp.meta, include_metrics);
    if (-1 == PyDict_SetItemString(pyObj_payload, "metadata", pyObject_metadata)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObject_metadata);

    if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_payload)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_payload);

    return res;
}

void
create_query_result(couchbase::operations::query_response resp,
                    bool include_metrics,
                    std::shared_ptr<rows_queue<PyObject*>> rows,
                    PyObject* pyObj_callback,
                    PyObject* pyObj_errback)
{

    auto set_exception = false;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();
    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing N1QL operation.");
        // lets clear any errors
        PyErr_Clear();
        rows->put(pyObj_exc);
    } else {
        auto res = create_result_from_query_response(resp, include_metrics);

        if (res == nullptr || PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else {
            // None indicates done (i.e. raise StopIteration)
            Py_INCREF(Py_None);
            rows->put(Py_None);
            rows->put(reinterpret_cast<PyObject*>(res));
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "N1QL operation error.");
        rows->put(pyObj_exc);
    }

    // This is for txcouchbase -- let it knows we're done w/ the query request
    if (pyObj_callback != nullptr) {
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "N1QL complete callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

streamed_result*
handle_n1ql_query([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    char* statement = nullptr;

    char* scan_consistency = nullptr;
    char* bucket_name = nullptr;
    char* scope_name = nullptr;
    char* scope_qualifier = nullptr;
    char* client_context_id = nullptr;
    char* profile_mode = nullptr;
    char* send_to_node = nullptr;
    uint64_t timeout = 0;
    uint64_t max_parallelism = 0;
    uint64_t scan_cap = 0;
    uint64_t scan_wait = 0;
    uint64_t pipeline_batch = 0;
    uint64_t pipeline_cap = 0;
    // booleans, but use int to read from kwargs
    int adhoc = 1;
    int metrics = 0;
    int readonly = 0;
    int flex_index = 0;
    int preserve_expiry = 0;

    PyObject* pyObj_mutation_state = nullptr;
    PyObject* pyObj_raw = nullptr;
    PyObject* pyObj_named_parameters = nullptr;
    PyObject* pyObj_positional_parameters = nullptr;
    PyObject* pyObj_serializer = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_row_callback = nullptr;

    static const char* kw_list[] = { "conn",
                                     "statement",
                                     "bucket_name",
                                     "scope_name",
                                     "scope_qualifier",
                                     "client_context_id",
                                     "scan_consistency",
                                     "profile_mode",
                                     "send_to_node",
                                     "timeout",
                                     "max_parallelism",
                                     "scan_cap",
                                     "scan_wait",
                                     "pipeline_batch",
                                     "pipeline_cap",
                                     "adhoc",
                                     "metrics",
                                     "readonly",
                                     "flex_index",
                                     "preserve_expiry",
                                     "positional_parameters",
                                     "named_parameters",
                                     "mutation_state",
                                     "raw",
                                     "serializer",
                                     "callback",
                                     "errback",
                                     "row_callback",
                                     nullptr };

    const char* kw_format = "O!s|sssssssLLLLLLiiiiiOOOOOOOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &statement,
                                          &bucket_name,
                                          &scope_name,
                                          &scope_qualifier,
                                          &client_context_id,
                                          &scan_consistency,
                                          &profile_mode,
                                          &send_to_node,
                                          &timeout,
                                          &max_parallelism,
                                          &scan_cap,
                                          &scan_wait,
                                          &pipeline_batch,
                                          &pipeline_cap,
                                          &adhoc,
                                          &metrics,
                                          &readonly,
                                          &flex_index,
                                          &preserve_expiry,
                                          &pyObj_positional_parameters,
                                          &pyObj_named_parameters,
                                          &pyObj_mutation_state,
                                          &pyObj_raw,
                                          &pyObj_serializer,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_row_callback);
    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse arguments");
        return nullptr;
    }

    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::query_timeout;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        PyErr_SetString(PyExc_ValueError, "passed null connection");
        return nullptr;
    }
    PyErr_Clear();

    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

    couchbase::operations::query_request req{ statement };
    // positional parameters
    std::vector<couchbase::json_string> positional_parameters{};
    if (pyObj_positional_parameters && PyList_Check(pyObj_positional_parameters)) {
        size_t nargs = static_cast<size_t>(PyList_Size(pyObj_positional_parameters));
        size_t ii;
        for (ii = 0; ii < nargs; ++ii) {
            PyObject* pyOb_param = PyList_GetItem(pyObj_positional_parameters, ii);
            if (!pyOb_param) {
                // TODO:  handle this better
                PyErr_SetString(PyExc_ValueError, "Unable to parse positional argument.");
                return nullptr;
            }
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyOb_param);
            if (PyUnicode_Check(pyOb_param)) {
                auto res = std::string(PyUnicode_AsUTF8(pyOb_param));
                positional_parameters.push_back(couchbase::json_string{ std::move(res) });
            }
            Py_DECREF(pyOb_param);
            pyOb_param = nullptr;
        }
    }
    if (positional_parameters.size() > 0) {
        req.positional_parameters = positional_parameters;
    }

    // named parameters
    std::map<std::string, couchbase::json_string> named_parameters{};
    if (pyObj_named_parameters && PyDict_Check(pyObj_named_parameters)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_named_parameters, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_value));
                named_parameters.emplace(k, couchbase::json_string{ std::move(res) });
            }
        }
    }
    if (named_parameters.size() > 0) {
        req.named_parameters = named_parameters;
    }

    req.timeout = timeout_ms;
    req.adhoc = adhoc == 1;
    req.metrics = metrics == 1;
    req.readonly = readonly == 1;
    req.flex_index = flex_index == 1;
    req.preserve_expiry = preserve_expiry == 1;

    if (0 < max_parallelism) {
        req.max_parallelism = max_parallelism;
    }
    if (0 < scan_cap) {
        req.scan_cap = scan_cap;
    }
    if (0 < scan_wait) {
        req.scan_wait = std::chrono::milliseconds(std::max(0ULL, scan_wait / 1000ULL));
    }
    if (0 < pipeline_batch) {
        req.pipeline_batch = pipeline_batch;
    }
    if (0 < pipeline_cap) {
        req.pipeline_cap = pipeline_cap;
    }

    if (scan_consistency != nullptr) {
        req.scan_consistency = str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency);
    }

    if (profile_mode != nullptr) {
        req.profile = str_to_profile_mode(profile_mode);
    }

    if (client_context_id != nullptr) {
        req.client_context_id = std::string(client_context_id);
    }

    if (send_to_node != nullptr) {
        req.send_to_node = std::string(send_to_node);
    }

    if (scope_qualifier != nullptr) {
        req.scope_qualifier = std::string(scope_qualifier);
    }

    if (pyObj_mutation_state != nullptr && PyList_Check(pyObj_mutation_state)) {
        req.mutation_state = get_mutation_state(pyObj_mutation_state);
    }

    // raw options
    std::map<std::string, couchbase::json_string> raw_options{};
    if (pyObj_raw && PyDict_Check(pyObj_raw)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_value));
                raw_options.emplace(k, couchbase::json_string{ std::move(res) });
            }
        }
    }
    if (raw_options.size() > 0) {
        req.raw = raw_options;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_callback);

    streamed_result* streamed_res = create_streamed_result_obj();

    req.row_callback = [rows = streamed_res->rows](std::string&& row) {
        PyGILState_STATE state = PyGILState_Ensure();
        PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
        rows->put(pyObj_row);
        PyGILState_Release(state);
        return couchbase::utils::json::stream_control::next_row;
    };

    {
        Py_BEGIN_ALLOW_THREADS conn->cluster_->execute(
          req,
          [rows = streamed_res->rows, include_metrics = req.metrics, pyObj_callback, pyObj_errback](
            couchbase::operations::query_response resp) {
              create_query_result(resp, include_metrics, rows, pyObj_callback, pyObj_errback);
          });
        Py_END_ALLOW_THREADS
    }
    return streamed_res;
}
