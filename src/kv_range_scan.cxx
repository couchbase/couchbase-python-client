#include "kv_range_scan.hxx"
#include "utils.hxx"

std::optional<couchbase::core::scan_term>
get_scan_term(PyObject* pyObj_scan_term)
{
    if (pyObj_scan_term == nullptr) {
        return std::nullopt;
    }
    PyObject* pyObj_term = PyDict_GetItemString(pyObj_scan_term, "term");
    if (pyObj_term == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Must provide term for ScanTerm.");
        return {};
    }
    if (!PyUnicode_Check(pyObj_term)) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Term should be a string.");
        return {};
    }

    couchbase::core::scan_term scan_term;

    try {
        scan_term = couchbase::core::scan_term{ PyUnicode_AsUTF8(pyObj_term) };
    } catch (const std::exception& e) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, e.what());
        return {};
    }
    PyObject* pyObj_exclusive = PyDict_GetItemString(pyObj_scan_term, "exclusive");
    if (pyObj_exclusive != nullptr && pyObj_exclusive != Py_None) {
        if (pyObj_exclusive == Py_True) {
            scan_term.exclusive = true;
        } else if (pyObj_exclusive == Py_False) {
            scan_term.exclusive = false;
        } else {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Exclusive must be a boolean or None.");
        }
    }
    return scan_term;
}

couchbase::core::range_scan
get_range_scan(PyObject* op_args)
{
    PyObject* pyObj_start_term = PyDict_GetItemString(op_args, "start");
    auto start_term = get_scan_term(pyObj_start_term);

    PyObject* pyObj_end_term = PyDict_GetItemString(op_args, "end");
    auto end_term = get_scan_term(pyObj_end_term);

    return couchbase::core::range_scan{ start_term, end_term };
}

couchbase::core::sampling_scan
get_sampling_scan(PyObject* op_args)
{
    PyObject* pyObj_limit = PyDict_GetItemString(op_args, "limit");
    if (pyObj_limit == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Must provide limit for sample scan.");
        return {};
    }
    auto scan_type = couchbase::core::sampling_scan{ static_cast<std::size_t>(PyLong_AsUnsignedLong(pyObj_limit)) };
    PyObject* pyObj_seed = PyDict_GetItemString(op_args, "seed");
    if (pyObj_seed != nullptr && pyObj_seed != Py_None) {
        scan_type.seed = static_cast<std::uint64_t>(PyLong_AsUnsignedLong(pyObj_seed));
    }

    return scan_type;
}

couchbase::core::prefix_scan
get_prefix_scan(PyObject* op_args)
{
    PyObject* pyObj_prefix = PyDict_GetItemString(op_args, "prefix");
    if (pyObj_prefix == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Must provide prefix for prefix scan.");
        return {};
    }
    if (!PyUnicode_Check(pyObj_prefix)) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Prefix should be a string.");
        return {};
    }
    return couchbase::core::prefix_scan{ PyUnicode_AsUTF8(pyObj_prefix) };
}

couchbase::core::range_scan_orchestrator_options
get_range_scan_orchestrator_options(PyObject* op_args)
{
    couchbase::core::range_scan_orchestrator_options opts{};

    PyObject* pyObj_ids_only = PyDict_GetItemString(op_args, "ids_only");
    opts.ids_only = pyObj_ids_only != nullptr && pyObj_ids_only == Py_True;

    PyObject* pyObj_consistent_with = PyDict_GetItemString(op_args, "consistent_with");
    if (pyObj_consistent_with != nullptr && PyList_Check(pyObj_consistent_with)) {
        auto mutation_state = get_mutation_state(pyObj_consistent_with);
        opts.consistent_with = couchbase::core::mutation_state{ mutation_state };
    }

    PyObject* pyObj_batch_byte_limit = PyDict_GetItemString(op_args, "batch_byte_limit");
    if (pyObj_batch_byte_limit != nullptr) {
        opts.batch_byte_limit = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_batch_byte_limit));
    }

    PyObject* pyObj_batch_item_limit = PyDict_GetItemString(op_args, "batch_item_limit");
    if (pyObj_batch_item_limit != nullptr) {
        opts.batch_item_limit = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_batch_item_limit));
    }

    PyObject* pyObj_concurrency = PyDict_GetItemString(op_args, "concurrency");
    if (pyObj_concurrency != nullptr) {
        opts.concurrency = static_cast<uint16_t>(PyLong_AsUnsignedLong(pyObj_concurrency));
    }

    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        auto timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        if (0 < timeout) {
            opts.timeout = timeout_ms;
        }
    }

    PyObject* pyObj_span = PyDict_GetItemString(op_args, "span");
    if (pyObj_span != nullptr) {
        opts.parent_span = std::make_shared<pycbc::request_span>(pyObj_span);
    }

    return opts;
}

scan_iterator*
handle_kv_range_scan_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_conn = nullptr;
    char* bucket_name = nullptr;
    char* scope_name = nullptr;
    char* collection_name = nullptr;
    Operations::OperationType op_type = Operations::UNKNOWN;
    PyObject* pyObj_op_args = nullptr;

    static const char* kw_list[] = { "conn", "bucket", "scope", "collection_name", "op_type", "op_args", nullptr };

    const char* kw_format = "O!sssI|O";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &bucket_name,
                                          &scope_name,
                                          &collection_name,
                                          &op_type,
                                          &pyObj_op_args);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform kv range scan operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    auto barrier = std::make_shared<std::promise<tl::expected<couchbase::core::topology::configuration, std::error_code>>>();
    auto f = barrier->get_future();
    conn->cluster_->with_bucket_configuration(
      bucket_name, [barrier](std::error_code ec, const couchbase::core::topology::configuration& config) mutable {
          if (ec) {
              return barrier->set_value(tl::unexpected(ec));
          }
          barrier->set_value(config);
      });
    auto config = f.get();
    if (!config.has_value()) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                                   __FILE__,
                                   __LINE__,
                                   "Cannot perform kv range scan operation.  Unable to get bucket configuration.");
        return nullptr;
    }
    if (!config->supports_range_scan()) {
        pycbc_set_python_exception(
          PycbcError::FeatureUnavailable, __FILE__, __LINE__, "The server does not support key-value scan operations.");
        return nullptr;
    }
    if (!config->vbmap || config->vbmap->empty()) {
        pycbc_set_python_exception(
          PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Cannot perform kv range scan operation.  Unable to get vbucket map.");
        return nullptr;
    }
    auto vbucket_map = config->vbmap.value();

    auto agent_group = couchbase::core::agent_group(conn->io_, couchbase::core::agent_group_config{ { conn->cluster_ } });
    agent_group.open_bucket(bucket_name);
    auto agent = agent_group.get_agent(bucket_name);

    if (!agent.has_value()) {
        pycbc_set_python_exception(
          PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Cannot perform kv range scan operation.  Unable to get operation agent.");
        return nullptr;
    }

    auto options = get_range_scan_orchestrator_options(pyObj_op_args);
    if (PyErr_Occurred() != nullptr) {
        return nullptr;
    }

    std::variant<std::monostate, couchbase::core::range_scan, couchbase::core::prefix_scan, couchbase::core::sampling_scan> scan_type{};
    if (op_type == Operations::KV_RANGE_SCAN) {
        scan_type = get_range_scan(pyObj_op_args);
    } else if (op_type == Operations::KV_PREFIX_SCAN) {
        scan_type = get_prefix_scan(pyObj_op_args);
    } else {
        scan_type = get_sampling_scan(pyObj_op_args);
    }

    if (PyErr_Occurred() != nullptr) {
        return nullptr;
    }

    auto orchestrator =
      couchbase::core::range_scan_orchestrator(conn->io_, agent.value(), vbucket_map, scope_name, collection_name, scan_type, options);
    auto scan_result = orchestrator.scan();
    if (!scan_result.has_value()) {
        pycbc_set_python_exception(
          PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Cannot perform kv scan operation.  Unable to start scan operation.");
        return nullptr;
    }

    return create_scan_iterator_obj(scan_result.value());
}
