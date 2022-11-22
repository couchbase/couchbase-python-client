#pragma once

#include "client.hxx"
#include <core/range_scan_options.hxx>
#include <core/range_scan_orchestrator.hxx>
#include <core/range_scan_orchestrator_options.hxx>

struct range_scan_create_options {
    // common - required
    connection* conn;

    // common - options
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_scan_timeout;
    std::string collection_name;
    std::string scope_name;
    std::uint32_t collection_id{ 0 };
    std::variant<couchbase::core::range_scan, couchbase::core::prefix_scan, couchbase::core::sampling_scan> scan_type;
    std::optional<couchbase::core::range_snapshot_requirements> snapshot_requirements{};
    bool ids_only{ false };

    // optional
    PyObject* span{ nullptr };
};

struct range_scan_continue_options {
    // common - required
    connection* conn;

    // common - options
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_scan_timeout;
    std::uint32_t batch_item_limit{ 0 };
    std::uint32_t batch_byte_limit{ 0 };
    bool ids_only{ false };
};

struct range_scan_cancel_options {
    // common - required
    connection* conn;

    // common - options
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_scan_timeout;
};

scan_iterator*
handle_kv_range_scan_op(PyObject* self, PyObject* args, PyObject* kwargs);
