#pragma once

#include "client.hxx"

struct counter_options {
    // required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type;
    uint64_t delta;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    uint32_t expiry;
    uint8_t replicate_to;
    uint8_t persist_to;
    couchbase::protocol::durability_level durability;
    uint64_t initial_value;
    PyObject* pyObj_span;
};

struct binary_mutation_options {
    // required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type;
    PyObject* pyObj_value;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    uint8_t replicate_to;
    uint8_t persist_to;
    couchbase::protocol::durability_level durability;
    uint64_t cas;
    PyObject* pyObj_span;
};

PyObject*
handle_binary_op(PyObject* self, PyObject* args, PyObject* kwargs);
