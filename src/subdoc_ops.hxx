#ifndef SUBDOC_OPS_H_
#define SUBDOC_OPS_H_

#include "client.hxx"

struct mutate_in_spec {
    uint8_t op;
    uint8_t flags;
    char* path;
    std::string value;

    PyObject* pyObj_value;
    bool create_parents;
    bool xattr;
    bool expand_macros;
};

struct lookup_in_spec {
    uint8_t op;
    uint8_t flags;
    char* path;
    bool xattr;
};

struct lookup_in_options {
    // required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type = Operations::LOOKUP_IN;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    bool access_deleted;
    PyObject* span;
    PyObject* specs;

    // TODO:
    // retries?
    // partition?
};

struct mutate_in_options {
    // required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type = Operations::MUTATE_IN;

    // optional
    uint8_t durability;
    uint8_t replicate_to;
    uint8_t persist_to;
    uint8_t semantics;
    uint32_t expiry;
    uint64_t cas;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    bool preserve_expiry;
    bool access_deleted;
    PyObject* span;
    PyObject* specs;

    // TODO:
    // durability_timeout;
    // create_as_deleted;
    // retries?
    // partition?
};

PyObject*
handle_subdoc_op(PyObject* self, PyObject* args, PyObject* kwargs);

#endif
