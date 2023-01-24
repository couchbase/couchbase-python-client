/*
 *   Copyright 2016-2022. Couchbase, Inc.
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

#ifndef SUBDOC_OPS_H_
#define SUBDOC_OPS_H_

#include "client.hxx"
#include <core/impl/subdoc/opcode.hxx>
#include <core/impl/subdoc/path_flags.hxx>

struct mutate_in_spec {
    uint8_t op;
    uint8_t flags;
    char* path;
    std::vector<std::byte> value;

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
    couchbase::core::document_id id;
    Operations::OperationType op_type{ Operations::LOOKUP_IN };

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    bool access_deleted{ false };
    PyObject* span{ nullptr };
    PyObject* specs{ nullptr };

    // TODO:
    // retries?
    // partition?
};

struct mutate_in_options {
    // required
    connection* conn;
    couchbase::core::document_id id;
    Operations::OperationType op_type{ Operations::MUTATE_IN };

    // optional
    couchbase::durability_level durability_level{ couchbase::durability_level::none };
    bool use_legacy_durability{ false };
    couchbase::replicate_to replicate_to{ couchbase::replicate_to::none };
    couchbase::persist_to persist_to{ couchbase::persist_to::none };
    couchbase::store_semantics store_semantics{ couchbase::store_semantics::replace };
    uint32_t expiry{ 0 };
    couchbase::cas cas;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    bool preserve_expiry{ false };
    bool access_deleted{ false };
    bool create_as_deleted{ false };
    PyObject* span{ nullptr };
    PyObject* specs{ nullptr };

    // TODO:
    // retries?
    // partition?
};

PyObject*
handle_subdoc_op(PyObject* self, PyObject* args, PyObject* kwargs);

#endif
