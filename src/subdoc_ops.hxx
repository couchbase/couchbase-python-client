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
