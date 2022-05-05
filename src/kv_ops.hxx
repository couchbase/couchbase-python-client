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

#ifndef KV_OPS_H_
#define KV_OPS_H_

#include "client.hxx"
#include <couchbase/cas.hxx>

/**
 * GET, GET_PROJECTED, GET_AND_LOCK, GET_AND_TOUCH
 * EXISTS, TOUCH, UNLOCK
 */
struct read_options {
    // common - required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type = Operations::UNKNOWN;

    // common - options
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;

    // optional
    bool with_expiry;
    uint32_t expiry;
    uint32_t lock_time;
    couchbase::cas cas;
    PyObject* span;
    PyObject* project;

    // TODO:
    // retries?
    // partition?
};

/**
 * INSERT, UPSERT, REPLACE, REMOVE
 */
struct mutation_options {
    // common - required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type = Operations::UNKNOWN;
    PyObject* value; // not for REMOVE

    // common - optional
    uint8_t durability;
    uint8_t replicate_to;
    uint8_t persist_to;
    uint32_t expiry;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    PyObject* span;

    // optional: REPLACE
    couchbase::cas cas;
    bool preserve_expiry;

    // TODO:
    // retries?
    // partition?
    // durability_timeout
};

PyObject*
handle_kv_op(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_kv_multi_op(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_kv_blocking_result(std::future<PyObject*>&& fut);

#endif
