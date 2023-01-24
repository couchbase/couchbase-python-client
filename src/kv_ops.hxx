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
#include <couchbase/insert_options.hxx>
#include <couchbase/remove_options.hxx>
#include <couchbase/replace_options.hxx>
#include <couchbase/upsert_options.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/replicate_to.hxx>

/**
 * GET, GET_PROJECTED, GET_AND_LOCK, GET_AND_TOUCH
 * EXISTS, TOUCH, UNLOCK
 */
struct read_options {
    // common - required
    connection* conn;
    couchbase::core::document_id id;
    Operations::OperationType op_type{ Operations::UNKNOWN };

    // common - options
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;

    // optional
    bool with_expiry{ false };
    uint32_t expiry{};
    uint32_t lock_time{};
    couchbase::cas cas;
    PyObject* span{ nullptr };
    PyObject* project{ nullptr };

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
    couchbase::core::document_id id;
    Operations::OperationType op_type{ Operations::UNKNOWN };
    PyObject* value{ nullptr }; // not for REMOVE

    // common - optional
    couchbase::durability_level durability_level{ couchbase::durability_level::none };
    bool use_legacy_durability{ false };
    couchbase::replicate_to replicate_to{ couchbase::replicate_to::none };
    couchbase::persist_to persist_to{ couchbase::persist_to::none };
    uint32_t expiry{ 0 };
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    PyObject* span = nullptr;

    // optional: REPLACE
    couchbase::cas cas;
    bool preserve_expiry{ false };

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

// couchbase::core::query_profile_mode
// str_to_profile_mode(std::string profile_mode);

// std::string
// profile_mode_to_str(couchbase::core::query_profile_mode profile_mode);

#endif
