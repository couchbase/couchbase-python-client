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

#ifndef CLIENT_H_
#define CLIENT_H_

// NOLINTNEXTLINE
#include "Python.h" // NOLINT
#include "structmember.h"
#include <core/cluster.hxx>
#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>
#include <list>
#include <thread>
#include "result.hxx"
#include "exceptions.hxx"

#define PY_SSIZE_T_CLEAN

class Operations
{
  public:
    enum OperationType {
        UNKNOWN,
        GET,
        GET_PROJECTED,
        GET_AND_LOCK,
        GET_AND_TOUCH,
        GET_ANY_REPLICA,
        GET_ALL_REPLICAS,
        EXISTS,
        TOUCH,
        UNLOCK,
        INSERT,
        UPSERT,
        REPLACE,
        REMOVE,
        MUTATE_IN,
        LOOKUP_IN,
        DIAGNOSTICS,
        PING,
        INCREMENT,
        DECREMENT,
        APPEND,
        PREPEND,
        N1QL_QUERY,
        CLUSTER_MGMT_CLUSTER_INFO
    };

    Operations()
      : Operations{ UNKNOWN }
    {
    }
    constexpr Operations(OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(Operations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(Operations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "GET "
                          "GET_PROJECTED "
                          "GET_AND_LOCK "
                          "GET_AND_TOUCH "
                          "GET_ANY_REPLICA "
                          "GET_ALL_REPLICAS "
                          "EXISTS "
                          "TOUCH "
                          "UNLOCK "
                          "INSERT "
                          "UPSERT "
                          "REPLACE "
                          "REMOVE "
                          "MUTATE_IN "
                          "LOOKUP_IN "
                          "DIAGNOSTICS "
                          "PING "
                          "INCREMENT "
                          "DECREMENT "
                          "APPEND "
                          "PREPEND "
                          "N1QL_QUERY "
                          "CLUSTER_MGMT_CLUSTER_INFO";

        return ops;
    }

  private:
    OperationType operation;
};

enum {
    PYCBC_FMT_LEGACY_JSON = 0x00,
    PYCBC_FMT_LEGACY_PICKLE = 0x01,
    PYCBC_FMT_LEGACY_BYTES = 0x02,
    PYCBC_FMT_LEGACY_UTF8 = 0x04,
    PYCBC_FMT_LEGACY_MASK = 0x07,

    PYCBC_FMT_COMMON_PICKLE = (0x01U << 24),
    PYCBC_FMT_COMMON_JSON = (0x02U << 24),
    PYCBC_FMT_COMMON_BYTES = (0x03U << 24),
    PYCBC_FMT_COMMON_UTF8 = (0x04U << 24),
    PYCBC_FMT_COMMON_MASK = (0xFFU << 24),

    PYCBC_FMT_JSON = PYCBC_FMT_LEGACY_JSON | PYCBC_FMT_COMMON_JSON,
    PYCBC_FMT_PICKLE = PYCBC_FMT_LEGACY_PICKLE | PYCBC_FMT_COMMON_PICKLE,
    PYCBC_FMT_BYTES = PYCBC_FMT_LEGACY_BYTES | PYCBC_FMT_COMMON_BYTES,
    PYCBC_FMT_UTF8 = PYCBC_FMT_LEGACY_UTF8 | PYCBC_FMT_COMMON_UTF8
};

struct callback_context {
  private:
    PyObject* callback;
    PyObject* errback;
    PyObject* transcoder;
    PyObject* row_callback;

  public:
    callback_context(PyObject* callback, PyObject* errback, PyObject* transcoder, PyObject* row_callback)
      : callback{ callback }
      , errback{ errback }
      , transcoder{ transcoder }
      , row_callback{ row_callback }
    {
        PyGILState_STATE state = PyGILState_Ensure();
        Py_XINCREF(callback);
        Py_XINCREF(errback);
        Py_XINCREF(transcoder);
        Py_XINCREF(row_callback);
        PyGILState_Release(state);
    }

    callback_context()
      : callback_context{ nullptr, nullptr, nullptr, nullptr }
    {
    }
    callback_context(PyObject* callback, PyObject* errback)
      : callback_context{ callback, errback, nullptr, nullptr }
    {
    }
    callback_context(PyObject* callback, PyObject* errback, PyObject* transcoder)
      : callback_context{ callback, errback, transcoder, nullptr }
    {
    }

    callback_context(const callback_context& ctx)
      : callback_context{ ctx.callback, ctx.errback, ctx.transcoder, ctx.row_callback }
    {
    }

    callback_context(callback_context&& ctx)
      : callback{ ctx.callback }
      , errback{ ctx.errback }
      , transcoder{ ctx.transcoder }
      , row_callback{ ctx.row_callback }
    {
        ctx.callback = nullptr;
        ctx.errback = nullptr;
        ctx.transcoder = nullptr;
        ctx.row_callback = nullptr;
    }

    ~callback_context()
    {
        PyGILState_STATE state = PyGILState_Ensure();
        Py_XDECREF(callback);
        Py_XDECREF(errback);
        Py_XDECREF(transcoder);
        Py_XDECREF(row_callback);
        PyGILState_Release(state);
    }

    // maybe use [[nodiscard]] to force assignment (or at least gen a compiler warning)?
    // const = no mucking w/ ref counts
    PyObject* get_callback() const
    {
        return callback;
    }

    PyObject* get_errback() const
    {
        return errback;
    }

    PyObject* get_transcoder() const
    {
        return transcoder;
    }

    PyObject* get_row_callback() const
    {
        return row_callback;
    }
};

#define RESULT_VALUE "value"
#define RESULT_CAS "cas"
#define RESULT_FLAGS "flags"
#define RESULT_EXPIRY "expiry"
#define RESULT_KEY "key"
#define RESULT_MUTATION_TOKEN "mutation_token"
#define RESULT_EXISTS "exists"
#define TRANSCODER_ENCODE "encode_value"
#define SERIALIZE "serialize"
#define TRANSCODER_DECODE "decode_value"
#define DESERIALIZE "deserialize"

struct connection {
    asio::io_context io_;
    std::shared_ptr<couchbase::core::cluster> cluster_;
    std::list<std::thread> io_threads_;

    connection(int num_io_threads)
    {
        cluster_ = couchbase::core::cluster::create(io_);
        for (int i = 0; i < num_io_threads; i++) {
            // TODO: consider maybe catching exceptions and running run() again?  For now, lets
            // log the exception and rethrow (which will lead to a crash)
            io_threads_.emplace_back([&] { io_.run(); });
        }
    }
};

void
add_ops_enum(PyObject* module);

void
add_constants(PyObject* module);

std::string
json_encode(PyObject* obj);

std::tuple<std::string, uint32_t>
encode_value(PyObject* transcoder, PyObject* value);

PyObject*
decode_value(const PyObject* transcoder, const char* value, size_t nvalue, uint32_t flags, bool deserialize = false);

PyObject*
json_decode(const char* value, size_t nvalue);

std::string
service_type_to_str(couchbase::core::service_type t);

couchbase::core::service_type
str_to_service_type(std::string svc);

extern PyTypeObject result_type;
extern PyTypeObject exception_base_type;
extern PyTypeObject streamed_result_type;
extern PyTypeObject mutation_token_type;

#endif
