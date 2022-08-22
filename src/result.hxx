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

#pragma once

#include "client.hxx"
#include <queue>
#include <couchbase/mutation_token.hxx>

template<class T>
class rows_queue
{
  public:
    rows_queue()
      : _rows()
      , _mut()
      , _cond()
    {
    }

    ~rows_queue()
    {
    }

    void put(T row)
    {
        std::lock_guard<std::mutex> lock(_mut);
        _rows.push(row);
        _cond.notify_one();
    }

    T get(std::chrono::milliseconds timeout_ms)
    {
        std::unique_lock<std::mutex> lock(_mut);

        while (_rows.empty()) {
            auto now = std::chrono::system_clock::now();
            if (_cond.wait_until(lock, now + timeout_ms) == std::cv_status::timeout) {
                // this will cause iternext to return nullptr, which stops iteration
                return nullptr;
            }
        }
        auto row = _rows.front();
        _rows.pop();
        return row;
    }

    int size()
    {
        return _rows.size();
    }

  private:
    std::queue<T> _rows;
    std::mutex _mut;
    std::condition_variable _cond;
};

struct result {
    PyObject_HEAD PyObject* dict;
    std::error_code ec;
};

int
pycbc_result_type_init(PyObject** ptr);

PyObject*
create_result_obj();

struct mutation_token {
    PyObject_HEAD couchbase::mutation_token* token;
};

int
pycbc_mutation_token_type_init(PyObject** ptr);

PyObject*
create_mutation_token_obj(struct couchbase::mutation_token mt);

struct streamed_result {
    PyObject_HEAD std::error_code ec;
    std::shared_ptr<rows_queue<PyObject*>> rows;
    std::chrono::milliseconds timeout_ms{};
};

int
pycbc_streamed_result_type_init(PyObject** ptr);

streamed_result*
create_streamed_result_obj(std::chrono::milliseconds timeout_ms);
