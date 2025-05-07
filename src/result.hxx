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
#include "utils.hxx"
#include <core/scan_result.hxx>
#include <queue>

template<class T>
class rows_queue
{
public:
  rows_queue()
    : rows_()
    , mut_()
    , cv_()
  {
  }

  ~rows_queue()
  {
  }

  void put(T row)
  {
    std::lock_guard<std::mutex> lock(mut_);
    rows_.push(row);
    cv_.notify_one();
  }

  T get(std::chrono::milliseconds timeout_ms)
  {
    std::unique_lock<std::mutex> lock(mut_);

    while (rows_.empty()) {
      if (cv_.wait_for(lock, timeout_ms) == std::cv_status::timeout) {
        // This timeout (e.g. timeout_ms) is the same timeout we pass to the C++ core.
        // If we timeout on the Python side this means:
        //   - Edge case where the C++ core is about to timeout. We want to use the C++ core error
        //   details,
        //     so wait a little longer to get the C++ core timeout.
        //   - The result set is large and since we don't have streaming support yet, we have to
        //   wait for
        //     the entire result set to be returned.  Again we should wait until we get the results.
        // PYCBC-1685: Instead of trying to do some tricky error handling we instead wait for the
        // C++ core results and log a message that can provide insight to users about the SDK
        // behavior.
        CB_LOG_DEBUG(
          "PYCBC: No results received from C++ core after {}ms.  Continue to wait for results.",
          timeout_ms.count());
      }
    }

    auto row = rows_.front();
    rows_.pop();
    return row;
  }

  int size()
  {
    std::lock_guard<std::mutex> lock(mut_);
    return rows_.size();
  }

private:
  std::queue<T> rows_;
  std::mutex mut_;
  bool cancel_streaming_{ false };
  std::condition_variable cv_;
};

struct result {
  PyObject_HEAD PyObject* dict;
};

PyObject*
create_result_obj();

struct mutation_token {
  PyObject_HEAD couchbase::mutation_token* token;
};

PyObject*
create_mutation_token_obj(struct couchbase::mutation_token mt);

struct streamed_result {
  PyObject_HEAD std::error_code ec;
  std::shared_ptr<rows_queue<PyObject*>> rows;
  std::chrono::milliseconds timeout_ms{};
};

streamed_result*
create_streamed_result_obj(std::chrono::milliseconds timeout_ms);

struct scan_iterator {
  PyObject_HEAD std::shared_ptr<couchbase::core::scan_result> scan_result;
};

scan_iterator*
create_scan_iterator_obj(couchbase::core::scan_result result);

PyObject*
add_result_objects(PyObject* pyObj_module);
