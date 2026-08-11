/*
 *   Copyright 2016-2026. Couchbase, Inc.
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

#include "Python.h"

namespace pycbc
{

// Releases the GIL for the guard's lifetime. Unlike Py_BEGIN/END_ALLOW_THREADS, the GIL is
// restored when a C++ exception unwinds out of the region, so catch blocks run with it held.
class gil_release_guard
{
public:
  gil_release_guard()
    : state_(PyEval_SaveThread())
  {
  }

  ~gil_release_guard()
  {
    PyEval_RestoreThread(state_);
  }

  gil_release_guard(const gil_release_guard&) = delete;
  gil_release_guard& operator=(const gil_release_guard&) = delete;
  gil_release_guard(gil_release_guard&&) = delete;
  gil_release_guard& operator=(gil_release_guard&&) = delete;

private:
  PyThreadState* state_;
};

// Acquires the GIL for the guard's lifetime, for threads that may hold no Python thread state
// at all (e.g. a core IO thread). Always releases on scope exit, including exception unwind,
// unlike a bare PyGILState_Ensure/Release pair with a release site in every early return.
class gil_acquire_guard
{
public:
  gil_acquire_guard()
    : state_(PyGILState_Ensure())
  {
  }

  ~gil_acquire_guard()
  {
    PyGILState_Release(state_);
  }

  gil_acquire_guard(const gil_acquire_guard&) = delete;
  gil_acquire_guard& operator=(const gil_acquire_guard&) = delete;
  gil_acquire_guard(gil_acquire_guard&&) = delete;
  gil_acquire_guard& operator=(gil_acquire_guard&&) = delete;

private:
  PyGILState_STATE state_;
};

} // namespace pycbc
