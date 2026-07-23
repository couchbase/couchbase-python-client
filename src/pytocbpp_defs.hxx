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
#include <core/tracing/wrapper_sdk_tracer.hxx>
#include <string>
#include <type_traits>

namespace pycbc
{

struct pycbc_kv_request;

// PyUnicode_AsUTF8 returns NULL (with an exception set) if pyObj cannot be
// encoded as UTF-8 -- e.g. a str containing lone surrogates. Constructing a
// std::string directly from that NULL is UB. On success, copies the decoded
// string into out and returns true; on failure, clears the pending Python
// error, leaves out untouched, and returns false.
static inline bool
safe_utf8_string(PyObject* pyObj, std::string& out)
{
  const char* str = PyUnicode_AsUTF8(pyObj);
  if (nullptr == str) {
    PyErr_Clear();
    return false;
  }
  out.assign(str);
  return true;
}

template<typename T, typename Enabled = void>
struct py_to_cbpp_t;

template<typename T>
static inline T
py_to_cbpp(PyObject* pyObj)
{
  return py_to_cbpp_t<T>::from_py(pyObj);
}

template<typename T>
static inline T
py_to_cbpp(PyObject* pyObj,
           std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span)
{
  return py_to_cbpp_t<T>::from_py(pyObj, wrapper_span);
}

template<typename T>
static inline T
py_to_cbpp(pycbc_kv_request* request)
{
  return py_to_cbpp_t<T>::from_py(request);
}

template<typename T>
static inline T
py_to_cbpp(pycbc_kv_request* request,
           std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span)
{
  return py_to_cbpp_t<T>::from_py(request, wrapper_span);
}

template<typename T>
static inline PyObject*
cbpp_to_py(const T& cppObj)
{
  return py_to_cbpp_t<T>::to_py(cppObj);
}

template<typename T>
static inline PyObject*
cbpp_to_py(const T& cppObj,
           std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapper_span)
{
  return py_to_cbpp_t<T>::to_py(cppObj, wrapper_span);
}

template<typename T>
static inline PyObject*
cbpp_wrapper_span_to_py(T wrapper_span)
{
  return py_to_cbpp_t<T>::cbpp_wrapper_span_to_py(wrapper_span);
}

} // namespace pycbc
