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
#include "pytocbpp_defs.hxx"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

namespace pycbc
{

// ======================================================================
// Integral types (int, uint32_t, uint64_t, etc.)
// ======================================================================
template<typename T>
struct py_to_cbpp_t<T,
                    typename std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>>> {
  static inline PyObject* to_py(T cppObj)
  {
    if constexpr (std::is_unsigned_v<T>) {
      return PyLong_FromUnsignedLongLong(static_cast<unsigned long long>(cppObj));
    } else {
      return PyLong_FromLongLong(static_cast<long long>(cppObj));
    }
  }

  static inline T from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return T{};
    }
    if (!PyLong_Check(pyObj)) {
      return T{};
    }
    if constexpr (std::is_unsigned_v<T>) {
      return static_cast<T>(PyLong_AsUnsignedLongLong(pyObj));
    } else {
      return static_cast<T>(PyLong_AsLongLong(pyObj));
    }
  }
};

// ======================================================================
// Boolean type
// ======================================================================
template<>
struct py_to_cbpp_t<bool> {
  static inline PyObject* to_py(bool cppObj)
  {
    if (cppObj) {
      Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
  }

  static inline bool from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return false;
    }
    return PyObject_IsTrue(pyObj) == 1;
  }
};

// ======================================================================
// Floating point types
// ======================================================================
template<typename T>
struct py_to_cbpp_t<T, typename std::enable_if_t<std::is_floating_point_v<T>>> {
  static inline PyObject* to_py(T cppObj)
  {
    return PyFloat_FromDouble(static_cast<double>(cppObj));
  }

  static inline T from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return T{};
    }
    if (!PyFloat_Check(pyObj) && !PyLong_Check(pyObj)) {
      return T{};
    }
    return static_cast<T>(PyFloat_AsDouble(pyObj));
  }
};

// ======================================================================
// std::string
// ======================================================================
template<>
struct py_to_cbpp_t<std::string> {
  static inline PyObject* to_py(const std::string& cppObj)
  {
    return PyUnicode_FromStringAndSize(cppObj.data(), static_cast<Py_ssize_t>(cppObj.size()));
  }

  static inline std::string from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }
    if (PyUnicode_Check(pyObj)) {
      const char* str = PyUnicode_AsUTF8(pyObj);
      if (str == nullptr) {
        return {};
      }
      return std::string(str);
    }
    if (PyBytes_Check(pyObj)) {
      char* str = nullptr;
      Py_ssize_t len = 0;
      if (PyBytes_AsStringAndSize(pyObj, &str, &len) == 0 && str != nullptr) {
        return std::string(str, static_cast<size_t>(len));
      }
    }
    return {};
  }
};

// ======================================================================
// std::chrono::nanoseconds
// ======================================================================
template<>
struct py_to_cbpp_t<std::chrono::nanoseconds> {
  static inline PyObject* to_py(const std::chrono::nanoseconds& cppObj)
  {
    return PyLong_FromLongLong(cppObj.count());
  }

  static inline std::chrono::nanoseconds from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return std::chrono::nanoseconds::zero();
    }
    if (!PyLong_Check(pyObj)) {
      return std::chrono::nanoseconds::zero();
    }
    // Input is in nanoseconds, convert to milliseconds
    long long nanoseconds = PyLong_AsLongLong(pyObj);
    return std::chrono::nanoseconds(nanoseconds);
  }
};

// ======================================================================
// std::chrono::microseconds
// ======================================================================
template<>
struct py_to_cbpp_t<std::chrono::microseconds> {
  static inline PyObject* to_py(const std::chrono::microseconds& cppObj)
  {
    return PyLong_FromLongLong(cppObj.count());
  }

  static inline std::chrono::microseconds from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return std::chrono::microseconds::zero();
    }
    if (!PyLong_Check(pyObj)) {
      return std::chrono::microseconds::zero();
    }
    long long microseconds = PyLong_AsLongLong(pyObj);
    return std::chrono::microseconds(microseconds);
  }
};

// ======================================================================
// std::chrono::milliseconds
// ======================================================================
template<>
struct py_to_cbpp_t<std::chrono::milliseconds> {
  static inline PyObject* to_py(const std::chrono::milliseconds& cppObj)
  {
    return PyLong_FromLongLong(cppObj.count());
  }

  static inline std::chrono::milliseconds from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return std::chrono::milliseconds::zero();
    }
    if (!PyLong_Check(pyObj)) {
      return std::chrono::milliseconds::zero();
    }
    long long milliseconds = PyLong_AsLongLong(pyObj);
    return std::chrono::milliseconds(milliseconds);
  }
};

// ======================================================================
// std::chrono::seconds
// ======================================================================
template<>
struct py_to_cbpp_t<std::chrono::seconds> {
  static inline PyObject* to_py(const std::chrono::seconds& cppObj)
  {
    return PyLong_FromLongLong(cppObj.count());
  }

  static inline std::chrono::seconds from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return std::chrono::seconds::zero();
    }
    if (!PyLong_Check(pyObj)) {
      return std::chrono::seconds::zero();
    }
    long long seconds = PyLong_AsLongLong(pyObj);
    return std::chrono::seconds(seconds);
  }
};

// ======================================================================
// std::chrono::system_clock::time_point
// ======================================================================
template<>
struct py_to_cbpp_t<std::chrono::system_clock::time_point> {
  static inline PyObject* to_py(const std::chrono::system_clock::time_point& cppObj)
  {
    // this is only used w/ the wrapper_sdk_span, so use nanos for consistency across tracing
    auto time_since_epoch = cppObj.time_since_epoch();
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(time_since_epoch);
    return PyLong_FromLongLong(nanoseconds.count());
  }

  static inline std::chrono::system_clock::time_point from_py(PyObject* pyObj)
  {
    // Set a Python runtime error as we don't have an equivalent conversion
    PyErr_SetString(PyExc_RuntimeError,
                    "Cannot convert Python object to std::chrono::system_clock::time_point");
    return std::chrono::system_clock::time_point{};
  }
};

// ======================================================================
// std::optional<T>
// ======================================================================
template<typename T>
struct py_to_cbpp_t<std::optional<T>> {
  static inline PyObject* to_py(const std::optional<T>& cppObj)
  {
    if (!cppObj.has_value()) {
      Py_RETURN_NONE;
    }
    return cbpp_to_py<T>(cppObj.value());
  }

  static inline std::optional<T> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return std::nullopt;
    }
    return py_to_cbpp<T>(pyObj);
  }
};

// ======================================================================
// std::byte
// ======================================================================
template<>
struct py_to_cbpp_t<std::byte> {
  static inline PyObject* to_py(std::byte cppObj)
  {
    return PyLong_FromUnsignedLong(static_cast<unsigned char>(cppObj));
  }

  static inline std::byte from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return std::byte{ 0 };
    }
    if (!PyLong_Check(pyObj)) {
      return std::byte{ 0 };
    }
    return static_cast<std::byte>(PyLong_AsUnsignedLong(pyObj));
  }
};

// ======================================================================
// std::vector<std::byte> - SPECIALIZED for binary data (Python bytes)
// This must come before the generic std::vector<T> to take precedence
// ======================================================================
template<>
struct py_to_cbpp_t<std::vector<std::byte>> {
  static inline PyObject* to_py(const std::vector<std::byte>& cppObj)
  {
    return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(cppObj.data()),
                                     static_cast<Py_ssize_t>(cppObj.size()));
  }

  static inline std::vector<std::byte> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PyBytes_Check(pyObj)) {
      return {};
    }

    char* buffer = nullptr;
    Py_ssize_t length = 0;
    if (PyBytes_AsStringAndSize(pyObj, &buffer, &length) != 0) {
      return {};
    }

    if (buffer == nullptr || length == 0) {
      return {};
    }

    auto* begin = reinterpret_cast<const std::byte*>(buffer);
    auto* end = begin + length;
    return std::vector<std::byte>(begin, end);
  }
};

// ======================================================================
// std::vector<T> - GENERIC (Python list)
// Converts std::vector<T> <-> Python list, using T's conversion
// ======================================================================
template<typename T>
struct py_to_cbpp_t<std::vector<T>> {
  static inline PyObject* to_py(const std::vector<T>& cppObj)
  {
    PyObject* list = PyList_New(static_cast<Py_ssize_t>(cppObj.size()));
    if (list == nullptr) {
      return nullptr;
    }

    for (size_t i = 0; i < cppObj.size(); ++i) {
      PyObject* item = cbpp_to_py<T>(cppObj[i]);
      if (item == nullptr) {
        Py_DECREF(list);
        return nullptr;
      }
      if (PyList_SetItem(list, static_cast<Py_ssize_t>(i), item) != 0) {
        Py_DECREF(item); // SetItem didn't steal the reference due to error
        Py_DECREF(list);
        return nullptr;
      }
      // PyList_SetItem steals the reference on success, so no DECREF needed
    }

    return list;
  }

  static inline std::vector<T> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PyList_Check(pyObj)) {
      return {};
    }

    Py_ssize_t size = PyList_Size(pyObj);
    std::vector<T> result;
    result.reserve(static_cast<size_t>(size));

    for (Py_ssize_t i = 0; i < size; ++i) {
      PyObject* item = PyList_GetItem(pyObj, i); // Borrowed ref
      result.push_back(py_to_cbpp<T>(item));
    }

    return result;
  }
};

// ==========================================================================================
// std::array
// ==========================================================================================
template<typename T, size_t N>
struct py_to_cbpp_t<std::array<T, N>> {
  static inline PyObject* to_py(const std::array<T, N>& cppObj)
  {
    PyObject* list = PyList_New(static_cast<Py_ssize_t>(N));
    if (list == nullptr) {
      return nullptr;
    }

    for (auto i = 0; i < N; ++i) {
      PyObject* item = cbpp_to_py<T>(cppObj[i]);
      if (item == nullptr) {
        Py_DECREF(list);
        return nullptr;
      }
      if (PyList_SetItem(list, static_cast<Py_ssize_t>(i), item) != 0) {
        Py_DECREF(item); // SetItem didn't steal the reference due to error
        Py_DECREF(list);
        return nullptr;
      }
      // PyList_SetItem steals the reference on success, so no DECREF needed
    }
    return list;
  }

  static inline std::array<T, N> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PyList_Check(pyObj)) {
      return {};
    }

    Py_ssize_t size = PyList_Size(pyObj);

    if (size != N) {
      PyErr_SetString(PyExc_RuntimeError, "Invalid array size");
      return {};
    }

    std::array<T, N> cppObj;
    for (Py_ssize_t i = 0; i < size; ++i) {
      PyObject* item = PyList_GetItem(pyObj, i); // Borrowed ref
      cppObj[i] = py_to_cbpp<T>(item);
    }
    return cppObj;
  }
};

// ==========================================================================================
// std::map<enum, T>
// ==========================================================================================
template<typename K, typename T>
struct py_to_cbpp_t<std::map<K, T>, typename std::enable_if_t<std::is_enum_v<K>>> {
  static inline PyObject* to_py(const std::map<K, T>& cppObj)
  {
    using enum_type_t = std::underlying_type_t<K>;
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    for (const auto& [key, value] : cppObj) {
      PyObject* py_key = cbpp_to_py<K>(key);
      if (py_key == nullptr) {
        Py_DECREF(dict);
        return nullptr;
      }

      PyObject* py_value = cbpp_to_py<T>(value);
      if (py_value == nullptr) {
        Py_DECREF(py_key);
        Py_DECREF(dict);
        return nullptr;
      }

      if (PyDict_SetItem(dict, py_key, py_value) != 0) {
        Py_DECREF(py_value);
        Py_DECREF(py_key);
        Py_DECREF(dict);
        return nullptr;
      }

      Py_DECREF(py_value);
      Py_DECREF(py_key);
    }

    return dict;
  }

  static inline std::map<K, T> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PyDict_Check(pyObj)) {
      return {};
    }

    std::map<K, T> cppObj;

    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(pyObj, &pos, &key, &value)) {
      K cpp_key = py_to_cbpp<K>(key);
      T cpp_value = py_to_cbpp<T>(value);
      cppObj.emplace(cpp_key, value);
    }

    return cppObj;
  }
};

// ==========================================================================================
// std::map<std::string, T, Args...> - GENERIC (Python dict with string keys)
// Converts std::map<std::string, T, ...> <-> Python dict, using T's conversion
// Handles any comparator and allocator types via Args...
// ==========================================================================================
template<typename T, typename... Args>
struct py_to_cbpp_t<std::map<std::string, T, Args...>> {
  static inline PyObject* to_py(const std::map<std::string, T, Args...>& cppObj)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    for (const auto& [key, value] : cppObj) {
      PyObject* py_key = cbpp_to_py(key);
      if (py_key == nullptr) {
        Py_DECREF(dict);
        return nullptr;
      }

      PyObject* py_value = cbpp_to_py<T>(value);
      if (py_value == nullptr) {
        Py_DECREF(py_key);
        Py_DECREF(dict);
        return nullptr;
      }

      if (PyDict_SetItem(dict, py_key, py_value) != 0) {
        Py_DECREF(py_value);
        Py_DECREF(py_key);
        Py_DECREF(dict);
        return nullptr;
      }

      Py_DECREF(py_value);
      Py_DECREF(py_key);
    }

    return dict;
  }

  static inline std::map<std::string, T, Args...> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PyDict_Check(pyObj)) {
      return {};
    }

    std::map<std::string, T, Args...> result;

    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(pyObj, &pos, &key, &value)) {
      // key and value are borrowed references
      std::string cpp_key = py_to_cbpp<std::string>(key);
      T cpp_value = py_to_cbpp<T>(value);
      result[cpp_key] = cpp_value;
    }

    return result;
  }
};

// ==========================================================================================
// std::monostate - represents empty state in variant
// ==========================================================================================
template<>
struct py_to_cbpp_t<std::monostate> {
  static inline PyObject* to_py(const std::monostate& cppObj)
  {
    Py_RETURN_NONE;
  }

  static inline std::monostate from_py(PyObject* pyObj)
  {
    // Set a Python runtime error as we don't have an equivalent conversion
    PyErr_SetString(PyExc_RuntimeError, "Cannot convert Python object to std::monostate");
    return std::monostate{};
  }
};

// ======================================================================
// std::set<T> - converts to/from Python set
// ======================================================================
template<typename T>
struct py_to_cbpp_t<std::set<T>> {
  static inline PyObject* to_py(const std::set<T>& cppObj)
  {
    PyObject* py_set = PySet_New(nullptr);
    if (py_set == nullptr) {
      return nullptr;
    }

    for (const auto& item : cppObj) {
      PyObject* py_item = cbpp_to_py<T>(item);
      if (py_item == nullptr) {
        Py_DECREF(py_set);
        return nullptr;
      }

      if (PySet_Add(py_set, py_item) != 0) {
        Py_DECREF(py_item);
        Py_DECREF(py_set);
        return nullptr;
      }

      Py_DECREF(py_item);
    }

    return py_set;
  }

  static inline std::set<T> from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PySet_Check(pyObj) && !PyFrozenSet_Check(pyObj)) {
      return {};
    }

    std::set<T> result;
    PyObject* iterator = PyObject_GetIter(pyObj);
    if (iterator == nullptr) {
      return {};
    }

    PyObject* item;
    while ((item = PyIter_Next(iterator)) != nullptr) {
      T cpp_item = py_to_cbpp<T>(item);
      result.insert(cpp_item);
      Py_DECREF(item);
    }

    Py_DECREF(iterator);

    // Check if iteration ended due to an error
    if (PyErr_Occurred()) {
      return {};
    }

    return result;
  }
};

// ======================================================================
// std::variant<Types...> - converts to Python by trying each type
// ======================================================================
template<typename... Types>
struct py_to_cbpp_t<std::variant<Types...>> {
  // Helper to convert variant to Python at runtime based on active type
  template<size_t I = 0>
  static inline PyObject* to_py_impl(const std::variant<Types...>& cppObj)
  {
    if constexpr (I < sizeof...(Types)) {
      if (cppObj.index() == I) {
        using CurrentType = std::variant_alternative_t<I, std::variant<Types...>>;
        return cbpp_to_py<CurrentType>(std::get<I>(cppObj));
      }
      return to_py_impl<I + 1>(cppObj);
    }
    // Should never reach here if variant is valid
    Py_RETURN_NONE;
  }

  static inline PyObject* to_py(const std::variant<Types...>& cppObj)
  {
    return to_py_impl(cppObj);
  }

  static inline std::variant<Types...> from_py(PyObject* pyObj)
  {
    // Set a Python runtime error as we don't have a way to determine
    // which variant type to convert to without additional context
    PyErr_SetString(PyExc_RuntimeError,
                    "Cannot convert Python object to std::variant - conversion requires explicit "
                    "type information");
    return std::variant<Types...>{};
  }
};

// ======================================================================
// std::exception
// ======================================================================
template<>
struct py_to_cbpp_t<std::exception> {
  static inline PyObject* to_py(const std::exception& except)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }
    PyObject* pyObj_what = cbpp_to_py<std::string>(except.what());
    PyDict_SetItemString(dict, "what", pyObj_what);
    Py_DECREF(pyObj_what);
    return dict;
  }
};

// ======================================================================
// std::error_code
// ======================================================================
template<>
struct py_to_cbpp_t<std::error_code> {
  static inline PyObject* to_py(const std::error_code& ec)
  {
    if (!ec) {
      Py_RETURN_NONE;
    }

    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }
    PyObject* pyObj_code = cbpp_to_py(ec.value());
    PyDict_SetItemString(dict, "code", pyObj_code);
    Py_DECREF(pyObj_code);

    PyObject* pyObj_message = cbpp_to_py<std::string>(ec.message());
    PyDict_SetItemString(dict, "message", pyObj_message);
    Py_DECREF(pyObj_message);
    return dict;
  }

  static inline std::error_code from_py(PyObject* pyObj)
  {
    PyErr_SetString(PyExc_RuntimeError, "Cannot convert Python object to std::error_code");
    return std::error_code{};
  }
};

} // namespace pycbc
