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

// ======================================================================
// Generic Enum Conversion Machinery using X-Macros
// ======================================================================
//
// This file provides the core macro-based system for defining bidirectional
// conversions between Python strings/integers and C++ enums.
//
// ARCHITECTURE OVERVIEW:
// - Uses X-macro pattern for single source of truth
// - Generates template specializations of py_to_cbpp_t<enum_type>
// - Provides both from_py() and to_py() methods
// - Supports string-based and integer-based enums (uint8_t and uint16_t)
//
// INTERNAL DETAILS:
// The mapping macros use __VA_ARGS__ to forward the enum_type parameter
// to helper macros (PYCBC_FROM_PY_CASE, etc.), which then use it to
// generate the appropriate case statements with fully qualified enum names.
//
// This ensures the mappings are independent of the enum type and can be
// reused or referenced without name collisions.
//
// FOR USAGE INSTRUCTIONS: See cpp_core_enums.hxx
//
// ======================================================================

// ======================================================================
// String-Based Enum Conversion Machinery
// ======================================================================

// Main conversion macro - generates py_to_cbpp_t specialization
#define PYCBC_DEFINE_ENUM_CONVERSION(enum_type, default_val, mappings)                             \
  template<>                                                                                       \
  struct py_to_cbpp_t<enum_type> {                                                                 \
    static inline enum_type from_py(PyObject* pyObj)                                               \
    {                                                                                              \
      if (pyObj == nullptr || !PyUnicode_Check(pyObj)) {                                           \
        return enum_type::default_val;                                                             \
      }                                                                                            \
      std::string str = PyUnicode_AsUTF8(pyObj);                                                   \
      mappings(PYCBC_FROM_PY_CASE, enum_type) return enum_type::default_val;                       \
    }                                                                                              \
    static inline PyObject* to_py(const enum_type& val)                                            \
    {                                                                                              \
      mappings(PYCBC_TO_PY_CASE, enum_type) return PyUnicode_FromString(#default_val);             \
    }                                                                                              \
  };

// Helper macros for generating case statements
// These receive the enum_type as a parameter from the mapping expansion
#define PYCBC_FROM_PY_CASE(cpp_name, py_name, enum_t)                                              \
  if (str == py_name)                                                                              \
    return enum_t::cpp_name;

#define PYCBC_TO_PY_CASE(cpp_name, py_name, enum_t)                                                \
  if (val == enum_t::cpp_name)                                                                     \
    return PyUnicode_FromString(py_name);

// ======================================================================
// Integer-Based Enum Conversion Machinery (uint8_t)
// ======================================================================
//
// Similar to the string-based system above, but for enums that are
// represented as integers on the Python side (via IntEnum.value).
//
// ======================================================================

// Main conversion macro for integer-based enums
#define PYCBC_DEFINE_INT_ENUM_CONVERSION(enum_type, default_val, mappings)                         \
  template<>                                                                                       \
  struct py_to_cbpp_t<enum_type> {                                                                 \
    static inline enum_type from_py(PyObject* pyObj)                                               \
    {                                                                                              \
      if (pyObj == nullptr || !PyLong_Check(pyObj)) {                                              \
        return enum_type::default_val;                                                             \
      }                                                                                            \
      auto int_val = static_cast<std::uint8_t>(PyLong_AsUnsignedLong(pyObj));                      \
      mappings(PYCBC_FROM_PY_INT_CASE, enum_type) return enum_type::default_val;                   \
    }                                                                                              \
    static inline PyObject* to_py(const enum_type& val)                                            \
    {                                                                                              \
      mappings(PYCBC_TO_PY_INT_CASE, enum_type) return PyLong_FromUnsignedLong(                    \
        static_cast<std::uint8_t>(enum_type::default_val));                                        \
    }                                                                                              \
  };

// Helper macros for generating case statements
#define PYCBC_FROM_PY_INT_CASE(cpp_name, int_value, enum_t)                                        \
  if (int_val == int_value)                                                                        \
    return enum_t::cpp_name;

#define PYCBC_TO_PY_INT_CASE(cpp_name, int_value, enum_t)                                          \
  if (val == enum_t::cpp_name)                                                                     \
    return PyLong_FromUnsignedLong(int_value);

// ======================================================================
// Integer-Based Enum Conversion Machinery (uint16_t)
// ======================================================================

// Main conversion macro for uint16_t integer-based enums
#define PYCBC_DEFINE_INT16_ENUM_CONVERSION(enum_type, default_val, mappings)                       \
  template<>                                                                                       \
  struct py_to_cbpp_t<enum_type> {                                                                 \
    static inline enum_type from_py(PyObject* pyObj)                                               \
    {                                                                                              \
      if (pyObj == nullptr || !PyLong_Check(pyObj)) {                                              \
        return enum_type::default_val;                                                             \
      }                                                                                            \
      auto int_val = static_cast<std::uint16_t>(PyLong_AsUnsignedLong(pyObj));                     \
      mappings(PYCBC_FROM_PY_INT16_CASE, enum_type) return enum_type::default_val;                 \
    }                                                                                              \
    static inline PyObject* to_py(const enum_type& val)                                            \
    {                                                                                              \
      mappings(PYCBC_TO_PY_INT16_CASE, enum_type) return PyLong_FromUnsignedLong(                  \
        static_cast<std::uint16_t>(enum_type::default_val));                                       \
    }                                                                                              \
  };

// Helper macros for uint16_t case statements
#define PYCBC_FROM_PY_INT16_CASE(cpp_name, int_value, enum_t)                                      \
  if (int_val == int_value)                                                                        \
    return enum_t::cpp_name;

#define PYCBC_TO_PY_INT16_CASE(cpp_name, int_value, enum_t)                                        \
  if (val == enum_t::cpp_name)                                                                     \
    return PyLong_FromUnsignedLong(int_value);
