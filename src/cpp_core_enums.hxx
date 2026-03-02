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
#include "cpp_core_enum_conversion_macros.hxx"
#include <core/logger/level.hxx>

namespace pycbc
{

// ==========================================================================================
// LEVEL enum
// Default: off
// ==========================================================================================
#define LEVEL_MAPPINGS(X, ...)                                                                     \
  X(trace, 5, __VA_ARGS__)                                                                         \
  X(debug, 10, __VA_ARGS__)                                                                        \
  X(info, 20, __VA_ARGS__)                                                                         \
  X(warn, 30, __VA_ARGS__)                                                                         \
  X(err, 40, __VA_ARGS__)                                                                          \
  X(critical, 50, __VA_ARGS__)                                                                     \
  X(off, 0, __VA_ARGS__)

PYCBC_DEFINE_INT_ENUM_CONVERSION(couchbase::core::logger::level, off, LEVEL_MAPPINGS)

} // namespace pycbc
