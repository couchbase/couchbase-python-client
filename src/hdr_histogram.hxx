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
#include <hdr/hdr_histogram.h>
#include <mutex>
#include <shared_mutex>

namespace pycbc
{

/**
 * Python wrapper for HDR (High Dynamic Range) Histogram.
 *
 * This structure wraps the C hdr_histogram implementation, providing
 * thread-safe access for recording values and querying percentiles.
 * Primarily used for latency tracking and metrics collection.
 */
struct pycbc_hdr_histogram {
  PyObject_HEAD hdr_histogram* histogram;
  std::shared_mutex mutex;
};

/**
 * Register the pycbc_hdr_histogram type with the Python module.
 *
 * @param module The Python module to register the type with
 * @return 0 on success, -1 on failure
 */
int
add_histogram_objects(PyObject* module);

} // namespace pycbc
