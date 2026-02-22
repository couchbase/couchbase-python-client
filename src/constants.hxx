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

namespace pycbc
{

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

} // namespace pycbc
