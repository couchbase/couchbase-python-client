#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__all__ = [
    'FMT_BYTES',
    'FMT_COMMON_MASK',
    'FMT_JSON',
    'FMT_LEGACY_MASK',
    'FMT_PICKLE',
    'FMT_UTF8',
]

# Each format sets both encodings of the document flags: the common format in the top byte, shared
# by every Couchbase SDK, and the legacy one in the low bits.
FMT_JSON = 0x02000000
FMT_PICKLE = 0x01000001
FMT_BYTES = 0x03000002
FMT_UTF8 = 0x04000004
FMT_LEGACY_MASK = 0x07
FMT_COMMON_MASK = 0xFF000000
