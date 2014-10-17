#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Constants defined in _libcouchbase module for use by users
"""
import couchbase._bootstrap
from couchbase._libcouchbase import (
    FMT_JSON,
    FMT_BYTES,
    FMT_UTF8,
    FMT_PICKLE,
    FMT_AUTO,
    FMT_COMMON_MASK,
    FMT_LEGACY_MASK,

    OBS_PERSISTED,
    OBS_FOUND,
    OBS_NOTFOUND,
    OBS_LOGICALLY_DELETED,

    OBS_MASK,

    LOCKMODE_WAIT,
    LOCKMODE_EXC,
    LOCKMODE_NONE
)
