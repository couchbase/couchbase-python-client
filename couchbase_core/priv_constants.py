#
# Copyright 2017, Couchbase, Inc.
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

# This file constains private constants. This should be used internally
# and is to avoid version/compilation dependencies on newer LCBs

CMDSUBDOC_F_UPSERT_DOC = 1 << 16
CMDSUBDOC_F_INSERT_DOC = 1 << 17
CMDSUBDOC_F_ACCESS_DELETED = 1 << 18
SDSPEC_F_MKDIR_P = 1 << 16
SDSPEC_F_XATTR = 1 << 18
SDSPEC_F_EXPANDMACROS = 1 << 19

SDCMD_GET_COUNT = 12
# Not exposed in public API but reserved for future use.
SDCMD_GET_FULLDOC = 13
SDCMD_UPSERT_FULLDOC = 14