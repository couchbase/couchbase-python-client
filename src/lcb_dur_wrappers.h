/**
 *     Copyright 2019 Couchbase, Inc.
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
 **/

#ifndef COUCHBASE_PYTHON_CLIENT_LCB_COMMON_WRAPPERS_H
#define COUCHBASE_PYTHON_CLIENT_LCB_COMMON_WRAPPERS_H
typedef struct {
    char persist_to;
    char replicate_to;
    pycbc_DURABILITY_LEVEL durability_level;
} pycbc_dur_params;

#endif // COUCHBASE_PYTHON_CLIENT_LCB_COMMON_WRAPPERS_H
