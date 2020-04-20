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

#ifndef COUCHBASE_PYTHON_CLIENT_PYCBC_SUBDOCOPS_H
#define COUCHBASE_PYTHON_CLIENT_PYCBC_SUBDOCOPS_H

#define PYCBC_X_SD_OPS(X, NP, VAL, MVAL, CTR, EXP_TYPE) \
    X(GET, get, EXP_TYPE)                               \
    X(EXISTS, exists, EXP_TYPE)                         \
    VAL(REPLACE, replace, EXP_TYPE)                     \
    VAL(DICT_ADD, dict_add, EXP_TYPE)                   \
    VAL(DICT_UPSERT, dict_upsert, EXP_TYPE)             \
    MVAL(ARRAY_ADD_FIRST, array_add_first, EXP_TYPE)    \
    MVAL(ARRAY_ADD_LAST, array_add_last, EXP_TYPE)      \
    VAL(ARRAY_ADD_UNIQUE, array_add_unique, EXP_TYPE)   \
    MVAL(ARRAY_INSERT, array_insert, EXP_TYPE)          \
    CTR(COUNTER, counter, EXP_TYPE)                     \
    X(REMOVE, remove, EXP_TYPE)                         \
    X(GET_COUNT, get_count, EXP_TYPE)                   \
    X(FULLDOC_GET, get, EXP_TYPE)


#endif //COUCHBASE_PYTHON_CLIENT_PYCBC_SUBDOCOPS_COMMON_H
