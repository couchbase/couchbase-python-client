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

#define PYCBC_X_SD_OPS(X, NP, VAL, MVAL, CTR, ...)       \
    X(GET, get, __VA_ARGS__)                             \
    X(EXISTS, exists, __VA_ARGS__)                       \
    VAL(REPLACE, replace, __VA_ARGS__)                   \
    VAL(DICT_ADD, dict_add, __VA_ARGS__)                 \
    VAL(DICT_UPSERT, dict_upsert, __VA_ARGS__)           \
    MVAL(ARRAY_ADD_FIRST, array_add_first, __VA_ARGS__)  \
    MVAL(ARRAY_ADD_LAST, array_add_last, __VA_ARGS__)    \
    VAL(ARRAY_ADD_UNIQUE, array_add_unique, __VA_ARGS__) \
    MVAL(ARRAY_INSERT, array_insert, __VA_ARGS__)        \
    CTR(COUNTER, counter, __VA_ARGS__)                   \
    X(REMOVE, remove, __VA_ARGS__)                       \
    X(GET_COUNT, get_count, __VA_ARGS__)                 \
    PYCBC_X_SD_OPS_FULLDOC(X, NP, VAL, MVAL, CTR, __VA_ARGS__)


#endif //COUCHBASE_PYTHON_CLIENT_PYCBC_SUBDOCOPS_COMMON_H
