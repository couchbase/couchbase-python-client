/*
 *   Copyright 2016-2022. Couchbase, Inc.
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

#include "bucket_management.hxx"
#include "../exceptions.hxx"
#include <core/management/bucket_settings.hxx>
#include <core/operations/management/bucket.hxx>
#include <core/operations/management/bucket_describe.hxx> // should be in the include above, but isn't
#include "../utils.hxx"

PyObject*
build_bucket_settings(couchbase::core::management::cluster::bucket_settings settings)
{
    PyObject* pyObj_bucket_settings = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(settings.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "name", pyObj_tmp)) {
        Py_XDECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    switch (settings.bucket_type) {
        case couchbase::core::management::cluster::bucket_type::couchbase: {
            pyObj_tmp = PyUnicode_FromString("membase");
            break;
        }
        case couchbase::core::management::cluster::bucket_type::memcached: {
            pyObj_tmp = PyUnicode_FromString("memcached");
            break;
        }
        case couchbase::core::management::cluster::bucket_type::ephemeral: {
            pyObj_tmp = PyUnicode_FromString("ephemeral");
            break;
        }
        case couchbase::core::management::cluster::bucket_type::unknown: {
            pyObj_tmp = PyUnicode_FromString("unknown");
            break;
        }
        default: {
            pyObj_tmp = PyUnicode_FromString("unknown");
            break;
        }
    }
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "bucketType", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(settings.ram_quota_mb);
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "ramQuotaMB", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLong(settings.max_expiry);
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "maxTTL", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLong(settings.max_expiry);
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "maxExpiry", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    switch (settings.compression_mode) {
        case couchbase::core::management::cluster::bucket_compression::off: {
            pyObj_tmp = PyUnicode_FromString("off");
            break;
        }
        case couchbase::core::management::cluster::bucket_compression::active: {
            pyObj_tmp = PyUnicode_FromString("active");
            break;
        }
        case couchbase::core::management::cluster::bucket_compression::passive: {
            pyObj_tmp = PyUnicode_FromString("passive");
            break;
        }
        case couchbase::core::management::cluster::bucket_compression::unknown: {
            pyObj_tmp = PyUnicode_FromString("unknown");
            break;
        }
        default: {
            pyObj_tmp = PyUnicode_FromString("unknown");
            break;
        }
    }
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "compressionMode", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (settings.minimum_durability_level.has_value()) {
        switch (settings.minimum_durability_level.value()) {
            case couchbase::durability_level::majority_and_persist_to_active: {
                pyObj_tmp = PyUnicode_FromString("majorityAndPersistActive");
                break;
            }
            case couchbase::durability_level::majority: {
                pyObj_tmp = PyUnicode_FromString("majority");
                break;
            }
            case couchbase::durability_level::persist_to_majority: {
                pyObj_tmp = PyUnicode_FromString("persistToMajority");
                break;
            }
            case couchbase::durability_level::none: {
                pyObj_tmp = PyUnicode_FromString("none");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("none");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "durabilityMinLevel", pyObj_tmp)) {
            Py_DECREF(pyObj_bucket_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyLong_FromUnsignedLong(settings.num_replicas);
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "numReplicas", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyBool_FromLong(settings.replica_indexes);
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "replicaIndex", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyBool_FromLong(settings.flush_enabled);
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "flushEnabled", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    switch (settings.eviction_policy) {
        case couchbase::core::management::cluster::bucket_eviction_policy::full: {
            pyObj_tmp = PyUnicode_FromString("fullEviction");
            break;
        }
        case couchbase::core::management::cluster::bucket_eviction_policy::value_only: {
            pyObj_tmp = PyUnicode_FromString("valueOnly");
            break;
        }
        case couchbase::core::management::cluster::bucket_eviction_policy::no_eviction: {
            pyObj_tmp = PyUnicode_FromString("noEviction");
            break;
        }
        case couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used: {
            pyObj_tmp = PyUnicode_FromString("nruEviction");
            break;
        }
        default: {
            pyObj_tmp = PyUnicode_FromString("noEviction");
            break;
        }
    }
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "evictionPolicy", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    switch (settings.conflict_resolution_type) {
        case couchbase::core::management::cluster::bucket_conflict_resolution::timestamp: {
            pyObj_tmp = PyUnicode_FromString("lww");
            break;
        }
        case couchbase::core::management::cluster::bucket_conflict_resolution::sequence_number: {
            pyObj_tmp = PyUnicode_FromString("seqno");
            break;
        }
        case couchbase::core::management::cluster::bucket_conflict_resolution::custom: {
            pyObj_tmp = PyUnicode_FromString("custom");
            break;
        }
        default: {
            pyObj_tmp = PyUnicode_FromString("seqno");
            break;
        }
    }
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "conflictResolutionType", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    switch (settings.storage_backend) {
        case couchbase::core::management::cluster::bucket_storage_backend::couchstore: {
            pyObj_tmp = PyUnicode_FromString("couchstore");
            break;
        }
        case couchbase::core::management::cluster::bucket_storage_backend::magma: {
            pyObj_tmp = PyUnicode_FromString("magma");
            break;
        }
        case couchbase::core::management::cluster::bucket_storage_backend::unknown: {
            pyObj_tmp = PyUnicode_FromString("undefined");
            break;
        }
        default: {
            pyObj_tmp = PyUnicode_FromString("undefined");
            break;
        }
    }
    if (-1 == PyDict_SetItemString(pyObj_bucket_settings, "storageBackend", pyObj_tmp)) {
        Py_DECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return pyObj_bucket_settings;
}

template<typename Response>
result*
create_result_from_bucket_mgmt_response([[maybe_unused]] const Response& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);
    return res;
}

template<>
result*
create_result_from_bucket_mgmt_response(const couchbase::core::operations::management::bucket_update_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_bucket_settings = build_bucket_settings(resp.bucket);
    if (pyObj_bucket_settings == nullptr) {
        Py_XDECREF(pyObj_result);
        return nullptr;
    }

    if (-1 == PyDict_SetItemString(res->dict, "bucket_settings", pyObj_bucket_settings)) {
        Py_XDECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_result);
        return nullptr;
    }
    Py_DECREF(pyObj_bucket_settings);
    return res;
}

template<>
result*
create_result_from_bucket_mgmt_response(const couchbase::core::operations::management::bucket_get_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_bucket_settings = build_bucket_settings(resp.bucket);
    if (pyObj_bucket_settings == nullptr) {
        Py_XDECREF(pyObj_result);
        return nullptr;
    }

    if (-1 == PyDict_SetItemString(res->dict, "bucket_settings", pyObj_bucket_settings)) {
        Py_XDECREF(pyObj_bucket_settings);
        Py_XDECREF(pyObj_result);
        return nullptr;
    }
    Py_DECREF(pyObj_bucket_settings);
    return res;
}

template<>
result*
create_result_from_bucket_mgmt_response(const couchbase::core::operations::management::bucket_get_all_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    PyObject* pyObj_buckets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& bucket : resp.buckets) {
        PyObject* pyObj_bucket_settings = build_bucket_settings(bucket);
        PyList_Append(pyObj_buckets, pyObj_bucket_settings);
        Py_DECREF(pyObj_bucket_settings);
    }
    if (-1 == PyDict_SetItemString(res->dict, "buckets", pyObj_buckets)) {
        Py_XDECREF(pyObj_buckets);
        Py_XDECREF(pyObj_result);
        return nullptr;
    }
    Py_DECREF(pyObj_buckets);
    return res;
}

template<>
result*
create_result_from_bucket_mgmt_response(const couchbase::core::operations::management::bucket_describe_response& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);

    PyObject* pyObj_bucket_info = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(resp.info.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_bucket_info, "name", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_XDECREF(pyObj_bucket_info);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(resp.info.uuid.c_str());
    if (-1 == PyDict_SetItemString(pyObj_bucket_info, "uuid", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_DECREF(pyObj_bucket_info);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyBool_FromLong(static_cast<int>(resp.info.number_of_nodes));
    if (-1 == PyDict_SetItemString(pyObj_bucket_info, "number_of_nodes", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_DECREF(pyObj_bucket_info);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyBool_FromLong(static_cast<int>(resp.info.number_of_replicas));
    if (-1 == PyDict_SetItemString(pyObj_bucket_info, "number_of_replicas", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_DECREF(pyObj_bucket_info);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    switch (resp.info.storage_backend) {
        case couchbase::core::management::cluster::bucket_storage_backend::couchstore: {
            pyObj_tmp = PyUnicode_FromString("couchstore");
            break;
        }
        case couchbase::core::management::cluster::bucket_storage_backend::magma: {
            pyObj_tmp = PyUnicode_FromString("magma");
            break;
        }
        case couchbase::core::management::cluster::bucket_storage_backend::unknown: {
            pyObj_tmp = PyUnicode_FromString("undefined");
            break;
        }
        default: {
            pyObj_tmp = PyUnicode_FromString("undefined");
            break;
        }
    }
    if (-1 == PyDict_SetItemString(pyObj_bucket_info, "storage_backend", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_DECREF(pyObj_bucket_info);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (-1 == PyDict_SetItemString(res->dict, "bucket_info", pyObj_bucket_info)) {
        Py_XDECREF(result_obj);
        Py_DECREF(pyObj_bucket_info);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_bucket_info);

    return res;
}

template<typename Response>
std::string
get_bucket_mgmt_error_msg(const Response& resp)
{
    return std::string();
}

template<>
std::string
get_bucket_mgmt_error_msg(const couchbase::core::operations::management::bucket_create_response& resp)
{
    return resp.error_message;
}

template<>
std::string
get_bucket_mgmt_error_msg(const couchbase::core::operations::management::bucket_update_response& resp)
{
    return resp.error_message;
}

template<typename Response>
void
create_result_from_bucket_mgmt_op_response(Response& resp,
                                           PyObject* pyObj_callback,
                                           PyObject* pyObj_errback,
                                           std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    PyGILState_STATE state = PyGILState_Ensure();
    if (resp.ctx.ec.value()) {
        // update and create responses might provide an erorr message
        auto error_msg = get_bucket_mgmt_error_msg(resp);
        if (error_msg.empty()) {
            error_msg = std::string("Error doing bucket mgmt operation.");
        }
        // make sure this is an HTTPException
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, error_msg, "BucketMgmt");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
        // lets clear any errors
        PyErr_Clear();
    } else {
        auto res = create_result_from_bucket_mgmt_response(resp);

        if (res == nullptr || PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else {
            if (pyObj_callback == nullptr) {
                barrier->set_value(reinterpret_cast<PyObject*>(res));
            } else {
                pyObj_func = pyObj_callback;
                pyObj_args = PyTuple_New(1);
                PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(res));
            }
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Bucket mgmt operation error.");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
    }

    if (!set_exception && pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_Call(pyObj_func, pyObj_args, pyObj_kwargs);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            PyErr_Print();
            // @TODO:  how to handle this situation?
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

couchbase::core::management::cluster::bucket_settings
get_bucket_settings(PyObject* settings)
{
    couchbase::core::management::cluster::bucket_settings bucket_settings{};

    PyObject* pyObj_name = PyDict_GetItemString(settings, "name");
    if (pyObj_name == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Expected bucket settings name to be provided.");
        throw std::invalid_argument("name");
    }
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_name));
    bucket_settings.name = bucket_name;

    PyObject* pyObj_bucket_type = PyDict_GetItemString(settings, "bucketType");
    if (pyObj_bucket_type) {
        auto b_type = std::string(PyUnicode_AsUTF8(pyObj_bucket_type));
        if (b_type.compare("couchbase") == 0) {
            bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;
        } else if (b_type.compare("memcached") == 0) {
            bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::memcached;
        } else if (b_type.compare("ephemeral") == 0) {
            bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::ephemeral;
        }
    }

    PyObject* pyObj_ram = PyDict_GetItemString(settings, "ramQuotaMB");
    if (pyObj_ram != nullptr) {
        bucket_settings.ram_quota_mb = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_ram));
    }

    PyObject* pyObj_max_expiry = PyDict_GetItemString(settings, "maxExpiry");
    if (pyObj_max_expiry != nullptr) {
        bucket_settings.max_expiry = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_max_expiry));
    }

    PyObject* pyObj_compression_mode = PyDict_GetItemString(settings, "compressionMode");
    if (pyObj_compression_mode) {
        auto comp_mode = std::string(PyUnicode_AsUTF8(pyObj_compression_mode));
        if (comp_mode.compare("off") == 0) {
            bucket_settings.compression_mode = couchbase::core::management::cluster::bucket_compression::off;
        } else if (comp_mode.compare("active") == 0) {
            bucket_settings.compression_mode = couchbase::core::management::cluster::bucket_compression::active;
        } else if (comp_mode.compare("passive") == 0) {
            bucket_settings.compression_mode = couchbase::core::management::cluster::bucket_compression::passive;
        }
    }

    PyObject* pyObj_durability_level = PyDict_GetItemString(settings, "durabilityMinLevel");
    if (pyObj_durability_level != nullptr) {
        bucket_settings.minimum_durability_level = PyObject_to_durability_level(pyObj_durability_level);
    }

    PyObject* pyObj_num_rep = PyDict_GetItemString(settings, "numReplicas");
    if (pyObj_num_rep != nullptr) {
        bucket_settings.num_replicas = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_num_rep));
    }

    PyObject* pyObj_replica_indexes = PyDict_GetItemString(settings, "replicaIndex");
    if (pyObj_replica_indexes != nullptr) {
        if (pyObj_replica_indexes == Py_True) {
            bucket_settings.replica_indexes = true;
        } else {
            bucket_settings.replica_indexes = false;
        }
    }

    PyObject* pyObj_flush_enabled = PyDict_GetItemString(settings, "flushEnabled");
    if (pyObj_flush_enabled != nullptr) {
        if (pyObj_flush_enabled == Py_True) {
            bucket_settings.flush_enabled = true;
        } else {
            bucket_settings.flush_enabled = false;
        }
    }

    PyObject* pyObj_eviction_policy = PyDict_GetItemString(settings, "evictionPolicy");
    if (pyObj_eviction_policy != nullptr) {
        auto evict = std::string(PyUnicode_AsUTF8(pyObj_eviction_policy));
        if (evict.compare("fullEviction") == 0) {
            bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::full;
        } else if (evict.compare("valueOnly") == 0) {
            bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::value_only;
        } else if (evict.compare("noEviction") == 0) {
            bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::no_eviction;
        } else if (evict.compare("nruEviction") == 0) {
            bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used;
        }
    }

    PyObject* pyObj_conflict_res_type = PyDict_GetItemString(settings, "conflictResolutionType");
    if (pyObj_conflict_res_type != nullptr) {
        auto crt = std::string(PyUnicode_AsUTF8(pyObj_conflict_res_type));
        if (crt.compare("lww") == 0) {
            bucket_settings.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::timestamp;
        } else if (crt.compare("seqno") == 0) {
            bucket_settings.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::sequence_number;
        } else if (crt.compare("custom") == 0) {
            bucket_settings.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::custom;
        }
    }

    PyObject* pyObj_storage_backend = PyDict_GetItemString(settings, "storageBackend");
    if (pyObj_storage_backend != nullptr) {
        auto backend = std::string(PyUnicode_AsUTF8(pyObj_storage_backend));
        if (backend.compare("couchstore") == 0) {
            bucket_settings.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::couchstore;
        }
        if (backend.compare("magma") == 0) {
            bucket_settings.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::magma;
        }
    }
    return bucket_settings;
}

template<typename Request>
Request
get_bucket_mgmt_with_bucket_settings_req(PyObject* op_args)
{
    Request req{};

    PyObject* pyObj_bucket_settings = PyDict_GetItemString(op_args, "bucket_settings");
    if (pyObj_bucket_settings == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Expected bucket settings to be provided.");
        throw std::invalid_argument("bucket_settings");
    }
    req.bucket = get_bucket_settings(pyObj_bucket_settings);

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

template<typename Request>
Request
get_bucket_mgmt_with_bucket_name_req(PyObject* op_args)
{
    Request req{};

    PyObject* pyObj_bucket_name = PyDict_GetItemString(op_args, "bucket_name");
    if (pyObj_bucket_name == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Expected bucket_name to be provided.");
        throw std::invalid_argument("bucket_name");
    }
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));
    req.name = bucket_name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

template<typename Request>
PyObject*
do_bucket_mgmt_op(connection& conn,
                  Request& req,
                  PyObject* pyObj_callback,
                  PyObject* pyObj_errback,
                  std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_bucket_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_bucket_mgmt_op(connection* conn, struct bucket_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    try {
        switch (options->op_type) {
            case BucketManagementOperations::CREATE_BUCKET: {
                auto req = get_bucket_mgmt_with_bucket_settings_req<couchbase::core::operations::management::bucket_create_request>(
                  options->op_args);
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_create_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case BucketManagementOperations::UPDATE_BUCKET: {
                auto req = get_bucket_mgmt_with_bucket_settings_req<couchbase::core::operations::management::bucket_update_request>(
                  options->op_args);
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_update_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case BucketManagementOperations::DROP_BUCKET: {
                auto req =
                  get_bucket_mgmt_with_bucket_name_req<couchbase::core::operations::management::bucket_drop_request>(options->op_args);
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_drop_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case BucketManagementOperations::GET_BUCKET: {
                auto req =
                  get_bucket_mgmt_with_bucket_name_req<couchbase::core::operations::management::bucket_get_request>(options->op_args);
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_get_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case BucketManagementOperations::GET_ALL_BUCKETS: {
                couchbase::core::operations::management::bucket_get_all_request req{};
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_get_all_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case BucketManagementOperations::FLUSH_BUCKET: {
                auto req =
                  get_bucket_mgmt_with_bucket_name_req<couchbase::core::operations::management::bucket_flush_request>(options->op_args);
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_flush_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case BucketManagementOperations::BUCKET_DESCRIBE: {
                auto req =
                  get_bucket_mgmt_with_bucket_name_req<couchbase::core::operations::management::bucket_describe_request>(options->op_args);
                req.timeout = options->timeout_ms;
                res = do_bucket_mgmt_op<couchbase::core::operations::management::bucket_describe_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            default: {
                pycbc_set_python_exception(
                  PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized bucket mgmt operation passed in.");
                barrier->set_value(nullptr);
                break;
            }
        };
    } catch (const std::invalid_argument&) {
    }

    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }

    if (res == nullptr) {
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
        return nullptr;
    }

    return res;
}

void
add_bucket_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(BucketManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("BucketManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "bucket_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
