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

#include "client.hxx"
#include <structmember.h>
#include <cstdlib>

#include "connection.hxx"
#include "exceptions.hxx"
#include "kv_ops.hxx"
#include "subdoc_ops.hxx"
#include "diagnostics.hxx"
#include "binary_ops.hxx"
#include "logger.hxx"
#include "n1ql.hxx"
#include "analytics.hxx"
#include "search.hxx"
#include "views.hxx"
#include "management/management.hxx"
#include "result.hxx"
#include "transactions/transactions.hxx"

void
add_ops_enum(PyObject* pyObj_module)
{
    PyObject* pyObj_enum_module = PyImport_ImportModule("enum");
    if (!pyObj_enum_module) {
        return;
    }
    PyObject* pyObj_enum_class = PyObject_GetAttrString(pyObj_enum_module, "Enum");

    PyObject* pyObj_enum_values = PyUnicode_FromString(Operations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("Operations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "operations", pyObj_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_operations);
        return;
    }

    add_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_cluster_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_bucket_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_collection_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_user_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_query_index_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_analytics_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_search_index_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_view_index_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
    add_eventing_function_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
}

void
add_constants(PyObject* module)
{
    if (PyModule_AddIntConstant(module, "FMT_JSON", PYCBC_FMT_JSON) < 0) {
        Py_XDECREF(module);
        return;
    }
    if (PyModule_AddIntConstant(module, "FMT_BYTES", PYCBC_FMT_BYTES) < 0) {
        Py_XDECREF(module);
        return;
    }
    if (PyModule_AddIntConstant(module, "FMT_UTF8", PYCBC_FMT_UTF8) < 0) {
        Py_XDECREF(module);
        return;
    }
    if (PyModule_AddIntConstant(module, "FMT_PICKLE", PYCBC_FMT_PICKLE) < 0) {
        Py_XDECREF(module);
        return;
    }
    if (PyModule_AddIntConstant(module, "FMT_LEGACY_MASK", PYCBC_FMT_LEGACY_MASK) < 0) {
        Py_XDECREF(module);
        return;
    }
    if (PyModule_AddIntConstant(module, "FMT_COMMON_MASK", PYCBC_FMT_COMMON_MASK) < 0) {
        Py_XDECREF(module);
        return;
    }
    auto cxxcbc_metadata = couchbase::core::meta::sdk_build_info_json();
    if (PyModule_AddStringConstant(module, "CXXCBC_METADATA", cxxcbc_metadata.c_str())) {
        Py_XDECREF(module);
        return;
    }
}

std::string
service_type_to_str(couchbase::core::service_type t)
{
    switch (t) {
        case couchbase::core::service_type::key_value: {
            return "kv";
        }
        case couchbase::core::service_type::query: {
            return "query";
        }
        case couchbase::core::service_type::analytics: {
            return "analytics";
        }
        case couchbase::core::service_type::search: {
            return "search";
        }
        case couchbase::core::service_type::management: {
            return "mgmt";
        }
        case couchbase::core::service_type::view: {
            return "views";
        }
        case couchbase::core::service_type::eventing: {
            return "eventing";
        }
        default: {
            // TODO: better exception
            PyErr_SetString(PyExc_ValueError, "Invalid service type.");
            return {};
        }
    }
}

couchbase::core::service_type
str_to_service_type(std::string svc)
{
    if (svc.compare("kv") == 0) {
        return couchbase::core::service_type::key_value;
    }
    if (svc.compare("query") == 0) {
        return couchbase::core::service_type::query;
    }
    if (svc.compare("analytics") == 0) {
        return couchbase::core::service_type::analytics;
    }
    if (svc.compare("search") == 0) {
        return couchbase::core::service_type::search;
    }
    if (svc.compare("mgmt") == 0) {
        return couchbase::core::service_type::management;
    }
    if (svc.compare("views") == 0) {
        return couchbase::core::service_type::view;
    }

    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, "Invalid service type.");
    return {};
}

static PyObject* json_module = nullptr;
static PyObject* json_dumps = nullptr;
static PyObject* json_loads = nullptr;

// json encode an object using python's json module
std::string
json_encode(PyObject* obj)
{
    // let's import json, we will use it as the default transcoder
    if (nullptr == json_dumps || nullptr == json_module) {
        json_module = PyImport_ImportModule("json");
        if (nullptr != json_module) {
            json_dumps = PyObject_GetAttrString(json_module, "dumps");
        } else {
            PyErr_PrintEx(1);
            return {};
        }
    }
    // call json.dumps(obj)
    PyObject* args = PyTuple_Pack(1, obj);
    PyObject* encoded = PyObject_CallObject(json_dumps, args);
    Py_XDECREF(args);
    std::string res = std::string();
    if (PyUnicode_Check(encoded)) {
        res = std::string(PyUnicode_AsUTF8(encoded));
    }
    Py_XDECREF(encoded);
    // CB_LOG_DEBUG("encoded document: {}", res);
    return res;
}

std::tuple<std::string, uint32_t>
encode_value(PyObject* transcoder, PyObject* value)
{
    PyObject* meth = nullptr;
    PyObject* args = nullptr;
    PyObject* result_tuple = nullptr;
    PyObject* new_value = nullptr;
    PyObject* flags_obj = nullptr;

    args = PyTuple_Pack(1, value);
    meth = PyObject_GetAttrString(transcoder, TRANSCODER_ENCODE);
    if (!meth) {
        // TODO:  better exception here
        PyErr_SetString(PyExc_Exception, "Transcoder did not provide encode_value method.");
        Py_XDECREF(args);
        return {};
    }

    result_tuple = PyObject_Call(meth, args, nullptr);
    // TODO: check if error flag is set
    Py_XDECREF(args);
    Py_XDECREF(meth);

    if (!PyTuple_Check(result_tuple) || PyTuple_GET_SIZE(result_tuple) != 2) {
        PyErr_SetString(PyExc_Exception, "Expected return value of (bytes, flags).");
        Py_XDECREF(result_tuple);
        return {};
    }

    new_value = PyTuple_GET_ITEM(result_tuple, 0);
    flags_obj = PyTuple_GET_ITEM(result_tuple, 1);

    if (new_value == nullptr || !PyBytes_Check(new_value)) {
        PyErr_SetString(PyExc_Exception, "Expected bytes object for value to encode.");
        Py_XDECREF(result_tuple);
        return {};
    }

    if (flags_obj == nullptr || !PyLong_Check(flags_obj)) {
        PyErr_SetString(PyExc_Exception, "Expected int object for flags.");
        Py_XDECREF(result_tuple);
        return {};
    }

    std::string res = std::string();
    if (PyUnicode_Check(new_value)) {
        res = std::string(PyUnicode_AsUTF8(new_value));
    } else {
        PyObject* unicode = PyUnicode_FromEncodedObject(new_value, "utf-8", "strict");
        res = std::string(PyUnicode_AsUTF8(unicode));
        Py_XDECREF(unicode);
    }

    auto result = std::tuple<std::string, uint32_t>{ res, static_cast<uint32_t>(PyLong_AsLong(flags_obj)) };
    // new_value and flags_obj are borrowed references
    // only decref the tuple
    Py_XDECREF(result_tuple);

    return result;
}

PyObject*
decode_value(const PyObject* transcoder, const char* value, size_t nvalue, uint32_t flags, bool deserialize)
{
    PyObject* pyObj_meth = nullptr;
    PyObject* pyObj_args = nullptr;

    if (deserialize) {
        // transcoder is actually a serializer
        pyObj_meth = PyObject_GetAttrString(const_cast<PyObject*>(transcoder), DESERIALIZE);
    } else {
        pyObj_meth = PyObject_GetAttrString(const_cast<PyObject*>(transcoder), TRANSCODER_DECODE);
    }

    if (!pyObj_meth) {
        // TODO:  better exception here
        PyErr_SetString(PyExc_Exception, "Transcoder did not provide decode_value method.");
        Py_XDECREF(pyObj_args);
        return {};
    }

    PyObject* pyObj_value = nullptr;
    PyObject* pyObj_result = nullptr;

    // TODO: verify if PyUnicode_DecodeUTF8(value, nvalue, "strict") might be needed?
    pyObj_value = PyBytes_FromStringAndSize(value, nvalue);
    if (deserialize) {
        pyObj_args = PyTuple_Pack(1, pyObj_value);
    } else {
        PyObject* pyObj_flags = nullptr;
        pyObj_flags = PyLong_FromUnsignedLong(flags);
        pyObj_args = PyTuple_Pack(2, pyObj_value, pyObj_flags);
        Py_XDECREF(pyObj_flags);
    }
    Py_XDECREF(pyObj_value);

    pyObj_result = PyObject_Call(pyObj_meth, pyObj_args, nullptr);
    // TODO: check if error flag is set
    Py_XDECREF(pyObj_args);
    Py_XDECREF(pyObj_meth);

    return pyObj_result;
}

PyObject*
json_decode(const char* value, size_t nvalue)
{
    if (nullptr == json_loads || nullptr == json_module) {
        json_module = PyImport_ImportModule("json");
        if (nullptr != json_module) {
            json_loads = PyObject_GetAttrString(json_module, "loads");
        } else {
            PyErr_PrintEx(1);
            return nullptr;
        }
    }

    PyObject* unicode = nullptr;

    // PyUnicode_FromString
    unicode = PyUnicode_DecodeUTF8(value, nvalue, "strict");

    if (!unicode) {
        // TODO:  raise an exception here
        PyErr_PrintEx(1);
        return nullptr;
    }

    PyObject* args = PyTuple_Pack(1, unicode);
    PyObject* decoded = PyObject_CallObject(json_loads, args);

    Py_XDECREF(args);
    return decoded;
}

static PyObject*
binary_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_binary_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform binary operation.");
    }
    return res;
}

static PyObject*
binary_multi_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_binary_multi_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform binary multi operation.");
    }
    return res;
}

static PyObject*
kv_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_kv_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform KV operation.");
    }
    return res;
}

static PyObject*
kv_multi_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_kv_multi_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform KV multi operation.");
    }
    return res;
}

static PyObject*
subdoc_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_subdoc_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform subdocument operation.");
    }
    return res;
}

static PyObject*
diagnostics_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_diagnostics_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform diagnostics operation.");
    }
    return res;
}

static PyObject*
n1ql_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
    streamed_result* res = handle_n1ql_query(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform N1QL query.");
    }
    return reinterpret_cast<PyObject*>(res);
}

static PyObject*
analytics_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyGILState_STATE state = PyGILState_Ensure();
    streamed_result* res = handle_analytics_query(self, args, kwargs);
    if (!res) {
        PyErr_SetString(PyExc_Exception, "Unable to perform analytics query.");
    }
    PyGILState_Release(state);
    return reinterpret_cast<PyObject*>(res);
}

static PyObject*
search_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
    streamed_result* res = handle_search_query(self, args, kwargs);
    if (!res) {
        PyErr_SetString(PyExc_Exception, "Unable to perform search query.");
    }
    return reinterpret_cast<PyObject*>(res);
}

static PyObject*
view_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
    streamed_result* res = handle_view_query(self, args, kwargs);
    if (!res) {
        PyErr_SetString(PyExc_Exception, "Unable to perform view query.");
    }
    return reinterpret_cast<PyObject*>(res);
}

static PyObject*
cluster_info(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_mgmt_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform cluster info operation.");
    }
    return res;
}

static PyObject*
management_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_mgmt_op(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform management operation.");
    }
    return res;
}

static PyObject*
open_or_close_bucket(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_open_or_close_bucket(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to open/close bucket.");
    }
    return res;
}

static PyObject*
create_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_create_connection(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to create connection.");
    }
    return res;
}

static PyObject*
get_connection_information(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = get_connection_info(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() == nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to get connection information.");
    }
    return res;
}

static PyObject*
close_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* res = handle_close_connection(self, args, kwargs);
    if (res == nullptr && PyErr_Occurred() != nullptr) {
        pycbc_set_python_exception(PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to close connection.");
    }
    return res;
}

static struct PyMethodDef methods[] = {
    { "create_connection", (PyCFunction)create_connection, METH_VARARGS | METH_KEYWORDS, "Create connection object" },
    { "get_connection_info", (PyCFunction)get_connection_information, METH_VARARGS | METH_KEYWORDS, "Get connection options" },
    { "open_or_close_bucket", (PyCFunction)open_or_close_bucket, METH_VARARGS | METH_KEYWORDS, "Open or close a bucket" },
    { "close_connection", (PyCFunction)close_connection, METH_VARARGS | METH_KEYWORDS, "Close a connection" },
    { "kv_operation", (PyCFunction)kv_operation, METH_VARARGS | METH_KEYWORDS, "Handle all key/value operations" },
    { "kv_multi_operation", (PyCFunction)kv_multi_operation, METH_VARARGS | METH_KEYWORDS, "Handle all key/value multi operations" },
    { "subdoc_operation", (PyCFunction)subdoc_operation, METH_VARARGS | METH_KEYWORDS, "Handle all subdoc operations" },
    { "binary_operation", (PyCFunction)binary_operation, METH_VARARGS | METH_KEYWORDS, "Handle all binary operations" },
    { "binary_multi_operation", (PyCFunction)binary_multi_operation, METH_VARARGS | METH_KEYWORDS, "Handle all binary multi operations" },
    { "diagnostics_operation", (PyCFunction)diagnostics_operation, METH_VARARGS | METH_KEYWORDS, "Handle all diagnostics operations" },
    { "n1ql_query", (PyCFunction)n1ql_query, METH_VARARGS | METH_KEYWORDS, "Execute N1QL Query" },
    { "analytics_query", (PyCFunction)analytics_query, METH_VARARGS | METH_KEYWORDS, "Execute analytics Query" },
    { "search_query", (PyCFunction)search_query, METH_VARARGS | METH_KEYWORDS, "Execute search Query" },
    { "view_query", (PyCFunction)view_query, METH_VARARGS | METH_KEYWORDS, "Execute map reduce views Query" },
    { "cluster_info", (PyCFunction)cluster_info, METH_VARARGS | METH_KEYWORDS, "Get information on the connected cluster" },
    { "management_operation", (PyCFunction)management_operation, METH_VARARGS | METH_KEYWORDS, "Handle all management operations" },
    { "create_transactions", (PyCFunction)pycbc_txns::create_transactions, METH_VARARGS | METH_KEYWORDS, "Create a transactions object" },
    { "run_transaction", (PyCFunction)pycbc_txns::run_transactions, METH_VARARGS | METH_KEYWORDS, "Run a transaction" },
    { "transaction_op", (PyCFunction)pycbc_txns::transaction_op, METH_VARARGS | METH_KEYWORDS, "perform a transaction kv operation" },
    { "transaction_query_op",
      (PyCFunction)pycbc_txns::transaction_query_op,
      METH_VARARGS | METH_KEYWORDS,
      "perform a transactional query" },
    { "destroy_transactions",
      (PyCFunction)pycbc_txns::destroy_transactions,
      METH_VARARGS | METH_KEYWORDS,
      "shut down transactions object" },
    { nullptr, nullptr, 0, nullptr }
};

static struct PyModuleDef pycbc_core_module = { { PyObject_HEAD_INIT(NULL) nullptr, 0, nullptr },
                                                "pycbc_core",
                                                "Python interface to couchbase-client-cxx",
                                                -1,
                                                methods,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                nullptr };

PyMODINIT_FUNC
PyInit_pycbc_core(void)
{
    Py_Initialize();
    PyObject* m = nullptr;

    PyObject* result_type;
    if (pycbc_result_type_init(&result_type) < 0) {
        return nullptr;
    }

    PyObject* exception_base_type;
    if (pycbc_exception_base_type_init(&exception_base_type) < 0) {
        return nullptr;
    }

    PyObject* streamed_result_type;
    if (pycbc_streamed_result_type_init(&streamed_result_type) < 0) {
        return nullptr;
    }

    PyObject* mutation_token_type;
    if (pycbc_mutation_token_type_init(&mutation_token_type) < 0) {
        return nullptr;
    }

    PyObject* pycbc_logger_type;
    if (pycbc_logger_type_init(&pycbc_logger_type) < 0) {
        return nullptr;
    }

    m = PyModule_Create(&pycbc_core_module);
    if (m == nullptr) {
        return nullptr;
    }

    Py_INCREF(result_type);
    if (PyModule_AddObject(m, "result", result_type) < 0) {
        Py_DECREF(result_type);
        Py_DECREF(m);
        return nullptr;
    }

    Py_INCREF(exception_base_type);
    if (PyModule_AddObject(m, "exception", exception_base_type) < 0) {
        Py_DECREF(exception_base_type);
        Py_DECREF(m);
        return nullptr;
    }

    Py_INCREF(streamed_result_type);
    if (PyModule_AddObject(m, "streamed_result", streamed_result_type) < 0) {
        Py_DECREF(streamed_result_type);
        Py_DECREF(m);
        return nullptr;
    }

    Py_INCREF(mutation_token_type);
    if (PyModule_AddObject(m, "mutation_token", mutation_token_type) < 0) {
        Py_DECREF(mutation_token_type);
        Py_DECREF(m);
        return nullptr;
    }

    Py_INCREF(pycbc_logger_type);
    if (PyModule_AddObject(m, "pycbc_logger", pycbc_logger_type) < 0) {
        Py_DECREF(pycbc_logger_type);
        Py_DECREF(m);
        return nullptr;
    }

    add_ops_enum(m);
    add_constants(m);
    return pycbc_txns::add_transaction_objects(m);
}
