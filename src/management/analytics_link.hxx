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

#pragma once

#include "analytics_management.hxx"
#include <core/management/analytics_link_couchbase_remote.hxx>

PyObject*
build_couchbase_remote_link_encryption_settings(couchbase::core::management::analytics::couchbase_link_encryption_settings settings)
{
    PyObject* pyObj_encryption = PyDict_New();
    PyObject* pyObj_tmp = nullptr;

    auto level = couchbase::core::management::analytics::to_string(settings.level);
    pyObj_tmp = PyUnicode_FromString(level.c_str());
    if (-1 == PyDict_SetItemString(pyObj_encryption, "encryption_level", pyObj_tmp)) {
        Py_XDECREF(pyObj_encryption);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (settings.certificate.has_value()) {
        pyObj_tmp = PyUnicode_FromString(settings.certificate.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_encryption, "certificate", pyObj_tmp)) {
            Py_DECREF(pyObj_encryption);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.client_certificate.has_value()) {
        pyObj_tmp = PyUnicode_FromString(settings.client_certificate.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_encryption, "client_certificate", pyObj_tmp)) {
            Py_DECREF(pyObj_encryption);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    // no need to read in the client_key

    return pyObj_encryption;
}

PyObject*
build_couchbase_remote_link(couchbase::core::management::analytics::couchbase_remote_link link)
{
    PyObject* pyObj_link = PyDict_New();
    PyObject* pyObj_tmp = nullptr;

    pyObj_tmp = PyUnicode_FromString(link.link_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "link_name", pyObj_tmp)) {
        Py_XDECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.dataverse.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "dataverse", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.hostname.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "hostname", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.hostname.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "hostname", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (link.username.has_value()) {
        pyObj_tmp = PyUnicode_FromString(link.username.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_link, "username", pyObj_tmp)) {
            Py_DECREF(pyObj_link);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    // no need to read in pw

    PyObject* pyObj_encryption_settings = build_couchbase_remote_link_encryption_settings(link.encryption);
    if (pyObj_encryption_settings == nullptr) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_link, "encryption_settings", pyObj_encryption_settings)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_encryption_settings);

    return pyObj_link;
}

PyObject*
build_s3_link(couchbase::core::management::analytics::s3_external_link link)
{
    PyObject* pyObj_link = PyDict_New();
    PyObject* pyObj_tmp = nullptr;

    pyObj_tmp = PyUnicode_FromString(link.link_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "link_name", pyObj_tmp)) {
        Py_XDECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.dataverse.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "dataverse", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.access_key_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "access_key_id", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.region.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "region", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (link.service_endpoint.has_value()) {
        pyObj_tmp = PyUnicode_FromString(link.service_endpoint.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_link, "service_endpoint", pyObj_tmp)) {
            Py_DECREF(pyObj_link);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return pyObj_link;
}

PyObject*
build_azure_blob_link(couchbase::core::management::analytics::azure_blob_external_link link)
{
    PyObject* pyObj_link = PyDict_New();
    PyObject* pyObj_tmp = nullptr;

    pyObj_tmp = PyUnicode_FromString(link.link_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "link_name", pyObj_tmp)) {
        Py_XDECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(link.dataverse.c_str());
    if (-1 == PyDict_SetItemString(pyObj_link, "dataverse", pyObj_tmp)) {
        Py_DECREF(pyObj_link);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (link.account_name.has_value()) {
        pyObj_tmp = PyUnicode_FromString(link.account_name.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_link, "account_name", pyObj_tmp)) {
            Py_DECREF(pyObj_link);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (link.blob_endpoint.has_value()) {
        pyObj_tmp = PyUnicode_FromString(link.blob_endpoint.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_link, "blob_endpoint", pyObj_tmp)) {
            Py_DECREF(pyObj_link);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (link.endpoint_suffix.has_value()) {
        pyObj_tmp = PyUnicode_FromString(link.endpoint_suffix.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_link, "endpoint_suffix", pyObj_tmp)) {
            Py_DECREF(pyObj_link);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return pyObj_link;
}

couchbase::core::management::analytics::couchbase_link_encryption_level
str_to_encryption_level(PyObject* pyObj_level)
{
    auto level = std::string(PyUnicode_AsUTF8(pyObj_level));
    if (level.compare("none") == 0) {
        return couchbase::core::management::analytics::couchbase_link_encryption_level::none;
    }
    if (level.compare("half") == 0) {
        return couchbase::core::management::analytics::couchbase_link_encryption_level::half;
    }
    if (level.compare("full") == 0) {
        return couchbase::core::management::analytics::couchbase_link_encryption_level::full;
    }
    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, "Invalid couchbase remote link encryption level.");
    return {};
}

couchbase::core::management::analytics::couchbase_link_encryption_settings
get_couchbase_remote_link_encryption_settings(PyObject* pyObj_settings)
{
    couchbase::core::management::analytics::couchbase_link_encryption_settings settings{};

    PyObject* pyObj_encryption_level = PyDict_GetItemString(pyObj_settings, "encryption_level");
    auto encryption_level = str_to_encryption_level(pyObj_encryption_level);
    settings.level = encryption_level;

    PyObject* pyObj_certificate = PyDict_GetItemString(pyObj_settings, "certificate");
    if (pyObj_certificate != nullptr) {
        auto certificate = std::string(PyUnicode_AsUTF8(pyObj_certificate));
        settings.certificate = certificate;
    }

    PyObject* pyObj_client_certificate = PyDict_GetItemString(pyObj_settings, "client_certificate");
    if (pyObj_client_certificate != nullptr) {
        auto client_certificate = std::string(PyUnicode_AsUTF8(pyObj_client_certificate));
        settings.client_certificate = client_certificate;
    }

    PyObject* pyObj_client_key = PyDict_GetItemString(pyObj_settings, "client_key");
    if (pyObj_client_key != nullptr) {
        auto client_key = std::string(PyUnicode_AsUTF8(pyObj_client_key));
        settings.client_key = client_key;
    }

    return settings;
}

template<typename analytics_link_type>
analytics_link_type
get_link([[maybe_unused]] PyObject* pyObj_link)
{
    analytics_link_type link{};
    return link;
}

template<>
couchbase::core::management::analytics::couchbase_remote_link
get_link([[maybe_unused]] PyObject* pyObj_link)
{
    couchbase::core::management::analytics::couchbase_remote_link link{};
    PyObject* pyObj_link_name = PyDict_GetItemString(pyObj_link, "link_name");
    auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
    link.link_name = link_name;

    PyObject* pyObj_dataverse = PyDict_GetItemString(pyObj_link, "dataverse");
    auto dataverse = std::string(PyUnicode_AsUTF8(pyObj_dataverse));
    link.dataverse = dataverse;

    PyObject* pyObj_hostname = PyDict_GetItemString(pyObj_link, "hostname");
    auto hostname = std::string(PyUnicode_AsUTF8(pyObj_hostname));
    link.hostname = hostname;

    PyObject* pyObj_username = PyDict_GetItemString(pyObj_link, "username");
    if (pyObj_username != nullptr) {
        auto username = std::string(PyUnicode_AsUTF8(pyObj_username));
        link.username = username;
    }

    PyObject* pyObj_password = PyDict_GetItemString(pyObj_link, "password");
    if (pyObj_password != nullptr) {
        auto password = std::string(PyUnicode_AsUTF8(pyObj_password));
        link.password = password;
    }

    PyObject* pyObj_encryption = PyDict_GetItemString(pyObj_link, "encryption");
    link.encryption = get_couchbase_remote_link_encryption_settings(pyObj_encryption);

    return link;
}

template<>
couchbase::core::management::analytics::s3_external_link
get_link([[maybe_unused]] PyObject* pyObj_link)
{
    couchbase::core::management::analytics::s3_external_link link{};
    PyObject* pyObj_link_name = PyDict_GetItemString(pyObj_link, "link_name");
    auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
    link.link_name = link_name;

    PyObject* pyObj_dataverse = PyDict_GetItemString(pyObj_link, "dataverse");
    auto dataverse = std::string(PyUnicode_AsUTF8(pyObj_dataverse));
    link.dataverse = dataverse;

    PyObject* pyObj_access_key_id = PyDict_GetItemString(pyObj_link, "access_key_id");
    auto access_key_id = std::string(PyUnicode_AsUTF8(pyObj_access_key_id));
    link.access_key_id = access_key_id;

    PyObject* pyObj_secret_access_key = PyDict_GetItemString(pyObj_link, "secret_access_key");
    auto secret_access_key = std::string(PyUnicode_AsUTF8(pyObj_secret_access_key));
    link.secret_access_key = secret_access_key;

    PyObject* pyObj_session_token = PyDict_GetItemString(pyObj_link, "session_token");
    if (pyObj_session_token != nullptr) {
        auto session_token = std::string(PyUnicode_AsUTF8(pyObj_session_token));
        link.session_token = session_token;
    }

    PyObject* pyObj_region = PyDict_GetItemString(pyObj_link, "region");
    auto region = std::string(PyUnicode_AsUTF8(pyObj_region));
    link.region = region;

    PyObject* pyObj_service_endpoint = PyDict_GetItemString(pyObj_link, "service_endpoint");
    if (pyObj_service_endpoint != nullptr) {
        auto service_endpoint = std::string(PyUnicode_AsUTF8(pyObj_service_endpoint));
        link.service_endpoint = service_endpoint;
    }

    return link;
}

template<>
couchbase::core::management::analytics::azure_blob_external_link
get_link([[maybe_unused]] PyObject* pyObj_link)
{
    couchbase::core::management::analytics::azure_blob_external_link link{};
    PyObject* pyObj_link_name = PyDict_GetItemString(pyObj_link, "link_name");
    auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
    link.link_name = link_name;

    PyObject* pyObj_dataverse = PyDict_GetItemString(pyObj_link, "dataverse");
    auto dataverse = std::string(PyUnicode_AsUTF8(pyObj_dataverse));
    link.dataverse = dataverse;

    PyObject* pyObj_connection_string = PyDict_GetItemString(pyObj_link, "connection_string");
    if (pyObj_connection_string != nullptr) {
        auto connection_string = std::string(PyUnicode_AsUTF8(pyObj_connection_string));
        link.connection_string = connection_string;
    }

    PyObject* pyObj_account_name = PyDict_GetItemString(pyObj_link, "account_name");
    if (pyObj_account_name != nullptr) {
        auto account_name = std::string(PyUnicode_AsUTF8(pyObj_account_name));
        link.account_name = account_name;
    }

    PyObject* pyObj_account_key = PyDict_GetItemString(pyObj_link, "account_key");
    if (pyObj_account_key != nullptr) {
        auto account_key = std::string(PyUnicode_AsUTF8(pyObj_account_key));
        link.account_key = account_key;
    }

    PyObject* pyObj_shared_access_signature = PyDict_GetItemString(pyObj_link, "shared_access_signature");
    if (pyObj_shared_access_signature != nullptr) {
        auto shared_access_signature = std::string(PyUnicode_AsUTF8(pyObj_shared_access_signature));
        link.shared_access_signature = shared_access_signature;
    }

    PyObject* pyObj_blob_endpoint = PyDict_GetItemString(pyObj_link, "blob_endpoint");
    if (pyObj_blob_endpoint != nullptr) {
        auto blob_endpoint = std::string(PyUnicode_AsUTF8(pyObj_blob_endpoint));
        link.blob_endpoint = blob_endpoint;
    }

    PyObject* pyObj_endpoint_suffix = PyDict_GetItemString(pyObj_link, "endpoint_suffix");
    if (pyObj_endpoint_suffix != nullptr) {
        auto endpoint_suffix = std::string(PyUnicode_AsUTF8(pyObj_endpoint_suffix));
        link.endpoint_suffix = endpoint_suffix;
    }

    return link;
}

template<typename analytics_link_type>
couchbase::core::operations::management::analytics_link_create_request<analytics_link_type>
get_analytics_link_create_request(struct analytics_mgmt_options* options)
{
    couchbase::core::operations::management::analytics_link_create_request<analytics_link_type> req{};

    PyObject* pyObj_link = PyDict_GetItemString(options->op_args, "link");
    req.link = get_link<analytics_link_type>(pyObj_link);

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

template<typename analytics_link_type>
couchbase::core::operations::management::analytics_link_replace_request<analytics_link_type>
get_analytics_link_replace_request(struct analytics_mgmt_options* options)
{
    couchbase::core::operations::management::analytics_link_replace_request<analytics_link_type> req{};

    PyObject* pyObj_link = PyDict_GetItemString(options->op_args, "link");
    req.link = get_link<analytics_link_type>(pyObj_link);

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_link_get_all_request
get_link_get_all_request(struct analytics_mgmt_options* options)
{
    couchbase::core::operations::management::analytics_link_get_all_request req{};

    PyObject* pyObj_link_type = PyDict_GetItemString(options->op_args, "link_type");
    if (pyObj_link_type != nullptr) {
        auto link_type = std::string(PyUnicode_AsUTF8(pyObj_link_type));
        req.link_type = link_type;
    }

    PyObject* pyObj_link_name = PyDict_GetItemString(options->op_args, "link_name");
    if (pyObj_link_name != nullptr) {
        auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
        req.link_name = link_name;
    }

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    if (pyObj_dataverse_name != nullptr) {
        auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));
        req.dataverse_name = dataverse_name;
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_link_drop_request
get_link_drop_request(struct analytics_mgmt_options* options)
{
    couchbase::core::operations::management::analytics_link_drop_request req{};

    PyObject* pyObj_link_name = PyDict_GetItemString(options->op_args, "link_name");
    auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
    req.link_name = link_name;

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));
    req.dataverse_name = dataverse_name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_link_disconnect_request
get_link_disconnect_request(struct analytics_mgmt_options* options)
{
    couchbase::core::operations::management::analytics_link_disconnect_request req{};

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    if (pyObj_dataverse_name != nullptr) {
        auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));
        req.dataverse_name = dataverse_name;
    }

    PyObject* pyObj_link_name = PyDict_GetItemString(options->op_args, "link_name");
    if (pyObj_link_name != nullptr) {
        auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
        req.link_name = link_name;
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_link_connect_request
get_link_connect_request(struct analytics_mgmt_options* options)
{
    couchbase::core::operations::management::analytics_link_connect_request req{};

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    if (pyObj_dataverse_name != nullptr) {
        auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));
        req.dataverse_name = dataverse_name;
    }

    PyObject* pyObj_link_name = PyDict_GetItemString(options->op_args, "link_name");
    if (pyObj_link_name != nullptr) {
        auto link_name = std::string(PyUnicode_AsUTF8(pyObj_link_name));
        req.link_name = link_name;
    }

    PyObject* pyObj_force = PyDict_GetItemString(options->op_args, "force");
    if (pyObj_force) {
        if (pyObj_force == Py_True) {
            req.force = true;
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}
