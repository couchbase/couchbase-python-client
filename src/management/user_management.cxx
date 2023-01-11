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

#include "user_management.hxx"
#include "../exceptions.hxx"
#include <core/management/rbac.hxx>

couchbase::core::management::rbac::auth_domain
str_to_auth_domain(std::string domain)
{
    if (domain.compare("external") == 0) {
        return couchbase::core::management::rbac::auth_domain::external;
    }
    return couchbase::core::management::rbac::auth_domain::local;
    ;
}

PyObject*
auth_domain_to_str(couchbase::core::management::rbac::auth_domain domain)
{
    PyObject* pyObj_domain = nullptr;

    if (domain == couchbase::core::management::rbac::auth_domain::local) {
        pyObj_domain = PyUnicode_FromString("local");
    } else if (domain == couchbase::core::management::rbac::auth_domain::external) {
        pyObj_domain = PyUnicode_FromString("external");
    } else {
        pyObj_domain = PyUnicode_FromString("unknown");
    }

    return pyObj_domain;
}

couchbase::core::management::rbac::role
get_role(PyObject* pyObj_role)
{
    couchbase::core::management::rbac::role role{};

    PyObject* role_name = PyDict_GetItemString(pyObj_role, "name");
    if (role_name) {
        role.name = std::string(PyUnicode_AsUTF8(role_name));
    }

    PyObject* pyObj_role_bucket = PyDict_GetItemString(pyObj_role, "bucket");
    if (pyObj_role_bucket && pyObj_role_bucket != Py_None) {
        role.bucket = std::string(PyUnicode_AsUTF8(pyObj_role_bucket));
    }

    PyObject* pyObj_role_scope = PyDict_GetItemString(pyObj_role, "scope");
    if (pyObj_role_scope && pyObj_role_scope != Py_None) {
        role.scope = std::string(PyUnicode_AsUTF8(pyObj_role_scope));
    }

    PyObject* pyObj_role_collection = PyDict_GetItemString(pyObj_role, "collection");
    if (pyObj_role_collection && pyObj_role_collection != Py_None) {
        role.collection = std::string(PyUnicode_AsUTF8(pyObj_role_collection));
    }

    return role;
}

couchbase::core::management::rbac::user
get_user(PyObject* pyObj_user)
{
    couchbase::core::management::rbac::user user{};

    PyObject* pyObj_username = PyDict_GetItemString(pyObj_user, "username");
    if (pyObj_username) {
        user.username = std::string(PyUnicode_AsUTF8(pyObj_username));
    }

    PyObject* pyObj_name = PyDict_GetItemString(pyObj_user, "name");
    if (pyObj_name && pyObj_name != Py_None) {
        user.display_name = std::string(PyUnicode_AsUTF8(pyObj_name));
    }

    PyObject* pyObj_password = PyDict_GetItemString(pyObj_user, "password");
    if (pyObj_password && pyObj_password != Py_None) {
        user.password = std::string(PyUnicode_AsUTF8(pyObj_password));
    }

    PyObject* pyObj_roles = PyDict_GetItemString(pyObj_user, "roles");
    if (pyObj_roles) {
        for (Py_ssize_t i = 0; i < PyList_Size(pyObj_roles); i++) {
            PyObject* pyObj_role = PyList_GetItem(pyObj_roles, i);
            auto role = get_role(pyObj_role);
            user.roles.emplace_back(role);
        }
    }

    PyObject* pyObj_groups = PyDict_GetItemString(pyObj_user, "groups");
    if (pyObj_groups) {
        for (Py_ssize_t i = 0; i < PyList_Size(pyObj_groups); i++) {
            PyObject* pyObj_group = PyList_GetItem(pyObj_groups, i);
            user.groups.emplace(std::string(PyUnicode_AsUTF8(pyObj_group)));
        }
    }

    return user;
}

couchbase::core::management::rbac::group
get_group(PyObject* pyObj_group)
{
    couchbase::core::management::rbac::group group{};

    PyObject* pyObj_name = PyDict_GetItemString(pyObj_group, "name");
    if (pyObj_name) {
        group.name = std::string(PyUnicode_AsUTF8(pyObj_name));
    }

    PyObject* pyObj_description = PyDict_GetItemString(pyObj_group, "description");
    if (pyObj_description && pyObj_description != Py_None) {
        group.description = std::string(PyUnicode_AsUTF8(pyObj_description));
    }

    PyObject* pyObj_roles = PyDict_GetItemString(pyObj_group, "roles");
    if (pyObj_roles) {
        for (Py_ssize_t i = 0; i < PyList_Size(pyObj_roles); i++) {
            PyObject* pyObj_role = PyList_GetItem(pyObj_roles, i);
            auto role = get_role(pyObj_role);
            group.roles.emplace_back(role);
        }
    }

    PyObject* pyObj_ldap_group_reference = PyDict_GetItemString(pyObj_group, "ldap_group_reference");
    if (pyObj_ldap_group_reference && pyObj_ldap_group_reference != Py_None) {
        group.ldap_group_reference = std::string(PyUnicode_AsUTF8(pyObj_ldap_group_reference));
    }

    return group;
}

template<typename T>
PyObject*
build_role(const T& role)
{
    PyObject* pyObj_role = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(role.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_role, "name", pyObj_tmp)) {
        Py_XDECREF(pyObj_role);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (role.bucket.has_value()) {
        pyObj_tmp = PyUnicode_FromString(role.bucket.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_role, "bucket_name", pyObj_tmp)) {
            Py_DECREF(pyObj_role);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (role.scope.has_value()) {
        pyObj_tmp = PyUnicode_FromString(role.scope.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_role, "scope_name", pyObj_tmp)) {
            Py_DECREF(pyObj_role);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (role.collection.has_value()) {
        pyObj_tmp = PyUnicode_FromString(role.collection.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_role, "collection_name", pyObj_tmp)) {
            Py_DECREF(pyObj_role);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return pyObj_role;
}

PyObject*
build_role_and_origins(couchbase::core::management::rbac::role_and_origins role)
{
    PyObject* pyObj_role_and_origin = PyDict_New();
    PyObject* pyObj_role = build_role(role);
    if (pyObj_role == nullptr) {
        Py_XDECREF(pyObj_role_and_origin);
        return nullptr;
    }

    if (-1 == PyDict_SetItemString(pyObj_role_and_origin, "role", pyObj_role)) {
        Py_XDECREF(pyObj_role_and_origin);
        Py_DECREF(pyObj_role);
        return nullptr;
    }
    Py_DECREF(pyObj_role);

    PyObject* pyObj_origins = PyList_New(static_cast<Py_ssize_t>(0));
    PyObject* pyObj_tmp = nullptr;
    for (auto const& origin : role.origins) {
        PyObject* pyObj_origin = PyDict_New();
        pyObj_tmp = PyUnicode_FromString(origin.type.c_str());
        if (-1 == PyDict_SetItemString(pyObj_origin, "type", pyObj_tmp)) {
            Py_XDECREF(pyObj_origin);
            Py_XDECREF(pyObj_origins);
            Py_DECREF(pyObj_role_and_origin);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        if (origin.name.has_value()) {
            pyObj_tmp = PyUnicode_FromString(origin.name.value().c_str());
            if (-1 == PyDict_SetItemString(pyObj_origin, "name", pyObj_tmp)) {
                Py_DECREF(pyObj_origin);
                Py_XDECREF(pyObj_origins);
                Py_DECREF(pyObj_role_and_origin);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        }

        PyList_Append(pyObj_origins, pyObj_origin);
        Py_DECREF(pyObj_origin);
    }

    if (-1 == PyDict_SetItemString(pyObj_role_and_origin, "origins", pyObj_origins)) {
        Py_DECREF(pyObj_origins);
        Py_DECREF(pyObj_role_and_origin);
        return nullptr;
    }
    Py_DECREF(pyObj_origins);

    return pyObj_role_and_origin;
}

PyObject*
build_user(couchbase::core::management::rbac::user_and_metadata uam)
{
    PyObject* pyObj_user = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(uam.username.c_str());
    if (-1 == PyDict_SetItemString(pyObj_user, "username", pyObj_tmp)) {
        Py_XDECREF(pyObj_user);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (uam.display_name.has_value()) {
        pyObj_tmp = PyUnicode_FromString(uam.display_name.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_user, "display_name", pyObj_tmp)) {
            Py_DECREF(pyObj_user);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    PyObject* pyObj_groups = PySet_New(nullptr);
    for (auto const& group : uam.groups) {
        pyObj_tmp = PyUnicode_FromString(group.c_str());
        if (-1 == PySet_Add(pyObj_groups, pyObj_tmp)) {
            Py_DECREF(pyObj_user);
            Py_XDECREF(pyObj_groups);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (-1 == PyDict_SetItemString(pyObj_user, "groups", pyObj_groups)) {
        Py_DECREF(pyObj_user);
        Py_XDECREF(pyObj_groups);
        return nullptr;
    }
    Py_DECREF(pyObj_groups);

    PyObject* pyObj_roles = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& role : uam.roles) {
        PyObject* pyObj_role = build_role(role);
        if (pyObj_role == nullptr) {
            Py_XDECREF(pyObj_roles);
            Py_DECREF(pyObj_user);
            return nullptr;
        }
        PyList_Append(pyObj_roles, pyObj_role);
        Py_DECREF(pyObj_role);
    }

    if (-1 == PyDict_SetItemString(pyObj_user, "roles", pyObj_roles)) {
        Py_DECREF(pyObj_user);
        Py_XDECREF(pyObj_roles);
        return nullptr;
    }
    Py_DECREF(pyObj_roles);

    return pyObj_user;
}

PyObject*
build_user_and_metadata(couchbase::core::management::rbac::user_and_metadata uam)
{
    PyObject* pyObj_uam = PyDict_New();

    PyObject* pyObj_user = build_user(uam);
    if (pyObj_user == nullptr) {
        Py_XDECREF(pyObj_uam);
        return nullptr;
    }

    if (-1 == PyDict_SetItemString(pyObj_uam, "user", pyObj_user)) {
        Py_DECREF(pyObj_user);
        Py_XDECREF(pyObj_uam);
        return nullptr;
    }
    Py_DECREF(pyObj_user);

    PyObject* pyObj_tmp = auth_domain_to_str(uam.domain);
    if (-1 == PyDict_SetItemString(pyObj_uam, "domain", pyObj_tmp)) {
        Py_DECREF(pyObj_uam);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_eff_roles = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& role : uam.effective_roles) {
        PyObject* pyObj_role_and_origins = build_role_and_origins(role);
        if (pyObj_role_and_origins == nullptr) {
            Py_XDECREF(pyObj_eff_roles);
            Py_DECREF(pyObj_uam);
            return nullptr;
        }
        PyList_Append(pyObj_eff_roles, pyObj_role_and_origins);
        Py_DECREF(pyObj_role_and_origins);
    }

    if (-1 == PyDict_SetItemString(pyObj_uam, "effective_roles", pyObj_eff_roles)) {
        Py_DECREF(pyObj_uam);
        Py_DECREF(pyObj_eff_roles);
        return nullptr;
    }
    Py_DECREF(pyObj_eff_roles);

    if (uam.password_changed.has_value()) {
        pyObj_tmp = PyUnicode_FromString(uam.password_changed.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_uam, "password_changed", pyObj_tmp)) {
            Py_DECREF(pyObj_uam);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    PyObject* pyObj_ext_groups = PySet_New(nullptr);
    for (auto const& group : uam.external_groups) {
        pyObj_tmp = PyUnicode_FromString(group.c_str());
        if (-1 == PySet_Add(pyObj_ext_groups, pyObj_tmp)) {
            Py_DECREF(pyObj_uam);
            Py_XDECREF(pyObj_ext_groups);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (-1 == PyDict_SetItemString(pyObj_uam, "external_groups", pyObj_ext_groups)) {
        Py_DECREF(pyObj_uam);
        Py_DECREF(pyObj_ext_groups);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_ext_groups);

    return pyObj_uam;
}

PyObject*
build_group(couchbase::core::management::rbac::group group)
{
    PyObject* pyObj_group = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(group.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_group, "name", pyObj_tmp)) {
        Py_XDECREF(pyObj_group);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (group.description.has_value()) {
        pyObj_tmp = PyUnicode_FromString(group.description.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_group, "description", pyObj_tmp)) {
            Py_DECREF(pyObj_group);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    PyObject* pyObj_roles = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& role : group.roles) {
        PyObject* pyObj_role = build_role(role);
        if (pyObj_role == nullptr) {
            Py_XDECREF(pyObj_roles);
            Py_DECREF(pyObj_group);
            return nullptr;
        }
        PyList_Append(pyObj_roles, pyObj_role);
        Py_DECREF(pyObj_role);
    }

    if (-1 == PyDict_SetItemString(pyObj_group, "roles", pyObj_roles)) {
        Py_DECREF(pyObj_group);
        Py_XDECREF(pyObj_roles);
        return nullptr;
    }
    Py_DECREF(pyObj_roles);

    if (group.ldap_group_reference.has_value()) {
        pyObj_tmp = PyUnicode_FromString(group.ldap_group_reference.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_group, "ldap_group_reference", pyObj_tmp)) {
            Py_DECREF(pyObj_group);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return pyObj_group;
}

PyObject*
get_error_messages(std::vector<std::string> messages)
{
    PyObject* pyObj_messages = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& message : messages) {
        PyObject* pyObj_message = PyUnicode_FromString(message.c_str());
        PyList_Append(pyObj_messages, pyObj_message);
        Py_DECREF(pyObj_message);
    }

    return pyObj_messages;
}

template<typename T>
result*
create_result_from_user_mgmt_response([[maybe_unused]] const T& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    return res;
}

template<>
result*
create_result_from_user_mgmt_response<couchbase::core::operations::management::user_get_response>(
  const couchbase::core::operations::management::user_get_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    PyObject* pyObj_uam = build_user_and_metadata(resp.user);
    if (pyObj_uam == nullptr) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_uam);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(res->dict, "user_and_metadata", pyObj_uam)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_uam);
        return nullptr;
    }
    Py_DECREF(pyObj_uam);
    return res;
}

template<>
result*
create_result_from_user_mgmt_response<couchbase::core::operations::management::user_get_all_response>(
  const couchbase::core::operations::management::user_get_all_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_users = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& uam : resp.users) {
        PyObject* pyObj_uam = build_user_and_metadata(uam);
        if (pyObj_uam == nullptr) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_users);
            Py_XDECREF(pyObj_uam);
            return nullptr;
        }
        PyList_Append(pyObj_users, pyObj_uam);
        Py_DECREF(pyObj_uam);
    }

    if (-1 == PyDict_SetItemString(res->dict, "users", pyObj_users)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_users);
        return nullptr;
    }
    Py_DECREF(pyObj_users);

    return res;
}

template<>
result*
create_result_from_user_mgmt_response<couchbase::core::operations::management::role_get_all_response>(
  const couchbase::core::operations::management::role_get_all_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_roles = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& role : resp.roles) {
        PyObject* pyObj_role = build_role(role);
        if (pyObj_role == nullptr) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_roles);
            return nullptr;
        }

        PyObject* pyObj_tmp = PyUnicode_FromString(role.display_name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_role, "display_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            Py_XDECREF(pyObj_role);
            Py_XDECREF(pyObj_roles);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(role.description.c_str());
        if (-1 == PyDict_SetItemString(pyObj_role, "description", pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            Py_DECREF(pyObj_role);
            Py_XDECREF(pyObj_roles);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
        PyList_Append(pyObj_roles, pyObj_role);
        Py_DECREF(pyObj_role);
    }

    if (-1 == PyDict_SetItemString(res->dict, "roles", pyObj_roles)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_roles);
        return nullptr;
    }
    Py_DECREF(pyObj_roles);

    return res;
}

template<>
result*
create_result_from_user_mgmt_response<couchbase::core::operations::management::group_get_response>(
  const couchbase::core::operations::management::group_get_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    PyObject* pyObj_group = build_group(resp.group);
    if (-1 == PyDict_SetItemString(res->dict, "group", pyObj_group)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_group);
        return nullptr;
    }
    Py_DECREF(pyObj_group);
    return res;
}

template<>
result*
create_result_from_user_mgmt_response<couchbase::core::operations::management::group_get_all_response>(
  const couchbase::core::operations::management::group_get_all_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_groups = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& group : resp.groups) {
        PyObject* pyObj_group = build_group(group);
        PyList_Append(pyObj_groups, pyObj_group);
        Py_DECREF(pyObj_group);
    }

    if (-1 == PyDict_SetItemString(res->dict, "groups", pyObj_groups)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_groups);
        return nullptr;
    }
    Py_DECREF(pyObj_groups);

    return res;
}

template<typename Response>
void
create_result_from_user_mgmt_op_response(const Response& resp,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing user mgmt operation.", "UserMgmt");
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
        auto res = create_result_from_user_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "User mgmt operation error.");
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

template<>
void
create_result_from_user_mgmt_op_response<couchbase::core::operations::management::user_upsert_response>(
  const couchbase::core::operations::management::user_upsert_response& resp,
  PyObject* pyObj_callback,
  PyObject* pyObj_errback,
  std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    PyObject* pyObj_err_msgs = nullptr;
    auto set_exception = false;

    PyGILState_STATE state = PyGILState_Ensure();
    if (resp.ctx.ec.value()) {
        // group might have error messages
        pyObj_err_msgs = get_error_messages(resp.errors);
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing user mgmt upsert operation.", "UserMgmt");
        pycbc_add_exception_info(pyObj_exc, "error_msgs", pyObj_err_msgs);
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
        auto res = create_result_from_user_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "User mgmt upsert operation error.");
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

template<>
void
create_result_from_user_mgmt_op_response<couchbase::core::operations::management::group_upsert_response>(
  const couchbase::core::operations::management::group_upsert_response& resp,
  PyObject* pyObj_callback,
  PyObject* pyObj_errback,
  std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    PyObject* pyObj_err_msgs = nullptr;
    auto set_exception = false;

    PyGILState_STATE state = PyGILState_Ensure();
    if (resp.ctx.ec.value()) {
        // group might have error messages
        pyObj_err_msgs = get_error_messages(resp.errors);
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing user mgmt group upsert operation.", "UserMgmt");
        pycbc_add_exception_info(pyObj_exc, "error_msgs", pyObj_err_msgs);
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
        auto res = create_result_from_user_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "User mgmt group upsert operation error.");
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

template<typename Request>
PyObject*
do_user_mgmt_op(connection& conn,
                Request& req,
                PyObject* pyObj_callback,
                PyObject* pyObj_errback,
                std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_user_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_user_mgmt_op(connection* conn, struct user_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    switch (options->op_type) {
        case UserManagementOperations::UPSERT_USER: {
            PyObject* pyObj_domain = PyDict_GetItemString(options->op_args, "domain");
            auto domain = str_to_auth_domain(std::string(PyUnicode_AsUTF8(pyObj_domain)));
            PyObject* pyObj_user = PyDict_GetItemString(options->op_args, "user");
            auto user = get_user(pyObj_user);

            couchbase::core::operations::management::user_upsert_request req{};
            req.domain = domain;
            req.user = user;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::user_upsert_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::GET_USER: {
            PyObject* pyObj_domain = PyDict_GetItemString(options->op_args, "domain");
            auto domain = str_to_auth_domain(std::string(PyUnicode_AsUTF8(pyObj_domain)));
            PyObject* pyObj_username = PyDict_GetItemString(options->op_args, "username");
            auto username = std::string(PyUnicode_AsUTF8(pyObj_username));

            couchbase::core::operations::management::user_get_request req{};
            req.domain = domain;
            req.username = username;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::user_get_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::GET_ALL_USERS: {
            PyObject* pyObj_domain = PyDict_GetItemString(options->op_args, "domain");
            auto domain = str_to_auth_domain(std::string(PyUnicode_AsUTF8(pyObj_domain)));

            couchbase::core::operations::management::user_get_all_request req{};
            req.domain = domain;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::user_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::DROP_USER: {
            PyObject* pyObj_domain = PyDict_GetItemString(options->op_args, "domain");
            auto domain = str_to_auth_domain(std::string(PyUnicode_AsUTF8(pyObj_domain)));
            PyObject* pyObj_username = PyDict_GetItemString(options->op_args, "username");
            auto username = std::string(PyUnicode_AsUTF8(pyObj_username));

            couchbase::core::operations::management::user_drop_request req{};
            req.domain = domain;
            req.username = username;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::user_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::CHANGE_PASSWORD: {
            PyObject* pyObj_newPassword = PyDict_GetItemString(options->op_args, "password");
            auto newPassword = std::string(PyUnicode_AsUTF8(pyObj_newPassword));

            couchbase::core::operations::management::change_password_request req{};
            req.newPassword = newPassword;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::change_password_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::GET_ROLES: {
            couchbase::core::operations::management::role_get_all_request req{};
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::role_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::UPSERT_GROUP: {
            PyObject* pyObj_group = PyDict_GetItemString(options->op_args, "group");
            auto group = get_group(pyObj_group);

            couchbase::core::operations::management::group_upsert_request req{};
            req.group = group;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::group_upsert_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::GET_GROUP: {
            PyObject* pyObj_name = PyDict_GetItemString(options->op_args, "name");
            auto name = std::string(PyUnicode_AsUTF8(pyObj_name));

            couchbase::core::operations::management::group_get_request req{};
            req.name = name;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::group_get_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::GET_ALL_GROUPS: {
            couchbase::core::operations::management::group_get_all_request req{};
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::group_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case UserManagementOperations::DROP_GROUP: {
            PyObject* pyObj_name = PyDict_GetItemString(options->op_args, "name");
            auto name = std::string(PyUnicode_AsUTF8(pyObj_name));

            couchbase::core::operations::management::group_drop_request req{};
            req.name = name;
            req.timeout = options->timeout_ms;

            res = do_user_mgmt_op<couchbase::core::operations::management::group_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized user mgmt operation passed in.");
            barrier->set_value(nullptr);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            break;
        }
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    return res;
}

void
add_user_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(UserManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("UserManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "user_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
