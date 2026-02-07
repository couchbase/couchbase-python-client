#  Copyright 2016-2023. Couchbase, Inc.
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

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Union)

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.user_mgmt_types import (USER_MGMT_ERROR_MAP,
                                                        AuthDomain,
                                                        ChangePasswordRequest,
                                                        DropGroupRequest,
                                                        DropUserRequest,
                                                        GetAllGroupsRequest,
                                                        GetAllUsersRequest,
                                                        GetGroupRequest,
                                                        GetRolesRequest,
                                                        GetUserRequest,
                                                        UpsertGroupRequest,
                                                        UpsertUserRequest)
from couchbase.options import forward_args
from couchbase.pycbc_core import mgmt_operations, user_mgmt_operations

if TYPE_CHECKING:
    from couchbase.management.logic.user_mgmt_types import Group, User


class UserMgmtRequestBuilder:
    VALID_AUTH_DOMAINS = ['local', 'external']
    VALID_GROUP_KEYS = ['name', 'description', 'ldap_group_reference']
    VALID_USER_KEYS = ['password', 'name', 'username', 'groups']

    def __init__(self) -> None:
        self._error_map = USER_MGMT_ERROR_MAP

    def _get_valid_domain(self, auth_domain: Union[AuthDomain, str]) -> str:
        if isinstance(auth_domain, str) and auth_domain in UserMgmtRequestBuilder.VALID_AUTH_DOMAINS:
            return auth_domain
        elif isinstance(auth_domain, AuthDomain):
            return AuthDomain.to_str(auth_domain)
        else:
            raise InvalidArgumentException(message='Unknown Authentication Domain')

    def _get_valid_user(self, user: User, domain: str) -> Dict[str, Any]:
        if not user.groups and (not user.roles or not isinstance(user.roles, set)):
            raise InvalidArgumentException('Roles must be a non-empty list')

        user_dict = {k: v for k, v in user.as_dict().items() if k in UserMgmtRequestBuilder.VALID_USER_KEYS}

        if user_dict['password'] and domain == 'external':
            raise InvalidArgumentException('External domains must not have passwords.')

        if user.roles:
            user_dict['roles'] = list(map(lambda r: r.as_dict(), user.roles))

        return user_dict

    def build_change_password_request(self,
                                      password: str,
                                      *options: object,
                                      **kwargs: object) -> ChangePasswordRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = ChangePasswordRequest(self._error_map,
                                    mgmt_operations.USER.value,
                                    user_mgmt_operations.CHANGE_PASSWORD.value,
                                    password=password,
                                    **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_group_request(self, group_name: str, *options: object, **kwargs: object) -> DropGroupRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = DropGroupRequest(self._error_map,
                               mgmt_operations.USER.value,
                               user_mgmt_operations.DROP_GROUP.value,
                               name=group_name,
                               **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_user_request(self, username: str, *options: object, **kwargs: object) -> DropUserRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        domain = self._get_valid_domain(final_args.pop('domain_name', 'local'))
        req = DropUserRequest(self._error_map,
                              mgmt_operations.USER.value,
                              user_mgmt_operations.DROP_USER.value,
                              username=username,
                              domain=domain,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_groups_request(self, *options: object, **kwargs: object) -> GetAllGroupsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetAllGroupsRequest(self._error_map,
                                  mgmt_operations.USER.value,
                                  user_mgmt_operations.GET_ALL_GROUPS.value,
                                  **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_users_request(self, *options: object, **kwargs: object) -> GetAllUsersRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        domain = self._get_valid_domain(final_args.pop('domain_name', 'local'))
        req = GetAllUsersRequest(self._error_map,
                                 mgmt_operations.USER.value,
                                 user_mgmt_operations.GET_ALL_USERS.value,
                                 domain=domain,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_group_request(self, group_name: str, *options: object, **kwargs: object) -> GetGroupRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetGroupRequest(self._error_map,
                              mgmt_operations.USER.value,
                              user_mgmt_operations.GET_GROUP.value,
                              name=group_name,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_roles_request(self, *options: object, **kwargs: object) -> GetRolesRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetRolesRequest(self._error_map,
                              mgmt_operations.USER.value,
                              user_mgmt_operations.GET_ROLES.value,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_user_request(self, username: str, *options: object, **kwargs: object) -> GetUserRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        domain = self._get_valid_domain(final_args.pop('domain_name', 'local'))
        req = GetUserRequest(self._error_map,
                             mgmt_operations.USER.value,
                             user_mgmt_operations.GET_USER.value,
                             username=username,
                             domain=domain,
                             **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_upsert_group_request(self, group: Group, *options: object, **kwargs: object) -> UpsertGroupRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        group_dict = {k: v for k, v in group.as_dict().items() if k in UserMgmtRequestBuilder.VALID_GROUP_KEYS}

        if group.roles:
            group_dict["roles"] = list(map(lambda r: r.as_dict(), group.roles))
        req = UpsertGroupRequest(self._error_map,
                                 mgmt_operations.USER.value,
                                 user_mgmt_operations.UPSERT_GROUP.value,
                                 group=group_dict,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_upsert_user_request(self, user: User, *options: object, **kwargs: object) -> UpsertUserRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        domain = self._get_valid_domain(final_args.pop('domain_name', 'local'))
        user = self._get_valid_user(user, domain)
        req = UpsertUserRequest(self._error_map,
                                mgmt_operations.USER.value,
                                user_mgmt_operations.UPSERT_USER.value,
                                user=user,
                                domain=domain,
                                **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req
