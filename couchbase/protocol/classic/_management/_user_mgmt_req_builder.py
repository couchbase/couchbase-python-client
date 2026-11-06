#  Copyright 2016-2026. Couchbase, Inc.
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

from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common.auth import AuthDomain
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.options import (ChangePasswordOptions,
                                                 DropGroupOptions,
                                                 DropUserOptions,
                                                 GetAllGroupsOptions,
                                                 GetAllUsersOptions,
                                                 GetGroupOptions,
                                                 GetRolesOptions,
                                                 GetUserOptions,
                                                 UpsertGroupOptions,
                                                 UpsertUserOptions)
from couchbase.common.management.users import Group, User
from couchbase.common.option_enums import OptionValidationMode
from couchbase.protocol.classic._management._user_mgmt_types import (USER_MGMT_ERROR_MAP,
                                                                     ChangePasswordRequest,
                                                                     DropGroupRequest,
                                                                     DropUserRequest,
                                                                     GetAllGroupsRequest,
                                                                     GetAllUsersRequest,
                                                                     GetGroupRequest,
                                                                     GetRolesRequest,
                                                                     GetUserRequest,
                                                                     UpsertGroupRequest,
                                                                     UpsertUserRequest,
                                                                     UserMgmtOptionsBlock)
from couchbase.protocol.classic._wire import forward_args


class UserMgmtRequestBuilder:
    VALID_AUTH_DOMAINS = ['local', 'external']
    VALID_GROUP_KEYS = ['name', 'description', 'ldap_group_reference']
    VALID_USER_KEYS = ['password', 'display_name', 'username', 'groups']

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._error_map = USER_MGMT_ERROR_MAP
        self._option_validation = option_validation

    def _get_valid_domain(self, auth_domain: AuthDomain | str) -> str:
        if isinstance(auth_domain, str) and auth_domain in UserMgmtRequestBuilder.VALID_AUTH_DOMAINS:
            return auth_domain
        elif isinstance(auth_domain, AuthDomain):
            return AuthDomain.to_str(auth_domain)
        else:
            raise InvalidArgumentException(message='Unknown Authentication Domain')

    def _get_valid_user(self, user: User, domain: str) -> dict[str, Any]:
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
                                      obs_handler: ObservableRequestHandler,
                                      options: ChangePasswordOptions | None = None,
                                      **kwargs: object) -> ChangePasswordRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.ChangePassword,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return ChangePasswordRequest(self._error_map, password=password, timeout=final_args.pop('timeout', None))

    def build_drop_group_request(self,
                                 group_name: str,
                                 obs_handler: ObservableRequestHandler,
                                 options: DropGroupOptions | None = None,
                                 **kwargs: object) -> DropGroupRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.DropGroup, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return DropGroupRequest(self._error_map, name=group_name, timeout=final_args.pop('timeout', None))

    def build_drop_user_request(self,
                                username: str,
                                obs_handler: ObservableRequestHandler,
                                options: DropUserOptions | None = None,
                                **kwargs: object) -> DropUserRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.DropUser, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return DropUserRequest(self._error_map,
                               username=username,
                               domain=self._get_valid_domain(final_args.pop('domain_name', 'local')),
                               timeout=final_args.pop('timeout', None))

    def build_get_all_groups_request(self,
                                     obs_handler: ObservableRequestHandler,
                                     options: GetAllGroupsOptions | None = None,
                                     **kwargs: object) -> GetAllGroupsRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.GetAllGroups,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetAllGroupsRequest(self._error_map, timeout=final_args.pop('timeout', None))

    def build_get_all_users_request(self,
                                    obs_handler: ObservableRequestHandler,
                                    options: GetAllUsersOptions | None = None,
                                    **kwargs: object) -> GetAllUsersRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.GetAllUsers,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetAllUsersRequest(self._error_map,
                                  domain=self._get_valid_domain(final_args.pop('domain_name', 'local')),
                                  timeout=final_args.pop('timeout', None))

    def build_get_group_request(self,
                                group_name: str,
                                obs_handler: ObservableRequestHandler,
                                options: GetGroupOptions | None = None,
                                **kwargs: object) -> GetGroupRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.GetGroup, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetGroupRequest(self._error_map, name=group_name, timeout=final_args.pop('timeout', None))

    def build_get_roles_request(self,
                                obs_handler: ObservableRequestHandler,
                                options: GetRolesOptions | None = None,
                                **kwargs: object) -> GetRolesRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.GetRoles, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetRolesRequest(self._error_map, timeout=final_args.pop('timeout', None))

    def build_get_user_request(self,
                               username: str,
                               obs_handler: ObservableRequestHandler,
                               options: GetUserOptions | None = None,
                               **kwargs: object) -> GetUserRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.GetUser, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetUserRequest(self._error_map,
                              username=username,
                              domain=self._get_valid_domain(final_args.pop('domain_name', 'local')),
                              timeout=final_args.pop('timeout', None))

    def build_upsert_group_request(self,
                                   group: Group,
                                   obs_handler: ObservableRequestHandler,
                                   options: UpsertGroupOptions | None = None,
                                   **kwargs: object) -> UpsertGroupRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.UpsertGroup,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        group_dict = {k: v for k, v in group.as_dict().items() if k in UserMgmtRequestBuilder.VALID_GROUP_KEYS}
        if group.roles:
            group_dict['roles'] = [r.as_dict() for r in group.roles]
        return UpsertGroupRequest(self._error_map, group=group_dict, timeout=final_args.pop('timeout', None))

    def build_upsert_user_request(self,
                                  user: User,
                                  obs_handler: ObservableRequestHandler,
                                  options: UpsertUserOptions | None = None,
                                  **kwargs: object) -> UpsertUserRequest:
        final_args = forward_args(kwargs, options, block=UserMgmtOptionsBlock.UpsertUser, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        domain = self._get_valid_domain(final_args.pop('domain_name', 'local'))
        return UpsertUserRequest(self._error_map,
                                 user=self._get_valid_user(user, domain),
                                 domain=domain,
                                 timeout=final_args.pop('timeout', None))
