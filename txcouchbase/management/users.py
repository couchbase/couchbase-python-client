#  Copyright 2016-2022. Couchbase, Inc.
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
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import UserMgmtOperationType
from couchbase.management.logic.user_mgmt_types import (Group,
                                                        RoleAndDescription,
                                                        User,
                                                        UserAndMetadata)
from txcouchbase.management.logic.user_mgmt_impl import TxUserMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (ChangePasswordOptions,
                                              DropGroupOptions,
                                              DropUserOptions,
                                              GetAllGroupsOptions,
                                              GetAllUsersOptions,
                                              GetGroupOptions,
                                              GetRolesOptions,
                                              GetUserOptions,
                                              UpsertGroupOptions,
                                              UpsertUserOptions)


class UserManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = TxUserMgmtImpl(client_adapter, observability_instruments)

    def get_user(self,
                 username,  # type: str
                 *options,  # type: GetUserOptions
                 **kwargs   # type: Any
                 ) -> Deferred[UserAndMetadata]:
        op_type = UserMgmtOperationType.UserGet
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_user_request(username, obs_handler, *options, **kwargs)
            d = self._impl.get_user_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_users(self,
                      *options,  # type: GetAllUsersOptions
                      **kwargs  # type: Any
                      ) -> Deferred[Iterable[UserAndMetadata]]:
        op_type = UserMgmtOperationType.UserGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_users_request(obs_handler, *options, **kwargs)
            d = self._impl.get_all_users_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def upsert_user(self,
                    user,     # type: User
                    *options,  # type: UpsertUserOptions
                    **kwargs  # type: Any
                    ) -> Deferred[None]:
        op_type = UserMgmtOperationType.UserUpsert
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_upsert_user_request(user, obs_handler, *options, **kwargs)
            d = self._impl.upsert_user_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_user(self,
                  username,  # type: str
                  *options,  # type: DropUserOptions
                  **kwargs   # type: Any
                  ) -> Deferred[None]:
        op_type = UserMgmtOperationType.UserDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_user_request(username, obs_handler, *options, **kwargs)
            d = self._impl.drop_user_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def change_password(self,
                        new_password,  # type: str
                        *options,     # type: ChangePasswordOptions
                        **kwargs      # type: Any
                        ) -> None:
        op_type = UserMgmtOperationType.ChangePassword
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_change_password_request(
                new_password, obs_handler, *options, **kwargs)
            d = self._impl.change_password_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_roles(self,
                  *options,  # type: GetRolesOptions
                  **kwargs   # type: Any
                  ) -> Deferred[Iterable[RoleAndDescription]]:
        op_type = UserMgmtOperationType.RoleGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_roles_request(obs_handler, *options, **kwargs)
            d = self._impl.get_roles_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_group(self,
                  group_name,   # type: str
                  *options,     # type: GetGroupOptions
                  **kwargs      # type: Any
                  ) -> Deferred[Group]:
        op_type = UserMgmtOperationType.GroupGet
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_group_request(group_name, obs_handler, *options, **kwargs)
            d = self._impl.get_group_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_groups(self,
                       *options,    # type: GetAllGroupsOptions
                       **kwargs     # type: Any
                       ) -> Deferred[Iterable[Group]]:
        op_type = UserMgmtOperationType.GroupGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_groups_request(obs_handler, *options, **kwargs)
            d = self._impl.get_all_groups_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def upsert_group(self,
                     group,     # type: Group
                     *options,  # type: UpsertGroupOptions
                     **kwargs   # type: Any
                     ) -> Deferred[None]:
        op_type = UserMgmtOperationType.GroupUpsert
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_upsert_group_request(group, obs_handler, *options, **kwargs)
            d = self._impl.upsert_group_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_group(self,
                   group_name,  # type: str
                   *options,    # type: DropGroupOptions
                   **kwargs     # type: Any
                   ) -> Deferred[None]:
        op_type = UserMgmtOperationType.GroupDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_group_request(group_name, obs_handler, *options, **kwargs)
            d = self._impl.drop_group_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
