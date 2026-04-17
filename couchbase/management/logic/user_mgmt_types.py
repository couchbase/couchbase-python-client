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

from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import datetime
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Set)

from couchbase.auth import AuthDomain
from couchbase.exceptions import (FeatureUnavailableException,
                                  GroupNotFoundException,
                                  InvalidArgumentException,
                                  RateLimitedException,
                                  UserNotFoundException)
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import UserMgmtOperationType
from couchbase.management.logic.mgmt_req import MgmtRequest


class Role:

    def __init__(self,
                 name: Optional[str] = None,
                 bucket: Optional[str] = None,
                 scope: Optional[str] = None,
                 collection: Optional[str] = None
                 ) -> None:

        if not name:
            raise InvalidArgumentException('A role must have a name')

        self._name = name
        self._bucket = bucket
        self._scope = scope
        self._collection = collection

    @property
    def name(self) -> str:
        return self._name

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def scope(self) -> str:
        return self._scope

    @property
    def collection(self) -> str:
        return self._collection

    def as_dict(self) -> Dict[str, str]:
        return {
            'name': self._name,
            'bucket': self._bucket,
            'scope': self._scope,
            'collection': self._collection
        }

    def __eq__(self, other: Role) -> bool:
        if not isinstance(other, Role):
            return False
        return (self.name == other.name
                and self.bucket == other.bucket
                and self.scope == other.scope
                and self.collection == other.collection)

    def __hash__(self) -> int:
        return hash((self.name, self.bucket, self.scope, self.collection))

    @classmethod
    def create_role(cls, raw_data: Dict[str, str]) -> Role:
        return cls(
            name=raw_data.get("name", None),
            bucket=raw_data.get("bucket", None),
            scope=raw_data.get("scope", None),
            collection=raw_data.get("collection", None)
        )


class RoleAndDescription:

    def __init__(self,
                 role: Optional[Role] = None,
                 display_name: Optional[str] = None,
                 description: Optional[str] = None,
                 ce: Optional[bool] = None,
                 ) -> None:

        self._role = role
        self._display_name = display_name
        self._description = description
        self._ce = ce

    @property
    def role(self) -> Optional[Role]:
        return self._role

    @property
    def display_name(self) -> Optional[str]:
        return self._display_name

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def ce(self) -> Optional[bool]:
        return self._ce

    @classmethod
    def create_role_and_description(cls, raw_data: Dict[str, str]) -> RoleAndDescription:
        return cls(
            role=Role.create_role(raw_data),
            display_name=raw_data.get('display_name', None),
            description=raw_data.get('description', None),
            ce=raw_data.get('ce', None)
        )


class Origin:
    """
        Indicates why the user has a specific role.
        If the type is "user" it means the role is assigned
        directly to the user. If the type is "group" it means
        the role is inherited from the group identified by
        the "name" field.
    """

    def __init__(self, type: Optional[str] = None, name: Optional[str] = None) -> None:
        self._type = type
        self._name = name

    @property
    def type(self) -> Optional[str]:
        return self._type

    @property
    def name(self) -> Optional[str]:
        return self._name


class RoleAndOrigins:

    def __init__(self, role: Optional[Role] = None, origins: Optional[List[Origin]] = None) -> None:
        self._role = role
        self._origins = origins

    @property
    def role(self) -> Optional[Role]:
        return self._role

    @property
    def origins(self) -> Optional[List[Origin]]:
        return self._origins

    @classmethod
    def create_role_and_origins(cls, raw_data: Dict[str, str]) -> RoleAndOrigins:

        # RBAC prior to v6.5 does not have origins
        origin_data = raw_data.get("origins", None)

        return cls(
            role=Role.create_role(raw_data),
            origins=list(map(lambda o: Origin(**o), origin_data))
            if origin_data else []
        )


class UserManagementUtils:
    """
    ** INTERNAL **
    """

    @staticmethod
    def to_set(value: Any, valid_type: Any, display_name: str) -> Set[Any]:

        if not value:
            return value
        elif isinstance(value, set):
            UserManagementUtils.validate_all_set_types(value, valid_type, display_name)
            return value
        elif isinstance(value, list):
            UserManagementUtils.validate_all_set_types(value, valid_type, display_name)
            return set(value)
        elif isinstance(value, tuple):
            UserManagementUtils.validate_all_set_types(value, valid_type, display_name)
            return set(value)
        elif isinstance(value, valid_type):
            return set([value])
        else:
            raise InvalidArgumentException(f'{display_name} must be of type {valid_type.__name__}.')

    @staticmethod
    def validate_all_set_types(value: Any, valid_type: Any, display_name: str) -> None:
        if all(map(lambda r: isinstance(r, type), value)):
            raise InvalidArgumentException(f'{display_name} must contain only objects of type {valid_type.__name__}.')


class User:

    def __init__(self,
                 username: Optional[str] = None,
                 display_name: Optional[str] = None,
                 groups: Optional[Set[str]] = None,
                 roles: Optional[Set[Role]] = None,
                 password: Optional[str] = None,
                 ) -> None:

        if not username:
            raise InvalidArgumentException('A user must have a username')

        self._username = username
        self._display_name = display_name
        self._groups = UserManagementUtils.to_set(groups, str, 'Groups')
        self._roles = UserManagementUtils.to_set(roles, Role, 'Roles')
        self._password = password

    @property
    def username(self) -> str:
        return self._username

    @property
    def display_name(self) -> Optional[str]:
        return self._display_name

    @display_name.setter
    def display_name(self, value: str) -> None:
        self._display_name = value

    @property
    def groups(self) -> Set[str]:
        """names of the groups"""
        return self._groups

    @groups.setter
    def groups(self, value: Set[str]) -> None:
        self._groups = UserManagementUtils.to_set(value, str, 'Groups')

    @property
    def roles(self) -> Set[Role]:
        """only roles assigned directly to the user (not inherited from groups)"""
        return self._roles

    @roles.setter
    def roles(self, value: Set[Role]) -> None:
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    def password(self, value: str) -> None:
        self._password = value

    password = property(None, password)

    def as_dict(self) -> Dict[str, Any]:
        output = {
            "username": self.username,
            "display_name": self.display_name,
            "password": self._password
        }

        if self.roles:
            output["roles"] = list(self.roles)

        if self.groups:
            output["groups"] = set(self.groups)

        return output

    @classmethod
    def create_user(cls, raw_data: Dict[str, Any], roles: Optional[Dict[str, Any]] = None) -> User:

        user_roles = roles
        if not user_roles:
            set(map(lambda r: Role.create_role(r),
                    raw_data.get("roles")))

        # RBAC prior to v6.5 does not have groups
        group_data = raw_data.get("groups", None)

        return cls(
            username=raw_data.get("username"),
            display_name=raw_data.get("display_name"),
            roles=user_roles,
            groups=set(group_data) if group_data else None
        )


class UserAndMetadata:
    """
        Models the "get user" / "get all users" response.

        Associates the mutable properties of a user with
        derived properties such as the effective roles
        inherited from groups.
    """

    def __init__(self,
                 domain: Optional[AuthDomain] = None,
                 user: Optional[User] = None,
                 effective_roles: Optional[List[RoleAndOrigins]] = None,
                 password_changed: Optional[datetime] = None,
                 external_groups: Optional[Set[str]] = None,
                 **kwargs: Any
                 ) -> None:

        self._domain = domain
        self._user = user
        self._effective_roles = effective_roles
        self._password_changed = password_changed
        self._external_groups = external_groups
        self._raw_data = kwargs.get("raw_data", None)

    @property
    def domain(self) -> Optional[AuthDomain]:
        """ AuthDomain is an enumeration with values "local" and "external".
        It MAY alternatively be represented as String."""
        return self._domain

    @property
    def user(self) -> Optional[User]:
        """returns a new mutable User object each time this method is called.
            Modifying the fields of the returned User MUST have no effect on the
            UserAndMetadata object it came from."""
        return deepcopy(self._user)

    @property
    def effective_roles(self) -> Optional[List[RoleAndOrigins]]:
        """all roles, regardless of origin."""
        return self._effective_roles

    @property
    def password_changed(self) -> Optional[datetime]:
        return self._password_changed

    @property
    def external_groups(self) -> Optional[Set[str]]:
        return self._external_groups

    @property
    def raw_data(self) -> Dict[str, Any]:
        return self._raw_data

    @classmethod
    def create_user_and_metadata(cls, raw_data: Dict[str, Any]) -> UserAndMetadata:

        effective_roles = list(map(lambda r: RoleAndOrigins.create_role_and_origins(r),
                                   raw_data.get("effective_roles")))

        user_roles = set(r.role for r in effective_roles
                         if any(map(lambda o: o.type == "user", r.origins)) or len(r.origins) == 0)

        # RBAC prior to v6.5 does not have groups
        ext_group_data = raw_data.get("external_groups", None)

        # password_change_date is optional
        pw_data = raw_data.get("password_changed", None)
        pw_changed = None
        formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
        for f in formats:
            if pw_changed:
                break

            try:
                pw_changed = datetime.strptime(pw_data, f)
            except Exception:  # nosec
                pass

        return cls(
            domain=AuthDomain.from_str(raw_data.get("domain")),
            effective_roles=effective_roles,
            user=User.create_user(raw_data, roles=user_roles),
            password_changed=pw_changed,
            external_groups=set(ext_group_data) if ext_group_data else None,
            raw_data=raw_data
        )


class Group:
    def __init__(self,
                 name: Optional[str] = None,
                 description: Optional[str] = None,
                 roles: Optional[set[Role]] = None,
                 ldap_group_reference: Optional[str] = None,
                 **kwargs: Any
                 ) -> None:

        if not name:
            raise InvalidArgumentException('A group must have a name')

        self._name = name
        self._description = description
        self._roles = UserManagementUtils.to_set(roles, Role, 'Roles')
        self._ldap_group_reference = ldap_group_reference
        self._raw_data = kwargs.get('raw_data', None)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        self._description = value

    @property
    def roles(self) -> Optional[Set[Role]]:
        return self._roles

    @roles.setter
    def roles(self, value: Set[Role]) -> None:
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    @property
    def ldap_group_reference(self) -> Optional[str]:
        return self._ldap_group_reference

    @ldap_group_reference.setter
    def ldap_group_reference(self, value: str) -> None:
        self._ldap_group_reference = value

    @property
    def raw_data(self) -> Dict[str, Any]:
        return self._raw_data

    def as_dict(self) -> Dict[str, Any]:
        rs = list(map(lambda r: r.as_dict(), self.roles))
        for r in self.roles:
            r.as_dict()
        return {
            'name': self.name,
            'description': self.description,
            'roles': rs,
            'ldap_group_reference': self.ldap_group_reference
        }

    @classmethod
    def create_group(cls, raw_data: Dict[str, Any]) -> Group:
        return cls(
            raw_data.get('name'),
            description=raw_data.get('description', None),
            roles=set(map(lambda r: Role.create_role(
                r), raw_data.get('roles'))),
            ldap_group_referenc=raw_data.get('ldap_group_ref', None)
        )


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: Dict[type, list] = {}


@dataclass
class UserMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: Optional[ObservableRequestHandler] = None,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        cls = type(self)
        cached_fields = _FIELDS_CACHE.get(cls)
        if cached_fields is None:
            cached_fields = [f for f in fields(cls) if f.name not in _OPARG_SKIP_SET]
            _FIELDS_CACHE[cls] = cached_fields

        mgmt_kwargs = {
            f.name: getattr(self, f.name)
            for f in cached_fields
            if getattr(self, f.name) is not None
        }

        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if obs_handler.is_legacy_tracer:
                legacy_request_span = obs_handler.legacy_request_span
                if legacy_request_span:
                    mgmt_kwargs['parent_span'] = legacy_request_span
            else:
                mgmt_kwargs['wrapper_span_name'] = obs_handler.wrapper_span_name

        return mgmt_kwargs


@dataclass
class ChangePasswordRequest(UserMgmtRequest):
    password: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.ChangePassword.value

    def req_to_dict(self,
                    obs_handler: Optional[ObservableRequestHandler] = None,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = super().req_to_dict(obs_handler=obs_handler, callback=callback, errback=errback)
        pw = op_kwargs.pop('password')
        op_kwargs['newPassword'] = pw
        return op_kwargs


@dataclass
class DropGroupRequest(UserMgmtRequest):
    name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.GroupDrop.value


@dataclass
class DropUserRequest(UserMgmtRequest):
    username: str
    domain: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.UserDrop.value


@dataclass
class GetAllGroupsRequest(UserMgmtRequest):
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.GroupGetAll.value


@dataclass
class GetAllUsersRequest(UserMgmtRequest):
    domain: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.UserGetAll.value


@dataclass
class GetGroupRequest(UserMgmtRequest):
    name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.GroupGet.value


@dataclass
class GetRolesRequest(UserMgmtRequest):
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.RoleGetAll.value


@dataclass
class GetUserRequest(UserMgmtRequest):
    username: str
    domain: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.UserGet.value


@dataclass
class UpsertGroupRequest(UserMgmtRequest):
    group: Dict[str, Any]
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.GroupUpsert.value


@dataclass
class UpsertUserRequest(UserMgmtRequest):
    user: Dict[str, Any]
    domain: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return UserMgmtOperationType.UserUpsert.value


USER_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'Unknown group.*': GroupNotFoundException,
    r'Unknown user.*': UserNotFoundException,
    r'Not found.*': FeatureUnavailableException,
    r'Method Not Allowed.*': FeatureUnavailableException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
