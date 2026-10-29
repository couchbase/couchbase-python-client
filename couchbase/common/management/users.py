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

__all__ = [
    'Group',
    'Origin',
    'Role',
    'RoleAndDescription',
    'RoleAndOrigins',
    'SetInputType',
    'User',
    'UserAndMetadata',
]

from copy import deepcopy
from datetime import datetime
from typing import (Any,
                    TypeAlias,
                    TypeVar)

from couchbase.common.auth import AuthDomain
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management._users import UserManagementUtils

_T = TypeVar('_T')

# What UserManagementUtils.to_set accepts, which is wider than the set it returns: it
# normalises a set, a list, a tuple, or a single bare element of the valid type.  Every
# parameter feeding to_set is annotated with this, so the signature states what the helper
# takes rather than only the one shape of the four it passes through unchanged.
SetInputType: TypeAlias = set[_T] | list[_T] | tuple[_T, ...] | _T


class Role:

    def __init__(self,
                 name: str | None = None,
                 bucket: str | None = None,
                 scope: str | None = None,
                 collection: str | None = None
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
    def bucket(self) -> str | None:
        return self._bucket

    @property
    def scope(self) -> str | None:
        return self._scope

    @property
    def collection(self) -> str | None:
        return self._collection

    def as_dict(self) -> dict[str, str | None]:
        return {
            'name': self._name,
            'bucket': self._bucket,
            'scope': self._scope,
            'collection': self._collection
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Role):
            return False
        return (self.name == other.name
                and self.bucket == other.bucket
                and self.scope == other.scope
                and self.collection == other.collection)

    def __hash__(self) -> int:
        return hash((self.name, self.bucket, self.scope, self.collection))

    @classmethod
    def create_role(cls, raw_data: dict[str, str]) -> Role:
        return cls(
            name=raw_data.get("name", None),
            bucket=raw_data.get("bucket", None),
            scope=raw_data.get("scope", None),
            collection=raw_data.get("collection", None)
        )


class RoleAndDescription:

    def __init__(self,
                 role: Role | None = None,
                 display_name: str | None = None,
                 description: str | None = None,
                 ce: bool | None = None,
                 ) -> None:

        self._role = role
        self._display_name = display_name
        self._description = description
        self._ce = ce

    @property
    def role(self) -> Role | None:
        return self._role

    @property
    def display_name(self) -> str | None:
        return self._display_name

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def ce(self) -> bool | None:
        return self._ce

    @classmethod
    def create_role_and_description(cls, raw_data: dict[str, Any]) -> RoleAndDescription:
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

    def __init__(self, type: str | None = None, name: str | None = None) -> None:
        self._type = type
        self._name = name

    @property
    def type(self) -> str | None:
        return self._type

    @property
    def name(self) -> str | None:
        return self._name


class RoleAndOrigins:

    def __init__(self, role: Role | None = None, origins: list[Origin] | None = None) -> None:
        self._role = role
        self._origins = origins

    @property
    def role(self) -> Role | None:
        return self._role

    @property
    def origins(self) -> list[Origin] | None:
        return self._origins

    @classmethod
    def create_role_and_origins(cls, raw_data: dict[str, Any]) -> RoleAndOrigins:

        # RBAC prior to v6.5 does not have origins
        origin_data = raw_data.get("origins", None)

        return cls(
            role=Role.create_role(raw_data),
            origins=list(map(lambda o: Origin(**o), origin_data))
            if origin_data else []
        )


class User:

    def __init__(self,
                 username: str | None = None,
                 display_name: str | None = None,
                 groups: SetInputType[str] | None = None,
                 roles: SetInputType[Role] | None = None,
                 password: str | None = None,
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
    def display_name(self) -> str | None:
        return self._display_name

    @display_name.setter
    def display_name(self, value: str) -> None:
        self._display_name = value

    @property
    def groups(self) -> set[str]:
        """names of the groups"""
        return self._groups

    @groups.setter
    def groups(self, value: SetInputType[str]) -> None:
        self._groups = UserManagementUtils.to_set(value, str, 'Groups')

    @property
    def roles(self) -> set[Role]:
        """only roles assigned directly to the user (not inherited from groups)"""
        return self._roles

    @roles.setter
    def roles(self, value: SetInputType[Role]) -> None:
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    @property
    def password(self) -> str:
        # Write-only.  The previous spelling was ``password = property(None, password)``, whose
        # absent getter made every read raise AttributeError; raising here keeps that, in a
        # shape a checker reads as a property rather than as a method being overwritten.
        raise AttributeError('The password property is write-only.')

    @password.setter
    def password(self, value: str) -> None:
        self._password = value

    def as_dict(self) -> dict[str, Any]:
        # Annotated because the literal below infers dict[str, str | None], which the roles
        # list and the groups set that follow do not fit.
        output: dict[str, Any] = {
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
    def create_user(cls, raw_data: dict[str, Any], roles: SetInputType[Role] | None = None) -> User:

        user_roles = roles
        if not user_roles:
            user_roles = set(map(lambda r: Role.create_role(r),
                                 raw_data.get("roles", [])))

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
                 domain: AuthDomain | None = None,
                 user: User | None = None,
                 effective_roles: list[RoleAndOrigins] | None = None,
                 password_changed: datetime | None = None,
                 external_groups: set[str] | None = None,
                 **kwargs: Any
                 ) -> None:

        self._domain = domain
        self._user = user
        self._effective_roles = effective_roles
        self._password_changed = password_changed
        self._external_groups = external_groups
        self._raw_data = kwargs.get("raw_data", None)

    @property
    def domain(self) -> AuthDomain | None:
        """ AuthDomain is an enumeration with values "local" and "external".
        It MAY alternatively be represented as String."""
        return self._domain

    @property
    def user(self) -> User | None:
        """returns a new mutable User object each time this method is called.
            Modifying the fields of the returned User MUST have no effect on the
            UserAndMetadata object it came from."""
        return deepcopy(self._user)

    @property
    def effective_roles(self) -> list[RoleAndOrigins] | None:
        """all roles, regardless of origin."""
        return self._effective_roles

    @property
    def password_changed(self) -> datetime | None:
        return self._password_changed

    @property
    def external_groups(self) -> set[str] | None:
        return self._external_groups

    @property
    def raw_data(self) -> dict[str, Any] | None:
        return self._raw_data

    @classmethod
    def create_user_and_metadata(cls, raw_data: dict[str, Any]) -> UserAndMetadata:

        effective_roles = list(map(lambda r: RoleAndOrigins.create_role_and_origins(r),
                                   raw_data["effective_roles"]))

        # RoleAndOrigins.role is Optional, so without the first clause this could admit a None
        # into the user's roles.  create_role_and_origins always supplies one, so the guard never
        # drops anything; it is here so the code holds to what the annotation says.
        user_roles = set(r.role for r in effective_roles
                         if r.role is not None
                         and (not r.origins or any(o.type == "user" for o in r.origins)))

        # RBAC prior to v6.5 does not have groups
        ext_group_data = raw_data.get("external_groups", None)

        # password_change_date is optional
        pw_data = raw_data.get("password_changed", None)
        pw_changed = None
        formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
        for f in formats:
            if pw_changed or not isinstance(pw_data, str):
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
                 name: str | None = None,
                 description: str | None = None,
                 roles: SetInputType[Role] | None = None,
                 ldap_group_reference: str | None = None,
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
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str | None:
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        self._description = value

    @property
    def roles(self) -> set[Role]:
        return self._roles

    @roles.setter
    def roles(self, value: SetInputType[Role]) -> None:
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    @property
    def ldap_group_reference(self) -> str | None:
        return self._ldap_group_reference

    @ldap_group_reference.setter
    def ldap_group_reference(self, value: str) -> None:
        self._ldap_group_reference = value

    @property
    def raw_data(self) -> dict[str, Any] | None:
        return self._raw_data

    def as_dict(self) -> dict[str, Any]:
        rs = list(map(lambda r: r.as_dict(), self.roles))
        return {
            'name': self.name,
            'description': self.description,
            'roles': rs,
            'ldap_group_reference': self.ldap_group_reference
        }

    @classmethod
    def create_group(cls, raw_data: dict[str, Any]) -> Group:
        return cls(
            raw_data.get('name'),
            description=raw_data.get('description', None),
            roles=set(map(lambda r: Role.create_role(
                r), raw_data.get('roles', []))),
            ldap_group_reference=raw_data.get('ldap_group_ref', None)
        )
