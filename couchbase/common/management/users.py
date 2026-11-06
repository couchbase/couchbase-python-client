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

from couchbase.common._utils import fields_repr, redacted
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
    """A role, optionally limited to a bucket, a scope or a collection.

    Args:
        name (str): The name of the role.
        bucket (str, optional): The bucket the role is limited to.
        scope (str, optional): The scope the role is limited to, within ``bucket``.
        collection (str, optional): The collection the role is limited to, within ``scope``.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If ``name`` is missing or empty.
    """

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
        """The name of the role."""
        return self._name

    @property
    def bucket(self) -> str | None:
        """The bucket the role is limited to, or ``None`` if it is not limited to one."""
        return self._bucket

    @property
    def scope(self) -> str | None:
        """The scope the role is limited to, or ``None`` if it is not limited to one."""
        return self._scope

    @property
    def collection(self) -> str | None:
        """The collection the role is limited to, or ``None`` if it is not limited to one."""
        return self._collection

    def as_dict(self) -> dict[str, str | None]:
        """**INTERNAL** The role as the dict the management request is built from."""
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
        """**INTERNAL** The role the server described."""
        return cls(
            name=raw_data.get("name", None),
            bucket=raw_data.get("bucket", None),
            scope=raw_data.get("scope", None),
            collection=raw_data.get("collection", None)
        )

    def __repr__(self) -> str:
        return fields_repr(self, name=self._name, bucket=self._bucket, scope=self._scope, collection=self._collection)


class RoleAndDescription:
    """A role the cluster offers, with its description, as :meth:`.UserManager.get_roles` returns it."""

    def __init__(self,
                 role: Role,
                 display_name: str | None = None,
                 description: str | None = None,
                 ce: bool | None = None,
                 ) -> None:

        self._role = role
        self._display_name = display_name
        self._description = description
        self._ce = ce

    @property
    def role(self) -> Role:
        """The role."""
        return self._role

    @property
    def display_name(self) -> str | None:
        """The role's name as the server displays it."""
        return self._display_name

    @property
    def description(self) -> str | None:
        """What the role allows."""
        return self._description

    @property
    def ce(self) -> bool | None:
        """Whether the role is available in Couchbase Server Community Edition."""
        return self._ce

    @classmethod
    def create_role_and_description(cls, raw_data: dict[str, Any]) -> RoleAndDescription:
        """**INTERNAL** The role the server described."""
        return cls(
            role=Role.create_role(raw_data),
            display_name=raw_data.get('display_name', None),
            description=raw_data.get('description', None),
            ce=raw_data.get('ce', None)
        )

    def __repr__(self) -> str:
        return fields_repr(self, role=self._role, display_name=self._display_name,
                           description=self._description, ce=self._ce)


class Origin:
    """Why a user has a role.

    A ``type`` of ``user`` means the role is assigned to the user directly. A ``type`` of ``group`` means the user has
    it through the group that ``name`` names.
    """

    def __init__(self, type: str | None = None, name: str | None = None) -> None:
        self._type = type
        self._name = name

    @property
    def type(self) -> str | None:
        """``user`` if the role is assigned to the user directly, ``group`` if it comes from a group."""
        return self._type

    @property
    def name(self) -> str | None:
        """The name of the group the role comes from, when ``type`` is ``group``."""
        return self._name

    def __repr__(self) -> str:
        return fields_repr(self, type=self._type, name=self._name)


class RoleAndOrigins:
    """A role a user has, with every reason the user has it."""

    def __init__(self, role: Role, origins: list[Origin]) -> None:
        self._role = role
        self._origins = origins

    @property
    def role(self) -> Role:
        """The role."""
        return self._role

    @property
    def origins(self) -> list[Origin]:
        """Every reason the user has the role."""
        return self._origins

    @classmethod
    def create_role_and_origins(cls, raw_data: dict[str, Any]) -> RoleAndOrigins:

        # RBAC prior to v6.5 does not have origins
        """**INTERNAL** The role the server described."""
        origin_data = raw_data.get("origins", None)

        return cls(
            role=Role.create_role(raw_data),
            origins=list(map(lambda o: Origin(**o), origin_data))
            if origin_data else []
        )

    def __repr__(self) -> str:
        return fields_repr(self, role=self._role, origins=self._origins)


class User:
    """A user, as :meth:`.UserManager.upsert_user` takes it.

    Args:
        username (str): The name the user authenticates with.
        display_name (str, optional): The name the server displays for the user.
        groups (SetInputType[str], optional): The names of the groups the user is in, as a set, list or tuple, or a
            single name.
        roles (SetInputType[:class:`.Role`], optional): The roles assigned to the user directly. A set, list or tuple of
            roles, or a single role.
        password (str, optional): The password to set for the user.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If ``username`` is missing or empty.
    """

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
        """The name the user authenticates with."""
        return self._username

    @property
    def display_name(self) -> str | None:
        """The name the server displays for the user."""
        return self._display_name

    @display_name.setter
    def display_name(self, value: str) -> None:
        self._display_name = value

    @property
    def groups(self) -> set[str]:
        """The names of the groups the user is in."""
        return self._groups

    @groups.setter
    def groups(self, value: SetInputType[str]) -> None:
        self._groups = UserManagementUtils.to_set(value, str, 'Groups')

    @property
    def roles(self) -> set[Role]:
        """The roles assigned to the user directly, leaving out those it has through a group."""
        return self._roles

    @roles.setter
    def roles(self, value: SetInputType[Role]) -> None:
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    @property
    def password(self) -> str:
        """The password to set for the user.

        It is write-only.

        Raises:
            AttributeError: On every read.
        """
        # A getter that raises, rather than none, so a checker reads this as a property and not
        # as a method being overwritten.
        raise AttributeError('The password property is write-only.')

    @password.setter
    def password(self, value: str) -> None:
        self._password = value

    def as_dict(self) -> dict[str, Any]:
        # Annotated because the literal below infers dict[str, str | None], which the roles
        # list and the groups set that follow do not fit.
        """**INTERNAL** The user as the dict the management request is built from."""
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
        """**INTERNAL** The user the server described."""
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

    def __repr__(self) -> str:
        return fields_repr(self, username=self._username, display_name=self._display_name, groups=self._groups,
                           roles=self._roles, password=redacted(self._password))


class UserAndMetadata:
    """A user and what the server derives from it, as :meth:`.UserManager.get_user` returns it.

    Besides the user itself, it holds what only the server knows, such as the roles the user has through its groups.
    """

    def __init__(self,
                 domain: AuthDomain,
                 user: User,
                 effective_roles: list[RoleAndOrigins],
                 password_changed: datetime | None = None,
                 external_groups: set[str] | None = None,
                 *,
                 raw_data: dict[str, Any],
                 ) -> None:

        self._domain = domain
        self._user = user
        self._effective_roles = effective_roles
        self._password_changed = password_changed
        self._external_groups = external_groups
        self._raw_data = raw_data

    @property
    def domain(self) -> AuthDomain:
        """Whether the user is local to the cluster or comes from an external directory."""
        return self._domain

    @property
    def user(self) -> User:
        """A copy of the user.

        Changing the copy, to pass it to :meth:`.UserManager.upsert_user`, does not change this object.
        """
        return deepcopy(self._user)

    @property
    def effective_roles(self) -> list[RoleAndOrigins]:
        """Every role the user has, whether assigned directly or through a group."""
        return self._effective_roles

    @property
    def password_changed(self) -> datetime | None:
        """When the user's password was last changed, if the server reported it."""
        return self._password_changed

    @property
    def external_groups(self) -> set[str] | None:
        """The groups in the external directory the user is in."""
        return self._external_groups

    @property
    def raw_data(self) -> dict[str, Any]:
        """The server's description of the user, as it was received."""
        return self._raw_data

    @classmethod
    def create_user_and_metadata(cls, raw_data: dict[str, Any]) -> UserAndMetadata:
        """**INTERNAL** The user the server described."""
        effective_roles = list(map(lambda r: RoleAndOrigins.create_role_and_origins(r),
                                   raw_data["effective_roles"]))

        user_roles = set(r.role for r in effective_roles
                         if not r.origins or any(o.type == "user" for o in r.origins))

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

    def __repr__(self) -> str:
        return fields_repr(self, domain=self._domain, user=self._user, effective_roles=self._effective_roles,
                           password_changed=self._password_changed, external_groups=self._external_groups)


class Group:
    """A group of users, which gives every user in it the same roles.

    Args:
        name (str): The name of the group.
        description (str, optional): What the group is for.
        roles (SetInputType[:class:`.Role`], optional): The roles the group gives its users. A set, list or tuple of
            roles, or a single role.
        ldap_group_reference (str, optional): The LDAP group whose members the group takes in.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If ``name`` is missing or empty.
    """

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
        """The name of the group."""
        return self._name

    @property
    def description(self) -> str | None:
        """What the group is for."""
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        self._description = value

    @property
    def roles(self) -> set[Role]:
        """The roles the group gives its users."""
        return self._roles

    @roles.setter
    def roles(self, value: SetInputType[Role]) -> None:
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    @property
    def ldap_group_reference(self) -> str | None:
        """The LDAP group whose members the group takes in."""
        return self._ldap_group_reference

    @ldap_group_reference.setter
    def ldap_group_reference(self, value: str) -> None:
        self._ldap_group_reference = value

    @property
    def raw_data(self) -> dict[str, Any] | None:
        """The data the group was created from, if it was given any."""
        return self._raw_data

    def as_dict(self) -> dict[str, Any]:
        """**INTERNAL** The group as the dict the management request is built from."""
        rs = list(map(lambda r: r.as_dict(), self.roles))
        return {
            'name': self.name,
            'description': self.description,
            'roles': rs,
            'ldap_group_reference': self.ldap_group_reference
        }

    @classmethod
    def create_group(cls, raw_data: dict[str, Any]) -> Group:
        """**INTERNAL** The group the server described."""
        return cls(
            raw_data.get('name'),
            description=raw_data.get('description', None),
            roles=set(map(lambda r: Role.create_role(
                r), raw_data.get('roles', []))),
            ldap_group_reference=raw_data.get('ldap_group_ref', None),
            raw_data=raw_data
        )

    def __repr__(self) -> str:
        return fields_repr(self, name=self._name, description=self._description, roles=self._roles,
                           ldap_group_reference=self._ldap_group_reference)
