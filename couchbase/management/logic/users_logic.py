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

from copy import deepcopy
from datetime import datetime
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Set,
                    Union)

from couchbase.auth import AuthDomain
from couchbase.exceptions import (FeatureUnavailableException,
                                  GroupNotFoundException,
                                  InvalidArgumentException,
                                  RateLimitedException,
                                  UserNotFoundException)
from couchbase.options import forward_args
from couchbase.pycbc_core import (management_operation,
                                  mgmt_operations,
                                  user_mgmt_operations)

if TYPE_CHECKING:
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


class UserManagerLogic:

    _ERROR_MAPPING = {r'Unknown group.*': GroupNotFoundException,
                      r'Unknown user.*': UserNotFoundException,
                      r'Not found.*': FeatureUnavailableException,
                      r'Method Not Allowed.*': FeatureUnavailableException,
                      r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException}

    def __init__(self, connection):
        self._connection = connection

    def _get_valid_domain(self, auth_domain  # type: Union[AuthDomain,str]
                          ) -> str:
        if isinstance(auth_domain, str) and auth_domain in [
                "local", "external"]:
            return auth_domain
        elif isinstance(auth_domain, AuthDomain):
            return AuthDomain.to_str(auth_domain)
        else:
            raise InvalidArgumentException(message="Unknown Authentication Domain")

    def get_user(self,
                 username,  # type: str
                 *options,  # type: GetUserOptions
                 **kwargs   # type: Any
                 ) -> Optional[UserAndMetadata]:

        final_args = forward_args(kwargs, *options)
        domain = final_args.pop("domain_name", "local")

        domain = self._get_valid_domain(domain)

        op_args = {
            "domain": domain,
            "username": username
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.GET_USER.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_users(self,
                      *options,  # type: GetAllUsersOptions
                      **kwargs  # type: Any
                      ) -> Optional[Iterable[UserAndMetadata]]:
        final_args = forward_args(kwargs, *options)
        domain = final_args.pop("domain_name", "local")
        domain = self._get_valid_domain(domain)

        op_args = {
            "domain": domain
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.GET_ALL_USERS.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def upsert_user(self,
                    user,     # type: User
                    *options,  # type: UpsertUserOptions
                    **kwargs  # type: Any
                    ) -> None:

        final_args = forward_args(kwargs, *options)
        domain = final_args.pop("domain_name", "local")
        domain = self._get_valid_domain(domain)

        if not user.groups and (not user.roles or not isinstance(user.roles, set)):
            raise InvalidArgumentException("Roles must be a non-empty list")

        user_dict = {k: v for k, v in user.as_dict().items() if k in {
            "password", "name", "username", "groups"}}

        if user_dict["password"] and domain == "external":
            raise InvalidArgumentException(
                "External domains must not have passwords")

        if user.roles:
            user_dict["roles"] = list(map(lambda r: r.as_dict(), user.roles))

        op_args = {
            "domain": domain,
            "user": user_dict
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.UPSERT_USER.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_user(self,
                  username,  # type: str
                  *options,  # type: DropUserOptions
                  **kwargs   # type: Any
                  ) -> None:

        final_args = forward_args(kwargs, *options)
        domain = final_args.pop("domain_name", "local")
        domain = self._get_valid_domain(domain)

        op_args = {
            "domain": domain,
            "username": username
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.DROP_USER.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def change_password(self,
                        new_password,  # type: str
                        *options,     # type: ChangePasswordOptions
                        **kwargs      # type: Any
                        ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {
            "password": new_password
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.CHANGE_PASSWORD.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_roles(self,
                  *options,  # type: GetRolesOptions
                  **kwargs   # type: Any
                  ) -> Optional[Iterable[RoleAndDescription]]:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.GET_ROLES.value
        }
        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_group(self,
                  group_name,   # type: str
                  *options,     # type: GetGroupOptions
                  **kwargs      # type: Any
                  ) -> Optional[Group]:

        op_args = {
            "name": group_name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.GET_GROUP.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_groups(self,
                       *options,    # type: GetAllGroupsOptions
                       **kwargs     # type: Any
                       ) -> Optional[Iterable[Group]]:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.GET_ALL_GROUPS.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def upsert_group(self,
                     group,     # type: Group
                     *options,  # type: UpsertGroupOptions
                     **kwargs   # type: Any
                     ) -> None:

        group_dict = {k: v for k, v in group.as_dict().items() if k in {
            "name", "description", "ldap_group_reference"}}

        if group.roles:
            group_dict["roles"] = list(map(lambda r: r.as_dict(), group.roles))

        op_args = {
            "group": group_dict
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.UPSERT_GROUP.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_group(self,
                   group_name,  # type: str
                   *options,    # type: DropGroupOptions
                   **kwargs     # type: Any
                   ) -> None:

        op_args = {
            "name": group_name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.USER.value,
            "op_type": user_mgmt_operations.DROP_GROUP.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)


class UserManagementUtils(object):
    """
    ** INTERNAL **
    """

    @classmethod
    def to_set(cls, value, valid_type, display_name):

        if not value:
            return value
        elif isinstance(value, set):
            cls.validate_all_set_types(value, valid_type, display_name)
            return value
        elif isinstance(value, list):
            cls.validate_all_set_types(value, valid_type, display_name)
            return set(value)
        elif isinstance(value, tuple):
            cls.validate_all_set_types(value, valid_type, display_name)
            return set(value)
        elif isinstance(value, valid_type):
            return set([value])
        else:
            raise InvalidArgumentException(
                '{} must be of type {}.'.format(display_name,
                                                valid_type.__name__))

    @classmethod
    def validate_all_set_types(cls, value, valid_type, display_name):

        if all(map(lambda r: isinstance(r, type), value)):
            raise InvalidArgumentException(
                '{} must contain only objects of type {}.'.format(display_name,
                                                                  valid_type.__name__))


class Role:

    def __init__(self,
                 name=None,         # type: str
                 bucket=None,       # type: str
                 scope=None,        # type: str
                 collection=None,   # type: str
                 ):

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

    def as_dict(self):
        return {
            'name': self._name,
            'bucket': self._bucket,
            'scope': self._scope,
            'collection': self._collection
        }

    def __eq__(self, other):
        if not isinstance(other, Role):
            return False
        return (self.name == other.name
                and self.bucket == other.bucket
                and self.scope == other.scope
                and self.collection == other.collection)

    def __hash__(self):
        return hash((self.name, self.bucket, self.scope, self.collection))

    @classmethod
    def create_role(cls, raw_data):
        return cls(
            name=raw_data.get("name", None),
            bucket=raw_data.get("bucket_name", None),
            scope=raw_data.get("scope_name", None),
            collection=raw_data.get("collection_name", None)
        )


class RoleAndDescription:

    def __init__(self,
                 role=None,          # type: Role
                 display_name=None,  # type: str
                 description=None,   # type: str
                 ce=None,            # type: bool
                 ):

        self._role = role
        self._display_name = display_name
        self._description = description
        self._ce = ce

    @property
    def role(self) -> Role:
        return self._role

    @property
    def display_name(self) -> str:
        return self._display_name

    @property
    def description(self) -> str:
        return self._description

    @property
    def ce(self) -> bool:
        return self._ce

    @classmethod
    def create_role_and_description(cls, raw_data):
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

    def __init__(self,
                 type=None,  # type: str
                 name=None   # type: str
                 ):

        self._type = type
        self._name = name

    @property
    def type(self) -> str:
        return self._type

    @property
    def name(self) -> str:
        return self._name


class RoleAndOrigins:

    def __init__(self,
                 role=None,  # type: Role
                 origins=[]  # type: List[Origin]
                 ):

        self._role = role
        self._origins = origins

    @property
    def role(self) -> Role:
        return self._role

    @property
    def origins(self) -> List[Origin]:
        return self._origins

    @classmethod
    def create_role_and_origins(cls, raw_data):

        # RBAC prior to v6.5 does not have origins
        origin_data = raw_data.get("origins", None)

        return cls(
            role=Role.create_role(raw_data.get("role")),
            origins=list(map(lambda o: Origin(**o), origin_data))
            if origin_data else []
        )


class User:

    def __init__(self,
                 username=None,      # type: str
                 display_name=None,  # type: str
                 groups=None,        # type: Set[str]
                 roles=None,         # type: Set[Role]
                 password=None       # type: str
                 ):

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
    def display_name(self) -> str:
        return self._display_name

    @display_name.setter
    def display_name(self,
                     value  # type: str
                     ):
        self._display_name = value

    @property
    def groups(self) -> Set[str]:
        """names of the groups"""
        return self._groups

    @groups.setter
    def groups(self,
               value  # type: Set[str]
               ):
        self._groups = UserManagementUtils.to_set(value, str, 'Groups')

    @property
    def roles(self) -> Set[Role]:
        """only roles assigned directly to the user (not inherited from groups)"""
        return self._roles

    @roles.setter
    def roles(self,
              value  # type: Set[Role]
              ):
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    def password(self, value):
        self._password = value

    password = property(None, password)

    def as_dict(self):
        output = {
            "username": self.username,
            "name": self.display_name,
            "password": self._password
        }

        if self.roles:
            output["roles"] = list(self.roles)

        if self.groups:
            output["groups"] = list(self.groups)

        return output

    @classmethod
    def create_user(cls, raw_data, roles=None):

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
                 domain=None,            # type: AuthDomain
                 user=None,              # type: User
                 effective_roles=[],     # type: List[RoleAndOrigins]
                 password_changed=None,  # type: datetime
                 external_groups=None,   # type: Set[str]
                 **kwargs                # type: Dict[str, Any]
                 ):

        self._domain = domain
        self._user = user
        self._effective_roles = effective_roles
        self._password_changed = password_changed
        self._external_groups = external_groups
        self._raw_data = kwargs.get("raw_data", None)

    @property
    def domain(self) -> AuthDomain:
        """ AuthDomain is an enumeration with values "local" and "external".
        It MAY alternatively be represented as String."""
        return self._domain

    @property
    def user(self) -> User:
        """returns a new mutable User object each time this method is called.
            Modifying the fields of the returned User MUST have no effect on the
            UserAndMetadata object it came from."""
        return deepcopy(self._user)

    @property
    def effective_roles(self) -> List[RoleAndOrigins]:
        """all roles, regardless of origin."""
        return self._effective_roles

    @property
    def password_changed(self) -> Optional[datetime]:
        return self._password_changed

    @property
    def external_groups(self) -> Set[str]:
        return self._external_groups

    @property
    def raw_data(self) -> Dict[str, Any]:
        return self._raw_data

    @classmethod
    def create_user_and_metadata(cls, raw_data):

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
            user=User.create_user(raw_data.get("user"), roles=user_roles),
            password_changed=pw_changed,
            external_groups=set(ext_group_data) if ext_group_data else None,
            raw_data=raw_data
        )


class Group:
    def __init__(self,
                 name=None,                 # type: str
                 description=None,          # type: str
                 roles=None,                # type: Set[Role]
                 ldap_group_reference=None,  # type: str
                 **kwargs                   # type: Any
                 ):

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
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self,
                    value  # type: str
                    ):
        self._description = value

    @property
    def roles(self) -> Set[Role]:
        return self._roles

    @roles.setter
    def roles(self,
              value  # type: Set[Role]
              ):
        self._roles = UserManagementUtils.to_set(value, Role, 'Roles')

    @property
    def ldap_group_reference(self) -> str:
        return self._ldap_group_reference

    @ldap_group_reference.setter
    def ldap_group_reference(self,
                             value  # type: str
                             ):
        self._ldap_group_reference = value

    @property
    def raw_data(self) -> Dict[str, Any]:
        return self._raw_data

    def as_dict(self):
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
    def create_group(cls, raw_data):
        return cls(
            raw_data.get('name'),
            description=raw_data.get('description', None),
            roles=set(map(lambda r: Role.create_role(
                r), raw_data.get('roles'))),
            ldap_group_referenc=raw_data.get('ldap_group_ref', None)
        )
