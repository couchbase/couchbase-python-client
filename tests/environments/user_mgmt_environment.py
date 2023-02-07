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

from datetime import datetime
from typing import Optional

from couchbase.auth import AuthDomain
from couchbase.management.users import (Role,
                                        RoleAndOrigins,
                                        User)
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class UserManagementTestEnvironment(TestEnvironment):

    def setup(self,
              collection_type=None,  # type: Optional[CollectionType]
              ):
        self.enable_user_mgmt()

    def teardown(self,
                 collection_type=None  # type: Optional[CollectionType]
                 ):

        self.disable_user_mgmt()

    def validate_group(self, group, roles=None):
        property_list = [
            'name', 'description', 'roles', 'ldap_group_reference'
        ]
        properties = list(n for n in dir(group) if n in property_list)
        for p in properties:
            value = getattr(group, p)
            if p == 'name':
                assert value is not None  # nosec
            elif p == 'description' and value:
                assert isinstance(value, str)  # nosec
            elif p == 'roles':
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, Role), value)) is True  # nosec
            elif p == 'ldap_group_reference' and value:
                assert isinstance(value, str)  # nosec

        if roles:
            assert len(roles) == len(group.roles)  # nosec
            assert set(roles) == group.roles  # nosec

        return True

    def validate_user(self, user, user_roles=None):
        # password is write-only
        property_list = ['username', 'groups', 'roles']
        properties = list(n for n in dir(user) if n in property_list)
        for p in properties:
            value = getattr(user, p)
            if p == 'username':
                assert value is not None  # nosec
            elif p == 'groups' and value:
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, str), value)) is True  # nosec

            elif p == 'roles':
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, Role), value)) is True  # nosec

        if user_roles:
            assert len(user_roles) == len(user.roles)  # nosec
            diff = set(user_roles).difference(user.roles)
            assert diff == set()  # nosec

        return True

    def validate_user_and_metadata(self,  # noqa: C901
                                   user_metadata,
                                   user_roles=None,
                                   groups=None):
        property_list = [
            'domain', 'user', 'effective_roles', 'password_changed',
            'external_groups'
        ]
        properties = list(n for n in dir(user_metadata) if n in property_list)
        for p in properties:
            value = getattr(user_metadata, p)
            if p == 'domain':
                assert isinstance(value, AuthDomain)  # nosec
            elif p == 'user':
                assert isinstance(value, User)  # nosec
                # per RFC, user property should return a mutable User object
                #   that will not have an effect on the UserAndMetadata object
                assert id(value) != id(user_metadata._user)  # nosec
                self.validate_user(value, user_roles)
            elif p == 'effective_roles':
                assert isinstance(value, list)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, RoleAndOrigins), value)) is True  # nosec
            elif p == 'password_changed' and value:
                assert isinstance(value, datetime)  # nosec
            elif p == 'external_groups' and value:
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, str), value)) is True  # nosec

        if user_roles:
            actual_roles = set(
                r.role for r in user_metadata.effective_roles
                if any(map(lambda o: o.type == 'user', r.origins))
                or len(r.origins) == 0)
            assert len(user_roles) == len(actual_roles)  # nosec
            assert set(user_roles) == actual_roles  # nosec

        if groups:
            actual_roles = set(
                r.role for r in user_metadata.effective_roles
                if any(map(lambda o: o.type != 'user', r.origins)))
            group_roles = set()
            for g in groups:
                group_roles.update(g.roles)

            assert len(group_roles) == len(actual_roles)  # nosec

        return True

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> UserManagementTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
