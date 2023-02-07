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

import re

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (AuthenticationException,
                                  CouchbaseException,
                                  FeatureUnavailableException,
                                  GroupNotFoundException,
                                  InvalidArgumentException,
                                  UserNotFoundException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import (DropUserOptions,
                                          GetUserOptions,
                                          UpsertUserOptions)
from couchbase.management.users import (Group,
                                        Role,
                                        User)
from couchbase.options import ClusterOptions
from tests.environments.test_environment import TestEnvironment
from tests.environments.user_mgmt_environment import UserManagementTestEnvironment
from tests.test_features import EnvironmentFeatures


class UserManagementTestSuite:
    TEST_MANIFEST = [
        'test_default_domain',
        'test_external_nopassword',
        'test_external_user',
        'test_get_all_groups',
        'test_get_roles',
        'test_get_roles_all_valid',
        'test_group',
        'test_group_feature_not_found',
        'test_internal_user',
        'test_internal_user_fail',
        'test_internal_user_kwargs',
        'test_invalid_domain_raises_argument_error',
        'test_missing_group',
        'test_missing_user',
        'test_user_and_groups',
        'test_user_change_password',
        'test_user_display_name',
        'test_user_scopes_collections',
    ]

    @pytest.fixture(scope='class')
    def check_collections_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('collections',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_user_groups_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('user_group_mgmt',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    def test_default_domain(self, cb_env):

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]

        cb_env.um.upsert_user(User(username=username, password=password, roles=roles))

        user_metadata = TestEnvironment.try_n_times(5, 1, cb_env.um.get_user, username)
        assert user_metadata is not None

        # handle 7.0 roles w/ scopes/collections
        test_roles = roles
        collections_supported = EnvironmentFeatures.is_feature_supported('collections',
                                                                         cb_env.server_version_short,
                                                                         cb_env.mock_server_type)
        if collections_supported:
            test_roles = []
            for r in roles:
                test_roles.append(
                    Role(name=r.name,
                         bucket=r.bucket,
                         scope='*',
                         collection='*'))

        cb_env.validate_user_and_metadata(user_metadata, user_roles=test_roles)

        users_metadata = cb_env.um.get_all_users()
        assert users_metadata is not None
        result = all(
            map(lambda um: cb_env.validate_user_and_metadata(um),
                users_metadata))
        assert result is True

        cb_env.um.drop_user(username)

    def test_external_nopassword(self, cb_env):

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]

        # password with external generates argument error
        with pytest.raises(InvalidArgumentException):
            cb_env.um.upsert_user(User(username=username,
                                       password=password,
                                       roles=roles),
                                  domain_name='external')

        with pytest.raises(InvalidArgumentException):
            cb_env.um.upsert_user(User(username=username,
                                       password=password,
                                       roles=None),
                                  domain_name='external')

        with pytest.raises(InvalidArgumentException):
            cb_env.um.upsert_user(User(username=username, password=password, roles=[]),
                                  domain_name='external')
        try:
            cb_env.um.upsert_user(
                User(username=username, password=None, roles=roles),
                UpsertUserOptions(domain_name='external'))
        except InvalidArgumentException:
            raise
        except CouchbaseException:
            pass
        finally:
            cb_env.um.drop_user(username, domain_name='external')

    def test_external_user(self, cb_env):
        """
            test_external_user()
            Tests create, retrieve, update and removal
            of external (domain_name='external')
            Uses *UserOptions() for options
        """

        username = 'custom-user'
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]
        initial_user = User(username=username, roles=roles)
        # create user
        cb_env.um.upsert_user(initial_user, UpsertUserOptions(domain_name='external'))

        # get user
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    GetUserOptions(domain_name='external'))

        # handle 7.0 roles w/ scopes/collections
        test_roles = roles
        collections_supported = EnvironmentFeatures.is_feature_supported('collections',
                                                                         cb_env.server_version_short,
                                                                         cb_env.mock_server_type)
        if collections_supported:
            test_roles = []
            for r in roles:
                test_roles.append(
                    Role(name=r.name,
                         bucket=r.bucket,
                         scope='*',
                         collection='*'))

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=test_roles)

        # update user
        user = user_metadata.user
        user.roles = Role('admin')

        cb_env.um.upsert_user(user, UpsertUserOptions(domain_name='external'))

        # get user and verify updates
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    GetUserOptions(domain_name='external'))

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=user.roles)
        user_update = user_metadata.user
        assert initial_user != user_update

        # remove user
        cb_env.um.drop_user(username, DropUserOptions(domain_name='external'))
        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   cb_env.um.get_user,
                                                   username, domain_name='external',
                                                   expected_exceptions=UserNotFoundException)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures('check_user_groups_supported')
    def test_get_all_groups(self, cb_env):
        roles = [
            Role(name='data_reader', bucket='*'),
            Role(name='data_writer', bucket='*')
        ]
        fresh_group = Group(name='my-test-group',
                            roles=roles,
                            description='test group description')
        cb_env.um.upsert_group(fresh_group)
        admin_group = Group('admin-test-group', roles=[Role(name='admin')])
        cb_env.um.upsert_group(admin_group)
        all_groups = cb_env.um.get_all_groups()
        # NOTE: we could well have other groups on this server, apart from the one we added, so
        # lets be ok with there being more of them.  However, the one we added
        # _MUST_ be there.
        assert len(all_groups) >= 2
        admin_group_dict = admin_group.as_dict()
        found = False
        for g in all_groups:
            if admin_group_dict == g.as_dict():
                found = True

        cb_env.um.drop_group('my-test-group')
        cb_env.um.drop_group('admin-test-group')
        assert found is True

    # @TODO: is this test useful?
    # @pytest.mark.usefixtures('check_user_groups_supported')
    # def test_timeout(self, cb_env):
    #     with pytest.raises(AmbiguousTimeoutException):
    #         cb_env.um.get_all_groups(timeout=timedelta(seconds=0.1))

    def test_get_roles(self, cb_env):
        roles = cb_env.um.get_roles()
        admin_desc = re.compile(
            r'.*all cluster features.*web console.*read and write all data.*$')
        for rad in reversed(roles):
            desc_matches = admin_desc.match(rad.description)
            if desc_matches:
                assert rad.role.name == 'admin'
                assert rad.display_name == 'Full Admin'
                return
        pytest.fail('No admin role found')

    # see PYCBC-1030
    def test_get_roles_all_valid(self, cb_env):
        roles = cb_env.um.get_roles()
        for r in roles:
            assert r is not None

    @pytest.mark.usefixtures('check_user_groups_supported')
    def test_group(self, cb_env):
        roles = Role(name='admin')
        test_group = Group(name='my-test-group',
                           roles=roles,
                           description='test group description')
        # add group
        cb_env.um.upsert_group(test_group)

        # get group
        result = TestEnvironment.try_n_times(5, 1, cb_env.um.get_group, test_group.name)
        cb_env.validate_group(result, test_group.roles)

        # remove group
        cb_env.um.drop_group(test_group.name)
        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   cb_env.um.get_group,
                                                   test_group.name,
                                                   expected_exceptions=GroupNotFoundException)

    def test_group_feature_not_found(self, cb_env):
        groups_supported = EnvironmentFeatures.is_feature_supported('user_group_mgmt',
                                                                    cb_env.server_version_short,
                                                                    cb_env.mock_server_type)
        if groups_supported:
            pytest.skip(f'Only test on server versions < 6.5. Using server version: {cb_env.server_version}')

        roles = Role(name='admin')
        test_group = Group(name='my-test-group',
                           roles=roles,
                           description='test group description')

        with pytest.raises(FeatureUnavailableException):
            cb_env.um.upsert_group(test_group)
        with pytest.raises(FeatureUnavailableException):
            cb_env.um.get_all_groups()
        with pytest.raises(FeatureUnavailableException):
            cb_env.um.get_group(test_group.name)
        with pytest.raises(FeatureUnavailableException):
            cb_env.um.drop_group(test_group.name)

    def test_internal_user(self, cb_env):
        """
            test_internal_user()
            Tests create, retrieve, update and removal
            of internal (domain_name='local')
            Uses *UserOptions() for options
        """

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]
        initial_user = User(username=username, roles=roles, password=password)

        # create user
        cb_env.um.upsert_user(User(username=username, roles=roles, password=password),
                              UpsertUserOptions(domain_name='local'))

        # get user
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    GetUserOptions(domain_name='local'))

        # handle 7.0 roles w/ scopes/collections
        test_roles = roles
        collections_supported = EnvironmentFeatures.is_feature_supported('collections',
                                                                         cb_env.server_version_short,
                                                                         cb_env.mock_server_type)
        if collections_supported:
            test_roles = []
            for r in roles:
                test_roles.append(
                    Role(name=r.name,
                         bucket=r.bucket,
                         scope='*',
                         collection='*'))

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=test_roles)

        # update user
        user = user_metadata.user
        user.roles = Role('admin')
        user.password = 's3cr3t_pa33w0rd'

        cb_env.um.upsert_user(user, UpsertUserOptions(domain_name='local'))

        # get user and verify updates
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    GetUserOptions(domain_name='local'))

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=user.roles)
        user_update = user_metadata.user
        assert initial_user != user_update

        # remove user
        cb_env.um.drop_user(username, DropUserOptions(domain_name='local'))
        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   cb_env.um.get_user,
                                                   username,
                                                   domain_name='local',
                                                   expected_exceptions=(UserNotFoundException,))

    def test_internal_user_fail(self, cb_env):
        """
            test_internal_user()
            Tests create, retrieve, update and removal
            of internal (domain_name='local')
            Uses *UserOptions() for options
        """

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='not-a-bucket'),
            Role(name='data_writer', bucket='not-a-bucket')
        ]

        # create user
        with pytest.raises(InvalidArgumentException):
            cb_env.um.upsert_user(User(username=username, roles=roles, password=password),
                                  UpsertUserOptions(domain_name='local'))

    def test_internal_user_kwargs(self, cb_env):
        """
            test_internal_user_kwargs()
            Tests create, retrieve, update and removal
            of internal (domain_name='local')
            Uses kwargs for options
        """

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]
        initial_user = User(username=username, roles=roles, password=password)

        # create user
        cb_env.um.upsert_user(initial_user, domain_name='local')

        # get single user
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    domain_name='local')

        # handle 7.0 roles w/ scopes/collections
        test_roles = roles
        collections_supported = EnvironmentFeatures.is_feature_supported('collections',
                                                                         cb_env.server_version_short,
                                                                         cb_env.mock_server_type)
        if collections_supported:
            test_roles = []
            for r in roles:
                test_roles.append(
                    Role(name=r.name,
                         bucket=r.bucket,
                         scope='*',
                         collection='*'))

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=test_roles)

        # update user
        user = user_metadata.user
        user.roles = Role('admin')
        user.password = 's3cr3t_pa33w0rd'

        cb_env.um.upsert_user(user, domain_name='local')

        # get user and verify updates
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    domain_name='local')

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=user.roles)
        user_update = user_metadata.user
        assert initial_user != user_update

        # remove user
        cb_env.um.drop_user(username, domain_name='local')
        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   cb_env.um.get_user,
                                                   username,
                                                   domain_name='local',
                                                   expected_exceptions=UserNotFoundException)

    def test_invalid_domain_raises_argument_error(self, cb_env):

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]

        # invalid domain generates argument error
        with pytest.raises(InvalidArgumentException):
            cb_env.um.get_all_users(domain_name='fake-domain')

        with pytest.raises(InvalidArgumentException):
            cb_env.um.get_user(username, domain_name='fake-domain')

        with pytest.raises(InvalidArgumentException):
            cb_env.um.upsert_user(User(username=username,
                                       password=password,
                                       roles=roles),
                                  domain_name='fake-domain')

        with pytest.raises(InvalidArgumentException):
            cb_env.um.drop_user(username, domain_name='fake-domain')

    @pytest.mark.usefixtures('check_user_groups_supported')
    def test_missing_group(self, cb_env):
        with pytest.raises(GroupNotFoundException):
            cb_env.um.get_group('fred')

    def test_missing_user(self, cb_env):
        with pytest.raises(UserNotFoundException):
            cb_env.um.get_user('keith')

    @pytest.mark.usefixtures('check_user_groups_supported')
    def test_user_and_groups(self, cb_env):
        user_roles = [
            Role(name='query_select', bucket='default'),
            Role(name='fts_searcher', bucket='default')
        ]
        group_roles = [
            Role(name='data_reader', bucket='*'),
            Role(name='data_writer', bucket='*')
        ]
        groups = [
            Group(name='my-test-group',
                  roles=group_roles,
                  description='test group description'),
            Group(name='my-test-group-1',
                  roles=Role(name='admin'),
                  description='test group description')
        ]

        # add groups
        for group in groups:
            cb_env.um.upsert_group(group)
            TestEnvironment.try_n_times(5, 1, cb_env.um.get_group, group.name)
        user_groups = list(map(lambda g: g.name, groups))

        # add user
        test_user = User(username='custom-user',
                         roles=user_roles,
                         groups=user_groups,
                         password='s3cr3t')
        cb_env.um.upsert_user(test_user, domain_name='local')

        # get user
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    test_user.username,
                                                    domain_name='local')

        # handle 7.0 roles w/ scopes/collections
        test_roles = user_roles
        collections_supported = EnvironmentFeatures.is_feature_supported('collections',
                                                                         cb_env.server_version_short,
                                                                         cb_env.mock_server_type)
        if collections_supported:
            test_roles = []
            for r in user_roles:
                test_roles.append(
                    Role(name=r.name,
                         bucket=r.bucket,
                         scope='*',
                         collection='*'))

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata,
                                          user_roles=test_roles,
                                          groups=groups)

        # remove group
        remove_group = groups.pop()
        cb_env.um.drop_group(remove_group.name)
        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   cb_env.um.get_group,
                                                   remove_group.name,
                                                   expected_exceptions=GroupNotFoundException)

        # get user to verify roles from removed group are removed
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    test_user.username,
                                                    domain_name='local')

        # handle 7.0 roles w/ scopes/collections
        if collections_supported:
            test_roles = []
            for r in user_roles:
                test_roles.append(Role(name=r.name,
                                       bucket=r.bucket,
                                       scope='*',
                                       collection='*'))
        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata,
                                          user_roles=test_roles,
                                          groups=groups)

        # cleanup
        cb_env.um.drop_user(test_user.username, domain_name='local')
        for group in groups:
            cb_env.um.drop_group(group.name)

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_user_change_password(self, cb_env):
        username = 'change-password-user'
        admin_role = Role(name='admin')
        original_password = 'original_password'
        new_password = 'new_password'

        change_password_user = User(username=username,
                                    display_name="Change Password User",
                                    roles=admin_role,
                                    password=original_password)
        # Upsert user
        cb_env.um.upsert_user(change_password_user, UpsertUserOptions(domain_name="local"))

        # Authenticate as change-password-user.
        # Done in a while loop to emulate retry
        auth = PasswordAuthenticator(username, original_password)
        new_cluster = None
        while True:
            try:
                new_cluster = Cluster.connect(cb_env.cluster._connstr, ClusterOptions(auth))
            except AuthenticationException:
                continue
            break

        # Change password
        new_cluster.users().change_password(new_password)

        # Assert can authenticate using new password
        success_auth = PasswordAuthenticator(username, new_password)
        while True:
            try:
                success_cluster = Cluster.connect(cb_env.cluster._connstr, ClusterOptions(success_auth))
            except AuthenticationException:
                continue
            success_cluster.close()
            break

        # Assert cannot authenticate using old password
        fail_auth = PasswordAuthenticator(username, original_password)
        with pytest.raises(AuthenticationException):
            Cluster.connect(cb_env.cluster._connstr, ClusterOptions(fail_auth))

        new_cluster.close()

    def test_user_display_name(self, cb_env):
        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]
        user = User(username='custom-user',
                    display_name="Custom User",
                    roles=roles,
                    password='s3cr3t')

        # create user
        cb_env.um.upsert_user(user, UpsertUserOptions(domain_name='local'))

        # get user
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    user.username,
                                                    GetUserOptions(domain_name='local'))

        assert user_metadata.user.display_name == user.display_name

        cb_env.um.drop_user(user.username, DropUserOptions(domain_name='local'))

    @pytest.mark.usefixtures('check_collections_supported')
    def test_user_scopes_collections(self, cb_env):

        if cb_env.cm is None:
            cm = cb_env.bucket.collections()

        cm.create_scope('um-test-scope')
        for _ in range(3):
            scopes = cm.get_all_scopes()
            scope = next((s for s in scopes if s.name == 'um-test-scope'), None)
            if scope:
                break
            TestEnvironment.sleep(1)

        collection = CollectionSpec('test-collection', scope_name='um-test-scope')
        cm.create_collection(collection)
        for _ in range(3):
            scopes = cm.get_all_scopes()
            scope = next((s for s in scopes if s.name == 'um-test-scope'), None)
            if scope:
                coll = next((c for c in scope.collections
                             if c.name == 'test-collection'), None)
                if coll:
                    break
            TestEnvironment.sleep(1)

        username = 'custom-user'
        password = 's3cr3t'
        roles = [
            Role(name='data_reader', bucket='default', scope='um-test-scope'),
            Role(name='data_writer',
                 bucket='default',
                 scope='um-test-scope',
                 collection='test-collection')
        ]
        initial_user = User(username=username, roles=roles, password=password)

        # create user
        cb_env.um.upsert_user(initial_user, domain_name='local')

        # get single user
        user_metadata = TestEnvironment.try_n_times(5,
                                                    1,
                                                    cb_env.um.get_user,
                                                    username,
                                                    domain_name='local')

        test_roles = []
        for r in roles:
            if not r.collection:
                test_roles.append(Role(name=r.name,
                                       bucket=r.bucket,
                                       scope=r.scope,
                                       collection='*'))
            else:
                test_roles.append(r)

        assert user_metadata is not None
        cb_env.validate_user_and_metadata(user_metadata, user_roles=test_roles)

        cb_env.um.drop_user(username)
        cm.drop_collection(collection)
        cm.drop_scope('um-test-scope')


class ClassicUserManagementTests(UserManagementTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicUserManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicUserManagementTests) if valid_test_method(meth)]
        compare = set(UserManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = UserManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.setup()
        yield cb_env
        cb_env.teardown()
