import os
from unittest import SkipTest
from functools import wraps

from flaky import flaky

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.users import User, Role, Group, RawRole, GroupNotFoundException, UserNotFoundException
from couchbase.auth import AuthDomain
from couchbase_tests.base import CollectionTestCase
from couchbase.exceptions import  NotSupportedException
from typing import *
import re
from datetime import timedelta

UG_WORKING = os.getenv("PYCBC_UPSERT_GROUP_WORKING")


def skip_if_no_groups(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if not self.supports_groups():
            raise SkipTest('groups not supported (server < 6.5?)')
        func(self, *args, **kwargs)

    return wrap


class UserManagementTests(CollectionTestCase):

    def supports_groups(self):
        # mock doesnt support it
        if self.is_mock:
            return False
        # get_all_groups will raise NotSupported when we are hiting < 6.5
        try:
            self.um.get_all_groups()
            return True
        except NotSupportedException:
            return False

    def setUp(self, *args, **kwargs):
        if self.config.mock_enabled:
            raise SkipTest('Real server must be used for admin tests')

        super(UserManagementTests, self).setUp(*args, **kwargs)
        self.um = self.cluster.users()
        if self.supports_groups():
            self.um.upsert_group(Group('qweqwe', roles={Role.of(name='admin')}))

    def tearDown(self):
        if self.supports_groups():
            self.um.drop_group('qweqwe')

    def test_create_list_get_remove_internal_user(self):

        userid = 'custom-user'
        password = 's3cr3t'
        roles = [Role.of(name='data_reader', bucket='default'), Role.of(name='data_writer', bucket='default')]

        # add user
        self.um.upsert_user(User(username=userid, roles=roles, password=password), domain=AuthDomain.Local)

        # get all users
        users = self.um.get_all_users(AuthDomain.Local)
        self.assertIsNotNone(users)

        # get single user
        user = self.um.get_user(userid, AuthDomain.Local)
        self.assertIsNotNone(user)

        # remove user
        self.um.drop_user(userid, AuthDomain.Local)

    def test_invalid_domain_raises_argument_error(self):

        userid = 'custom-user'
        password = 's3cr3t'
        roles = [Role.of(name='data_reader', bucket='default'), Role.of(name='data_writer', bucket='default')]

        # invalid domain generates argument error
        self.assertRaises(InvalidArgumentException, self.um.get_all_users, None)
        self.assertRaises(InvalidArgumentException, self.um.get_user, userid, None)
        self.assertRaises(InvalidArgumentException, self.um.upsert_user, User(username=userid, password=password, roles=roles),
                          domain=None)
        self.assertRaises(InvalidArgumentException, self.um.drop_user, userid, None)

    def test_external_nopassword(self):

        userid = 'custom-user'
        password = 's3cr3t'
        roles = [Role.of(name='data_reader', bucket='default'), Role.of(name='data_writer', bucket='default')]

        # password with external generates argument error
        self.assertRaises(InvalidArgumentException, self.um.upsert_user, User(username=userid, password=password, roles=roles),
                          domain=AuthDomain.External)
        self.assertRaises(InvalidArgumentException, self.um.upsert_user, User(username=userid, password=password, roles=None),
                          domain=AuthDomain.External)
        self.assertRaises(InvalidArgumentException, self.um.upsert_user, User(username=userid, password=password, roles=[]),
                          domain=AuthDomain.External)
        try:
            self.um.upsert_user(User(username=userid, password=None, roles=roles), domain=AuthDomain.External)
        except InvalidArgumentException:
            raise
        except:
            pass

    @flaky(10,1)
    def test_user_api_aliases(self):

        userid = 'custom-user'
        password = 's3cr3t'
        roles = [('data_reader', 'default'), ('data_writer', 'default')]

        # add user
        self.um.upsert_user(User(username=userid, password=password, roles=roles), domain=AuthDomain.Local)

        # get all users
        users = self.um.get_all_users(AuthDomain.Local)
        self.assertIsNotNone(users)

        # get single user
        user = self.um.get_user(username=userid, domain_name=AuthDomain.Local)
        self.assertIsNotNone(user)

        # remove user
        self.um.drop_user(userid, AuthDomain.Local)

    @skip_if_no_groups
    def test_groups(self):
        role = Role.of(name='admin')
        fresh_group = Group(name='qweqwe', roles={role})
        self.um.upsert_group(fresh_group)
        result = self.um.get_group('qweqwe')
        admin_role = Role.of(name='admin')
        expected_roles = {admin_role}
        actual_roles = result.roles
        self.assertSetEqual(expected_roles, actual_roles)

    @skip_if_no_groups
    def test_get_all_groups(self):
        all_groups = self.um.get_all_groups()
        # NOTE: we could well have other groups on this server, apart from the one we added, so
        # lets be ok with there being more of them.  However, the one we added _MUST_ be there.
        known_group = Group('qweqwe', roles={RawRole('admin', None)})
        for g in all_groups:
          if known_group == g:
            return
        self.fail("didn't find expected group in get_all_groups")

    @skip_if_no_groups
    def test_timeout(self):
        self.um.get_all_groups(timeout=timedelta(seconds = 0.1))

    def test_get_roles(self):
        roles = self.um.get_roles()
        admin_desc = re.compile(r'.*all cluster features.*web console.*read and write all data.*$')
        for rad in reversed(roles):
            desc_matches = admin_desc.match(rad.description)
            if desc_matches:
                self.assertTrue(rad.role == 'admin' and rad.display_name == 'Full Admin')
                return
        self.fail("No admin role found")

    @skip_if_no_groups
    def test_missing_group(self):
        self.assertRaises(GroupNotFoundException, self.um.get_group, 'fred')

    def test_missing_user(self):
        self.assertRaises(UserNotFoundException, self.um.get_user, 'keith')
