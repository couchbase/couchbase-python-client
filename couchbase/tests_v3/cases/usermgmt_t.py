import os
from unittest import SkipTest

from couchbase import ArgumentError
from couchbase.management.users import User, Role, Group, RawRole, GroupNotFoundException, UserNotFoundException
from couchbase_core.auth_domain import AuthDomain
from couchbase_tests.base import CollectionTestCase
from typing import *
import re


UG_WORKING = os.getenv("PYCBC_UPSERT_GROUP_WORKING")


class UserManagementTests(CollectionTestCase):
    def setUp(self, *args, **kwargs):
        super(UserManagementTests, self).setUp(*args, **kwargs)
        self.um = self.cluster.users()
        if not self.is_realserver:
            raise SkipTest('Real server must be used for admin tests')
        try:
            self.um.upsert_group(Group('qweqwe'))
        except:
            pass

    def tearDown(self):
        try:
            if UG_WORKING:
                self.um.drop_group('qweqwe')
            pass
        except:
            pass

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
        self.assertRaises(ArgumentError, self.um.get_all_users, None)
        self.assertRaises(ArgumentError, self.um.get_user, userid, None)
        self.assertRaises(ArgumentError, self.um.upsert_user, User(username=userid, password=password, roles=roles),
                          domain=None)
        self.assertRaises(ArgumentError, self.um.drop_user, userid, None)

    def test_external_nopassword(self):

        userid = 'custom-user'
        password = 's3cr3t'
        roles = [Role.of(name='data_reader', bucket='default'), Role.of(name='data_writer', bucket='default')]

        # password with external generates argument error
        self.assertRaises(ArgumentError, self.um.upsert_user, User(username=userid, password=password, roles=roles),
                          domain=AuthDomain.External)
        self.assertRaises(ArgumentError, self.um.upsert_user, User(username=userid, password=password, roles=None),
                          domain=AuthDomain.External)
        self.assertRaises(ArgumentError, self.um.upsert_user, User(username=userid, password=password, roles=[]),
                          domain=AuthDomain.External)
        try:
            self.um.upsert_user(User(username=userid, password=None, roles=roles), domain=AuthDomain.External)
        except ArgumentError:
            raise
        except:
            pass

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

    def test_groups(self):
        fresh_group = Group(name='qweqwe', roles={Role.of(name='admin')})
        if UG_WORKING:
            self.um.upsert_group(fresh_group)
        result = self.um.get_group('qweqwe')
        admin_role = Role.of(name='admin')
        expected_roles = {admin_role}
        actual_roles = result.roles
        self.assertSetEqual(expected_roles, actual_roles)

    def test_get_all_groups(self):
        all_groups = self.um.get_all_groups()
        self.assertEqual([Group('qweqwe', roles={RawRole('admin', None)})], all_groups)

    def test_timeout(self):
        self.um.get_all_groups(timeout=0.1)

    def test_get_roles(self):
        roles = self.um.get_roles()
        admin_desc = re.compile(r'.*all cluster features.*web console.*read and write all data.*$')
        for rad in reversed(roles):
            desc_matches = admin_desc.match(rad.description)
            if desc_matches:
                self.assertTrue(rad.role == 'admin' and rad.display_name == 'Full Admin')
                return
        self.fail("No admin role found")

    def test_missing_group(self):
        self.assertRaises(GroupNotFoundException, self.um.get_group, 'fred')

    def test_missing_user(self):
        self.assertRaises(UserNotFoundException, self.um.get_user, 'keith')
