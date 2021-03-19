#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from couchbase.management.admin import Admin
from couchbase_core.result import HttpResult
from couchbase.exceptions import (
    InvalidArgumentException, AuthenticationException, CouchbaseException,
    NetworkException, HTTPException)
from couchbase_tests.base import CouchbaseTestCase, SkipTest
from couchbase.auth import AuthDomain
from couchbase.management.users import Role

import time


class AdminSimpleTest(CouchbaseTestCase):
    def setUp(self):
        super(AdminSimpleTest, self).setUp()
        self.admin = self.make_admin_connection()

    def tearDown(self):
        super(AdminSimpleTest, self).tearDown()
        if self.should_check_refcount:
            rc = sys.getrefcount(self.admin)
            #TODO: revaluate GC - fragile assumption
            #self.assertEqual(rc, 2)

        del self.admin

    def test_http_request(self):
        htres = self.admin.http_request(path='pools/')
        self.assertIsInstance(htres, HttpResult)
        self.assertIsInstance(htres.value, dict)
        self.assertEqual(htres.http_status, 200)
        self.assertEqual(htres.url, 'pools/')
        self.assertTrue(htres.success)

    def test_connection_string_param(self):
        # if using mock, we need a bucket in the connstr.  Note the admin
        # constructor just adds a bucket (if given) when it constructs the
        # connection string.  If you give it one, you need to put it in yourself.
        # But, only for the mock
        if self.is_mock:
            conn_str = 'http://{0}:{1}/{2}'.format(self.cluster_info.host, self.cluster_info.port, 'default')
        else:
            conn_str = 'http://{0}:{1}'.format(self.cluster_info.host, self.cluster_info.port)

        admin = Admin('Administrator',
                      'password',
                      connection_string=conn_str)
        self.assertIsNotNone(admin)

    def test_bucket_param(self):

        admin = Admin('Administrator',
                      'password',
                      host=self.cluster_info.host,
                      port=self.cluster_info.port,
                      bucket='default')
        self.assertIsNotNone(admin)

    def test_bad_request(self):
        self.assertRaises(HTTPException, self.admin.http_request, '/badpath')

        excraised = 0
        try:
            self.admin.http_request("/badpath")
        except HTTPException as e:
            excraised = 1
            self.assertIsInstance(e.objextra, HttpResult)

        self.assertTrue(excraised)

    def test_bad_args(self):
        self.assertRaises(InvalidArgumentException,
                          self.admin.http_request,
                          None)

        self.assertRaises(InvalidArgumentException,
                          self.admin.http_request,
                          '/',
                          method='blahblah')

    def test_bad_auth(self):
        self.assertRaises(AuthenticationException, Admin,
                          'baduser', 'badpass',
                          host=self.cluster_info.host,
                          port=self.cluster_info.port, **self.cluster_info.mock_hack_options(self.is_mock).kwargs)

    def test_bad_host(self):
        # admin connections don't really connect until an action is performed
        try:
            admin = Admin('username', 'password', host='127.0.0.1', port=1)
            self.assertRaises(NetworkException, admin.bucket_info, 'default')
        except NetworkException:
            pass

    def test_bad_handle(self):
        self.assertRaises(CouchbaseException, self.admin.upsert, "foo", "bar")
        self.assertRaises(CouchbaseException, self.admin.get, "foo")
        self.assertRaises(CouchbaseException, self.admin.append, "foo", "bar")
        self.assertRaises(CouchbaseException, self.admin.remove, "foo")
        self.assertRaises(CouchbaseException, self.admin.unlock, "foo", 1)
        str(None)

    def test_create_ephemeral_bucket_and_use(self):
        # if not self.is_realserver:
        #     raise SkipTest('Mock server must be used for admin tests')
        bucket_name = 'ephemeral'
        password = 'letmein'

        def basic_upsert_test(bucket):

            # create a doc then read it back
            key = 'mike'
            doc = {'name': 'mike'}
            bucket.upsert(key, doc)
            result = bucket.get(key)
            # original and result should be the same
            self.assertEqual(doc, result.value)

        # create ephemeral test bucket
        self.act_on_special_bucket(bucket_name, password,
                                   basic_upsert_test)

    def act_on_special_bucket(self, bucket_name, password, action, perm_generator=None):

        try:
            if self.is_realserver:
                self.admin.bucket_remove("default")
            time.sleep(10)
            self.admin.bucket_create(name=bucket_name,
                                     bucket_type='ephemeral',
                                     ram_quota=100,
                                     bucket_password=password)
            self.admin.wait_ready(bucket_name, timeout=100)

            if perm_generator:
                roles = perm_generator(bucket_name)
            else:
                roles = [Role(name='data_reader', bucket=bucket_name), Role(name='data_writer', bucket=bucket_name)]

            self.admin.user_upsert(bucket_name, AuthDomain.Local, password, roles)
            # connect to bucket to ensure we can use it
            conn_str = "http://{0}:{1}/{2}".format(self.cluster_info.host, self.cluster_info.port,
                                                   bucket_name) + "?ipv6="+self.cluster_info.ipv6
            bucket = self.factory(connection_string=conn_str, password=password)
            self.assertIsNotNone(bucket)

            action(bucket)
        finally:
            try:
                self.admin.user_remove(bucket_name, AuthDomain.Local)
                self.admin.bucket_delete(bucket_name)
            finally:
                if self.is_realserver:
                    time.sleep(10)
                    self.admin.bucket_create(name="default",
                                             bucket_type='couchbase',
                                             ram_quota=100,
                                             bucket_password=password)
                    self.admin.wait_ready("default", timeout=100)




    def test_build_user_management_path(self):

        path = self.admin._get_management_path(AuthDomain.Local)
        self.assertEqual('/settings/rbac/users/local', path)

        path = self.admin._get_management_path(AuthDomain.Local, 'user')
        self.assertEqual('/settings/rbac/users/local/user', path)

        path = self.admin._get_management_path(AuthDomain.Local)
        self.assertEqual('/settings/rbac/users/local', path)

        path = self.admin._get_management_path(AuthDomain.Local, 'user')
        self.assertEqual('/settings/rbac/users/local/user', path)
