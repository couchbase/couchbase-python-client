#
# Copyright 2017, Couchbase, Inc.
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
import weakref

from couchbase.admin import Admin
from couchbase.bucket import Bucket
from couchbase.connstr import ConnectionString
from couchbase.exceptions import CouchbaseError


class MixedAuthError(CouchbaseError):
    """
    Cannot use old and new style auth together in the same cluster
    """
    pass


class NoBucketError(CouchbaseError):
    """
    Operation requires at least a single bucket to be open
    """


class Cluster(object):
    def __init__(self, connection_string='couchbase://localhost',
                 bucket_class=Bucket):
        """
        Creates a new Cluster object
        :param connection_string: Base connection string. It is an error to
            specify a bucket in the string.
        :param bucket_class: :class:`couchbase.bucket.Bucket` implementation to
            use.
        """
        self.connstr = ConnectionString.parse(str(connection_string))
        self.bucket_class = bucket_class
        self.authenticator = None
        self._buckets = {}
        if self.connstr.bucket:
            raise ValueError('Cannot pass bucket to connection string: ' + self.connstr.bucket)
        if 'username' in self.connstr.options:
            raise ValueError('username must be specified in the authenticator, '
                             'not the connection string')

    def authenticate(self, authenticator=None, username=None, password=None):
        """
        Set the type of authenticator to use when opening buckets or performing
        cluster management operations
        :param authenticator: The new authenticator to use
        :param username: The username to authenticate with
        :param password: The password to authenticate with
        """
        if authenticator is None:
            if not username:
                raise ValueError('username must not be empty.')
            if not password:
                raise ValueError('password must not be empty.')
            authenticator = PasswordAuthenticator(username, password)

        self.authenticator = authenticator

    def open_bucket(self, bucket_name, **kwargs):
        """
        Open a new connection to a Couchbase bucket
        :param bucket_name: The name of the bucket to open
        :param kwargs: Additional arguments to provide to the constructor
        :return: An instance of the `bucket_class` object provided to
            :meth:`__init__`
        """
        if self.authenticator:
            username, password = self.authenticator.get_credentials(bucket_name)
        else:
            username, password = None, None

        connstr = ConnectionString.parse(str(self.connstr))
        connstr.bucket = bucket_name
        if username:
            connstr.set_option('username', username)

        if 'password' in kwargs:
            if isinstance(self.authenticator, PasswordAuthenticator):
                raise MixedAuthError("Cannot override "
                                     "PasswordAuthenticators password")
        else:
            kwargs['password'] = password

        rv = self.bucket_class(str(connstr), **kwargs)
        self._buckets[bucket_name] = weakref.ref(rv)
        if isinstance(self.authenticator, ClassicAuthenticator):
            for bucket, passwd in self.authenticator.buckets.items():
                if passwd:
                    rv.add_bucket_creds(bucket, passwd)
        return rv

    def cluster_manager(self):
        """
        Returns an instance of :class:`~.couchbase.admin.Admin` which may be
        used to create and manage buckets in the cluster.
        """
        username, password = self.authenticator.get_credentials()
        connection_string = str(self.connstr)
        return Admin(username, password, connection_string=connection_string)

    def n1ql_query(self, query, *args, **kwargs):
        """
        Issue a "cluster-level" query. This requires that at least one
        connection to a bucket is active.
        :param query: The query string or object
        :param args: Additional arguments to :cb_bmeth:`n1ql_query`

        .. seealso:: :cb_bmeth:`n1ql_query`
        """
        from couchbase.n1ql import N1QLQuery
        if not isinstance(query, N1QLQuery):
            query = N1QLQuery(query)

        query.cross_bucket = True

        to_purge = []
        for k, v in self._buckets.items():
            bucket = v()
            if bucket:
                return bucket.n1ql_query(query, *args, **kwargs)
            else:
                to_purge.append(k)

        for k in to_purge:
            del self._buckets[k]

        raise NoBucketError('Must have at least one active bucket for query')


class Authenticator(object):
    def get_credentials(self, bucket=None):
        """
        Gets the username and password for a specified bucket. If bucket is
        `None`, gets the username and password for the entire cluster, if
        different.
        :param bucket: The bucket to act as context
        :return: A tuple of `username, password`
        """
        raise NotImplementedError()


class PasswordAuthenticator(Authenticator):
    def __init__(self, username, password):
        """
        This class uses a single credential pair of username and password, and
        is designed to be used either with cluster management operations or
        with Couchbase 5.0 style usernames with role based access control.

        For older cluster versions, or if you are only using a bucket's "SASL"
        password, use :class:`~.ClassicAuthenticator`

        :param username:
        :param password:

        .. warning:: This functionality is experimental both in API and
            implementation.

        """
        self.username = username
        self.password = password

    def get_credentials(self, *unused):
        return self.username, self.password


class ClassicAuthenticator(Authenticator):
    def __init__(self, cluster_username=None,
                 cluster_password=None,
                 buckets=None):
        """
        Classic authentication mechanism.
        :param cluster_username:
            Global cluster username. Only required for management operations
        :param cluster_password:
            Global cluster password. Only required for management operations
        :param buckets:
            A dictionary of `{bucket_name: bucket_password}`.
        """
        self.username = cluster_username
        self.password = cluster_password
        self.buckets = buckets if buckets else {}

    def get_credentials(self, bucket=None):
        if not bucket:
            return self.username, self.password
        return None, self.buckets.get(bucket)
