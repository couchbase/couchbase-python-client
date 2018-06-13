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
import logging
import weakref

from couchbase.admin import Admin
from couchbase.bucket import Bucket
from couchbase.connstr import ConnectionString
from couchbase.exceptions import CouchbaseError
import itertools
from collections import defaultdict
import warnings

class MixedAuthError(CouchbaseError):
    """
    Cannot use old and new style auth together in the same cluster
    """
    pass


class NoBucketError(CouchbaseError):
    """
    Operation requires at least a single bucket to be open
    """


class OverrideSet(set):
    def __init__(self, *args, **kwargs):
        super(OverrideSet,self).__init__(*args, **kwargs)


class OverrideDict(dict):
    def __init__(self, *args, **kwargs):
        super(OverrideDict,self).__init__(*args, **kwargs)


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
            auth_credentials_full = self.authenticator.get_credentials(bucket_name)
        else:
            auth_credentials_full = {'options': {}}

        auth_credentials = auth_credentials_full['options']

        connstr = ConnectionString.parse(str(self.connstr))

        connstr.bucket = bucket_name
        for attrib in set(auth_credentials) - {'password'}:
            connstr.set_option(attrib, auth_credentials[attrib])

        if isinstance(self.authenticator, CertAuthenticator):
            # TODO: _assert_clash_free_params/_assert_no_unwanted_keys including connstr options
            # should probably apply to PasswordAuthenticator as well,
            # but outside remit of PYCBC-487

            # TODO: do we accept clashing/unwanted options in connstr/kwargs e.g. password?
            # Here, we throw an exception
            # in the case of any clash/unwanted options, but we could scrub
            # clashing/unwanted options from connstr/kwargs

            param_dicts = dict(kwargs=OverrideDict(kwargs),
                              connstr=connstr.options,
                              auth_credential=auth_credentials)
            normalizer = Cluster.ParamNormaliser(param_dicts, self.authenticator)
            normalizer.handle_unwanted_keys()
            normalizer.assert_clash_free_params()

        if 'password' in kwargs:
            if isinstance(self.authenticator, PasswordAuthenticator):
                raise MixedAuthError("Cannot override "
                                     "PasswordAuthenticators password")
        else:
            kwargs['password'] = auth_credentials.get('password', None)
        connstr.scheme = auth_credentials_full.get('scheme', connstr.scheme)
        rv = self.bucket_class(str(connstr), **kwargs)
        self._buckets[bucket_name] = weakref.ref(rv)
        if isinstance(self.authenticator, ClassicAuthenticator):
            for bucket, passwd in self.authenticator.buckets.items():
                if passwd:
                    rv.add_bucket_creds(bucket, passwd)
        return rv

    class ParamNormaliser:
        def __init__(self, param_dicts, authenticator):
            typemap = {dict: set, OverrideDict: OverrideSet}
            self.param_keys = {k:typemap[type(v)](v) for k,v in param_dicts.items()}
            self.authenticator = authenticator
            self.clash_dict = defaultdict(defaultdict)
            for item in itertools.combinations(self.param_keys.items(), 2):
                clashes = item[0][1] & item[1][1]
                if clashes:
                    warnings.warn("{} and {} options overlap on keys {}".format(item[0][0], item[1][0], clashes))
                    self.clash_dict[item[0][0]][item[1][0]] = clashes
                    self.clash_dict[item[1][0]][item[0][0]] = clashes

        def handle_unwanted_keys(self):
            unwanted_keys = self.authenticator.unwanted_keys()
            clash_list=((k, unwanted_keys & self.param_keys[k]) for k in set(self.param_keys) - {'auth-credential'})
            clashes = {k:v for k,v in clash_list if v}
            self._handle_clashes(clashes)

        def assert_clash_free_params(self):
            auth_clashes = self.clash_dict.get('auth_credential')
            if auth_clashes:
                complaint = ', and'.join(
                    "param set {}: [{}] ".format(second_set, clashes)
                    for second_set, clashes in auth_clashes.items())
                raise MixedAuthError(
                    "{} param set: {} clashes with {}".format(type(self.authenticator),self.param_keys['auth_credential'], complaint))

        @staticmethod
        def _gen_complaint(param_dict):
            complaint = ", and".join(("{} contains {}".format(k, list(*entry)) for k, entry in param_dict.items()))
            return complaint

        def _get_generic_complaint(self,clash_param_dict):
            return "Invalid parameters used with {} - {}".format(type(self.authenticator),
                                                                 Cluster.ParamNormaliser._gen_complaint(
                                                                     clash_param_dict))

        def exception(self,clash_param_dict):
            raise MixedAuthError(self._get_generic_complaint(clash_param_dict))

        def warning(self,clash_param_dict):
            warnings.warn(self._get_generic_complaint(clash_param_dict))

        def _handle_clashes(self, clashes):
            if len(clashes):
                clash_dict = defaultdict(lambda: defaultdict(list))
                for clash, intersection in clashes.items():
                    clash_dict[isinstance(self.param_keys[clash], OverrideSet)][clash].append(intersection)

                action = {False: self.warning, True: self.exception}
                for is_override, param_dict in clash_dict.items():
                    action[is_override](param_dict)





    def cluster_manager(self):
        """
        Returns an instance of :class:`~.couchbase.admin.Admin` which may be
        used to create and manage buckets in the cluster.
        """
        credentials = self.authenticator.get_credentials()['options']
        connection_string = str(self.connstr)
        return Admin(credentials.get('username'), credentials.get('password'), connection_string=connection_string)

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
        :return: A dictionary of (optional) scheme and credentials e.g. `{'scheme':'couchbases',options:{'username':'fred', 'password':'opensesame'}}`
        """
        raise NotImplementedError()

    def unwanted_keys(self):
        return set()

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
        self.username=username
        self.password=password

    def get_credentials(self, *unused):
        return {'options':{'username':self.username,'password':self.password}}

    def unwanted_keys(self):
        return {'password'}


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
            return {'options':{'username':self.username, 'password':self.password}}
        return {'options':{'password':self.buckets.get(bucket)}}


class CertAuthenticator(Authenticator):
    def __init__(self, cert_path=None, key_path=None, trust_store_path=None, cluster_username=None,
                 cluster_password=None):
        """
        Certificate authentication mechanism.
        :param cluster_username:
            Global cluster username. Only required for management operations
        :param cluster_password:
            Global cluster password. Only required for management operations
        :param cert_path:
            Path of the CA key
        :param key_path:
            Path of the key
        :param trust_store_path:
            Path of the certificate trust store.
        """
        self.username = cluster_username
        self.password = cluster_password
        self.cert_path = cert_path
        self.key_path = key_path
        self.trust_store_path = trust_store_path

    def get_credentials(self, bucket=None):
        result = {'options':{'username':self.username, 'certpath':self.cert_path, 'keypath':self.key_path,
                      'truststorepath':self.trust_store_path}, 'scheme':'couchbases'}
        if not bucket:
            result['password']=self.password
        return result

    def unwanted_keys(self):
        return {'password'}
