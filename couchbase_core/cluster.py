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

from .admin import Admin
from .bucket import Bucket
from .connstr import ConnectionString
from .exceptions import CouchbaseError
import itertools
from collections import defaultdict
import warnings
from typing import *


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
        super(OverrideSet, self).__init__(*args, **kwargs)


class OverrideDict(dict):
    def __init__(self, *args, **kwargs):
        super(OverrideDict, self).__init__(*args, **kwargs)


class Cluster(object):
    # list of all authentication types, keep up to date, used to identify connstr/kwargs auth styles

    def __init__(self, connection_string='couchbase://localhost',
                 bucket_class=Bucket):
        """
        Creates a new Cluster object
        :param connection_string: Base connection string. It is an error to
            specify a bucket in the string.
        :param bucket_class: :class:`couchbase_core.bucket.Bucket` implementation to
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
        # type: (str, str) -> Bucket
        """
        Open a new connection to a Couchbase bucket
        :param bucket_name: The name of the bucket to open
        :param kwargs: Additional arguments to provide to the constructor
        :return: An instance of the `bucket_class` object provided to
            :meth:`__init__`
        """
        if self.authenticator:
            auth_credentials_full = self.authenticator.get_auto_credentials(bucket_name)
        else:
            auth_credentials_full = {'options': {}}

        auth_credentials = auth_credentials_full['options']

        connstr = ConnectionString.parse(str(self.connstr))

        connstr.bucket = bucket_name
        for attrib in set(auth_credentials) - {'password'}:
            connstr.set_option(attrib, auth_credentials[attrib])

        # Check if there are conflicting authentication types in any of the parameters
        # Also sets its own 'auth_type' field to the type of authentication it
        # thinks is being specified

        normalizer = Cluster.ParamNormaliser(self.authenticator, connstr, **kwargs)

        # we don't do anything with this information unless the Normaliser thinks
        # Cert Auth is involved as this is outside the remit of PYCBC-487/488/489

        if issubclass(normalizer.auth_type, CertAuthenticator) or issubclass(type(self.authenticator),
                                                                             CertAuthenticator):
            # TODO: check_clash_free_params/check_no_unwanted_keys including connstr options
            # should probably apply to PasswordAuthenticator as well,
            # but outside remit of PYCBC-487

            # TODO: do we accept clashing/unwanted options in connstr/kwargs e.g. password?
            # Here, we throw an exception
            # in the case of any clash/unwanted options, but we could scrub
            # clashing/unwanted options from connstr/kwargs

            normalizer.check_no_unwanted_keys()
            normalizer.check_clash_free_params()
            normalizer.assert_no_critical_complaints()

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

        _authentication_types = None
        _auth_unique_params = None

        @staticmethod
        def auth_types():
            # cache this calculation
            if not Cluster.ParamNormaliser._authentication_types:
                Cluster.ParamNormaliser._authentication_types = {CertAuthenticator, ClassicAuthenticator,
                                                                 PasswordAuthenticator}
                Cluster.ParamNormaliser._auth_unique_params = {k.__name__: k.unique_keys() for k in
                                                               Cluster.ParamNormaliser._authentication_types}

        @property
        def authentication_types(self):
            Cluster.ParamNormaliser.auth_types()
            return Cluster.ParamNormaliser._authentication_types

        @property
        def auth_unique_params(self):
            Cluster.ParamNormaliser.auth_types()
            return Cluster.ParamNormaliser._auth_unique_params

        def __init__(self, authenticator, connstr, **kwargs):

            # build a dictionary of all potentially overlapping/conflicting parameter names

            self.param_keys = dict(kwargs=OverrideSet(kwargs), connstr=set(connstr.options))
            self.param_keys.update({'auth_credential': authenticator.unique_keys()} if authenticator else {})
            self.param_keys.update(self.auth_unique_params)

            self.authenticator = authenticator

            # compare each parameter set with one another to look for overlaps

            self.critical_complaints = []

            self._build_clash_dict()
            self.auth_type = type(self.authenticator)
            self._check_for_auth_type_clashes()

        def assert_no_critical_complaints(self):
            if self.critical_complaints:
                raise MixedAuthError(str(self.critical_complaints))

        def check_no_unwanted_keys(self):
            """
            Check for definitely unwanted keys in any of the options
            for the active authentication type in use and
            throw a MixedAuthError if found.
            """
            unwanted_keys = self.auth_type.unwanted_keys() if self.auth_type else set()

            clash_list = ((k, self._entry(unwanted_keys, k)) for k in set(self.param_keys) - {'auth-credential'})
            self._handle_clashes({k: v for k, v in clash_list if v})

        def check_clash_free_params(self):
            """
            Check for clashes with the authenticator in use, and thrown a MixedAuthError if found.
            """
            auth_clashes = self.clash_dict.get('auth_credential')
            if auth_clashes:
                actual_clashes = {k: v for k, v in auth_clashes.items() if self.auth_type.__name__ != k}
                if actual_clashes:
                    complaint = ', and'.join(
                        "param set {}: [{}] ".format(second_set, clashes)
                        for second_set, clashes in auth_clashes.items())
                    self.critical_complaints.append(
                        "{} param set: {} clashes with {}".format(self.auth_type.__name__,
                                                                  self.param_keys['auth_credential'], complaint))

        def _build_clash_dict(self):
            self.clash_dict = defaultdict(defaultdict)
            # build a dictionary {'first_set_name':{'second_set_name':{'key1','key2'}}} listing all overlaps
            for item in itertools.combinations(self.param_keys.items(), 2):
                clashes = item[0][1] & item[1][1]
                if clashes:
                    warnings.warn("{} and {} options overlap on keys {}".format(item[0][0], item[1][0], clashes))
                    # make dictionary bidirectional, so we can list all clashes for a given param set directly
                    self.clash_dict[item[0][0]][item[1][0]] = clashes
                    self.clash_dict[item[1][0]][item[0][0]] = clashes

        def _check_for_auth_type_clashes(self):
            connstr_auth_type_must_be = self._get_types_with_unique_parameters()

            # are there multiple types this should definitely be, or have we
            # is an Authenticator already set which clashes with the detected
            # Authenticator types from the parameters?

            if len(connstr_auth_type_must_be) > 1 or (
                    self.authenticator and connstr_auth_type_must_be - {self.auth_type}):
                self._build_auth_type_complaints(connstr_auth_type_must_be)

            if connstr_auth_type_must_be:
                self.auth_type, = connstr_auth_type_must_be

        def _get_types_with_unique_parameters(self):
            """
            :return: Set of Authenticator types which the params potentially could
            represent

            """
            connstr_auth_type_must_be = set()
            for auth_type in self.authentication_types - {self.auth_type}:
                auth_clashes = self.clash_dict.get(auth_type.__name__)
                if auth_clashes:
                    connstr_auth_type_must_be.add(auth_type)
            return connstr_auth_type_must_be

        def _build_auth_type_complaints(self, connstr_auth_type_must_be):
            complaints = []
            for auth_type in connstr_auth_type_must_be:
                complaints.append("parameters {params} overlap on {auth_type}".format(auth_type=auth_type.__name__,
                                                                                      params=self.clash_dict.get(
                                                                                          auth_type.__name__)))
            self.critical_complaints.append("clashing params: {}{}".format(
                "got authenticator type {} but ".format(
                    type(self.authenticator).__name__) if self.authenticator else "",
                ", and".join(complaints)))

        def _entry(self, unwanted_keys, key):
            return unwanted_keys & self.param_keys[key]

        def _handle_clashes(self, clashes):
            if len(clashes):
                clash_dict = defaultdict(lambda: defaultdict(list))
                for clash, intersection in clashes.items():
                    clash_dict[isinstance(self.param_keys[clash], OverrideSet)][clash].append(intersection)

                action = {False: self._warning, True: self._exception}
                for is_override, param_dict in clash_dict.items():
                    action[is_override](param_dict, self.auth_type)

        @staticmethod
        def _gen_complaint(param_dict):
            complaint = ", and".join(("{} contains {}".format(k, list(*entry)) for k, entry in param_dict.items()))
            return complaint

        def _get_generic_complaint(self, clash_param_dict, auth_type):
            return "Invalid parameters used with {}-style authentication - {}".format(auth_type.__name__,
                                                                                      Cluster.ParamNormaliser._gen_complaint(
                                                                                          clash_param_dict))

        def _exception(self, clash_param_dict, auth_type):
            self.critical_complaints.append(self._get_generic_complaint(clash_param_dict, auth_type))

        def _warning(self, clash_param_dict, auth_type):
            warnings.warn(self._get_generic_complaint(clash_param_dict, auth_type))

    def cluster_manager(self):
        """
        Returns an instance of :class:`~.couchbase_core.admin.Admin` which may be
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
        from couchbase_core.n1ql import N1QLQuery
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
        Gets the credentials for a specified bucket. If bucket is
        `None`, gets the username and password for the entire cluster, if
        different.
        :param bucket: The bucket to act as context
        :return: A dictionary of (optional) scheme and credentials e.g. `{'scheme':'couchbases',options:{'username':'fred', 'password':'opensesame'}}`
        """
        return self.get_auto_credentials(bucket)

    @classmethod
    def unwanted_keys(cls):
        """
        The set of option keys that are definitely incompatible with this authentication style.
        """
        return set()

    @classmethod
    def unique_keys(cls):
        """
        The set of option keys, if any, that this authenticator uniquely possesses.
        """
        return set(cls.get_unique_creds_dict().keys())

    @classmethod
    def get_unique_creds_dict(cls):
        """
        User overridable
        A dictionary of authenticator-unique options and functions/lambdas of the form:
            function(self):
                return self.password
        e.g.
        {'certpath': lambda self: self.certpath}
        """
        return {}

    def get_cred_bucket(self, bucket):
        """
        :param bucket:
        :return: returns the non-unique parts of the credentials for bucket authentication,
        as a dictionary of functions, e.g.:
        'options': {'username': self.username}, 'scheme': 'couchbases'}
        """
        raise NotImplementedError()

    def get_cred_not_bucket(self):
        """
        :param bucket:
        :return: returns the non-unique parts of the credentials for admin access
        as a dictionary of functions, e.g.:
        {'options':{'password': self.password}}
        """
        raise NotImplementedError()

    def get_auto_credentials(self, bucket):
        """
        :param bucket:
        :return: returns a dictionary of credentials for bucket/admin
        authentication
        """

        result = {k: v(self) for k, v in self.get_unique_creds_dict().items()}
        if bucket:
            result.update(self.get_cred_bucket(bucket))
        else:
            result.update(self.get_cred_not_bucket())
        return result


class PasswordAuthenticator(Authenticator):
    def __init__(self,
                 username,  # type: str
                 password  # type: str
                 ):
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

    def get_cred_bucket(self, *unused):
        return {'options': {'username': self.username, 'password': self.password}}

    def get_cred_not_bucket(self):
        return self.get_cred_bucket()

    @classmethod
    def unwanted_keys(cls):
        return {'password'}


class ClassicAuthenticator(Authenticator):
    def __init__(self, cluster_username=None,
                 cluster_password=None,
                 buckets=None):
        # type: (str, str, Mapping[str,str]) -> None
        """
        Classic authentication mechanism.
        :param cluster_username:
            Global cluster username. Only required for management operations
        :type cluster_username: str
        :param cluster_password:
            Global cluster password. Only required for management operations
        :param buckets:
            A dictionary of `{bucket_name: bucket_password}`.
        """
        self.username = cluster_username
        self.password = cluster_password
        self.buckets = buckets if buckets else {}

    def get_cred_not_bucket(self):
        return {'options': {'username': self.username, 'password': self.password}}

    def get_cred_bucket(self, bucket):
        return {'options': {'password': self.buckets.get(bucket)}}


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
        self.certpath = cert_path
        self.keypath = key_path
        self.trust_store_path = trust_store_path

    @classmethod
    def get_unique_creds_dict(clazz):
        return {'certpath': lambda self: self.certpath, 'keypath': lambda self: self.keypath,
                'truststorepath': lambda self: self.trust_store_path}

    def get_cred_bucket(self, *unused):
        return {'options': {'username': self.username}, 'scheme': 'couchbases'}

    def get_cred_not_bucket(self):
        return {'options': {'password': self.password}}

    @classmethod
    def unwanted_keys(cls):
        return {'password'}

    def get_credentials(self, bucket=None):
        return self.get_auto_credentials(bucket)
