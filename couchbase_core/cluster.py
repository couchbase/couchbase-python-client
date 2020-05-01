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

from couchbase.auth import MixedAuthException, PasswordAuthenticator, ClassicAuthenticator, CertAuthenticator
from .client import Client
from .connstr import ConnectionString
import itertools
from collections import defaultdict
import warnings
from typing import *


class _OverrideSet(set):
    def __init__(self, *args, **kwargs):
        super(_OverrideSet, self).__init__(*args, **kwargs)


class _Cluster(object):
    # list of all authentication types, keep up to date, used to identify connstr/kwargs auth styles

    def __init__(self, connection_string='couchbase://localhost',
                 bucket_factory=Client):
        """
        Creates a new Cluster object
        :param connection_string: Base connection string. It is an error to
            specify a bucket in the string.
        :param Callable bucket_factory: factory that open_bucket will use to instantiate buckets
        """
        self.connstr = ConnectionString.parse(str(connection_string))
        self.bucket_factory = bucket_factory
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

    def open_bucket(self,  # type: _Cluster
                    bucket_name,  # type: str
                    **kwargs  # type: Any
                    ):
        # type: (...) -> Client
        """
        Open a new connection to a Couchbase bucket
        :param bucket_name: The name of the bucket to open
        :param kwargs: Additional arguments to provide to the constructor
        :return: The output of the `bucket_factory` object provided to
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

        normalizer = _Cluster._ParamNormaliser(self.authenticator, connstr, **kwargs)

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
                raise MixedAuthException("Cannot override "
                                         "PasswordAuthenticators password")
        else:
            kwargs['password'] = auth_credentials.get('password', None)
        connstr.scheme = auth_credentials_full.get('scheme', connstr.scheme)
        kwargs['bucket'] = bucket_name
        rv = self.bucket_factory(str(connstr), **kwargs)
        self._buckets[bucket_name] = weakref.ref(rv)
        if isinstance(self.authenticator, ClassicAuthenticator):
            for bucket, passwd in self.authenticator.buckets.items():
                if passwd:
                    rv.add_bucket_creds(bucket, passwd)
        return rv

    class _ParamNormaliser(object):

        _authentication_types = None
        _auth_unique_params = None

        @staticmethod
        def auth_types():
            # cache this calculation
            if not _Cluster._ParamNormaliser._authentication_types:
                _Cluster._ParamNormaliser._authentication_types = {CertAuthenticator, ClassicAuthenticator,
                                                                   PasswordAuthenticator}
                _Cluster._ParamNormaliser._auth_unique_params = {k.__name__: k.unique_keys() for k in
                                                                 _Cluster._ParamNormaliser._authentication_types}

        @property
        def authentication_types(self):
            _Cluster._ParamNormaliser.auth_types()
            return _Cluster._ParamNormaliser._authentication_types

        @property
        def auth_unique_params(self):
            _Cluster._ParamNormaliser.auth_types()
            return _Cluster._ParamNormaliser._auth_unique_params

        def __init__(self, authenticator, connstr, **kwargs):

            # build a dictionary of all potentially overlapping/conflicting parameter names

            self.param_keys = dict(kwargs=_OverrideSet(kwargs), connstr=set(connstr.options))
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
                raise MixedAuthException(str(self.critical_complaints))

        def check_no_unwanted_keys(self):
            """
            Check for definitely unwanted keys in any of the options
            for the active authentication type in use and
            throw a MixedAuthException if found.
            """
            unwanted_keys = self.auth_type.unwanted_keys() if self.auth_type else set()

            clash_list = ((k, self._entry(unwanted_keys, k)) for k in set(self.param_keys) - {'auth-credential'})
            self._handle_clashes({k: v for k, v in clash_list if v})

        def check_clash_free_params(self):
            """
            Check for clashes with the authenticator in use, and thrown a MixedAuthException if found.
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
                    clash_dict[isinstance(self.param_keys[clash], _OverrideSet)][clash].append(intersection)

                action = {False: self._warning, True: self._exception}
                for is_override, param_dict in clash_dict.items():
                    action[is_override](param_dict, self.auth_type)

        @staticmethod
        def _gen_complaint(param_dict):
            complaint = ", and".join(("{} contains {}".format(k, list(*entry)) for k, entry in param_dict.items()))
            return complaint

        def _get_generic_complaint(self, clash_param_dict, auth_type):
            return "Invalid parameters used with {}-style authentication - {}".format(auth_type.__name__,
                                                                                      _Cluster._ParamNormaliser._gen_complaint(
                                                                                          clash_param_dict))

        def _exception(self, clash_param_dict, auth_type):
            self.critical_complaints.append(self._get_generic_complaint(clash_param_dict, auth_type))

        def _warning(self, clash_param_dict, auth_type):
            warnings.warn(self._get_generic_complaint(clash_param_dict, auth_type))


