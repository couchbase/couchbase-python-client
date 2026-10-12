#  Copyright 2016-2026. Couchbase, Inc.
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

from typing import Any

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.eventing_function_mgmt_types import (EventingFunction,
                                                                     EventingFunctionBucketAccess,
                                                                     EventingFunctionBucketBinding,
                                                                     EventingFunctionUrlAuthBasic,
                                                                     EventingFunctionUrlAuthBearer,
                                                                     EventingFunctionUrlAuthDigest,
                                                                     EventingFunctionUrlBinding,
                                                                     EventingFunctionUrlNoAuth)

FULL_KEYSPACE = {'bucket': 'meta-bucket', 'scope': 'meta-scope', 'collection': 'meta-collection'}
DEFAULT_KEYSPACE = {'bucket': 'meta-bucket', 'scope': '_default', 'collection': '_default'}

# (built, exchanged, read back).  The third column is not the first for basic and digest: the
# server does not return their passwords, so `from_server` cannot put one back.
URL_BINDING_AUTH = [
    (EventingFunctionUrlNoAuth(),
     {'auth_type': 'no-auth'},
     EventingFunctionUrlNoAuth()),
    (EventingFunctionUrlAuthBasic('username', 'password'),
     {'auth_type': 'basic', 'username': 'username', 'password': 'password'},
     EventingFunctionUrlAuthBasic('username')),
    (EventingFunctionUrlAuthDigest('username', 'password'),
     {'auth_type': 'digest', 'username': 'username', 'password': 'password'},
     EventingFunctionUrlAuthDigest('username')),
    (EventingFunctionUrlAuthBearer('bearer-key'),
     {'auth_type': 'bearer', 'key': 'bearer-key'},
     EventingFunctionUrlAuthBearer('bearer-key')),
]


def _function(**overrides: Any) -> EventingFunction:
    return EventingFunction.from_server({'name': 'test-function', **overrides})


def _url_binding(auth: Any) -> EventingFunctionUrlBinding:
    return EventingFunctionUrlBinding(hostname='http://localhost:5000',
                                      alias='urlBinding',
                                      allow_cookies=True,
                                      validate_ssl_certificate=False,
                                      auth=auth)


class EventingFunctionTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: every test feeds a server-shaped dict to a `from_server`
    # classmethod, and `couchbase.management.logic.eventing_function_mgmt_types` holds the only
    # definitions.  Not generated for that reason.
    #
    # Decoding is the half `eventingmgmt_t` cannot reach: it builds keyspaces and bindings as
    # fixtures and hands them to management calls, which exercises the constructors and the
    # outbound `as_dict()` and never reads a payload back.  The inbound and outbound spellings
    # differ in two separate places, neither visible to a suite that only writes.

    def test_keyspaces_from_server(self) -> None:
        func = _function(metadata_keyspace=FULL_KEYSPACE,
                         source_keyspace={'bucket': 'src-bucket', 'scope': 'src-scope',
                                          'collection': 'src-collection'})

        assert func.metadata_keyspace.bucket == 'meta-bucket'
        assert func.metadata_keyspace.scope == 'meta-scope'
        assert func.metadata_keyspace.collection == 'meta-collection'
        assert func.source_keyspace.bucket == 'src-bucket'
        assert func.source_keyspace.scope == 'src-scope'
        assert func.source_keyspace.collection == 'src-collection'

    def test_keyspaces_default_scope_and_collection_from_server(self) -> None:
        # The server names the default scope and collection explicitly; the SDK reports them
        # as absent, so `scope` reads None rather than the string '_default'.
        func = _function(metadata_keyspace=DEFAULT_KEYSPACE)

        assert func.metadata_keyspace.bucket == 'meta-bucket'
        assert func.metadata_keyspace.scope is None
        assert func.metadata_keyspace.collection is None

    def test_keyspaces_round_trip(self) -> None:
        func = _function(metadata_keyspace=FULL_KEYSPACE)

        assert func.as_dict()['metadata_keyspace'] == FULL_KEYSPACE

    def test_a_default_keyspace_does_not_round_trip(self) -> None:
        # The consequence of the test above: decoding drops the two '_default' names and
        # `as_dict()` omits what is None, so a function read and written back names the bucket
        # alone.  That is safe only because absent and '_default' mean the same thing to the
        # server, which is not something this decode can promise.
        func = _function(metadata_keyspace=DEFAULT_KEYSPACE)

        assert func.as_dict()['metadata_keyspace'] == {'bucket': 'meta-bucket'}

    def test_bucket_binding_keyspace_from_server(self) -> None:
        binding = EventingFunctionBucketBinding.from_server({
            'alias': 'alias',
            'access': 'r',
            'name': {'bucket': 'bound-bucket', 'scope': 'bound-scope',
                     'collection': 'bound-collection'},
        })

        assert binding.alias == 'alias'
        assert binding.access == EventingFunctionBucketAccess.ReadOnly
        assert binding.name.bucket == 'bound-bucket'
        assert binding.name.scope == 'bound-scope'
        assert binding.name.collection == 'bound-collection'

    @pytest.mark.parametrize('access, expected, written',
                             [('r', EventingFunctionBucketAccess.ReadOnly, 'read_only'),
                              ('read_only', EventingFunctionBucketAccess.ReadOnly, 'read_only'),
                              ('rw', EventingFunctionBucketAccess.ReadWrite, 'read_write'),
                              ('read_write', EventingFunctionBucketAccess.ReadWrite, 'read_write')])
    def test_bucket_access_reads_either_spelling_and_writes_the_long_one(
            self,
            access: str,
            expected: EventingFunctionBucketAccess,
            written: str) -> None:
        # Three spellings for two values: the enum member's own value is the short form,
        # decoding accepts either, and `to_str()` always emits the long one.  So reading the
        # enum value is not the same as reading what will be sent.
        binding = EventingFunctionBucketBinding.from_server(
            {'alias': 'alias', 'access': access, 'name': {'bucket': 'bound-bucket'}})

        assert binding.access is expected
        assert binding.as_dict()['access'] == written

    def test_bucket_access_rejects_an_unknown_value(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            EventingFunctionBucketAccess.from_server('write_only')

        assert 'write_only' in str(exc_info.value)

    # The four auth kinds, read three ways: out, in, and out and back.  Separate tests because
    # the table's first and third columns differ for two of the four, so one round-trip test
    # could not say which half dropped the password.
    @pytest.mark.parametrize('auth, binding_auth, _', URL_BINDING_AUTH)
    def test_url_binding_auth_as_dict(self,
                                      auth: Any,
                                      binding_auth: dict[str, Any],
                                      _: Any) -> None:
        assert _url_binding(auth).as_dict()['auth'] == binding_auth

    @pytest.mark.parametrize('_, binding_auth, expected_auth', URL_BINDING_AUTH)
    def test_url_binding_auth_from_server(self,
                                          _: Any,
                                          binding_auth: dict[str, Any],
                                          expected_auth: Any) -> None:
        binding = EventingFunctionUrlBinding.from_server({
            'hostname': 'http://localhost:5000',
            'alias': 'urlBinding',
            'allow_cookies': True,
            'validate_ssl_certificate': False,
            'auth': binding_auth,
        })

        assert binding.alias == 'urlBinding'
        assert binding.auth == expected_auth

    @pytest.mark.parametrize('auth, _, expected_auth', URL_BINDING_AUTH)
    def test_url_binding_auth_round_trip(self,
                                         auth: Any,
                                         _: Any,
                                         expected_auth: Any) -> None:
        binding = _url_binding(auth)

        assert EventingFunctionUrlBinding.from_server(binding.as_dict()).auth == expected_auth
