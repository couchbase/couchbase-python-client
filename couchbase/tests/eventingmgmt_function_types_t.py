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

from datetime import timedelta
from typing import Any

import pytest

from couchbase.common.management.eventing import (EventingFunction,
                                                  EventingFunctionBucketAccess,
                                                  EventingFunctionBucketBinding,
                                                  EventingFunctionDcpBoundary,
                                                  EventingFunctionDeploymentStatus,
                                                  EventingFunctionKeyspace,
                                                  EventingFunctionLanguageCompatibility,
                                                  EventingFunctionLogLevel,
                                                  EventingFunctionProcessingStatus,
                                                  EventingFunctionSettings,
                                                  EventingFunctionUrlAuthBasic,
                                                  EventingFunctionUrlAuthBearer,
                                                  EventingFunctionUrlAuthDigest,
                                                  EventingFunctionUrlBinding,
                                                  EventingFunctionUrlNoAuth)
from couchbase.exceptions import InvalidArgumentException

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
    return EventingFunction.from_server({'name': 'test-function', 'code': 'function OnUpdate(doc, meta) {}',
                                         **overrides})


def _keyspace(keyspace: EventingFunctionKeyspace | None) -> EventingFunctionKeyspace:
    assert keyspace is not None
    return keyspace


def _url_binding(auth: Any) -> EventingFunctionUrlBinding:
    return EventingFunctionUrlBinding(hostname='http://localhost:5000',
                                      alias='urlBinding',
                                      allow_cookies=True,
                                      validate_ssl_certificate=False,
                                      auth=auth)


class EventingFunctionTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: every test feeds a server-shaped dict to a `from_server`
    # classmethod, and `couchbase.common.management.eventing` holds the only
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

        metadata, source = _keyspace(func.metadata_keyspace), _keyspace(func.source_keyspace)
        assert metadata.bucket == 'meta-bucket'
        assert metadata.scope == 'meta-scope'
        assert metadata.collection == 'meta-collection'
        assert source.bucket == 'src-bucket'
        assert source.scope == 'src-scope'
        assert source.collection == 'src-collection'

    def test_keyspaces_default_scope_and_collection_from_server(self) -> None:
        # The server names the default scope and collection explicitly; the SDK reports them
        # as absent, so `scope` reads None rather than the string '_default'.
        metadata = _keyspace(_function(metadata_keyspace=DEFAULT_KEYSPACE).metadata_keyspace)

        assert metadata.bucket == 'meta-bucket'
        assert metadata.scope is None
        assert metadata.collection is None

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
        name = _keyspace(binding.name)
        assert name.bucket == 'bound-bucket'
        assert name.scope == 'bound-scope'
        assert name.collection == 'bound-collection'

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

    def test_bucket_access_reads_an_unknown_value_as_unknown(self) -> None:
        binding = EventingFunctionBucketBinding.from_server(
            {'alias': 'alias', 'access': 'write_only', 'name': {'bucket': 'bound-bucket'}})

        assert binding.access is EventingFunctionBucketAccess.Unknown
        assert binding.as_dict()['access'] == 'unknown'

    # A server release can add a value, and reading the function must not fail on it.  Writing it
    # back is refused by upsert_function, since the binding would send its default in its place.
    @pytest.mark.parametrize('field, unknown',
                             [('dcp_stream_boundary', EventingFunctionDcpBoundary.Unknown),
                              ('deployment_status', EventingFunctionDeploymentStatus.Unknown),
                              ('processing_status', EventingFunctionProcessingStatus.Unknown),
                              ('log_level', EventingFunctionLogLevel.Unknown),
                              ('language_compatibility', EventingFunctionLanguageCompatibility.Unknown)])
    def test_a_setting_the_sdk_cannot_name_reads_as_unknown(self, field: str, unknown: object) -> None:
        settings = EventingFunctionSettings.from_server({field: 'not-a-real-value'})

        assert getattr(settings, field) is unknown
        assert settings.as_dict()[field] == 'unknown'

    # The binding leaves a missing access at read_write, the documented default.
    def test_bucket_binding_without_access_omits_it(self) -> None:
        binding = EventingFunctionBucketBinding(alias='alias', name=EventingFunctionKeyspace('bound-bucket'))

        assert binding.as_dict() == {'alias': 'alias', 'name': {'bucket': 'bound-bucket'}}

    def test_bucket_binding_without_a_name_is_rejected(self) -> None:
        binding = EventingFunctionBucketBinding(alias='alias', access=EventingFunctionBucketAccess.ReadOnly)

        with pytest.raises(InvalidArgumentException):
            binding.as_dict()

    def test_function_instance_id_from_server(self) -> None:
        assert _function(function_instance_id='instance-id').function_instance_id == 'instance-id'

    # The binding reads lcb_timeout in whole seconds, as it does execution_timeout.
    def test_settings_send_lcb_timeout(self) -> None:
        settings = EventingFunctionSettings(lcb_timeout=timedelta(seconds=5),
                                            execution_timeout=timedelta(seconds=7))

        assert settings.as_dict() == {'lcb_timeout': 5, 'execution_timeout': 7}

    def test_settings_lcb_timeout_round_trip(self) -> None:
        settings = EventingFunctionSettings(lcb_timeout=timedelta(seconds=5))

        assert EventingFunctionSettings.from_server(settings.as_dict()).lcb_timeout == timedelta(seconds=5)

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
