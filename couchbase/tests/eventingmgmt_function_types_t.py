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

import pytest

from couchbase.management.logic.eventing_function_mgmt_types import (EventingFunction,
                                                                     EventingFunctionBucketAccess,
                                                                     EventingFunctionBucketBinding,
                                                                     EventingFunctionUrlAuthBasic,
                                                                     EventingFunctionUrlAuthBearer,
                                                                     EventingFunctionUrlAuthDigest,
                                                                     EventingFunctionUrlBinding,
                                                                     EventingFunctionUrlNoAuth)

# (auth, the tagged dict the binding layer exchanges, what a read back yields).
# from_server() does not read the basic and digest passwords back, since the server
# does not return them.
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


class EventingFunctionTypesTestSuite:
    TEST_MANIFEST = [
        'test_bucket_binding_keyspace_from_server',
        'test_keyspaces_default_scope_and_collection_from_server',
        'test_keyspaces_from_server',
        'test_keyspaces_round_trip',
        'test_url_binding_auth_as_dict',
        'test_url_binding_auth_from_server',
        'test_url_binding_auth_round_trip',
    ]

    def test_keyspaces_from_server(self):
        server_dict = {
            'name': 'test-function',
            'metadata_keyspace': {'bucket': 'meta-bucket', 'scope': 'meta-scope',
                                  'collection': 'meta-collection'},
            'source_keyspace': {'bucket': 'src-bucket', 'scope': 'src-scope',
                                'collection': 'src-collection'},
        }
        func = EventingFunction.from_server(server_dict)

        assert func.metadata_keyspace.bucket == 'meta-bucket'
        assert func.metadata_keyspace.scope == 'meta-scope'
        assert func.metadata_keyspace.collection == 'meta-collection'
        assert func.source_keyspace.bucket == 'src-bucket'
        assert func.source_keyspace.scope == 'src-scope'
        assert func.source_keyspace.collection == 'src-collection'

    def test_keyspaces_default_scope_and_collection_from_server(self):
        server_dict = {
            'name': 'test-function',
            'metadata_keyspace': {'bucket': 'meta-bucket', 'scope': '_default',
                                  'collection': '_default'},
        }
        func = EventingFunction.from_server(server_dict)

        assert func.metadata_keyspace.bucket == 'meta-bucket'
        assert func.metadata_keyspace.scope is None
        assert func.metadata_keyspace.collection is None

    def test_keyspaces_round_trip(self):
        keyspace = {'bucket': 'meta-bucket', 'scope': 'meta-scope', 'collection': 'meta-collection'}
        func = EventingFunction.from_server({'name': 'test-function', 'metadata_keyspace': keyspace})

        assert func.as_dict()['metadata_keyspace'] == keyspace

    def test_bucket_binding_keyspace_from_server(self):
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

    @pytest.mark.parametrize('auth, binding_auth, _', URL_BINDING_AUTH)
    def test_url_binding_auth_as_dict(self, auth, binding_auth, _):
        binding = EventingFunctionUrlBinding(hostname='http://localhost:5000',
                                             alias='urlBinding',
                                             allow_cookies=True,
                                             validate_ssl_certificate=False,
                                             auth=auth)

        assert binding.as_dict()['auth'] == binding_auth

    @pytest.mark.parametrize('_, binding_auth, expected_auth', URL_BINDING_AUTH)
    def test_url_binding_auth_from_server(self, _, binding_auth, expected_auth):
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
    def test_url_binding_auth_round_trip(self, auth, _, expected_auth):
        binding = EventingFunctionUrlBinding(hostname='http://localhost:5000',
                                             alias='urlBinding',
                                             allow_cookies=True,
                                             validate_ssl_certificate=False,
                                             auth=auth)

        assert EventingFunctionUrlBinding.from_server(binding.as_dict()).auth == expected_auth


class ClassicEventingFunctionTypesTests(EventingFunctionTypesTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicEventingFunctionTypesTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicEventingFunctionTypesTests) if valid_test_method(meth)]
        manifest_invalid = set(EventingFunctionTypesTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
