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

import warnings

import pytest

from couchbase.logic.scope_req_builder import ScopeRequestBuilder
from couchbase.logic.supportability import CouchbaseDeprecationWarning
from couchbase.options import SearchOptions
from couchbase.search import TermQuery


def scope_name_warnings(recorded):
    return [w for w in recorded if issubclass(w.category, CouchbaseDeprecationWarning)
            and 'scope_name' in str(w.message)]


class ScopeRequestBuilderTestSuite:
    TEST_MANIFEST = [
        'test_search_request_falls_back_to_the_scope',
        'test_search_request_prefers_the_scope_name_option',
        'test_search_request_without_the_option_does_not_warn',
    ]

    def test_search_request_without_the_option_does_not_warn(self):
        builder = ScopeRequestBuilder('default', 'test-scope')

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            builder.build_search_request('test-index', TermQuery('term'), None)

        assert scope_name_warnings(recorded) == []

    def test_search_request_falls_back_to_the_scope(self):
        builder = ScopeRequestBuilder('default', 'test-scope')
        req = builder.build_search_request('test-index', TermQuery('term'), None)

        assert req.bucket_name == 'default'
        assert req.scope_name == 'test-scope'

    def test_search_request_prefers_the_scope_name_option(self):
        builder = ScopeRequestBuilder('default', 'test-scope')

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            options = SearchOptions(scope_name='other-scope')
            req = builder.build_search_request('test-index', TermQuery('term'), None, options)

        assert req.scope_name == 'other-scope'
        # setting the option is the caller's own choice and warns once; reading it back
        # to build the request is not, so nothing here may add a second warning
        assert len(scope_name_warnings(recorded)) == 1


class ClassicScopeRequestBuilderTests(ScopeRequestBuilderTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicScopeRequestBuilderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicScopeRequestBuilderTests) if valid_test_method(meth)]
        manifest_invalid = set(ScopeRequestBuilderTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
