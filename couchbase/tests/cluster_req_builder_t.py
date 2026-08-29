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

import json
import warnings

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.cluster_req_builder import ClusterRequestBuilder
from couchbase.logic.supportability import CouchbaseDeprecationWarning
from couchbase.search import SearchRequest, TermQuery
from couchbase.vector_search import VectorQuery, VectorSearch


def search_query_warnings(recorded):
    return [w for w in recorded if issubclass(w.category, CouchbaseDeprecationWarning)
            and 'SearchQuery' in str(w.message)]


class ClusterRequestBuilderTestSuite:
    TEST_MANIFEST = [
        'test_search_query_request_accepts_a_query',
        'test_search_query_request_rejects_a_request',
        'test_search_request_accepts_a_query_with_a_warning',
        'test_search_request_carries_what_only_a_request_can_express',
    ]

    def test_search_request_accepts_a_query_with_a_warning(self):
        builder = ClusterRequestBuilder()

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = builder.build_search_request('test-index', SearchRequest.create(TermQuery('term')), None)
            assert search_query_warnings(recorded) == []

            deprecated = builder.build_search_request('test-index', TermQuery('term'), None)

        assert req is not None and deprecated is not None
        assert len(search_query_warnings(recorded)) == 1

    def test_search_request_carries_what_only_a_request_can_express(self):
        builder = ClusterRequestBuilder()
        vector_search = VectorSearch.from_vector_query(VectorQuery('vector_field', [0.1, 0.2]))

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = builder.build_search_request('test-index', SearchRequest.create(vector_search), None)

        encoded = req.query_builder.as_encodable()
        # a vector-only request has no search query of its own, so the builder substitutes one
        assert json.loads(encoded['query']) == {'match_none': None}
        assert 'vector_search' in encoded
        assert search_query_warnings(recorded) == []

    def test_search_query_request_accepts_a_query(self):
        builder = ClusterRequestBuilder()

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = builder.build_search_query_request('test-index', TermQuery('term'), None)

        assert req is not None
        assert search_query_warnings(recorded) == []

    def test_search_query_request_rejects_a_request(self):
        builder = ClusterRequestBuilder()

        with pytest.raises(InvalidArgumentException):
            builder.build_search_query_request('test-index', SearchRequest.create(TermQuery('term')), None)


class ClassicClusterRequestBuilderTests(ClusterRequestBuilderTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicClusterRequestBuilderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicClusterRequestBuilderTests) if valid_test_method(meth)]
        manifest_invalid = set(ClusterRequestBuilderTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
