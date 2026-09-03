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

import json
import warnings
from typing import Any, cast

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.cluster_req_builder import ClusterRequestBuilder
from couchbase.logic.observability.handler import ObservableRequestHandler
from couchbase.logic.observability.no_op import NoOpMeter, NoOpTracer
from couchbase.logic.observability.observability_types import (ObservabilityInstruments,
                                                               TracerProtocol,
                                                               WrappedTracer)
from couchbase.logic.operation_types import StreamingOperationType
from couchbase.logic.supportability import CouchbaseDeprecationWarning
from couchbase.search import (SearchQuery,
                              SearchRequest,
                              TermQuery)
from couchbase.vector_search import VectorQuery, VectorSearch

INDEX_NAME = 'test-index'


def _obs_handler() -> ObservableRequestHandler:
    # A builder stores the handler and never calls it, so no-op instruments are enough.  The
    # cast is not about NoOpTracer: `RequestTracerProtocol.request_span` declares
    # `parent_span: Optional[RequestSpanProtocol]` where the public `RequestTracer` ABC every
    # tracer derives from declares the narrower `Optional[RequestSpan]`, so no subclass of the
    # documented base satisfies the protocol structurally.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(StreamingOperationType.SearchQuery, instruments)


def _search_request(request: SearchRequest | SearchQuery, *options: Any) -> dict[str, Any]:
    req = ClusterRequestBuilder().build_search_request(INDEX_NAME, request, _obs_handler(), *options)
    return dict(req.query_builder.as_encodable())


def _search_query_request(query: SearchQuery, *options: Any) -> dict[str, Any]:
    req = ClusterRequestBuilder().build_search_query_request(INDEX_NAME, query, _obs_handler(), *options)
    return dict(req.query_builder.as_encodable())


def _search_query_deprecations(recorded: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    return [w for w in recorded
            if issubclass(w.category, CouchbaseDeprecationWarning) and 'SearchQuery' in str(w.message)]


class ClusterRequestBuilderUnitTests:
    # No cb_env and no TEST_MANIFEST: each test builds a request and reads the dict it would
    # hand the core, through the one `ClusterRequestBuilder` all three trees use.  Not
    # generated for that reason.
    #
    # Two builders and not one: `search()` takes a `SearchRequest` and accepts a bare
    # `SearchQuery` with a deprecation warning, while `search_query()` takes a `SearchQuery`
    # and refuses a `SearchRequest` outright.  The pair to compare is the accepting test
    # against the refusing one.

    def test_search_request_accepts_a_search_request(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            encoded = _search_request(SearchRequest.create(TermQuery('term')))

        assert encoded['index_name'] == INDEX_NAME
        assert json.loads(encoded['query']) == {'term': 'term'}
        assert _search_query_deprecations(recorded) == []

    def test_search_request_accepts_a_query_with_one_warning(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            deprecated = _search_request(TermQuery('term'))

        # Built the same way down either branch, so the deprecated spelling is equivalent
        # rather than merely tolerated.
        assert json.loads(deprecated['query']) == {'term': 'term'}
        assert len(_search_query_deprecations(recorded)) == 1

    def test_only_the_request_branch_sends_show_request(self) -> None:
        # The one key the two branches differ by: `create_search_query_from_request` writes
        # it explicitly and `create_search_query_object` leaves it out.  The property reads
        # `_params.get('show_request', False)` either way, so the difference exists only in
        # what is sent.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
            from_query = _search_request(TermQuery('term'))
        from_request = _search_request(SearchRequest.create(TermQuery('term')))

        assert from_request['show_request'] is False
        assert 'show_request' not in from_query

    def test_search_request_carries_what_only_a_request_can_express(self) -> None:
        vector_search = VectorSearch.from_vector_query(VectorQuery('vector_field', [0.1, 0.2]))

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            encoded = _search_request(SearchRequest.create(vector_search))

        # A vector-only request has no search query of its own, so the builder substitutes one
        # that matches nothing rather than sending a request with no query.
        assert json.loads(encoded['query']) == {'match_none': None}
        assert json.loads(encoded['vector_search']) == [{'field': 'vector_field',
                                                         'k': 3,
                                                         'vector': [0.1, 0.2]}]
        assert _search_query_deprecations(recorded) == []

    def test_search_query_request_accepts_a_query(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            encoded = _search_query_request(TermQuery('term'))

        assert encoded['index_name'] == INDEX_NAME
        assert json.loads(encoded['query']) == {'term': 'term'}
        assert _search_query_deprecations(recorded) == []

    def test_search_query_request_rejects_a_request(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            _search_query_request(SearchRequest.create(TermQuery('term')))  # type: ignore[arg-type]

        # Naming the entry point that does take a SearchRequest is the actionable half.
        assert 'search()' in str(exc_info.value)
