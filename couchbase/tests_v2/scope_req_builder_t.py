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

import warnings
from typing import Any, cast

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability.handler import ObservableRequestHandler
from couchbase.logic.observability.no_op import NoOpMeter, NoOpTracer
from couchbase.logic.observability.observability_types import (ObservabilityInstruments,
                                                               TracerProtocol,
                                                               WrappedTracer)
from couchbase.logic.operation_types import StreamingOperationType
from couchbase.logic.scope_req_builder import ScopeRequestBuilder
from couchbase.logic.scope_types import SearchQueryRequest
from couchbase.logic.supportability import CouchbaseDeprecationWarning
from couchbase.options import SearchOptions
from couchbase.search import (SearchQuery,
                              SearchRequest,
                              TermQuery)

INDEX_NAME = 'test-index'
BUCKET_NAME = 'default'
SCOPE_NAME = 'test-scope'


def _obs_handler() -> ObservableRequestHandler:
    # See the twin helper in cluster_req_builder_t.py for why the tracer is cast: the public
    # `RequestTracer` ABC declares a narrower `parent_span` than `RequestTracerProtocol` does,
    # so no subclass of the documented base satisfies the protocol structurally.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(StreamingOperationType.SearchQuery, instruments)


def _builder() -> ScopeRequestBuilder:
    return ScopeRequestBuilder(BUCKET_NAME, SCOPE_NAME)


def _search_request(request: SearchRequest | SearchQuery, *options: Any) -> SearchQueryRequest:
    return _builder().build_search_request(INDEX_NAME, request, _obs_handler(), *options)


def _search_query_request(query: SearchQuery, *options: Any) -> SearchQueryRequest:
    return _builder().build_search_query_request(INDEX_NAME, query, _obs_handler(), *options)


def _deprecations(recorded: list[warnings.WarningMessage], subject: str) -> list[warnings.WarningMessage]:
    return [w for w in recorded
            if issubclass(w.category, CouchbaseDeprecationWarning) and subject in str(w.message)]


class ScopeRequestBuilderUnitTests:
    # No cb_env and no TEST_MANIFEST: each test builds a request and reads what it carries,
    # through the one `ScopeRequestBuilder` all three trees use.  Not generated for that
    # reason.
    #
    # The scope-level counterpart of cluster_req_builder_t, and the difference is the subject:
    # a scope builder knows the bucket and scope it was constructed for and stamps both onto
    # every request.  So a request's scope has two possible sources, the builder's own or a
    # `scope_name` option, and each test below has a counterpart on the other method.

    def test_search_request_accepts_a_search_request(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = _search_request(SearchRequest.create(TermQuery('term')))

        assert req.scope_name == SCOPE_NAME
        assert _deprecations(recorded, 'SearchQuery') == []

    def test_search_request_accepts_a_query_with_one_warning(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = _search_request(TermQuery('term'))

        # Addressed to the same scope as the current spelling, which makes it equivalent
        # rather than merely tolerated.
        assert req.scope_name == SCOPE_NAME
        assert len(_deprecations(recorded, 'SearchQuery')) == 1

    def test_search_request_falls_back_to_the_scope(self) -> None:
        req = _search_request(SearchRequest.create(TermQuery('term')))

        assert req.bucket_name == BUCKET_NAME
        assert req.scope_name == SCOPE_NAME

    def test_search_request_prefers_the_scope_name_option(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
            req = _search_request(SearchRequest.create(TermQuery('term')),
                                  SearchOptions(scope_name='other-scope'))

        assert req.scope_name == 'other-scope'
        # The bucket has no such option, so overriding the scope still leaves the request in
        # the bucket the builder was constructed for.
        assert req.bucket_name == BUCKET_NAME

    def test_search_query_request_prefers_the_scope_name_option(self) -> None:
        # The two methods resolve the scope with the same one-line expression, written twice.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
            req = _search_query_request(TermQuery('term'), SearchOptions(scope_name='other-scope'))

        assert req.scope_name == 'other-scope'
        assert req.bucket_name == BUCKET_NAME

    def test_scope_name_warns_once_per_request_and_not_at_all_per_options_object(self) -> None:
        # Building `SearchOptions` never warns: the option is a dict entry there, and the
        # deprecation lives on the `SearchQueryBuilder.scope_name` property, reached only when
        # the builder `setattr`s the option onto the query.  So the count follows requests
        # built, and reusing one options object for several searches warns each time.
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            options = SearchOptions(scope_name='other-scope')
            assert _deprecations(recorded, 'scope_name') == []

            _search_request(SearchRequest.create(TermQuery('term')), options)
            assert len(_deprecations(recorded, 'scope_name')) == 1

            _search_query_request(TermQuery('term'), options)

        assert len(_deprecations(recorded, 'scope_name')) == 2

    def test_search_request_without_the_option_does_not_warn(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            _search_request(SearchRequest.create(TermQuery('term')))

        assert _deprecations(recorded, 'scope_name') == []

    def test_search_query_request_accepts_a_query(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = _search_query_request(TermQuery('term'))

        assert req.bucket_name == BUCKET_NAME
        assert req.scope_name == SCOPE_NAME
        assert _deprecations(recorded, 'SearchQuery') == []

    def test_search_query_request_rejects_a_request(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            _search_query_request(SearchRequest.create(TermQuery('term')))  # type: ignore[arg-type]

        # Naming the entry point that does take a SearchRequest is the actionable half.
        assert 'search()' in str(exc_info.value)
