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
from collections.abc import Callable
from datetime import timedelta
from typing import Any, cast

import pytest

from couchbase.common._observability.handler import ObservableRequestHandler
from couchbase.common._observability.no_op import (NoOpMeter,
                                                   NoOpSpan,
                                                   NoOpTracer)
from couchbase.common._observability.observability_types import (ObservabilityInstruments,
                                                                 TracerProtocol,
                                                                 WrappedTracer)
from couchbase.common._operations import Op
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.warnings import (CouchbaseApplicabilityWarning,
                                       CouchbaseDeprecationWarning,
                                       CouchbaseOptionWarning)
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import (AnalyticsOptions,
                               DiagnosticsOptions,
                               PingOptions,
                               QueryOptions,
                               SearchOptions,
                               ViewOptions)
from couchbase.protocol.classic._bucket_req_builder import BucketRequestBuilder
from couchbase.protocol.classic._cluster_req_builder import ClusterRequestBuilder
from couchbase.search import (SearchQuery,
                              SearchRequest,
                              TermQuery)
from couchbase.vector_search import VectorQuery, VectorSearch
from tests.test_features import TestProtocol

# What is checked here is classic's request builder, which writes what its binding reads.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

INDEX_NAME = 'test-index'


def _obs_handler() -> ObservableRequestHandler:
    # A builder stores the handler and never calls it, so no-op instruments are enough.  The
    # cast is not about NoOpTracer: `RequestTracerProtocol.request_span` declares
    # `parent_span: Optional[RequestSpanProtocol]` where the public `RequestTracer` ABC every
    # tracer derives from declares the narrower `Optional[RequestSpan]`, so no subclass of the
    # documented base satisfies the protocol structurally.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(Op.SearchQuery, instruments)


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

    # Neither operation is traced, so a parent span has nowhere to go: it is accepted, not sent,
    # and reported as having no effect, which strict mode refuses.
    @pytest.mark.parametrize('build', [
        lambda mode: ClusterRequestBuilder(mode).build_ping_request(PingOptions(parent_span=NoOpSpan('p'))),
        lambda mode: BucketRequestBuilder('default', mode).build_ping_request(PingOptions(parent_span=NoOpSpan('p'))),
        lambda mode: ClusterRequestBuilder(mode).build_diagnostics_request(
            DiagnosticsOptions(parent_span=NoOpSpan('p'))),
    ], ids=['cluster ping', 'bucket ping', 'diagnostics'])
    def test_parent_span_is_accepted_and_not_sent(self, build: Callable[[OptionValidationMode], Any]) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            req = build(OptionValidationMode.Warn)

        assert 'parent_span' not in vars(req)
        assert len([w for w in recorded if issubclass(w.category, CouchbaseApplicabilityWarning)]) == 1
        with pytest.raises(InvalidArgumentException, match='parent_span'):
            build(OptionValidationMode.Strict)

    @pytest.mark.parametrize('build, statement', [('build_query_request', 'n1ql_query'),
                                                  ('build_analytics_query_request', 'analytics_query')])
    def test_num_workers_is_accepted_and_not_sent(self, build: str, statement: str) -> None:
        # Any other keyword is a named parameter of the statement.
        req = getattr(ClusterRequestBuilder(), build)('SELECT $a', _obs_handler(), num_workers=2, a=1)

        assert getattr(req, statement).params['named_parameters'] == {'$a': b'1'}

    @pytest.mark.parametrize('options, kwargs, expected', [((), {}, None),
                                                           ((QueryOptions(adhoc=False),), {}, False),
                                                           ((), {'adhoc': False}, False)])
    def test_query_request_carries_adhoc_only_when_set(self,
                                                       options: tuple[QueryOptions, ...],
                                                       kwargs: dict[str, Any],
                                                       expected: bool | None) -> None:
        req = ClusterRequestBuilder().build_query_request('SELECT 1', _obs_handler(), *options, **kwargs)

        assert req.n1ql_query.params.get('adhoc', None) is expected

    @pytest.mark.parametrize('build, statement, options_type, written, meant',
                             [('build_query_request', 'n1ql_query', QueryOptions, 'readonly', 'read_only'),
                              ('build_analytics_query_request', 'analytics_query', AnalyticsOptions,
                               'priorty', 'priority')])
    def test_near_miss_warns_and_is_still_sent(self,
                                               build: str,
                                               statement: str,
                                               options_type: type[dict[str, Any]],
                                               written: str,
                                               meant: str) -> None:
        # Any keyword these blocks do not declare is a named parameter, so only one close to a
        # declared option is reported, and it is sent all the same.  The options object's keys
        # are judged with the keywords, since an undeclared one is sent the same way.
        for options, kwargs in (((), {written: True}), ((options_type(**{written: True}),), {})):
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter('always')
                req = getattr(ClusterRequestBuilder(), build)('SELECT 1', _obs_handler(), *options, **kwargs)

            assert [str(w.message) for w in recorded if issubclass(w.category, CouchbaseOptionWarning)] == [
                f'Option {written} is not carried by {options_type.__name__}, so it is treated as a named '
                f'query parameter. Did you mean {meant}? To send it as a parameter, pass it in '
                'named_parameters.']
            assert f'${written}' in getattr(req, statement).params['named_parameters']

    def test_named_parameters_far_from_an_option_do_not_warn(self) -> None:
        # `type` is documented usage, and a dict is a positional parameter rather than options.
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            ClusterRequestBuilder().build_query_request('SELECT 1', _obs_handler(), {'readonly': True}, type='hotel')

        assert [w for w in recorded if issubclass(w.category, CouchbaseOptionWarning)] == []

    def test_near_miss_raises_in_strict_mode(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            ClusterRequestBuilder(OptionValidationMode.Strict).build_query_request(
                'SELECT 1', _obs_handler(), readonly=True)

        assert 'Did you mean read_only? To send it as a parameter, pass it in named_parameters.' in str(
            exc_info.value)

    @pytest.mark.parametrize('build, statement, options_type',
                             [('build_query_request', 'n1ql_query', QueryOptions),
                              ('build_analytics_query_request', 'analytics_query', AnalyticsOptions)])
    def test_near_miss_in_named_parameters_is_sent_in_strict_mode(self,
                                                                  build: str,
                                                                  statement: str,
                                                                  options_type: type[dict[str, Any]]) -> None:
        req = getattr(ClusterRequestBuilder(OptionValidationMode.Strict), build)(
            'SELECT 1', _obs_handler(), options_type(named_parameters={'time': 1}))

        assert getattr(req, statement).params['named_parameters'] == {'$time': b'1'}

    # View and search builders read the keys they know and drop the rest, so a near miss is
    # reported here or not at all.
    @pytest.mark.parametrize('build, options_type', [
        (lambda mode, options, **kwargs: BucketRequestBuilder('default', mode).build_view_query_request(
            'ddoc', 'view', _obs_handler(), options, **kwargs).view_query.as_encodable(), ViewOptions),
        (lambda mode, options, **kwargs: ClusterRequestBuilder(mode).build_search_request(
            INDEX_NAME, SearchRequest.create(TermQuery('term')), _obs_handler(), options,
            **kwargs).query_builder.as_encodable(), SearchOptions),
        (lambda mode, options, **kwargs: ClusterRequestBuilder(mode).build_search_query_request(
            INDEX_NAME, TermQuery('term'), _obs_handler(), options, **kwargs).query_builder.as_encodable(),
         SearchOptions),
    ], ids=['view', 'search', 'search_query'])
    def test_near_miss_warns_and_is_not_sent(self,
                                             build: Callable[..., dict[str, Any]],
                                             options_type: type[dict[str, Any]]) -> None:
        for options, kwargs in ((None, {'limt': 5}), (options_type(limt=5), {})):
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter('always')
                encoded = build(None, options, **kwargs)

            assert [str(w.message) for w in recorded if issubclass(w.category, CouchbaseOptionWarning)] == [
                f'Option limt is not carried by {options_type.__name__}. Did you mean limit?']
            assert 'limt' not in encoded and 'limit' not in encoded

            with pytest.raises(InvalidArgumentException, match='Did you mean limit?'):
                build(OptionValidationMode.Strict, options, **kwargs)

    @pytest.mark.parametrize('build, options', [
        (lambda options, **kwargs: BucketRequestBuilder('default', OptionValidationMode.Strict)
         .build_view_query_request('ddoc', 'view', _obs_handler(), options, **kwargs),
         ViewOptions(limit=5, skip=1)),
        (lambda options, **kwargs: ClusterRequestBuilder(OptionValidationMode.Strict).build_search_request(
            INDEX_NAME, SearchRequest.create(TermQuery('term')), _obs_handler(), options, **kwargs),
         SearchOptions(limit=5, skip=1)),
    ], ids=['view', 'search'])
    def test_declared_options_pass_strict_mode(self, build: Callable[..., Any], options: dict[str, Any]) -> None:
        build(options)
        build(None, limit=5, skip=1, timeout=timedelta(seconds=5))
