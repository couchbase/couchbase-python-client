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

from couchbase.common._observability.handler import ObservableRequestHandler
from couchbase.common._observability.no_op import NoOpMeter, NoOpTracer
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
                               QueryOptions,
                               SearchOptions)
from couchbase.protocol.classic._scope_req_builder import ScopeRequestBuilder
from couchbase.protocol.classic._scope_types import SearchQueryRequest
from couchbase.search import (SearchQuery,
                              SearchRequest,
                              TermQuery)
from tests.test_features import TestProtocol

# What is checked here is classic's request builder, which writes what its binding reads.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

INDEX_NAME = 'test-index'
BUCKET_NAME = 'default'
SCOPE_NAME = 'test-scope'


def _obs_handler() -> ObservableRequestHandler:
    # See the twin helper in cluster_req_builder_t.py for why the tracer is cast: the public
    # `RequestTracer` ABC declares a narrower `parent_span` than `RequestTracerProtocol` does,
    # so no subclass of the documented base satisfies the protocol structurally.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(Op.SearchQuery, instruments)


def _builder(option_validation: OptionValidationMode | None = None) -> ScopeRequestBuilder:
    return ScopeRequestBuilder(BUCKET_NAME, SCOPE_NAME, option_validation)


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

    @pytest.mark.parametrize('build, statement', [('build_query_request', 'n1ql_query'),
                                                  ('build_analytics_query_request', 'analytics_query')])
    def test_num_workers_is_accepted_and_not_sent(self, build: str, statement: str) -> None:
        # Any other keyword is a named parameter of the statement.
        req = getattr(_builder(), build)('SELECT $a', _obs_handler(), num_workers=2, a=1)

        assert getattr(req, statement).params['named_parameters'] == {'$a': b'1'}

    @pytest.mark.parametrize('options, kwargs, expected', [((), {}, None),
                                                           ((QueryOptions(adhoc=False),), {}, False),
                                                           ((), {'adhoc': False}, False)])
    def test_query_request_carries_adhoc_only_when_set(self,
                                                       options: tuple[QueryOptions, ...],
                                                       kwargs: dict[str, Any],
                                                       expected: bool | None) -> None:
        req = _builder().build_query_request('SELECT 1', _obs_handler(), *options, **kwargs)

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
                req = getattr(_builder(), build)('SELECT 1', _obs_handler(), *options, **kwargs)

            assert [str(w.message) for w in recorded if issubclass(w.category, CouchbaseOptionWarning)] == [
                f'Option {written} is not carried by {options_type.__name__}, so it is treated as a named '
                f'query parameter. Did you mean {meant}? To send it as a parameter, pass it in '
                'named_parameters.']
            assert f'${written}' in getattr(req, statement).params['named_parameters']

    def test_named_parameters_far_from_an_option_do_not_warn(self) -> None:
        # `type` is documented usage, and a dict is a positional parameter rather than options.
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            _builder().build_query_request('SELECT 1', _obs_handler(), {'readonly': True}, type='hotel')

        assert [w for w in recorded if issubclass(w.category, CouchbaseOptionWarning)] == []

    def test_near_miss_raises_in_strict_mode(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            _builder(OptionValidationMode.Strict).build_query_request('SELECT 1', _obs_handler(), readonly=True)

        assert 'Did you mean read_only? To send it as a parameter, pass it in named_parameters.' in str(
            exc_info.value)

    @pytest.mark.parametrize('build, statement, options_type',
                             [('build_query_request', 'n1ql_query', QueryOptions),
                              ('build_analytics_query_request', 'analytics_query', AnalyticsOptions)])
    def test_near_miss_in_named_parameters_is_sent_in_strict_mode(self,
                                                                  build: str,
                                                                  statement: str,
                                                                  options_type: type[dict[str, Any]]) -> None:
        req = getattr(_builder(OptionValidationMode.Strict), build)(
            'SELECT 1', _obs_handler(), options_type(named_parameters={'time': 1}))

        assert getattr(req, statement).params['named_parameters'] == {'$time': b'1'}

    # The search builder reads the keys it knows and drops the rest, so a near miss is
    # reported here or not at all.
    @pytest.mark.parametrize('build', ['build_search_request', 'build_search_query_request'])
    def test_search_near_miss_warns_and_is_not_sent(self, build: str) -> None:
        # Bound to the wider type, because the typed constructor refuses the misspelt keyword.
        options_type: type[dict[str, Any]] = SearchOptions
        for options, kwargs in ((None, {'limt': 5}), (options_type(limt=5), {})):
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter('always')
                req = getattr(_builder(), build)(INDEX_NAME, TermQuery('term'), _obs_handler(), options, **kwargs)

            assert [str(w.message) for w in recorded if issubclass(w.category, CouchbaseOptionWarning)] == [
                'Option limt is not carried by SearchOptions. Did you mean limit?']
            assert 'limt' not in req.query_builder.as_encodable()

            with pytest.raises(InvalidArgumentException, match='Did you mean limit?'):
                getattr(_builder(OptionValidationMode.Strict), build)(
                    INDEX_NAME, TermQuery('term'), _obs_handler(), options, **kwargs)

    @pytest.mark.parametrize('build', ['build_search_request', 'build_search_query_request'])
    def test_scope_name_is_judged_as_an_option_that_acts(self, build: str) -> None:
        # A scope search sends the request to the scope it names, so the judgement must not
        # call it inert: that would warn on a working call, and raise on it in strict mode.
        for mode in (None, OptionValidationMode.Strict):
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter('always')
                req = getattr(_builder(mode), build)(INDEX_NAME, TermQuery('term'), _obs_handler(),
                                                     SearchOptions(scope_name='other-scope'))

            assert req.scope_name == 'other-scope'
            assert [w for w in recorded if issubclass(w.category, CouchbaseApplicabilityWarning)] == []
