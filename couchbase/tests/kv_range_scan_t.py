#  Copyright 2016-2024. Couchbase, Inc.
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

from collections.abc import Generator
from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from couchbase.exceptions import (AlreadyQueriedException,
                                  DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException)
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.logic.collection_req_builder import CollectionRequestBuilder
from couchbase.logic.collection_types import CollectionDetails
from couchbase.mutation_state import MutationState
from couchbase.options import ScanOptions
from couchbase.result import (MutationToken,
                              ScanResult,
                              ScanResultIterable)
from couchbase.transcoder import JSONTranscoder
from tests.assert_helpers import cb_assert_eq, cb_assert_isinstance
from tests.environments.test_environment import (CollectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.test_features import EnvironmentFeatures, ServerFeatures


class RangeScanTestSuite(SuiteRequirements):
    # Runs against both the default collection and a NAMED_EMPTY one.  DEFAULT covers
    # scanning `_default`.`_default`, a distinct keyspace argument on the scan request;
    # NAMED_EMPTY covers a collection holding nothing but this suite's corpus, which is what
    # lets sampling scans validate row content.  Under DEFAULT a sampling scan can
    # legitimately return another suite's document, so content validation is skipped there.

    # The scan corpus is written by the test_mutation_state fixture, not load_data().
    NUM_DOCS = 0

    NUM_SCAN_DOCS = 100

    # Deliberately no REQUIRED_FEATURES.  This suite gates in both directions:
    # test_range_scan_feature_unavailable asserts the behaviour when KeyValueRangeScan is
    # absent, so a class-level gate would skip the one test that needs the feature missing.

    TEST_MANIFEST = [
        'test_prefix_scan',
        'test_prefix_scan_with_batch_byte_limit',
        'test_prefix_scan_with_batch_item_limit',
        'test_prefix_scan_with_concurrency',
        'test_range_scan',
        'test_range_scan_cancel',
        'test_range_scan_default_terms',
        'test_range_scan_exclusive',
        'test_range_scan_feature_unavailable',
        'test_range_scan_ids_only',
        'test_range_scan_with_batch_byte_limit',
        'test_range_scan_with_batch_item_limit',
        'test_range_scan_with_concurrency',
        'test_range_scan_with_zero_concurrency',
        'test_sampling_scan',
        'test_sampling_scan_with_batch_byte_limit',
        'test_sampling_scan_with_batch_item_limit',
        'test_sampling_scan_with_concurrency',
        'test_sampling_scan_with_negative_limit',
        'test_sampling_scan_with_seed',
        'test_sampling_scan_with_zero_limit',
    ]

    @pytest.fixture(scope='class')
    def check_range_scan_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.KeyValueRangeScan, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_range_scan_not_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_not_supported(ServerFeatures.KeyValueRangeScan, cb_env.server_context)

    @pytest.fixture(scope='class')
    def test_id(self) -> str:
        return str(uuid4())

    @pytest.fixture(scope='class')
    def test_ids(self, test_id: str) -> list[str]:
        return [f'{test_id}-{i}' for i in range(self.NUM_SCAN_DOCS)]

    @pytest.fixture(scope='class')
    def test_mutation_state(self, cb_env: SyncTestEnvironment, test_ids: list[str]) -> MutationState:
        # Passed as ScanOptions(consistent_with=...) so a scan never races the upserts.
        results = [cb_env.collection.upsert(doc_id, {'id': doc_id}) for doc_id in test_ids]
        return MutationState(*results)

    def _purge_temp_docs(self, cb_env: SyncTestEnvironment, test_ids: list[str]) -> None:
        for doc_id in test_ids:
            try:
                cb_env.collection.remove(doc_id)
            except DocumentNotFoundException:
                pass

    def _validate_result(self,
                         result: ScanResultIterable,
                         expected_count: int = 0,
                         ids_only: bool = False,
                         from_sample: bool = False,
                         skip_content_validation: bool = False) -> list[ScanResult]:
        cb_assert_isinstance(result, ScanResultIterable, what='scan result')
        rows: list[ScanResult] = []
        for r in result:
            ctx = f'scan row {len(rows)}'
            cb_assert_isinstance(r, ScanResult, what='scan row', ctx=ctx)
            if ids_only:
                # An ids_only row carries no metadata and no content; every accessor
                # below is expected to reject rather than return a placeholder.
                with pytest.raises(InvalidArgumentException):
                    r.expiry_time
                with pytest.raises(InvalidArgumentException):
                    r.cas
                with pytest.raises(InvalidArgumentException):
                    r.content_as[str]
            else:
                assert r.cas is not None, f'{ctx}: cas must be set on a content row.'
                assert r.id is not None, f'{ctx}: id must be set on a content row.'
                if not skip_content_validation:
                    # Deserialization is inside the guard on purpose.  A sampling scan on the
                    # shared `_default` returns whatever other suites left there, and bare
                    # integers make `content_as[dict]` raise on a document this suite never
                    # wrote.
                    content = r.content_as[dict]
                    assert content is not None, f'{ctx}: content must be set on a content row.'
                    cb_assert_eq(content, {'id': r.id}, what='row content', ctx=f'{ctx} id={r.id!r}')
                # Regression guard (PYCBC-1778): ScanResult.expiry_time previously read the
                # 'cas' field instead of 'expiry', handing a huge HLC value to
                # datetime.fromtimestamp() and raising.  Accessing it on a content row must
                # not raise; the value is either None (no expiry) or a datetime.
                expiry_time = r.expiry_time
                assert expiry_time is None or isinstance(expiry_time, datetime), (
                    f'{ctx}: expiry_time must be None or datetime, got {expiry_time!r}.'
                )
            rows.append(r)

        if from_sample is True:
            assert len(rows) <= expected_count, (
                f'Sampling scan returned {len(rows)} rows, more than the limit {expected_count}.'
            )
        else:
            assert len(rows) >= expected_count, (
                f'Scan returned {len(rows)} rows, fewer than the expected {expected_count}.'
            )

        # Returned unconditionally: a `return_rows` flag would only make the return type
        # Optional and force a narrowing assert on every caller that did want them.
        return rows

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_prefix_scan(self,
                         cb_env: SyncTestEnvironment,
                         test_id: str,
                         test_ids: list[str],
                         test_mutation_state: MutationState) -> None:
        scan_type = PrefixScan(f'{test_id}')
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = self._validate_result(res, self.NUM_SCAN_DOCS, ids_only=True)
        for r in rows:
            assert r.id in test_ids

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_byte_limit', [0, 1, 25, 100])
    def test_prefix_scan_with_batch_byte_limit(self,
                                               cb_env: SyncTestEnvironment,
                                               test_id: str,
                                               test_mutation_state: MutationState,
                                               batch_byte_limit: int) -> None:
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_byte_limit=batch_byte_limit,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, self.NUM_SCAN_DOCS)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_item_limit', [0, 1, 25, 100])
    def test_prefix_scan_with_batch_item_limit(self,
                                               cb_env: SyncTestEnvironment,
                                               test_id: str,
                                               test_mutation_state: MutationState,
                                               batch_item_limit: int) -> None:
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_item_limit=batch_item_limit,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, self.NUM_SCAN_DOCS)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('concurrency', [1, 2, 4, 16, 64, 128])
    def test_prefix_scan_with_concurrency(self,
                                          cb_env: SyncTestEnvironment,
                                          test_id: str,
                                          test_mutation_state: MutationState,
                                          concurrency: int) -> None:
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 concurrency=concurrency,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, self.NUM_SCAN_DOCS)

    # First scan of the class, and the one that absorbs the switch between collection
    # strategies: on the NAMED_EMPTY leg the collection was created moments earlier and its
    # manifest still has to reach every KV node (possibly CXXCBC-492).
    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_range_scan(self, cb_env: SyncTestEnvironment, test_id: str, test_mutation_state: MutationState) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, 12)

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_range_scan_cancel(self,
                               cb_env: SyncTestEnvironment,
                               test_id: str,
                               test_ids: list[str],
                               test_mutation_state: MutationState) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}'))
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = []
        # A hand-rolled counter rather than enumerate(): under acouchbase res.rows() is an
        # async iterator, which enumerate() neither accepts nor has a counterpart for, so an
        # enumerate() source has no valid `async for` form to generate.
        idx = 0
        for r in res.rows():
            if idx == 2:
                res.cancel_scan()
            rows.append(r)
            idx += 1
        assert len(rows) < len(test_ids)
        assert len(rows) == 3
        with pytest.raises(AlreadyQueriedException):
            res.rows()

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_range_scan_default_terms(self, cb_env: SyncTestEnvironment, test_ids: list[str]) -> None:
        # Default terms scan the entire collection: exactly this suite's corpus on the
        # NAMED_EMPTY leg, and that corpus plus whatever else lives in `_default` on the
        # other. Hence the >= comparison rather than an equality.
        scan_type = RangeScan()
        res = cb_env.collection.scan(scan_type, ids_only=True)
        self._validate_result(res, len(test_ids), ids_only=True)

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_range_scan_exclusive(self,
                                  cb_env: SyncTestEnvironment,
                                  test_id: str,
                                  test_mutation_state: MutationState) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1', True), ScanTerm(f'{test_id}-2', True))
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            consistent_with=test_mutation_state))
        rows = self._validate_result(res, 10)
        ids = [r.id for r in rows]
        assert f'{test_id}-1' not in ids
        assert f'{test_id}-2' not in ids

    @pytest.mark.usefixtures('check_range_scan_not_supported')
    def test_range_scan_feature_unavailable(self,
                                            cb_env: SyncTestEnvironment,
                                            test_id: str,
                                            test_mutation_state: MutationState) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 consistent_with=test_mutation_state))
        # scan() itself succeeds; the server's rejection surfaces on iteration.
        with pytest.raises(FeatureUnavailableException):
            self._validate_result(res)

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_range_scan_ids_only(self,
                                 cb_env: SyncTestEnvironment,
                                 test_id: str,
                                 test_mutation_state: MutationState) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        self._validate_result(res, 12, ids_only=True)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_byte_limit', [0, 1, 25, 100])
    def test_range_scan_with_batch_byte_limit(self,
                                              cb_env: SyncTestEnvironment,
                                              test_id: str,
                                              test_mutation_state: MutationState,
                                              batch_byte_limit: int) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_byte_limit=batch_byte_limit,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, 12)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_item_limit', [0, 1, 25, 100])
    def test_range_scan_with_batch_item_limit(self,
                                              cb_env: SyncTestEnvironment,
                                              test_id: str,
                                              test_mutation_state: MutationState,
                                              batch_item_limit: int) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_item_limit=batch_item_limit,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, 12)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('concurrency', [1, 2, 4, 16, 64, 128])
    def test_range_scan_with_concurrency(self,
                                         cb_env: SyncTestEnvironment,
                                         test_id: str,
                                         test_mutation_state: MutationState,
                                         concurrency: int) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 concurrency=concurrency,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, 12)

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_range_scan_with_zero_concurrency(self,
                                              cb_env: SyncTestEnvironment,
                                              test_id: str,
                                              test_mutation_state: MutationState) -> None:
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.scan(scan_type,
                                   ScanOptions(timeout=timedelta(seconds=10),
                                               concurrency=0,
                                               consistent_with=test_mutation_state))

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_sampling_scan(self, cb_env: SyncTestEnvironment, test_mutation_state: MutationState) -> None:
        limit = 10
        skip_content = not cb_env.is_clean_collection
        scan_type = SamplingScan(limit)
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=False,
                                                            consistent_with=test_mutation_state))
        self._validate_result(res, limit, ids_only=False, from_sample=True,
                              skip_content_validation=skip_content)

    # @FINDING(PYCBC-1858): the three test_sampling_scan_with_* cases below build a
    # PrefixScan, not a SamplingScan, so they duplicate the test_prefix_scan_with_* cases and
    # add no sampling coverage.  Switching them runs three option paths against a scan type
    # they have never been combined with, which wants its own review.
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_byte_limit', [0, 1, 25, 100])
    def test_sampling_scan_with_batch_byte_limit(self,
                                                 cb_env: SyncTestEnvironment,
                                                 test_id: str,
                                                 test_mutation_state: MutationState,
                                                 batch_byte_limit: int) -> None:
        skip_content = not cb_env.is_clean_collection
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_byte_limit=batch_byte_limit,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, self.NUM_SCAN_DOCS, from_sample=True,
                              skip_content_validation=skip_content)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_item_limit', [0, 1, 25, 100])
    def test_sampling_scan_with_batch_item_limit(self,
                                                 cb_env: SyncTestEnvironment,
                                                 test_id: str,
                                                 test_mutation_state: MutationState,
                                                 batch_item_limit: int) -> None:
        skip_content = not cb_env.is_clean_collection
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_item_limit=batch_item_limit,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, self.NUM_SCAN_DOCS, from_sample=True,
                              skip_content_validation=skip_content)

    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('concurrency', [1, 2, 4, 16, 64, 128])
    def test_sampling_scan_with_concurrency(self,
                                            cb_env: SyncTestEnvironment,
                                            test_id: str,
                                            test_mutation_state: MutationState,
                                            concurrency: int) -> None:
        skip_content = not cb_env.is_clean_collection
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 concurrency=concurrency,
                                                 consistent_with=test_mutation_state))
        self._validate_result(res, self.NUM_SCAN_DOCS, from_sample=True,
                              skip_content_validation=skip_content)

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_sampling_scan_with_negative_limit(self,
                                               cb_env: SyncTestEnvironment,
                                               test_mutation_state: MutationState) -> None:
        scan_type = SamplingScan(-10)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.scan(scan_type,
                                   ScanOptions(timeout=timedelta(seconds=10),
                                               consistent_with=test_mutation_state))

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_sampling_scan_with_seed(self, cb_env: SyncTestEnvironment, test_mutation_state: MutationState) -> None:
        limit = 10
        skip_content = not cb_env.is_clean_collection
        scan_type = SamplingScan(limit, 50)
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = self._validate_result(res, limit, ids_only=True, from_sample=True,
                                     skip_content_validation=skip_content)
        result_ids = [r.id for r in rows]

        # Same seed, same sample.
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = self._validate_result(res, limit, ids_only=True, from_sample=True,
                                     skip_content_validation=skip_content)
        compare_ids = [r.id for r in rows]
        # Only a collection nothing else writes to holds still between the two scans.  In
        # `_default` another worker's documents land in the sample, so the seed is checked on
        # the NAMED_EMPTY run alone.
        if cb_env.is_clean_collection:
            assert result_ids == compare_ids

    @pytest.mark.usefixtures('check_range_scan_supported')
    def test_sampling_scan_with_zero_limit(self,
                                           cb_env: SyncTestEnvironment,
                                           test_mutation_state: MutationState) -> None:
        scan_type = SamplingScan(0)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.scan(scan_type,
                                   ScanOptions(timeout=timedelta(seconds=10),
                                               consistent_with=test_mutation_state))


class RangeScanTests(RangeScanTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(RangeScanTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env',
                    params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED_EMPTY])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest,
                                   test_ids: list[str]) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, RangeScanTestSuite, request.param)
        yield env
        if request.param == CollectionStrategy.DEFAULT:
            # The corpus went into the shared `_default` collection, so it has to be
            # removed by hand or it leaks into every other suite that scans there.  The
            # NAMED_EMPTY leg needs no equivalent: teardown drops the scope, corpus
            # included.
            self._purge_temp_docs(env, test_ids)
        teardown_environment(env, RangeScanTestSuite, request.param)


class RangeScanRequestBuilderUnitTests:
    # Regression coverage for the consistent_with serialization bug (PYCBC-1778).  No cb_env:
    # nothing here talks to a cluster.

    @staticmethod
    def _builder() -> CollectionRequestBuilder:
        # A real transcoder rather than None: the field is declared non-optional and the SDK
        # always populates it.  Nothing under test reads it, so it only has to be well-typed.
        details = CollectionDetails('default', '_default', '_default', JSONTranscoder())
        return CollectionRequestBuilder(details)

    @staticmethod
    def _mutation_state(count: int) -> MutationState:
        state = MutationState()
        for i in range(count):
            state.add_mutation_token(MutationToken({'partition_id': i,
                                                    'partition_uuid': 1000 + i,
                                                    'sequence_number': 1,
                                                    'bucket_name': 'default'}))
        return state

    def test_consistent_with_serialized_as_mutation_state(self) -> None:
        opts = {'consistent_with': self._mutation_state(3), 'concurrency': 1}
        self._builder()._process_scan_orchestrator_ops(opts)

        # Must be a mutation_state dict ({'tokens': [...]}), not a bare list, or the C++
        # binding drops every token.
        consistent_with = opts['consistent_with']
        assert isinstance(consistent_with, dict)
        assert list(consistent_with.keys()) == ['tokens']

        tokens = consistent_with['tokens']
        assert isinstance(tokens, list)
        assert len(tokens) == 3
        for token in tokens:
            assert set(token.keys()) == {'partition_id', 'partition_uuid',
                                         'sequence_number', 'bucket_name'}

    def test_empty_consistent_with_raises(self) -> None:
        opts = {'consistent_with': MutationState()}
        with pytest.raises(InvalidArgumentException):
            self._builder()._process_scan_orchestrator_ops(opts)

    def test_zero_concurrency_raises(self) -> None:
        opts = {'concurrency': 0}
        with pytest.raises(InvalidArgumentException):
            self._builder()._process_scan_orchestrator_ops(opts)
