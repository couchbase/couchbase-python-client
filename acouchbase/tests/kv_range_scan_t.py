#  Copyright 2016-2023. Couchbase, Inc.
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

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import (DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException)
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.mutation_state import MutationState
from couchbase.options import ScanOptions
from couchbase.result import ScanResult, ScanResultIterable
from tests.environments import CollectionType
from tests.test_features import EnvironmentFeatures


class RangeScanTestSuite:
    TEST_MANIFEST = [
        'test_range_scan',
        'test_range_scan_exclusive',
        'test_range_scan_ids_only',
        'test_range_scan_default_terms',
        'test_prefix_scan',
        'test_sampling_scan',
        'test_sampling_scan_with_seed',
        'test_range_scan_with_batch_byte_limit',
        'test_range_scan_with_batch_item_limit',
        'test_range_scan_with_concurrency',
        'test_prefix_scan_with_batch_byte_limit',
        'test_prefix_scan_with_batch_item_limit',
        'test_prefix_scan_with_concurrency',
        'test_sampling_scan_with_batch_byte_limit',
        'test_sampling_scan_with_batch_item_limit',
        'test_sampling_scan_with_concurrency',
        'test_range_scan_with_zero_concurrency',
        'test_sampling_scan_with_zero_limit',
        'test_sampling_scan_with_negative_limit',
        'test_range_scan_feature_unavailable',
    ]

    @pytest_asyncio.fixture(scope='session')
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope='class')
    def check_range_scan_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('kv_range_scan',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_range_scan_not_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_not_supported('kv_range_scan',
                                                           cb_env.server_version_short,
                                                           cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def test_id(self):
        scan_uuid = str(uuid4())
        return scan_uuid

    @pytest.fixture(scope="class")
    def test_ids(self, test_id):
        scan_ids = [f'{test_id}-{i}' for i in range(100)]
        return scan_ids

    @pytest_asyncio.fixture(scope="class")
    async def test_mutation_state(self, test_ids, cb_env):
        results = []
        for doc_id in test_ids:
            res = await cb_env.collection.upsert(doc_id, {'id': doc_id})
            results.append(res)

        return MutationState(*results)

    async def _purge_temp_docs(self, cb_env, test_ids):
        for doc_id in test_ids:
            try:
                await cb_env.collection.remove(doc_id)
            except DocumentNotFoundException:
                pass

    async def _validate_result(self, result, expected_count=0, ids_only=False, return_rows=False):
        assert isinstance(result, ScanResultIterable)
        rows = []
        async for r in result:
            assert isinstance(r, ScanResult)
            if ids_only:
                with pytest.raises(InvalidArgumentException):
                    r.expiry_time
                with pytest.raises(InvalidArgumentException):
                    r.cas
                with pytest.raises(InvalidArgumentException):
                    r.content_as[str]
            else:
                assert r.cas is not None
                assert r.id is not None
                content = r.content_as[dict]
                assert content is not None
                assert content == {'id': r.id}
            rows.append(r)

        assert len(rows) >= expected_count

        if return_rows:
            return rows

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_range_scan(self, cb_env, test_id, test_mutation_state):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 12)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_range_scan_exclusive(self, cb_env, test_id, test_mutation_state):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1', True), ScanTerm(f'{test_id}-2', True))
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            consistent_with=test_mutation_state))
        rows = await self._validate_result(res, 10, return_rows=True)
        ids = [r.id for r in rows]
        assert f'{test_id}-1' not in ids
        assert f'{test_id}-2' not in ids

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_range_scan_ids_only(self, cb_env, test_id, test_mutation_state):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        await self._validate_result(res, 12, ids_only=True)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_range_scan_default_terms(self, cb_env, test_ids):
        scan_type = RangeScan()
        res = cb_env.collection.scan(scan_type, ids_only=True)
        await self._validate_result(res, len(test_ids), ids_only=True)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_sampling_scan(self, cb_env, test_mutation_state):
        limit = 10
        scan_type = SamplingScan(limit)
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=False,
                                                            consistent_with=test_mutation_state))
        await self._validate_result(res, limit, ids_only=False, return_rows=False)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_sampling_scan_with_seed(self, cb_env, test_ids, test_mutation_state):
        limit = 10
        scan_type = SamplingScan(limit, 50)
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = await self._validate_result(res, limit, ids_only=True, return_rows=True)
        result_ids = []
        for r in rows:
            result_ids.append(r.id)

        # w/ the seed, we should get the same results
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = await self._validate_result(res, limit, ids_only=True, return_rows=True)
        compare_ids = list(map(lambda r: r.id, rows))
        assert result_ids == compare_ids

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_prefix_scan(self, cb_env, test_id, test_ids, test_mutation_state):
        scan_type = PrefixScan(f'{test_id}')
        res = cb_env.collection.scan(scan_type, ScanOptions(timeout=timedelta(seconds=10),
                                                            ids_only=True,
                                                            consistent_with=test_mutation_state))
        rows = await self._validate_result(res, 100, ids_only=True, return_rows=True)
        for r in rows:
            assert r.id in test_ids

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_byte_limit', [0, 1, 25, 100])
    async def test_range_scan_with_batch_byte_limit(self, cb_env, test_id, test_mutation_state, batch_byte_limit):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_byte_limit=batch_byte_limit,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 12)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_item_limit', [0, 1, 25, 100])
    async def test_range_scan_with_batch_item_limit(self, cb_env, test_id, test_mutation_state, batch_item_limit):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_item_limit=batch_item_limit,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 12)

    @pytest.mark.skip('skipped until CXXCBC-345 is resolved')
    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('concurrency', [1, 2, 4, 16, 64, 128])
    async def test_range_scan_with_concurrency(self, cb_env, test_id, test_mutation_state, concurrency):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 concurrency=concurrency,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 12)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_byte_limit', [0, 1, 25, 100])
    async def test_prefix_scan_with_batch_byte_limit(self, cb_env, test_id, test_mutation_state, batch_byte_limit):
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_byte_limit=batch_byte_limit,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 100)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_item_limit', [0, 1, 25, 100])
    async def test_prefix_scan_with_batch_item_limit(self, cb_env, test_id, test_mutation_state, batch_item_limit):
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_item_limit=batch_item_limit,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 100)

    @pytest.mark.skip('skipped until CXXCBC-345 is resolved')
    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('concurrency', [1, 2, 4, 16, 64, 128])
    async def test_prefix_scan_with_concurrency(self, cb_env, test_id, test_mutation_state, concurrency):
        scan_type = PrefixScan(test_id)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 concurrency=concurrency,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 100)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_byte_limit', [0, 1, 25, 100])
    async def test_sampling_scan_with_batch_byte_limit(self, cb_env, test_id, test_mutation_state, batch_byte_limit):
        scan_type = SamplingScan(50)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_byte_limit=batch_byte_limit,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 50)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('batch_item_limit', [0, 1, 25, 100])
    async def test_sampling_scan_with_batch_item_limit(self, cb_env, test_id, test_mutation_state, batch_item_limit):
        scan_type = SamplingScan(50)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 batch_item_limit=batch_item_limit,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 50)

    @pytest.mark.skip('skipped until CXXCBC-345 is resolved')
    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    @pytest.mark.parametrize('concurrency', [1, 2, 4, 16, 64, 128])
    async def test_sampling_scan_with_concurrency(self, cb_env, test_id, test_mutation_state, concurrency):
        scan_type = SamplingScan(50)
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 concurrency=concurrency,
                                                 consistent_with=test_mutation_state))
        await self._validate_result(res, 50)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_range_scan_with_zero_concurrency(self, cb_env, test_id, test_mutation_state):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.scan(scan_type,
                                   ScanOptions(timeout=timedelta(seconds=10),
                                               concurrency=0,
                                               consistent_with=test_mutation_state))

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_sampling_scan_with_zero_limit(self, cb_env, test_id, test_mutation_state):
        scan_type = SamplingScan(0)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.scan(scan_type,
                                   ScanOptions(timeout=timedelta(seconds=10),
                                               consistent_with=test_mutation_state))

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_supported')
    async def test_sampling_scan_with_negative_limit(self, cb_env, test_id, test_mutation_state):
        scan_type = SamplingScan(-10)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.scan(scan_type,
                                   ScanOptions(timeout=timedelta(seconds=10),
                                               consistent_with=test_mutation_state))

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_range_scan_not_supported')
    async def test_range_scan_feature_unavailable(self, cb_env, test_id, test_mutation_state):
        scan_type = RangeScan(ScanTerm(f'{test_id}-1'), ScanTerm(f'{test_id}-2'))
        res = cb_env.collection.scan(scan_type,
                                     ScanOptions(timeout=timedelta(seconds=10),
                                                 consistent_with=test_mutation_state))
        with pytest.raises(FeatureUnavailableException):
            await self._validate_result(res)


class ClassicRangeScanTests(RangeScanTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicRangeScanTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ClassicRangeScanTests) if valid_test_method(meth)]
        compare = set(ClassicRangeScanTests.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request, test_ids):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        await acb_base_env.setup(collection_type=request.param, num_docs=0)
        yield acb_base_env
        await self._purge_temp_docs(acb_base_env, test_ids)
        await acb_base_env.teardown(request.param)
