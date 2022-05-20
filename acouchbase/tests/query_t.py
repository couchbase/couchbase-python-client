#  Copyright 2016-2022. Couchbase, Inc.
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

import asyncio
from datetime import datetime, timedelta

import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from acouchbase.cluster import get_event_loop
from couchbase.exceptions import (CouchbaseException,
                                  KeyspaceNotFoundException,
                                  QueryErrorContext,
                                  QueryIndexNotFoundException,
                                  ScopeNotFoundException)
from couchbase.n1ql import (QueryMetaData,
                            QueryMetrics,
                            QueryProfile,
                            QueryStatus,
                            QueryWarning)
from couchbase.options import (QueryOptions,
                               UnsignedInt64,
                               UpsertOptions)
from couchbase.result import QueryResult

from ._test_utils import TestEnvironment


class QueryTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_query_indexes=True)

        await cb_env.try_n_times(10, 3, cb_env.ixm.create_primary_index,
                                 cb_env.bucket.name,
                                 timeout=timedelta(seconds=60),
                                 ignore_if_exists=True)

        await cb_env.try_n_times(3, 5, cb_env.load_data)
        # let it load a bit...
        for _ in range(5):
            row_count_good = await self._check_row_count(cb_env, 5)
            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            await asyncio.sleep(5)

        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)

        await cb_env.try_n_times_till_exception(10, 3,
                                                cb_env.ixm.drop_primary_index,
                                                cb_env.bucket.name,
                                                expected_exceptions=(QueryIndexNotFoundException))

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('preserve_expiry')

    async def _check_row_count(self, cb_env,
                               min_count  # type: int
                               ) -> bool:

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE 'United%' LIMIT 5")
        count = 0
        async for _ in result.rows():
            count += 1
        return count >= min_count

    async def assert_rows(self,
                          result,  # type: QueryResult
                          expected_count):
        count = 0
        assert isinstance(result, QueryResult)
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    @pytest.mark.asyncio
    async def test_simple_query(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        await self.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    @pytest.mark.asyncio
    async def test_simple_query_prepared(self, cb_env):
        # @TODO(CXXCBC-174)
        if cb_env.server_version_short < 6.5:
            pytest.skip(f'Skipped on server versions < 6.5 (using {cb_env.server_version_short}). Pending CXXCBC-174')
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        await self.assert_rows(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._request.params.get('adhoc', None) is False

    @pytest.mark.asyncio
    async def test_simple_query_with_positional_params(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 2", 'United%')
        await self.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_simple_query_with_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 2",
                                      country='United%')
        await self.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_simple_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 2",
                                      QueryOptions(positional_parameters=['United%']))
        await self.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_simple_query_with_named_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 1",
                                      QueryOptions(named_parameters={'country': 'United%'}))
        await self.assert_rows(result, 1)

    # # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # # now, kwargs override the corresponding value in the options, only.
    @pytest.mark.asyncio
    async def test_simple_query_without_options_with_kwargs_positional_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 1",
                                      positional_parameters=['United%'])
        await self.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_simple_query_without_options_with_kwargs_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 1",
                                      named_parameters={'country': 'United%'})
        await self.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_with_profile(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(profile=QueryProfile.TIMINGS))
        await self.assert_rows(result, 1)
        assert result.metadata().profile() is not None

    @pytest.mark.asyncio
    async def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(metrics=True))
        await self.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        assert isinstance(metadata, QueryMetaData)
        metrics = metadata.metrics()
        assert isinstance(metrics, QueryMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert UnsignedInt64(expected) == count_result, fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert metrics.error_count() == UnsignedInt64(0)
        assert metadata.profile() is None

    @pytest.mark.asyncio
    async def test_query_metadata(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        await self.assert_rows(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    @pytest.mark.asyncio
    async def test_mixed_positional_parameters(self, cb_env):
        # we assume that positional overrides one in the Options
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 1",
                                      QueryOptions(positional_parameters=['xgfflq']), 'United%')
        await self.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_mixed_named_parameters(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 1",
                                      QueryOptions(named_parameters={'country': 'xxffqqlx'}), country='United%')
        await self.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_raw_options(self, cb_env):
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT $1",
                                      QueryOptions(raw={"$country": "United%", "args": [2]}))
        await self.assert_rows(result, 2)

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 1",
                                      QueryOptions(raw={"args": ['United%']}))
        await self.assert_rows(result, 1)

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @pytest.mark.asyncio
    async def test_preserve_expiry(self, cb_env):
        key = "imakey"
        content = {"a": "aaa", "b": "bbb"}

        await cb_env.collection.upsert(key, content, UpsertOptions(
            expiry=timedelta(days=1)))

        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb_env.collection.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        await cb_env.cluster.query(f"UPDATE `{cb_env.bucket.name}` AS content USE KEYS '{key}' SET content.a = 'aaaa'",
                                   QueryOptions(preserve_expiry=True)).execute()

        res = await cb_env.try_n_times(10, 3, cb_env.collection.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2

    @pytest.mark.asyncio
    async def test_query_error_context(self, cb_env):
        try:
            await cb_env.cluster.query("SELECT * FROM no_such_bucket").execute()
        except CouchbaseException as ex:
            assert isinstance(ex.context, QueryErrorContext)
            assert ex.context.statement is not None
            assert ex.context.first_error_code is not None
            assert ex.context.first_error_message is not None
            assert ex.context.client_context_id is not None
            assert ex.context.response_body is not None
            # @TODO:  these are diff from 3.x -> 4.x
            # self.assertIsNotNone(ex.context.endpoint)
            # self.assertIsNotNone(ex.context.error_response_body)


class QueryCollectionTests:
    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_collections=True,
                                                       manage_query_indexes=True)

        await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)
        await cb_env.try_n_times(10, 3, cb_env.ixm.create_primary_index,
                                 cb_env.bucket.name,
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION,
                                 timeout=timedelta(seconds=60))

        await cb_env.try_n_times(5, 3, cb_env.load_data)
        # let it load a bit...
        for _ in range(5):
            row_count_good = await self._check_row_count(cb_env, 5)
            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            await asyncio.sleep(5)

        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        await cb_env.try_n_times_till_exception(10, 3,
                                                cb_env.ixm.drop_primary_index,
                                                cb_env.bucket.name,
                                                scope_name=self.TEST_SCOPE,
                                                collection_name=self.TEST_COLLECTION,
                                                expected_exceptions=(QueryIndexNotFoundException,))

        await cb_env.try_n_times_till_exception(5, 3,
                                                cb_env.teardown_named_collections,
                                                raise_if_no_exception=False)

    async def _check_row_count(self, cb_env,
                               min_count  # type: int
                               ) -> bool:

        result = cb_env.cluster.query(f"SELECT * FROM {cb_env.fqdn} WHERE country LIKE 'United%' LIMIT 5")
        count = 0
        async for _ in result.rows():
            count += 1
        return count >= min_count

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('preserve_expiry')

    async def assert_rows(self,
                          result,  # type: QueryResult
                          expected_count):
        count = 0
        assert isinstance(result, QueryResult)
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    @pytest.mark.asyncio
    async def test_query_fully_qualified(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")
        await self.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    @pytest.mark.asyncio
    async def test_cluster_query_context(self, cb_env):
        q_context = f'{cb_env.bucket.name}.{cb_env.scope.name}'
        # test with QueryOptions
        q_opts = QueryOptions(query_context=q_context, adhoc=True)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", q_opts)
        await self.assert_rows(result, 2)

        # test with kwargs
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", query_context=q_context)
        await self.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_bad_query_context(self, cb_env):
        # test w/ no context
        # @TODO:  what about KeyspaceNotFoundException?

        # correct_exc = False
        # try:
        with pytest.raises(KeyspaceNotFoundException):
            await cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2").execute()
        # except ParsingFailedException:
        #     correct_exc = True
        # except Exception:
        #     pytest.fail('Expected ParsingFailedException')

        # assert correct_exc is True
        # correct_exc = False
        # test w/ bad scope
        # @TODO:  what about ScopeNotFoundException?
        q_context = f'{cb_env.bucket.name}.`fake-scope`'

        # try:
        with pytest.raises(ScopeNotFoundException):
            await cb_env.cluster.query(
                f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                QueryOptions(query_context=q_context)).execute()
        # except ParsingFailedException:
        #     correct_exc = True
        # except Exception:
        #     pytest.fail('Expected ParsingFailedException')

        # assert correct_exc is True

    @pytest.mark.asyncio
    async def test_scope_query(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        await self.assert_rows(result, 2)
        result = cb_env.scope.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")

    @pytest.mark.asyncio
    async def test_bad_scope_query(self, cb_env):
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        # try:
        with pytest.raises(ScopeNotFoundException):
            await cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                                     QueryOptions(query_context=q_context)).execute()
        # except ParsingFailedException:
        #     correct_exc = True
        # except Exception:
        #     pytest.fail('Expected ParsingFailedException')

        # assert correct_exc is True
        # correct_exc = False

        q_context = f'`fake-bucket`.`{cb_env.scope.name}`'
        # try:
        with pytest.raises(KeyspaceNotFoundException):
            await cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                                     query_context=q_context).execute()
        # except ParsingFailedException:
        #     correct_exc = True
        # except Exception:
        #     pytest.fail('Expected ParsingFailedException')

    @pytest.mark.asyncio
    async def test_scope_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE country LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=['United%']))
        await self.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_scope_query_with_named_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE country LIKE $country LIMIT 1",
                                    QueryOptions(named_parameters={'country': 'United%'}))
        await self.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.scope.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 1", QueryOptions(metrics=True))
        await self.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        assert isinstance(metadata, QueryMetaData)
        metrics = metadata.metrics()
        assert isinstance(metrics, QueryMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert UnsignedInt64(expected) == count_result, fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert metrics.error_count() == UnsignedInt64(0)
        assert metadata.profile() is None

    @pytest.mark.asyncio
    async def test_query_metadata(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        await self.assert_rows(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)
