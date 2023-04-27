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


from __future__ import annotations

import json
import pathlib
import time
from datetime import timedelta
from os import path
from typing import (List,
                    Optional,
                    Union)

import couchbase.search as search
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  SearchIndexNotFoundException,
                                  UnAmbiguousTimeoutException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.search import SearchIndex
from couchbase.result import SearchResult
from couchbase.search import SearchRow
from tests.environments import CollectionType, CouchbaseTestEnvironmentException
from tests.environments.test_environment import AsyncTestEnvironment, TestEnvironment


class SearchTestEnvironment(TestEnvironment):
    OTHER_COLLECTION = 'other-collection'
    TEST_COLLECTION_INDEX_NAME = 'test-search-coll-index'
    TEST_COLLECTION_INDEX_PATH = path.join(pathlib.Path(__file__).parent.parent,
                                           'test_cases',
                                           f'{TEST_COLLECTION_INDEX_NAME}-params-new.json')
    TEST_INDEX_NAME = 'test-search-index'
    TEST_INDEX_PATH = path.join(pathlib.Path(__file__).parent.parent,
                                'test_cases',
                                f'{TEST_INDEX_NAME}-params-new.json')

    def assert_rows(self,
                    result,  # type: SearchResult
                    expected_count,  # type: int
                    return_rows=False  # type: bool
                    ) -> Optional[List[Union[SearchRow, dict]]]:
        rows = []
        assert isinstance(result, SearchResult)  # nosec
        for row in result.rows():
            assert row is not None  # nosec
            self.validate_search_row(row)
            rows.append(row)
        assert len(rows) >= expected_count  # nosec

        self.validate_metadata(result, expected_count)

        if return_rows is True:
            return rows

    def create_and_load_other_collection(self):
        collection_spec = CollectionSpec(self.OTHER_COLLECTION, self.scope.name)
        self.cm.create_collection(collection_spec)
        collection = None
        for _ in range(5):
            collection = self.get_collection(self.scope.name, self.OTHER_COLLECTION, bucket_name=self.bucket.name)
            if collection:
                break
            TestEnvironment.sleep(5)

        if not collection:
            raise CouchbaseTestEnvironmentException("Unabled to create other-collection for FTS collection testing")

        coll = self.scope.collection(self.OTHER_COLLECTION)
        for _ in range(25):
            new_doc = self.data_provider.get_new_vehicle()
            for _ in range(3):
                try:
                    key = f'{new_doc["id"]}'
                    _ = coll.upsert(key, new_doc)
                    self._loaded_docs[key] = new_doc
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    time.sleep(3)
                    continue
                except Exception as ex:
                    raise ex

    def get_encoded_query(self, search_query):
        encoded_q = search_query.as_encodable()
        encoded_q['query'] = json.loads(encoded_q['query'])
        if 'facets' in encoded_q:
            encoded_q['facets'] = json.loads(encoded_q['facets'])
        if 'sort_specs' in encoded_q:
            encoded_q['sort'] = json.loads(encoded_q['sort_specs'])

        return encoded_q

    def load_search_index(self,
                          sixm,
                          collection_type,  # type: CollectionType
                          ):
        if collection_type == CollectionType.NAMED:
            idx_name = self.TEST_COLLECTION_INDEX_NAME
            idx_path = self.TEST_COLLECTION_INDEX_PATH
        else:
            idx_name = self.TEST_INDEX_NAME
            idx_path = self.TEST_INDEX_PATH

        with open(idx_path) as params_file:
            input = params_file.read()
            params_json = json.loads(input)
            TestEnvironment.try_n_times(10,
                                        3,
                                        sixm.upsert_index,
                                        SearchIndex(name=idx_name,
                                                    idx_type='fulltext-index',
                                                    source_name='default',
                                                    source_type='couchbase',
                                                    params=params_json))

    def setup(self,
              collection_type,  # type: CollectionType
              num_docs=None,  # type: Optional[int]
              test_suite=None,  # type: Optional[str]
              ):

        if test_suite == 'ClassicSearchParamTests':
            return

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)
            TestEnvironment.try_n_times(5, 3, self.load_data)
            self.create_and_load_other_collection()
            self.load_search_index(self.sixm, collection_type)
            # make sure the index loads...
            num_docs = self._check_doc_count(self.sixm, self.TEST_COLLECTION_INDEX_NAME, 20, retries=10, delay=3)
        else:
            TestEnvironment.try_n_times(5, 3, self.load_data)
            self.load_search_index(self.sixm, collection_type)
            # make sure the index loads...
            num_docs = self._check_doc_count(self.sixm, self.TEST_INDEX_NAME, 20, retries=10, delay=3)

        if num_docs == 0:
            raise CouchbaseTestEnvironmentException('No docs loaded into the index')

    def teardown(self,
                 collection_type,  # type: CollectionType
                 test_suite=None,  # type: Optional[str]
                 ):
        if test_suite == 'ClassicSearchParamTests':
            return

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.sixm.drop_index,
                                                       self.TEST_COLLECTION_INDEX_NAME,
                                                       expected_exceptions=(SearchIndexNotFoundException, ))
            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
        else:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.sixm.drop_index,
                                                       self.TEST_INDEX_NAME,
                                                       expected_exceptions=(SearchIndexNotFoundException, ))

    def validate_metadata(self,
                          result,  # type: SearchResult
                          expected_count  # type: int
                          ) -> None:
        meta = result.metadata()
        assert isinstance(meta, search.SearchMetaData)  # nosec
        metrics = meta.metrics()
        assert isinstance(metrics, search.SearchMetrics)  # nosec
        assert isinstance(metrics.error_partition_count(), int)  # nosec
        assert isinstance(metrics.max_score(), float)  # nosec
        assert isinstance(metrics.success_partition_count(), int)  # nosec
        assert isinstance(metrics.total_partition_count(), int)  # nosec
        total_count = metrics.error_partition_count() + metrics.success_partition_count()
        assert total_count == metrics.total_partition_count()  # nosec
        assert isinstance(metrics.took(), timedelta)  # nosec
        assert metrics.took().total_seconds() >= 0  # nosec
        assert metrics.total_rows() >= expected_count  # nosec

    def validate_search_row(self, row):
        assert isinstance(row, SearchRow)  # nosec
        assert isinstance(row.index, str)  # nosec
        assert isinstance(row.id, str)  # nosec
        assert isinstance(row.score, float)  # nosec
        assert isinstance(row.explanation, dict)  # nosec
        assert isinstance(row.fragments, dict)  # nosec

        if row.locations:
            assert isinstance(row.locations, search.SearchRowLocations)  # nosec

        if row.fields:
            assert isinstance(row.fields, search.SearchRowFields)  # nosec

    def _check_doc_count(self,
                         sixm,
                         idx_name,  # type: str
                         min_count,  # type: int
                         retries=20,  # type: int
                         delay=30  # type: int
                         ) -> bool:

        indexed_docs = 0
        no_docs_cutoff = 300
        for i in range(retries):
            # if no docs after waiting for a period of time, exit
            if indexed_docs == 0 and i * delay >= no_docs_cutoff:
                return 0
            indexed_docs = TestEnvironment.try_n_times(10,
                                                       10,
                                                       sixm.get_indexed_documents_count,
                                                       idx_name)
            if indexed_docs >= min_count:
                break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            TestEnvironment.sleep(delay)

        return indexed_docs

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> SearchTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env


class AsyncSearchTestEnvironment(AsyncTestEnvironment):
    OTHER_COLLECTION = 'other-collection'
    TEST_COLLECTION_INDEX_NAME = 'test-search-coll-index'
    TEST_COLLECTION_INDEX_PATH = path.join(pathlib.Path(__file__).parent.parent,
                                           'test_cases',
                                           f'{TEST_COLLECTION_INDEX_NAME}-params-new.json')
    TEST_INDEX_NAME = 'test-search-index'
    TEST_INDEX_PATH = path.join(pathlib.Path(__file__).parent.parent,
                                'test_cases',
                                f'{TEST_INDEX_NAME}-params-new.json')

    async def assert_rows(self,
                          result,  # type: SearchResult
                          expected_count,  # type: int
                          return_rows=False  # type: bool
                          ) -> Optional[List[Union[SearchRow, dict]]]:
        rows = []
        assert isinstance(result, SearchResult)  # nosec
        async for row in result.rows():
            assert row is not None  # nosec
            self.validate_search_row(row)
            rows.append(row)
        assert len(rows) >= expected_count  # nosec

        self.validate_metadata(result, expected_count)

        if return_rows is True:
            return rows

    async def create_and_load_other_collection(self):
        collection_spec = CollectionSpec(self.OTHER_COLLECTION, self.scope.name)
        await self.cm.create_collection(collection_spec)
        collection = None
        for _ in range(5):
            collection = await self.get_collection(self.scope.name, self.OTHER_COLLECTION, bucket_name=self.bucket.name)
            if collection:
                break
            await AsyncTestEnvironment.sleep(5)

        if not collection:
            raise CouchbaseTestEnvironmentException("Unabled to create other-collection for FTS collection testing")

        coll = self.scope.collection(self.OTHER_COLLECTION)
        for _ in range(25):
            new_doc = self.data_provider.get_new_vehicle()
            for _ in range(3):
                try:
                    key = f'{new_doc["id"]}'
                    _ = await coll.upsert(key, new_doc)
                    self._loaded_docs[key] = new_doc
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    await AsyncTestEnvironment.sleep(3)
                    continue
                except Exception as ex:
                    raise ex

    async def load_search_index(self,
                                sixm,
                                collection_type,  # type: CollectionType
                                ):
        if collection_type == CollectionType.NAMED:
            idx_name = self.TEST_COLLECTION_INDEX_NAME
            idx_path = self.TEST_COLLECTION_INDEX_PATH
        else:
            idx_name = self.TEST_INDEX_NAME
            idx_path = self.TEST_INDEX_PATH

        with open(idx_path) as params_file:
            input = params_file.read()
            params_json = json.loads(input)
            await AsyncTestEnvironment.try_n_times(10,
                                                   3,
                                                   sixm.upsert_index,
                                                   SearchIndex(name=idx_name,
                                                               idx_type='fulltext-index',
                                                               source_name='default',
                                                               source_type='couchbase',
                                                               params=params_json))

    async def setup(self,
                    collection_type,  # type: CollectionType
                    num_docs=None,  # type: Optional[int]
                    test_suite=None,  # type: Optional[str]
                    ):

        if test_suite == 'ClassicSearchParamTests':
            return

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            await AsyncTestEnvironment.try_n_times(5, 3, self.setup_named_collections)
            await AsyncTestEnvironment.try_n_times(5, 3, self.load_data)
            await self.create_and_load_other_collection()
            await self.load_search_index(self.sixm, collection_type)
            # make sure the index loads...
            num_docs = await self._check_doc_count(self.sixm, self.TEST_COLLECTION_INDEX_NAME, 20, retries=10, delay=3)
        else:
            await AsyncTestEnvironment.try_n_times(5, 3, self.load_data)
            await self.load_search_index(self.sixm, collection_type)
            # make sure the index loads...
            num_docs = await self._check_doc_count(self.sixm, self.TEST_INDEX_NAME, 20, retries=10, delay=3)

        if num_docs == 0:
            raise CouchbaseTestEnvironmentException('No docs loaded into the index')

    async def teardown(self,
                       collection_type,  # type: CollectionType
                       test_suite=None,  # type: Optional[str]
                       ):
        if test_suite == 'ClassicSearchParamTests':
            return

        await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                                  3,
                                                                  self.sixm.drop_index,
                                                                  self.TEST_COLLECTION_INDEX_NAME,
                                                                  expected_exceptions=(SearchIndexNotFoundException, ))
            await AsyncTestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
        else:
            await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                                  3,
                                                                  self.sixm.drop_index,
                                                                  self.TEST_INDEX_NAME,
                                                                  expected_exceptions=(SearchIndexNotFoundException, ))

    def validate_metadata(self,
                          result,  # type: SearchResult
                          expected_count  # type: int
                          ) -> None:
        meta = result.metadata()
        assert isinstance(meta, search.SearchMetaData)  # nosec
        metrics = meta.metrics()
        assert isinstance(metrics, search.SearchMetrics)  # nosec
        assert isinstance(metrics.error_partition_count(), int)  # nosec
        assert isinstance(metrics.max_score(), float)  # nosec
        assert isinstance(metrics.success_partition_count(), int)  # nosec
        assert isinstance(metrics.total_partition_count(), int)  # nosec
        total_count = metrics.error_partition_count() + metrics.success_partition_count()
        assert total_count == metrics.total_partition_count()  # nosec
        assert isinstance(metrics.took(), timedelta)  # nosec
        assert metrics.took().total_seconds() >= 0  # nosec
        assert metrics.total_rows() >= expected_count  # nosec

    def validate_search_row(self, row):
        assert isinstance(row, SearchRow)  # nosec
        assert isinstance(row.index, str)  # nosec
        assert isinstance(row.id, str)  # nosec
        assert isinstance(row.score, float)  # nosec
        assert isinstance(row.explanation, dict)  # nosec
        assert isinstance(row.fragments, dict)  # nosec

        if row.locations:
            assert isinstance(row.locations, search.SearchRowLocations)  # nosec

        if row.fields:
            assert isinstance(row.fields, search.SearchRowFields)  # nosec

    async def _check_doc_count(self,
                               sixm,
                               idx_name,  # type: str
                               min_count,  # type: int
                               retries=20,  # type: int
                               delay=30  # type: int
                               ) -> bool:

        indexed_docs = 0
        no_docs_cutoff = 300
        for i in range(retries):
            # if no docs after waiting for a period of time, exit
            if indexed_docs == 0 and i * delay >= no_docs_cutoff:
                return 0
            indexed_docs = await AsyncTestEnvironment.try_n_times(10,
                                                                  10,
                                                                  sixm.get_indexed_documents_count,
                                                                  idx_name)
            if indexed_docs >= min_count:
                break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            await AsyncTestEnvironment.sleep(delay)

        return indexed_docs

    @classmethod
    def from_environment(cls,
                         env  # type: AsyncTestEnvironment
                         ) -> AsyncSearchTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
