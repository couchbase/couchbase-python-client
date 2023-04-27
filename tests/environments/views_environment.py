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

import pathlib
import time
from os import path
from typing import Optional

from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from couchbase.result import ViewResult
from tests.environments import CollectionType
from tests.environments.test_environment import AsyncTestEnvironment, TestEnvironment


class ViewsTestEnvironment(TestEnvironment):

    TEST_VIEW_NAME = 'test-view'
    TEST_VIEW_PATH = path.join(pathlib.Path(__file__).parent.parent,
                               'test_cases',
                               f'{TEST_VIEW_NAME}-new.txt')

    DOCNAME = 'test-ddoc'

    @property
    def test_ddoc(self):
        return self._test_ddoc

    def add_test_ddoc(self):
        TestEnvironment.try_n_times(3,
                                    5,
                                    self.vixm.upsert_design_document,
                                    self.test_ddoc,
                                    DesignDocumentNamespace.DEVELOPMENT)

    def assert_rows(self,
                    result,  # type: ViewResult
                    expected_count,
                    return_rows=False):
        assert isinstance(result, ViewResult)
        rows = []
        for row in result.rows():
            assert row is not None
            rows.append(row)
        assert len(rows) >= expected_count

        if return_rows is True:
            return rows

    def create_test_ddoc(self):
        view_data = None
        with open(self.TEST_VIEW_PATH) as view_file:
            view_data = view_file.read()

        view = View(map=view_data)
        self._test_ddoc = DesignDocument(name=self.DOCNAME, views={self.TEST_VIEW_NAME: view})

    def drop_ddoc(self, from_prod=False):
        if from_prod is True:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.vixm.drop_design_document,
                                                       self.test_ddoc.name,
                                                       DesignDocumentNamespace.PRODUCTION,
                                                       expected_exceptions=(DesignDocumentNotFoundException, ))
        TestEnvironment.try_n_times_till_exception(10,
                                                   3,
                                                   self.vixm.drop_design_document,
                                                   self.test_ddoc.name,
                                                   DesignDocumentNamespace.DEVELOPMENT,
                                                   expected_exceptions=(DesignDocumentNotFoundException, ))

    def get_batch_id(self):
        if hasattr(self, '_batch_id'):
            return self._batch_id

        doc = list(self._loaded_docs.values())[0]
        self._batch_id = doc['batch']
        return self._batch_id

    def setup(self,
              collection_type,  # type: CollectionType
              test_suite=None,  # type: Optional[str]
              num_docs=50,  # type: Optional[int]
              ):

        if test_suite == 'ClassicViewsParamTests':
            return
        elif test_suite == 'ClassicViewIndexManagementTests':
            self.enable_views_mgmt()
            self.create_test_ddoc()
            TestEnvironment.try_n_times(5, 3, self.load_data, num_docs=num_docs)
        else:
            self.create_test_ddoc()
            self.add_test_ddoc()
            TestEnvironment.try_n_times(5, 3, self.load_data, num_docs=num_docs)

            for _ in range(5):
                row_count_good = self._check_row_count(5)
                if row_count_good:
                    break
                print('Waiting for view to load, sleeping a bit...')
                time.sleep(5)

    def teardown(self,
                 collection_type,  # type: CollectionType
                 test_suite=None,  # type: Optional[str]
                 ):
        if test_suite == 'ClassicViewsParamTests':
            return
        elif test_suite == 'ClassicViewIndexManagementTests':
            self.disable_views_mgmt()
            TestEnvironment.try_n_times(5, 3, self.purge_data)
        else:
            TestEnvironment.try_n_times(5, 3, self.purge_data)
            self.drop_ddoc()

    def _check_row_count(self,
                         min_count  # type: int
                         ) -> bool:

        view_result = self.bucket.view_query(self.DOCNAME,
                                             self.TEST_VIEW_NAME,
                                             limit=min_count,
                                             namespace=DesignDocumentNamespace.DEVELOPMENT)
        count = 0
        for _ in view_result:
            count += 1
        return count >= min_count

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> ViewsTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env


class AsyncViewsTestEnvironment(AsyncTestEnvironment):

    TEST_VIEW_NAME = 'test-view'
    TEST_VIEW_PATH = path.join(pathlib.Path(__file__).parent.parent,
                               'test_cases',
                               f'{TEST_VIEW_NAME}-new.txt')

    DOCNAME = 'test-ddoc'

    @property
    def test_ddoc(self):
        return self._test_ddoc

    async def add_test_ddoc(self):
        await AsyncTestEnvironment.try_n_times(3,
                                               5,
                                               self.vixm.upsert_design_document,
                                               self.test_ddoc,
                                               DesignDocumentNamespace.DEVELOPMENT)

    async def assert_rows(self,
                          result,  # type: ViewResult
                          expected_count,
                          return_rows=False):
        assert isinstance(result, ViewResult)
        rows = []
        async for row in result.rows():
            assert row is not None
            rows.append(row)
        assert len(rows) >= expected_count

        if return_rows is True:
            return rows

    def create_test_ddoc(self):
        view_data = None
        with open(self.TEST_VIEW_PATH) as view_file:
            view_data = view_file.read()

        view = View(map=view_data)
        self._test_ddoc = DesignDocument(name=self.DOCNAME, views={self.TEST_VIEW_NAME: view})

    async def drop_ddoc(self, from_prod=False):
        if from_prod is True:
            await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                                  3,
                                                                  self.vixm.drop_design_document,
                                                                  self.test_ddoc.name,
                                                                  DesignDocumentNamespace.PRODUCTION,
                                                                  expected_exceptions=(DesignDocumentNotFoundException, ))  # noqa: E501
        await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                              3,
                                                              self.vixm.drop_design_document,
                                                              self.test_ddoc.name,
                                                              DesignDocumentNamespace.DEVELOPMENT,
                                                              expected_exceptions=(DesignDocumentNotFoundException, ))

    def get_batch_id(self):
        if hasattr(self, '_batch_id'):
            return self._batch_id

        doc = list(self._loaded_docs.values())[0]
        self._batch_id = doc['batch']
        return self._batch_id

    async def setup(self,
                    collection_type,  # type: CollectionType
                    test_suite=None,  # type: Optional[str]
                    num_docs=50,  # type: Optional[int]
                    ):

        if test_suite == 'ClassicViewsParamTests':
            return
        elif test_suite == 'ClassicViewIndexManagementTests':
            self.enable_views_mgmt()
            self.create_test_ddoc()
            await AsyncTestEnvironment.try_n_times(5, 3, self.load_data, num_docs=num_docs)
        else:
            self.create_test_ddoc()
            await self.add_test_ddoc()
            await AsyncTestEnvironment.try_n_times(5, 3, self.load_data, num_docs=num_docs)

            for _ in range(5):
                row_count_good = await self._check_row_count(5)
                if row_count_good:
                    break
                print('Waiting for view to load, sleeping a bit...')
                await AsyncTestEnvironment.sleep(5)

    async def teardown(self,
                       collection_type,  # type: CollectionType
                       test_suite=None,  # type: Optional[str]
                       ):
        if test_suite == 'ClassicViewsParamTests':
            return
        elif test_suite == 'ClassicViewIndexManagementTests':
            self.disable_views_mgmt()
            await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)
        else:
            await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)
            await self.drop_ddoc()

    async def _check_row_count(self,
                               min_count  # type: int
                               ) -> bool:

        view_result = self.bucket.view_query(self.DOCNAME,
                                             self.TEST_VIEW_NAME,
                                             limit=min_count,
                                             namespace=DesignDocumentNamespace.DEVELOPMENT)
        count = 0
        async for _ in view_result:
            count += 1
        return count >= min_count

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> ViewsTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
