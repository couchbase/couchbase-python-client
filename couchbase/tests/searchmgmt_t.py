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

import pytest

from couchbase.exceptions import (InvalidArgumentException,
                                  QueryIndexAlreadyExistsException,
                                  SearchIndexNotFoundException)
from couchbase.management.search import SearchIndex
from tests.environments.test_environment import TestEnvironment


class SearchIndexManagementTestSuite:
    IDX_NAME = 'test-fts-index'

    TEST_MANIFEST = [
        'test_analyze_doc',
        'test_drop_index',
        'test_drop_index_fail',
        'test_get_all_index_stats',
        'test_get_all_indexes',
        'test_get_index',
        'test_get_index_doc_count',
        'test_get_index_fail',
        'test_get_index_fail_no_index_name',
        'test_get_index_stats',
        'test_ingestion_control',
        'test_plan_freeze_control',
        'test_query_control',
        'test_upsert_index',
    ]

    @pytest.fixture(scope='class', name='test_idx')
    def get_test_index(self):
        return SearchIndex(name=self.IDX_NAME, source_name='default')

    @pytest.fixture()
    def create_test_index(self, cb_env, test_idx):
        TestEnvironment.try_n_times_till_exception(10,
                                                   3,
                                                   cb_env.sixm.upsert_index,
                                                   test_idx,
                                                   expected_exceptions=(QueryIndexAlreadyExistsException, ))

    @pytest.fixture()
    def drop_test_index(self, cb_env, test_idx):
        yield
        TestEnvironment.try_n_times_till_exception(10,
                                                   3,
                                                   cb_env.sixm.drop_index,
                                                   test_idx.name,
                                                   expected_exceptions=(SearchIndexNotFoundException, ))

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_analyze_doc(self, cb_env, test_idx):
        if cb_env.server_version_short < 6.5:
            pytest.skip((f'FTS analyzeDoc only supported on server versions >= 6.5. '
                        f'Using server version: {cb_env.server_version}.'))
        # like getting the doc count, this can fail immediately after index
        # creation
        doc = {"field": "I got text in here"}
        analysis = TestEnvironment.try_n_times(5,
                                               2,
                                               cb_env.sixm.analyze_document,
                                               test_idx.name,
                                               doc)

        assert analysis.get('analysis', None) is not None
        assert isinstance(analysis.get('analysis'), (list, dict))
        assert analysis.get('status', None) == 'ok'

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_drop_index(self, cb_env, test_idx):
        res = TestEnvironment.try_n_times(3, 3, cb_env.sixm.get_index, test_idx.name)
        assert isinstance(res, SearchIndex)
        cb_env.sixm.drop_index(test_idx.name)
        with pytest.raises(SearchIndexNotFoundException):
            cb_env.sixm.drop_index(test_idx.name)

    def test_drop_index_fail(self, cb_env, test_idx):
        with pytest.raises(SearchIndexNotFoundException):
            cb_env.sixm.drop_index(test_idx.name)

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_get_all_index_stats(self, cb_env):
        # like getting the doc count, this can fail immediately after index
        # creation
        stats = TestEnvironment.try_n_times(5, 2, cb_env.sixm.get_all_index_stats)

        assert stats is not None
        assert isinstance(stats, dict)

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_get_all_indexes(self, cb_env, test_idx):
        # lets add one more
        new_idx = SearchIndex(name='new-search-idx', source_name='default')
        res = cb_env.sixm.upsert_index(new_idx)
        assert res is None
        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.get_index, new_idx.name)
        assert isinstance(res, SearchIndex)

        indexes = cb_env.sixm.get_all_indexes()
        assert isinstance(indexes, list)
        assert len(indexes) >= 2
        assert next((idx for idx in indexes if idx.name == test_idx.name), None) is not None
        assert next((idx for idx in indexes if idx.name == new_idx.name), None) is not None

        cb_env.sixm.drop_index(new_idx.name)
        TestEnvironment.try_n_times_till_exception(10,
                                                   3,
                                                   cb_env.sixm.get_index,
                                                   new_idx.name,
                                                   expected_exceptions=(SearchIndexNotFoundException,))

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_get_index(self, cb_env, test_idx):
        res = TestEnvironment.try_n_times(3, 3, cb_env.sixm.get_index, test_idx.name)
        assert isinstance(res, SearchIndex)
        assert res.name == test_idx.name

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_get_index_doc_count(self, cb_env, test_idx):
        # like getting the doc count, this can fail immediately after index
        # creation
        doc_count = TestEnvironment.try_n_times(5, 2, cb_env.sixm.get_indexed_documents_count, test_idx.name)

        assert doc_count is not None
        assert isinstance(doc_count, int)

    def test_get_index_fail(self, cb_env):
        with pytest.raises(SearchIndexNotFoundException):
            cb_env.sixm.get_index('not-an-index')

    def test_get_index_fail_no_index_name(self, cb_env):
        with pytest.raises(InvalidArgumentException):
            cb_env.sixm.get_index('')
        with pytest.raises(InvalidArgumentException):
            cb_env.sixm.get_index(None)

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_get_index_stats(self, cb_env, test_idx):
        # like getting the doc count, this can fail immediately after index
        # creation
        stats = TestEnvironment.try_n_times(5, 2, cb_env.sixm.get_index_stats, test_idx.name)

        assert stats is not None
        assert isinstance(stats, dict)

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_ingestion_control(self, cb_env, test_idx):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.pause_ingest, test_idx.name)
        assert res is None

        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.resume_ingest, test_idx.name)
        assert res is None

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_plan_freeze_control(self, cb_env, test_idx):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.freeze_plan, test_idx.name)
        assert res is None

        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.unfreeze_plan, test_idx.name)
        assert res is None

    @pytest.mark.usefixtures('create_test_index')
    @pytest.mark.usefixtures('drop_test_index')
    def test_query_control(self, cb_env, test_idx):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.disallow_querying, test_idx.name)
        assert res is None

        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.allow_querying, test_idx.name)
        assert res is None

    @pytest.mark.usefixtures('drop_test_index')
    def test_upsert_index(self, cb_env, test_idx):
        res = cb_env.sixm.upsert_index(test_idx)
        assert res is None
        res = TestEnvironment.try_n_times(10, 3, cb_env.sixm.get_index, test_idx.name)
        assert isinstance(res, SearchIndex)


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicSearchIndexManagementTests(SearchIndexManagementTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSearchIndexManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSearchIndexManagementTests) if valid_test_method(meth)]
        compare = set(SearchIndexManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.enable_search_mgmt()
        yield cb_base_env
        cb_base_env.disable_search_mgmt()
