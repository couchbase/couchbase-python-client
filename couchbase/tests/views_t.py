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

import json
import pathlib
from datetime import timedelta
from os import path

import pytest

from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from couchbase.options import ViewOptions
from couchbase.result import ViewResult
from couchbase.serializer import DefaultJsonSerializer
from couchbase.views import (ViewErrorMode,
                             ViewMetaData,
                             ViewOrdering,
                             ViewQuery,
                             ViewScanConsistency)

from ._test_utils import TestEnvironment


class ViewTests:

    TEST_VIEW_NAME = 'test-view'
    TEST_VIEW_PATH = path.join(pathlib.Path(__file__).parent.parent.parent,
                               'tests',
                               'test_cases',
                               f'{TEST_VIEW_NAME}.txt')

    DOCNAME = 'test-ddoc'

    @pytest.fixture(scope='class')
    def test_ddoc(self):
        view_data = None
        with open(self.TEST_VIEW_PATH) as view_file:
            view_data = view_file.read()

        view = View(map=view_data)
        ddoc = DesignDocument(name=self.DOCNAME, views={self.TEST_VIEW_NAME: view})
        return ddoc

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config, test_ddoc):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_view_indexes=True)

        self.create_ddoc(cb_env, test_ddoc)
        cb_env.load_data()
        # let it load a bit...
        for _ in range(5):
            row_count_good = self._check_row_count(cb_env, 5)
            if not row_count_good:
                print('Waiting for view to load, sleeping a bit...')
                cb_env.sleep(5)
        yield cb_env
        cb_env.purge_data()
        self.drop_ddoc(cb_env, test_ddoc)

    def create_ddoc(self, cb_env, test_ddoc):
        cb_env.vixm.upsert_design_document(test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    def drop_ddoc(self, cb_env, test_ddoc):
        try:
            cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex

    def _check_row_count(self, cb_env,
                         min_count  # type: int
                         ) -> bool:

        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               limit=min_count,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)
        count = 0
        for _ in view_result:
            count += 1
        return count >= min_count

    def assert_rows(self,
                    result,  # type: ViewResult
                    expected_count):
        count = 0
        assert isinstance(result, ViewResult)
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def test_view_query(self, cb_env):

        expected_count = 10
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               limit=expected_count,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_key(self, cb_env):

        expected_count = 1
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           key=["101 Coffee Shop", "landmark_11769"])
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               opts)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_keys(self, cb_env):

        expected_count = 5
        keys = [["101 Coffee Shop", "landmark_11769"],
                ["Ace Hotel DTLA", "hotel_16630"],
                ["airline_1316", "route_25068"],
                ["airline_1355", "route_14484"],
                ["airline_1355", "route_14817"]]
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           keys=keys)
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               opts)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_startkey(self, cb_env):

        expected_count = 5
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           startkey=["101 Coffee Shop", "landmark_11769"])
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               opts)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_endkey(self, cb_env):

        expected_count = 5
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           endkey=["airline_1355", "route_14817"])
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               opts)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_startkey_docid(self, cb_env):

        expected_count = 5
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           startkey_docid="landmark_11769")
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               opts)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_endkey_docid(self, cb_env):

        expected_count = 5
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           endkey_docid="route_14817")
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               opts)

        self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count


class ViewParamTests:

    TEST_VIEW_NAME = 'test-view'
    DOCNAME = 'test-ddoc'

    @pytest.fixture(scope='class')
    def base_opts(self):
        return {'bucket_name': 'default',
                'document_name': self.DOCNAME,
                'view_name': self.TEST_VIEW_NAME
                }

    def test_params_base(self, base_opts):
        opts = ViewOptions()
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)
        params = query.as_encodable()
        assert params == base_opts

    def test_params_timeout(self, base_opts):
        opts = ViewOptions(timeout=timedelta(seconds=20))
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        params = query.as_encodable()
        assert params == exp_opts

        opts = ViewOptions(timeout=20)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        params = query.as_encodable()
        assert params == exp_opts

        opts = ViewOptions(timeout=25.5)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25500000
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_scan_consistency(self, base_opts):
        opts = ViewOptions(scan_consistency=ViewScanConsistency.REQUEST_PLUS)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_consistency'] = ViewScanConsistency.REQUEST_PLUS.value
        params = query.as_encodable()
        assert params == exp_opts
        assert query.consistency == ViewScanConsistency.REQUEST_PLUS

    def test_params_limit(self, base_opts):
        opts = ViewOptions(limit=10)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['limit'] = 10
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_skip(self, base_opts):
        opts = ViewOptions(skip=10)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['skip'] = 10
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_startkey(self, base_opts):
        key = ["101 Coffee Shop", "landmark_11769"]
        opts = ViewOptions(startkey=key)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['start_key'] = json.dumps(key)
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_endkey(self, base_opts):
        key = ["101 Coffee Shop", "landmark_11769"]
        opts = ViewOptions(endkey=key)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['end_key'] = json.dumps(key)
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_startkey_docid(self, base_opts):
        key = "landmark_11769"
        opts = ViewOptions(startkey_docid=key)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['start_key_doc_id'] = key
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_endkey_docid(self, base_opts):
        key = "landmark_11769"
        opts = ViewOptions(endkey_docid=key)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['end_key_doc_id'] = key
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_inclusive_end(self, base_opts):
        opts = ViewOptions(inclusive_end=True)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['inclusive_end'] = True
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_group(self, base_opts):
        opts = ViewOptions(group=True)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['group'] = True
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_group_level(self, base_opts):
        opts = ViewOptions(group_level=10)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['group_level'] = 10
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_key(self, base_opts):
        opts = ViewOptions(key='test-key')
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['key'] = json.dumps('test-key')
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_keys(self, base_opts):
        test_keys = ['test-key1', 'test-key2']
        opts = ViewOptions(keys=test_keys)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['keys'] = list(map(lambda k: json.dumps(k), test_keys))
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_order(self, base_opts):
        opts = ViewOptions(order=ViewOrdering.ASCENDING)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['order'] = ViewOrdering.ASCENDING.value
        params = query.as_encodable()
        assert params == exp_opts
        assert query.order == ViewOrdering.ASCENDING

    def test_params_reduce(self, base_opts):
        opts = ViewOptions(reduce=True)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['reduce'] = True
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_on_error(self, base_opts):
        opts = ViewOptions(on_error=ViewErrorMode.CONTINUE)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['on_error'] = ViewErrorMode.CONTINUE.value
        params = query.as_encodable()
        assert params == exp_opts
        assert query.on_error == ViewErrorMode.CONTINUE

    def test_params_namespace(self, base_opts):
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['namespace'] = DesignDocumentNamespace.DEVELOPMENT.value
        params = query.as_encodable()
        assert params == exp_opts
        assert query.namespace == DesignDocumentNamespace.DEVELOPMENT

    def test_params_client_context_id(self, base_opts):
        opts = ViewOptions(client_context_id='test-context-id')
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['client_context_id'] = 'test-context-id'
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_serializer(self, base_opts):
        serializer = DefaultJsonSerializer()
        opts = ViewOptions(serializer=serializer)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['serializer'] = serializer
        params = query.as_encodable()
        assert params == exp_opts

    def test_params_debug(self, base_opts):
        opts = ViewOptions(debug=True)
        query = ViewQuery.create_view_query_object('default', self.DOCNAME, self.TEST_VIEW_NAME, opts)

        exp_opts = base_opts.copy()
        exp_opts['debug'] = True
        params = query.as_encodable()
        assert params == exp_opts
