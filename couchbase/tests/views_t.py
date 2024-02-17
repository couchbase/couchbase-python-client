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
import threading
from datetime import timedelta

import pytest

from couchbase.exceptions import (AmbiguousTimeoutException,
                                  DesignDocumentNotFoundException,
                                  InvalidArgumentException)
from couchbase.management.views import DesignDocumentNamespace
from couchbase.options import ViewOptions
from couchbase.views import (ViewMetaData,
                             ViewOrdering,
                             ViewRow)
from tests.environments import CollectionType
from tests.environments.views_environment import ViewsTestEnvironment


class ViewsTestSuite:
    TEST_MANIFEST = [
        'test_bad_view_query',
        'test_view_query',
        'test_view_query_ascending',
        'test_view_query_descending',
        'test_view_query_endkey_docid',
        'test_view_query_in_thread',
        'test_view_query_key',
        'test_view_query_keys',
        'test_view_query_raw',
        'test_view_query_raw_fail',
        'test_view_query_startkey_docid',
        'test_view_query_timeout',
    ]

    def test_bad_view_query(self, cb_env):
        view_result = cb_env.bucket.view_query('fake-ddoc',
                                               'fake-view',
                                               limit=10,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)

        with pytest.raises(DesignDocumentNotFoundException):
            [r for r in view_result]

    def test_view_query(self, cb_env):
        expected_count = 10
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               full_set=True,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)

        cb_env.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_ascending(self, cb_env):
        # set the batch id
        cb_env.get_batch_id()
        keys = cb_env.get_keys()
        expected_docids = [i for k in sorted(keys)
                           for i in sorted(cb_env.get_docids_by_key(k))]
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT,
                                               order=ViewOrdering.ASCENDING)

        rows = cb_env.assert_rows(view_result, cb_env.num_docs, return_rows=True)
        row_ids = list(map(lambda r: r.id, rows))
        assert row_ids == expected_docids

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= cb_env.num_docs

    def test_view_query_descending(self, cb_env):
        # set the batch id
        cb_env.get_batch_id()
        keys = cb_env.get_keys()
        expected_docids = [i for k in sorted(keys, reverse=True)
                           for i in sorted(cb_env.get_docids_by_key(k), reverse=True)]
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT,
                                               order=ViewOrdering.DESCENDING)

        rows = cb_env.assert_rows(view_result, cb_env.num_docs, return_rows=True)
        row_ids = list(map(lambda r: r.id, rows))
        assert row_ids == expected_docids

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= cb_env.num_docs

    def test_view_query_endkey_docid(self, cb_env):
        # set the batch id
        cb_env.get_batch_id()
        keys = cb_env.get_keys()
        # take the 2nd-smallest key so that we can purposefully select results of previous key group
        key_idx = keys.index(sorted(keys)[1])
        key = keys[key_idx]
        key_docids = cb_env.get_docids_by_key(key)
        # purposefully select a docid in the middle of key group
        endkey_docid = key_docids[4]
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT,
                           endkey=key,
                           endkey_docid=endkey_docid)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)
        # all docs w/in first key (10 docs) + half of docs w/in next key (5 docs)
        expected_count = 15
        rows = cb_env.assert_rows(view_result, expected_count, True)
        # last doc in results should be the endkey and endkey_docid
        assert rows[-1].key == key
        assert rows[-1].id == endkey_docid

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_in_thread(self, cb_env):
        results = [None]

        def run_test(bucket, doc_name, view_name, opts, assert_fn, results):
            try:
                result = bucket.view_query(doc_name, view_name, opts)
                assert_fn(result, opts['limit'])
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        expected_count = 5
        opts = ViewOptions(limit=expected_count,
                           namespace=DesignDocumentNamespace.DEVELOPMENT)
        t = threading.Thread(target=run_test,
                             args=(cb_env.bucket,
                                   cb_env.DOCNAME,
                                   cb_env.TEST_VIEW_NAME,
                                   opts,
                                   cb_env.assert_rows,
                                   results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_view_query_key(self, cb_env):
        batch_id = cb_env.get_batch_id()
        expected_count = 10
        keys = cb_env.get_keys()
        key = keys[0]
        docids = cb_env.get_docids_by_key(key)
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT,
                           key=key)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)

        rows = cb_env.assert_rows(view_result, expected_count, True)
        for row in rows:
            assert isinstance(row, ViewRow)
            assert isinstance(row.id, str)
            assert isinstance(row.key, str)
            assert isinstance(row.value, dict)
            assert row.key == key
            assert row.value['batch'] == batch_id
            assert row.value['id'] in docids
        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_keys(self, cb_env):
        keys = cb_env.get_keys()
        expected_keys = keys[:2]
        expected_count = 20
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT,
                           keys=expected_keys)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)

        rows = cb_env.assert_rows(view_result, expected_count, True)
        assert all(map(lambda r: r.key in expected_keys, rows)) is True

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_raw(self, cb_env):
        # set the batch id
        cb_env.get_batch_id()
        keys = cb_env.get_keys()
        # take the largest key so that we can purposefully narrow the result to 1 record
        key_idx = keys.index(max(keys))
        key = keys[key_idx]
        key_docids = cb_env.get_docids_by_key(key)
        # purposefully use the last doc w/in the key
        startkey_docid = key_docids[-1]
        # execute a query so we can have pagination
        opts = ViewOptions(limit=5,
                           namespace=DesignDocumentNamespace.DEVELOPMENT)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)
        # need to iterate over the result to execute the query
        [r for r in view_result]
        raw = {
            'limit': '5',
            'startkey': json.dumps(key),
            'startkey_docid': startkey_docid,
            'full_set': 'true'
        }
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT, raw=raw)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)
        # expect only a single record to be returned
        expected_count = 1
        rows = cb_env.assert_rows(view_result, expected_count, True)
        assert rows[0].key == key
        assert rows[0].id == startkey_docid

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    def test_view_query_raw_fail(self, cb_env):
        raw = {
            'limit': '5',
            # this will fail as it is not encoded JSON
            'startkey': 'view-key',
            'startkey_docid': 'fake-doc-id'
        }
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT, raw=raw)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)
        with pytest.raises(InvalidArgumentException):
            [r for r in view_result]

    def test_view_query_startkey_docid(self, cb_env):
        # set the batch id
        cb_env.get_batch_id()
        keys = cb_env.get_keys()
        # take the largest key so that we can purposefully narrow the result to 1 record
        key_idx = keys.index(max(keys))
        key = keys[key_idx]
        key_docids = cb_env.get_docids_by_key(key)
        # purposefully use the last doc w/in the key
        startkey_docid = key_docids[-1]
        # execute a query so we can have pagination
        opts = ViewOptions(limit=5,
                           namespace=DesignDocumentNamespace.DEVELOPMENT)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)
        # need to iterate over the result to execute the query
        [r for r in view_result]
        opts = ViewOptions(limit=5,
                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                           startkey=key,
                           startkey_docid=startkey_docid)
        view_result = cb_env.bucket.view_query(cb_env.DOCNAME,
                                               cb_env.TEST_VIEW_NAME,
                                               opts)
        # expect only a single record to be returned
        expected_count = 1
        rows = cb_env.assert_rows(view_result, expected_count, True)
        assert rows[0].key == key
        assert rows[0].id == startkey_docid

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_view_query_timeout(self, cb_env):
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions
        conn_string = cb_env.config.get_connection_string()
        username, pw = cb_env.config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        # Prior to PYCBC-1521, this test would fail as each request would override the cluster level views_timeout.
        # If a timeout was not provided in the request, the default 75s timeout would be used.  PYCBC-1521 corrects
        # this behavior so this test will pass as we are essentially forcing an AmbiguousTimeoutException because
        # we are setting the cluster level views_timeout such a small value.
        timeout_opts = ClusterTimeoutOptions(views_timeout=timedelta(milliseconds=1))
        cluster = Cluster.connect(f'{conn_string}', ClusterOptions(auth, timeout_options=timeout_opts))
        # don't need to do this except for older server versions
        bucket = cluster.bucket(f'{cb_env.bucket.name}')
        with pytest.raises(AmbiguousTimeoutException):
            res = bucket.view_query(cb_env.DOCNAME,
                                    cb_env.TEST_VIEW_NAME,
                                    limit=10,
                                    namespace=DesignDocumentNamespace.DEVELOPMENT)
            [r for r in res.rows()]

        # if we override the timeout w/in the request the query should succeed.
        res = bucket.view_query(cb_env.DOCNAME,
                                cb_env.TEST_VIEW_NAME,
                                limit=10,
                                namespace=DesignDocumentNamespace.DEVELOPMENT,
                                timeout=timedelta(seconds=10))
        rows = [r for r in res.rows()]
        assert len(rows) > 0


class ClassicViewsTests(ViewsTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicViewsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicViewsTests) if valid_test_method(meth)]
        compare = set(ViewsTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = ViewsTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_views_mgmt()
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)
