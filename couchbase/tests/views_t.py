import pathlib
from os import path

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from couchbase.options import ClusterOptions
from couchbase.result import ViewResult
from couchbase.views import ViewMetaData

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
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster.connect(conn_string, opts)
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
        cluster.cluster_info()

        coll = bucket.default_collection()
        cb_env = TestEnvironment(cluster,
                                 bucket,
                                 coll,
                                 couchbase_config,
                                 manage_buckets=True,
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
