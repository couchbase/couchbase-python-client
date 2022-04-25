import asyncio
import pathlib
from os import path

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
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

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope='class')
    def test_ddoc(self):
        view_data = None
        with open(self.TEST_VIEW_PATH) as view_file:
            view_data = view_file.read()

        view = View(map=view_data)
        ddoc = DesignDocument(name=self.DOCNAME, views={self.TEST_VIEW_NAME: view})
        return ddoc

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config, test_ddoc):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = await Cluster.connect(conn_string, opts)
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
        await bucket.on_connect()
        await cluster.cluster_info()

        coll = bucket.default_collection()
        cb_env = TestEnvironment(cluster,
                                 bucket,
                                 coll,
                                 couchbase_config,
                                 manage_buckets=True,
                                 manage_view_indexes=True)

        await self.create_ddoc(cb_env, test_ddoc)
        await cb_env.try_n_times(5, 3, cb_env.load_data)
        # let it load a bit...
        for _ in range(5):
            row_count_good = await self._check_row_count(cb_env, 5)
            if not row_count_good:
                print('Waiting for view to load, sleeping a bit...')
                await asyncio.sleep(5)
        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        await self.drop_ddoc(cb_env, test_ddoc)
        await cluster.close()

    async def create_ddoc(self, cb_env, test_ddoc):
        await cb_env.vixm.upsert_design_document(test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    async def drop_ddoc(self, cb_env, test_ddoc):
        try:
            await cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex

    async def _check_row_count(self, cb_env,
                               min_count  # type: int
                               ) -> bool:

        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               limit=min_count,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)
        count = 0
        async for _ in view_result.rows():
            count += 1
        return count >= min_count

    async def assert_rows(self,
                          result,  # type: ViewResult
                          expected_count):
        count = 0
        assert isinstance(result, ViewResult)
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    @pytest.mark.asyncio
    async def test_view_query(self, cb_env):

        expected_count = 10
        view_result = cb_env.bucket.view_query(self.DOCNAME,
                                               self.TEST_VIEW_NAME,
                                               limit=expected_count,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)

        await self.assert_rows(view_result, expected_count)

        metadata = view_result.metadata()
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() >= expected_count
