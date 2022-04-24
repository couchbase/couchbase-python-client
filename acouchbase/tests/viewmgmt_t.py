import pathlib
from datetime import timedelta
from os import path

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.options import (GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions)
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from couchbase.options import ClusterOptions

from ._test_utils import TestEnvironment


class ViewIndexManagementTests:

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

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        c = Cluster(
            conn_string, opts)
        await c.on_connect()
        await c.cluster_info()
        b = c.bucket(f"{couchbase_config.bucket_name}")
        await b.on_connect()

        coll = b.default_collection()
        cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True, manage_view_indexes=True)

        yield cb_env
        await c.close()

    @pytest.fixture(scope='class')
    def test_ddoc(self):
        view_data = None
        with open(self.TEST_VIEW_PATH) as view_file:
            view_data = view_file.read()

        # print(view_data)
        view = View(map=view_data)
        ddoc = DesignDocument(name=self.DOCNAME, views={self.TEST_VIEW_NAME: view})
        return ddoc

    @pytest_asyncio.fixture()
    async def create_test_view(self, cb_env, test_ddoc):
        await cb_env.vixm.upsert_design_document(test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    @pytest_asyncio.fixture()
    async def drop_test_view(self, cb_env, test_ddoc):
        yield
        try:
            await cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex

    @pytest_asyncio.fixture()
    async def drop_test_view_from_prod(self, cb_env, test_ddoc):
        yield
        # drop from PROD
        try:
            await cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex
        # now drop from DEV
        try:
            await cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex

    @pytest.mark.usefixtures("drop_test_view")
    @pytest.mark.asyncio
    async def test_upsert_design_doc(self, cb_env, test_ddoc):
        # we started with this already in here, so this isn't really
        # necessary...`
        await cb_env.vixm.upsert_design_document(test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.asyncio
    async def test_drop_design_doc(self, cb_env, test_ddoc):
        await cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)

    @pytest.mark.asyncio
    async def test_drop_design_doc_fail(self, cb_env, test_ddoc):
        with pytest.raises(DesignDocumentNotFoundException):
            await cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    @pytest.mark.asyncio
    async def test_get_design_document_fail(self, cb_env, test_ddoc):
        with pytest.raises(DesignDocumentNotFoundException):
            await cb_env.vixm.get_design_document(test_ddoc.name,
                                                  DesignDocumentNamespace.PRODUCTION,
                                                  GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    @pytest.mark.asyncio
    async def test_get_design_document(self, cb_env, test_ddoc):
        ddoc = await cb_env.vixm.get_design_document(test_ddoc.name,
                                                     DesignDocumentNamespace.DEVELOPMENT,
                                                     GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
        assert ddoc.name == test_ddoc.name

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    @pytest.mark.asyncio
    async def test_get_all_design_documents(self, cb_env, test_ddoc):
        # should start out in _some_ state.  Since we don't know for sure, but we
        # do know it does have self.DOCNAME in it in development ONLY, lets assert on that and that
        # it succeeds, meaning we didn't get an exception.
        result = await cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.DEVELOPMENT,
                                                            GetAllDesignDocumentsOptions(timeout=timedelta(seconds=10)))
        names = [doc.name for doc in result if doc.name == test_ddoc.name]
        assert names.count(test_ddoc.name) > 0

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    @pytest.mark.asyncio
    async def test_get_all_design_documents_excludes_namespaces(self, cb_env, test_ddoc):
        # we know the test_ddoc.name is _only_ in development, so...
        result = await cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.PRODUCTION)
        names = [doc.name for doc in result if doc.name == test_ddoc.name]
        assert names.count(test_ddoc.name) == 0

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view_from_prod")
    @pytest.mark.asyncio
    async def test_publish_design_doc(self, cb_env, test_ddoc):
        # starts off not in prod
        with pytest.raises(DesignDocumentNotFoundException):
            await cb_env.vixm.get_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

        await cb_env.vixm.publish_design_document(test_ddoc.name,
                                                  PublishDesignDocumentOptions(timeout=timedelta(seconds=10)))
        # should be in prod now
        await cb_env.try_n_times(
            10,
            3,
            cb_env.vixm.get_design_document,
            test_ddoc.name,
            DesignDocumentNamespace.PRODUCTION)
        # and still in dev
        await cb_env.try_n_times(
            10,
            3,
            cb_env.vixm.get_design_document,
            test_ddoc.name,
            DesignDocumentNamespace.DEVELOPMENT)
