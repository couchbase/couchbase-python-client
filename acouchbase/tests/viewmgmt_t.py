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

import pathlib
from datetime import timedelta
from os import path

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.options import (GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions)
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)

from ._test_utils import TestEnvironment


@pytest.mark.flaky(reruns=5, reruns_delay=2)
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
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_view_indexes=True)

        yield cb_env

    @pytest.fixture(scope='class')
    def test_ddoc(self):
        view_data = None
        with open(self.TEST_VIEW_PATH) as view_file:
            view_data = view_file.read()

        view = View(map=view_data)
        ddoc = DesignDocument(name=self.DOCNAME, views={self.TEST_VIEW_NAME: view})
        return ddoc

    @pytest_asyncio.fixture()
    async def create_test_view(self, cb_env, test_ddoc):
        await cb_env.try_n_times(3, 5,
                                 cb_env.vixm.upsert_design_document,
                                 test_ddoc,
                                 DesignDocumentNamespace.DEVELOPMENT)

    @pytest_asyncio.fixture()
    async def drop_test_view(self, cb_env, test_ddoc):
        yield
        await cb_env.try_n_times_till_exception(10, 1,
                                                cb_env.vixm.drop_design_document,
                                                test_ddoc.name,
                                                DesignDocumentNamespace.DEVELOPMENT,
                                                expected_exceptions=(DesignDocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

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
        ddoc = await cb_env.try_n_times(10, 3,
                                        cb_env.vixm.get_design_document,
                                        test_ddoc.name,
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
        # make sure it is there
        ddoc = await cb_env.try_n_times(10, 3,
                                        cb_env.vixm.get_design_document,
                                        test_ddoc.name,
                                        DesignDocumentNamespace.DEVELOPMENT,
                                        GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
        # should also be in the get_all response
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
        # make sure we have the ddoc
        ddoc = await cb_env.try_n_times(10, 3,
                                        cb_env.vixm.get_design_document,
                                        test_ddoc.name,
                                        DesignDocumentNamespace.DEVELOPMENT,
                                        GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
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
