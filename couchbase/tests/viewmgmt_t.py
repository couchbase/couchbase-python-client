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

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
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

    @pytest.fixture()
    def create_test_view(self, cb_env, test_ddoc):
        cb_env.try_n_times(3, 5, cb_env.vixm.upsert_design_document, test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    @pytest.fixture()
    def drop_test_view(self, cb_env, test_ddoc):
        yield
        cb_env.try_n_times_till_exception(3, 5,
                                          cb_env.vixm.drop_design_document,
                                          test_ddoc.name,
                                          DesignDocumentNamespace.DEVELOPMENT,
                                          expected_exceptions=(DesignDocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture()
    def drop_test_view_from_prod(self, cb_env, test_ddoc):
        yield
        # drop from PROD
        try:
            cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex
        # now drop from DEV
        try:
            cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)
        except DesignDocumentNotFoundException:
            pass
        except Exception as ex:
            raise ex

    @pytest.mark.usefixtures("drop_test_view")
    def test_upsert_design_doc(self, cb_env, test_ddoc):
        # we started with this already in here, so this isn't really
        # necessary...`
        cb_env.vixm.upsert_design_document(test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    @pytest.mark.usefixtures("create_test_view")
    def test_drop_design_doc(self, cb_env, test_ddoc):
        cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)

    def test_drop_design_doc_fail(self, cb_env, test_ddoc):
        with pytest.raises(DesignDocumentNotFoundException):
            cb_env.vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    def test_get_design_document_fail(self, cb_env, test_ddoc):
        with pytest.raises(DesignDocumentNotFoundException):
            cb_env.vixm.get_design_document(test_ddoc.name,
                                            DesignDocumentNamespace.PRODUCTION,
                                            GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    def test_get_design_document(self, cb_env, test_ddoc):
        ddoc = cb_env.try_n_times(10, 3,
                                  cb_env.vixm.get_design_document,
                                  test_ddoc.name,
                                  DesignDocumentNamespace.DEVELOPMENT,
                                  GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
        assert ddoc.name == test_ddoc.name

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    def test_get_all_design_documents(self, cb_env, test_ddoc):
        # should start out in _some_ state.  Since we don't know for sure, but we
        # do know it does have self.DOCNAME in it in development ONLY, lets assert on that and that
        # it succeeds, meaning we didn't get an exception.
        # make sure it is there
        ddoc = cb_env.try_n_times(10, 3,
                                  cb_env.vixm.get_design_document,
                                  test_ddoc.name,
                                  DesignDocumentNamespace.DEVELOPMENT,
                                  GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
        # should also be in the get_all response
        result = cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.DEVELOPMENT,
                                                      GetAllDesignDocumentsOptions(timeout=timedelta(seconds=10)))
        names = [doc.name for doc in result if doc.name == test_ddoc.name]
        assert names.count(test_ddoc.name) > 0

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view")
    def test_get_all_design_documents_excludes_namespaces(self, cb_env, test_ddoc):
        # we know the test_ddoc.name is _only_ in development, so...
        result = cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.PRODUCTION)
        names = [doc.name for doc in result if doc.name == test_ddoc.name]
        assert names.count(test_ddoc.name) == 0

    @pytest.mark.usefixtures("create_test_view")
    @pytest.mark.usefixtures("drop_test_view_from_prod")
    def test_publish_design_doc(self, cb_env, test_ddoc):
        # make sure we have the ddoc
        ddoc = cb_env.try_n_times(10, 3,
                                  cb_env.vixm.get_design_document,
                                  test_ddoc.name,
                                  DesignDocumentNamespace.DEVELOPMENT,
                                  GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None

        # starts off not in prod
        with pytest.raises(DesignDocumentNotFoundException):
            cb_env.vixm.get_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

        cb_env.vixm.publish_design_document(test_ddoc.name, PublishDesignDocumentOptions(timeout=timedelta(seconds=10)))
        # should be in prod now
        cb_env.try_n_times(
            10,
            3,
            cb_env.vixm.get_design_document,
            test_ddoc.name,
            DesignDocumentNamespace.PRODUCTION)
        # and still in dev
        cb_env.try_n_times(
            10,
            3,
            cb_env.vixm.get_design_document,
            test_ddoc.name,
            DesignDocumentNamespace.DEVELOPMENT)
