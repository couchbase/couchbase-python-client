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

from datetime import timedelta

import pytest

from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.options import (GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions)
from couchbase.management.views import DesignDocumentNamespace
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment
from tests.environments.views_environment import ViewsTestEnvironment


class ViewIndexManagementTestSuite:
    TEST_MANIFEST = [
        'test_drop_design_doc',
        'test_drop_design_doc_fail',
        'test_get_all_design_documents',
        'test_get_all_design_documents_excludes_namespaces',
        'test_get_design_document',
        'test_get_design_document_fail',
        'test_publish_design_doc',
        'test_upsert_design_doc',
    ]

    @pytest.fixture()
    def create_test_view(self, cb_env):
        cb_env.add_test_ddoc()

    @pytest.fixture()
    def drop_test_view(self, cb_env):
        yield
        cb_env.drop_ddoc()

    @pytest.fixture()
    def drop_test_view_from_prod(self, cb_env):
        yield
        cb_env.drop_ddoc(from_prod=True)

    @pytest.mark.usefixtures('create_test_view')
    def test_drop_design_doc(self, cb_env):
        cb_env.add_test_ddoc()
        cb_env.vixm.drop_design_document(cb_env.test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)

    def test_drop_design_doc_fail(self, cb_env):
        with pytest.raises(DesignDocumentNotFoundException):
            cb_env.vixm.drop_design_document(cb_env.test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

    @pytest.mark.usefixtures('create_test_view')
    @pytest.mark.usefixtures('drop_test_view')
    def test_get_all_design_documents(self, cb_env):
        # should start out in _some_ state.  Since we don't know for sure, but we
        # do know it does have self.DOCNAME in it in development ONLY, lets assert on that and that
        # it succeeds, meaning we didn't get an exception.
        # make sure it is there
        ddoc = TestEnvironment.try_n_times(10,
                                           3,
                                           cb_env.vixm.get_design_document,
                                           cb_env.test_ddoc.name,
                                           DesignDocumentNamespace.DEVELOPMENT,
                                           GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
        # should also be in the get_all response
        result = cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.DEVELOPMENT,
                                                      GetAllDesignDocumentsOptions(timeout=timedelta(seconds=10)))
        names = [doc.name for doc in result if doc.name == cb_env.test_ddoc.name]
        assert names.count(cb_env.test_ddoc.name) > 0

    @pytest.mark.usefixtures('create_test_view')
    @pytest.mark.usefixtures('drop_test_view')
    def test_get_all_design_documents_excludes_namespaces(self, cb_env):
        # we know the test_ddoc.name is _only_ in development, so...
        result = cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.PRODUCTION)
        names = [doc.name for doc in result if doc.name == cb_env.test_ddoc.name]
        assert names.count(cb_env.test_ddoc.name) == 0

    @pytest.mark.usefixtures('create_test_view')
    @pytest.mark.usefixtures('drop_test_view')
    def test_get_design_document_fail(self, cb_env):
        with pytest.raises(DesignDocumentNotFoundException):
            cb_env.vixm.get_design_document(cb_env.test_ddoc.name,
                                            DesignDocumentNamespace.PRODUCTION,
                                            GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

    @pytest.mark.usefixtures('create_test_view')
    @pytest.mark.usefixtures('drop_test_view')
    def test_get_design_document(self, cb_env):
        ddoc = TestEnvironment.try_n_times(10,
                                           3,
                                           cb_env.vixm.get_design_document,
                                           cb_env.test_ddoc.name,
                                           DesignDocumentNamespace.DEVELOPMENT,
                                           GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None
        assert ddoc.name == cb_env.test_ddoc.name

    @pytest.mark.usefixtures('create_test_view')
    @pytest.mark.usefixtures('drop_test_view_from_prod')
    def test_publish_design_doc(self, cb_env):
        # make sure we have the ddoc
        ddoc = TestEnvironment.try_n_times(10,
                                           3,
                                           cb_env.vixm.get_design_document,
                                           cb_env.test_ddoc.name,
                                           DesignDocumentNamespace.DEVELOPMENT,
                                           GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        assert ddoc is not None

        # starts off not in prod
        with pytest.raises(DesignDocumentNotFoundException):
            cb_env.vixm.get_design_document(cb_env.test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

        cb_env.vixm.publish_design_document(cb_env.test_ddoc.name,
                                            PublishDesignDocumentOptions(timeout=timedelta(seconds=10)))
        # should be in prod now
        TestEnvironment.try_n_times(10,
                                    3,
                                    cb_env.vixm.get_design_document,
                                    cb_env.test_ddoc.name,
                                    DesignDocumentNamespace.PRODUCTION)
        # and still in dev
        TestEnvironment.try_n_times(10,
                                    3,
                                    cb_env.vixm.get_design_document,
                                    cb_env.test_ddoc.name,
                                    DesignDocumentNamespace.DEVELOPMENT)

    @pytest.mark.usefixtures('drop_test_view')
    def test_upsert_design_doc(self, cb_env):
        # we started with this already in here, so this isn't really
        # necessary...`
        cb_env.vixm.upsert_design_document(cb_env.test_ddoc, DesignDocumentNamespace.DEVELOPMENT)


class ClassicViewIndexManagementTests(ViewIndexManagementTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicViewIndexManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicViewIndexManagementTests) if valid_test_method(meth)]
        compare = set(ViewIndexManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = ViewsTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_views_mgmt()
        cb_env.setup(request.param, test_suite=self.__class__.__name__, num_docs=10)
        yield cb_env
        cb_env.teardown(request.param, test_suite=self.__class__.__name__)
