#  Copyright 2016-2024. Couchbase, Inc.
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

# One suite over the five design-document operations bucket.view_indexes() offers.  Nothing
# here ever runs a view: a design document is created, read back, published and dropped, and
# its map functions are payloads rather than code, so this suite needs no bucket of its own,
# no seeded corpus and no wait for an index to catch up.

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest

from couchbase.exceptions import DesignDocumentNotFoundException
from couchbase.management.options import (GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions)
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from tests.assert_helpers import cb_assert_eq
from tests.environments.managers import TestEnvironmentViewIndexManager
from tests.environments.test_environment import (CollectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.test_features import ServerFeatures
from tests.view_defs import (MGMT_MAP_FUNCTION,
                             MGMT_SECONDARY_MAP_FUNCTION,
                             MGMT_SECONDARY_VIEW_NAME,
                             MGMT_VIEW_NAME,
                             mgmt_design_doc_name)


class ViewIndexManagementTestSuite(SuiteRequirements):
    MANAGERS = [TestEnvironmentViewIndexManager]
    COLLECTION_STRATEGY = CollectionStrategy.DEFAULT

    # Every test asserts on design-document metadata and none of them queries a view.
    NUM_DOCS = 0

    # Managing design documents needs no bucket management: unlike the query suite, this one
    # uses the shared test bucket rather than creating its own.
    REQUIRED_FEATURES = [ServerFeatures.ViewIndexManagement]

    # A design document name no test creates.
    MISSING_DESIGN_DOC_NAME = 'not-a-design-document'

    TEST_MANIFEST = [
        'test_drop_design_doc',
        'test_drop_design_doc_fail',
        'test_get_all_design_documents',
        'test_get_all_design_documents_excludes_namespaces',
        'test_get_design_document',
        'test_get_design_document_fail',
        'test_publish_design_doc',
        'test_publish_design_doc_fail',
        'test_upsert_design_doc',
        'test_upsert_design_doc_replaces',
    ]

    @staticmethod
    def _assert_views(ddoc: DesignDocument, expected: dict[str, str]) -> None:
        # `name` survives a manager that loses every view, so comparing it alone says nothing
        # about whether the document round-tripped; the views dict is the entire content.
        # Exactly, not at least: a design document is stored and returned whole, which is
        # also what makes this usable for the replace test.
        cb_assert_eq(sorted(ddoc.views.keys()), sorted(expected.keys()),
                     what='design document view names')
        for view_name, map_function in expected.items():
            view = ddoc.views[view_name]
            assert isinstance(view, View)
            cb_assert_eq(view.map, map_function, what=f'map function stored for view {view_name}')

    @pytest.fixture(name='test_ddoc')
    def get_test_ddoc(self) -> DesignDocument:
        return DesignDocument(name=mgmt_design_doc_name(),
                              views={MGMT_VIEW_NAME: View(map=MGMT_MAP_FUNCTION)})

    @pytest.fixture()
    def create_test_ddoc(self,
                         cb_env: SyncTestEnvironment,
                         test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        TestEnvironment.try_n_times(3, 5, vixm.upsert_design_document,
                                    test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

    @pytest.fixture()
    def drop_test_ddoc(self,
                       cb_env: SyncTestEnvironment,
                       test_ddoc: DesignDocument) -> Generator[None, None, None]:
        yield
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        # Retrying until the drop raises is what confirms it landed, and it costs nothing when
        # the test dropped the document itself: the first call then raises straight away.
        TestEnvironment.try_n_times_till_exception(
            10, 1, vixm.drop_design_document,
            test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
            expected_exceptions=(DesignDocumentNotFoundException,))

    @pytest.fixture()
    def drop_test_ddoc_from_prod(self,
                                 cb_env: SyncTestEnvironment,
                                 test_ddoc: DesignDocument) -> Generator[None, None, None]:
        yield
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        # Publishing copies the document into PRODUCTION and leaves the DEVELOPMENT original in
        # place, so a published document needs dropping from both namespaces.  Leaving the
        # PRODUCTION copy behind would break test_drop_design_doc_fail and
        # test_get_design_document_fail, which both assert this name is absent from PRODUCTION.
        for namespace in (DesignDocumentNamespace.PRODUCTION, DesignDocumentNamespace.DEVELOPMENT):
            TestEnvironment.try_n_times_till_exception(
                10, 1, vixm.drop_design_document, test_ddoc.name, namespace,
                expected_exceptions=(DesignDocumentNotFoundException,))

    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_upsert_design_doc(self,
                               cb_env: SyncTestEnvironment,
                               test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        vixm.upsert_design_document(test_ddoc, DesignDocumentNamespace.DEVELOPMENT)

        # Reading it back is the assertion.  The upsert call returning is not: it is typed to
        # return None, so on its own this test would prove only that nothing raised.
        ddoc = TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                           test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
                                           GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        cb_assert_eq(ddoc.name, test_ddoc.name, what='design document name')
        self._assert_views(ddoc, {MGMT_VIEW_NAME: MGMT_MAP_FUNCTION})

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_upsert_design_doc_replaces(self,
                                        cb_env: SyncTestEnvironment,
                                        test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        replacement = DesignDocument(
            name=test_ddoc.name,
            views={MGMT_SECONDARY_VIEW_NAME: View(map=MGMT_SECONDARY_MAP_FUNCTION)})
        vixm.upsert_design_document(replacement, DesignDocumentNamespace.DEVELOPMENT)

        # Replaces, rather than merging the two views dicts.  Asserting the absence of the
        # first view is the whole test: a merge would leave both, and every other test here
        # would still pass.  The retry polls for the replacement, not a readable document.
        def replaced() -> None:
            ddoc = vixm.get_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)
            self._assert_views(ddoc, {MGMT_SECONDARY_VIEW_NAME: MGMT_SECONDARY_MAP_FUNCTION})

        TestEnvironment.try_n_times(10, 3, replaced)

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_drop_design_doc(self,
                             cb_env: SyncTestEnvironment,
                             test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT)

        # Polling until the read raises is what "dropped" means.  Stopping at the call above
        # cannot distinguish a drop that worked from one the cluster ignored.
        TestEnvironment.try_n_times_till_exception(
            10, 3, vixm.get_design_document,
            test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
            expected_exceptions=(DesignDocumentNotFoundException,))

    def test_drop_design_doc_fail(self,
                                  cb_env: SyncTestEnvironment,
                                  test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        with pytest.raises(DesignDocumentNotFoundException):
            vixm.drop_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_get_design_document(self,
                                 cb_env: SyncTestEnvironment,
                                 test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        ddoc = TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                           test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
                                           GetDesignDocumentOptions(timeout=timedelta(seconds=5)))
        cb_assert_eq(ddoc.name, test_ddoc.name, what='design document name')
        self._assert_views(ddoc, {MGMT_VIEW_NAME: MGMT_MAP_FUNCTION})

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_get_design_document_fail(self,
                                      cb_env: SyncTestEnvironment,
                                      test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        with pytest.raises(DesignDocumentNotFoundException):
            vixm.get_design_document(test_ddoc.name,
                                     DesignDocumentNamespace.PRODUCTION,
                                     GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_get_all_design_documents(self,
                                      cb_env: SyncTestEnvironment,
                                      test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        # Wait for the document to be readable on its own before asking for the listing, so a
        # miss below is the listing being wrong rather than the upsert not having propagated.
        TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                    test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
                                    GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

        result = vixm.get_all_design_documents(
            DesignDocumentNamespace.DEVELOPMENT,
            GetAllDesignDocumentsOptions(timeout=timedelta(seconds=10)))
        matches = [doc for doc in result if doc.name == test_ddoc.name]
        # Exactly one, not at least one.  The listing carries whole design documents, so this
        # is also the one place the views can be checked as they were listed rather than as they
        # were fetched by name.
        cb_assert_eq(len(matches), 1,
                     what=f'copies of {test_ddoc.name} listed in the DEVELOPMENT namespace')
        self._assert_views(matches[0], {MGMT_VIEW_NAME: MGMT_MAP_FUNCTION})

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc')
    def test_get_all_design_documents_excludes_namespaces(self,
                                                          cb_env: SyncTestEnvironment,
                                                          test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                    test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
                                    GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

        prod = vixm.get_all_design_documents(DesignDocumentNamespace.PRODUCTION)
        prod_names = [doc.name for doc in prod]
        assert test_ddoc.name not in prod_names

        # The same listing against DEVELOPMENT does find it.  Without this the test passes on an
        # empty PRODUCTION response for any reason at all, including a listing that silently
        # returns nothing, so the absence above would say nothing about namespace filtering.
        dev = vixm.get_all_design_documents(DesignDocumentNamespace.DEVELOPMENT)
        dev_names = [doc.name for doc in dev]
        assert test_ddoc.name in dev_names

    @pytest.mark.usefixtures('create_test_ddoc')
    @pytest.mark.usefixtures('drop_test_ddoc_from_prod')
    def test_publish_design_doc(self,
                                cb_env: SyncTestEnvironment,
                                test_ddoc: DesignDocument) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                    test_ddoc.name, DesignDocumentNamespace.DEVELOPMENT,
                                    GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

        # Not in PRODUCTION to start with, which is what makes the reads afterwards mean
        # something: without it a document left over from an earlier run would satisfy them.
        with pytest.raises(DesignDocumentNotFoundException):
            vixm.get_design_document(test_ddoc.name, DesignDocumentNamespace.PRODUCTION)

        vixm.publish_design_document(
            test_ddoc.name, PublishDesignDocumentOptions(timeout=timedelta(seconds=10)))

        published = TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                                test_ddoc.name, DesignDocumentNamespace.PRODUCTION)
        cb_assert_eq(published.name, test_ddoc.name, what='published design document name')
        self._assert_views(published, {MGMT_VIEW_NAME: MGMT_MAP_FUNCTION})

        # Publish copies rather than moves, so the DEVELOPMENT original is still there.
        still_in_dev = TestEnvironment.try_n_times(10, 3, vixm.get_design_document,
                                                   test_ddoc.name,
                                                   DesignDocumentNamespace.DEVELOPMENT)
        cb_assert_eq(still_in_dev.name, test_ddoc.name,
                     what='design document remaining in the DEVELOPMENT namespace')
        self._assert_views(still_in_dev, {MGMT_VIEW_NAME: MGMT_MAP_FUNCTION})

    def test_publish_design_doc_fail(self, cb_env: SyncTestEnvironment) -> None:
        vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
        with pytest.raises(DesignDocumentNotFoundException):
            vixm.publish_design_document(self.MISSING_DESIGN_DOC_NAME)


class ViewIndexManagementTests(ViewIndexManagementTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(ViewIndexManagementTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, ViewIndexManagementTestSuite)
        yield env
        teardown_environment(env, ViewIndexManagementTestSuite)
