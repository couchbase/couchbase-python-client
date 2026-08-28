#  Copyright 2016-2026. Couchbase, Inc.
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

from couchbase.management.logic.view_index_mgmt_types import DesignDocument, View

MAP_FN = 'function(doc, meta) { emit(meta.id, null); }'


class ViewTypesTestSuite:
    TEST_MANIFEST = [
        'test_design_document_from_json',
        'test_view_from_json',
        'test_view_from_json_carrying_name',
    ]

    def test_view_from_json(self):
        view = View.from_json({'map': MAP_FN, 'reduce': '_count'})

        assert view.map == MAP_FN
        assert view.reduce == '_count'

    def test_view_from_json_carrying_name(self):
        # the core adds name to every view dict it builds, so from_json has to tolerate it
        view = View.from_json({'map': MAP_FN, 'name': 'by_id'})

        assert view.map == MAP_FN
        assert view.reduce is None

    def test_design_document_from_json(self):
        ddoc = DesignDocument.from_json({
            'name': 'test-ddoc',
            'namespace': 'production',
            'views': {'by_id': {'map': MAP_FN, 'reduce': '_count', 'name': 'by_id'}},
        })

        assert ddoc.name == 'test-ddoc'
        assert ddoc.get_view('by_id').map == MAP_FN
        assert ddoc.get_view('by_id').reduce == '_count'


class ClassicViewTypesTests(ViewTypesTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicViewTypesTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicViewTypesTests) if valid_test_method(meth)]
        manifest_invalid = set(ViewTypesTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
