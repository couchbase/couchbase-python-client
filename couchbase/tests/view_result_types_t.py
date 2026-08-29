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

import json

import pytest

from couchbase.views import ViewMetaData, ViewRow


class ViewResultTypesTestSuite:
    TEST_MANIFEST = [
        'test_metadata_total_rows_absent',
        'test_metadata_total_rows_is_a_plain_int',
        'test_row_from_json_decodes_key_and_value',
        'test_row_from_json_without_key_or_value',
    ]

    def test_metadata_total_rows_is_a_plain_int(self):
        # the other services wrap their counts in UnsignedInt64; views does not, and the
        # comparisons in views_t.py depend on it, since UnsignedInt64 has no __ge__
        metadata = ViewMetaData({'total_rows': 12})

        assert metadata.total_rows() == 12
        assert metadata.total_rows() >= 12

    def test_metadata_total_rows_absent(self):
        assert ViewMetaData({}).total_rows() is None

    def test_row_from_json_decodes_key_and_value(self):
        row = ViewRow.from_json({
            'id': 'doc-1',
            'key': json.dumps(['compound', 1]),
            'value': json.dumps({'count': 2}),
        })

        assert row.id == 'doc-1'
        assert row.key == ['compound', 1]
        assert row.value == {'count': 2}

    def test_row_from_json_without_key_or_value(self):
        row = ViewRow.from_json({'id': 'doc-1'})

        assert row.id == 'doc-1'
        assert row.key is None
        assert row.value is None


class ClassicViewResultTypesTests(ViewResultTypesTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicViewResultTypesTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicViewResultTypesTests) if valid_test_method(meth)]
        manifest_invalid = set(ViewResultTypesTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
