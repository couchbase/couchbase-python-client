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

from __future__ import annotations

import json

from couchbase.views import ViewMetaData, ViewRow


class ViewResultTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: both classes are built straight from the dict the core
    # would have handed them, and `couchbase.views` is the only definition of either.  Not
    # generated for that reason.  `views_t` counts rows from real queries, which is a stronger
    # claim than any metadata field; what it cannot reach is the count's type and the
    # difference between an absent count and a zero one.

    def test_metadata_total_rows_is_a_plain_int(self) -> None:
        metadata = ViewMetaData({'total_rows': 12})
        total_rows = metadata.total_rows()

        assert total_rows == 12
        # Search and analytics wrap their counts in `UnsignedInt64`, which is neither orderable
        # against an int nor equal to one, so this line is specific to views.
        assert total_rows is not None and total_rows >= 12

    def test_metadata_total_rows_absent(self) -> None:
        assert ViewMetaData({}).total_rows() is None

    def test_metadata_total_rows_zero(self) -> None:
        assert ViewMetaData({'total_rows': 0}).total_rows() == 0

    def test_row_from_json_decodes_key_and_value(self) -> None:
        # Views sends both as JSON text rather than as JSON values, so a compound key arrives
        # as a list and not as the string that carried it.
        row = ViewRow.from_json({
            'id': 'doc-1',
            'key': json.dumps(['compound', 1]),
            'value': json.dumps({'count': 2}),
        })

        assert row.id == 'doc-1'
        assert row.key == ['compound', 1]
        assert row.value == {'count': 2}

    def test_row_from_json_without_key_or_value(self) -> None:
        row = ViewRow.from_json({'id': 'doc-1'})

        assert row.id == 'doc-1'
        assert row.key is None
        assert row.value is None
        # Only a query asked to include the documents fills this in, after the row is decoded.
        assert row.document is None
