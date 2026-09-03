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

from typing import Any, cast

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.logic.collection_req_builder import CollectionRequestBuilder
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.pycbc_core import pycbc_connection
from couchbase.transcoder import JSONTranscoder

# (the scan, the tagged dict the binding layer reads it as).
SCAN_TYPES = [
    (RangeScan(),
     {'scan_type': 'range_scan'}),
    (RangeScan(ScanTerm('start'), ScanTerm('end', True)),
     {'scan_type': 'range_scan',
      'from': {'term': 'start', 'exclusive': None},
      'to': {'term': 'end', 'exclusive': True}}),
    (PrefixScan('prefix'),
     {'scan_type': 'prefix_scan', 'prefix': 'prefix'}),
    (SamplingScan(10),
     {'scan_type': 'sampling_scan', 'limit': 10}),
    (SamplingScan(10, 5),
     {'scan_type': 'sampling_scan', 'limit': 10, 'seed': 5}),
]


def _request_builder() -> CollectionRequestBuilder:
    return CollectionRequestBuilder(
        CollectionDetails('default', 'test-scope', 'test-collection', JSONTranscoder()))


class CollectionRequestBuilderUnitTests:
    # No cb_env and no TEST_MANIFEST: every test builds a `CollectionRequestBuilder` directly
    # and reads the scan arguments it would hand the core.  Not generated either, since all
    # three trees tag a scan through this one builder.
    #
    # `kv_range_scan_t` drives real scans, which reaches the tagging only for the types a
    # server accepts and not at all on a build without the feature.  The rejections below have
    # no live form: `scan()` never gets as far as a request.

    @pytest.mark.parametrize('scan, tagged', SCAN_TYPES)
    def test_scan_type_tagged_dict(self, scan: Any, tagged: dict[str, Any]) -> None:
        assert _request_builder()._get_scan_type(scan) == tagged

    def test_scan_type_rejects_an_unknown_scan(self) -> None:
        # The string is the tag `_get_scan_type` emits for a RangeScan, so this is the near
        # miss rather than an arbitrary bad value.
        with pytest.raises(InvalidArgumentException):
            _request_builder()._get_scan_type('range_scan')  # type: ignore[arg-type]

    @pytest.mark.parametrize('limit', [0, -10])
    def test_sampling_scan_rejects_a_non_positive_limit(self, limit: int) -> None:
        # `SamplingScan(0)` constructs; the limit is checked here, on the way out.
        with pytest.raises(InvalidArgumentException):
            _request_builder()._get_scan_type(SamplingScan(limit))

    def test_range_scan_request_carries_the_tagged_scan_type(self) -> None:
        # The builder stores the connection on the request and never reads it, so None stands
        # in for one.  A cast rather than an ignore, because that is the whole claim.
        req = _request_builder().build_range_scan_request(cast(pycbc_connection, None),
                                                          PrefixScan('prefix'))

        assert req._scan_args['scan_type'] == {'scan_type': 'prefix_scan', 'prefix': 'prefix'}
        # `scan_config` is a `_core` key the Python builder has never set.
        assert 'scan_config' not in req._scan_args
