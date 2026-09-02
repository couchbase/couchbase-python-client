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

from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.logic.collection_req_builder import CollectionRequestBuilder
from couchbase.logic.collection_types import CollectionDetails
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


def request_builder():
    details = CollectionDetails('default', 'test-scope', 'test-collection', JSONTranscoder())
    return CollectionRequestBuilder(details)


class CollectionRequestBuilderTestSuite:
    TEST_MANIFEST = [
        'test_range_scan_request_carries_the_tagged_scan_type',
        'test_sampling_scan_rejects_a_non_positive_limit',
        'test_scan_type_rejects_an_unknown_scan',
        'test_scan_type_tagged_dict',
    ]

    @pytest.mark.parametrize('scan, tagged', SCAN_TYPES)
    def test_scan_type_tagged_dict(self, scan, tagged):
        assert request_builder()._get_scan_type(scan) == tagged

    def test_scan_type_rejects_an_unknown_scan(self):
        with pytest.raises(InvalidArgumentException):
            request_builder()._get_scan_type('range_scan')

    @pytest.mark.parametrize('limit', [0, -10])
    def test_sampling_scan_rejects_a_non_positive_limit(self, limit):
        with pytest.raises(InvalidArgumentException):
            request_builder()._get_scan_type(SamplingScan(limit))

    def test_range_scan_request_carries_the_tagged_scan_type(self):
        req = request_builder().build_range_scan_request(None, PrefixScan('prefix'))

        assert req._scan_args['scan_type'] == {'scan_type': 'prefix_scan', 'prefix': 'prefix'}
        assert 'scan_config' not in req._scan_args


class ClassicCollectionRequestBuilderTests(CollectionRequestBuilderTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicCollectionRequestBuilderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicCollectionRequestBuilderTests) if valid_test_method(meth)]
        manifest_invalid = set(CollectionRequestBuilderTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
