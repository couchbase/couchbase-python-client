#
# Copyright 2015, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from couchbase.tests.base import RealServerTestCase
from couchbase.user_constants import FMT_JSON
from couchbase.views.params import SpatialQuery

DESIGN_JSON = {
    'language': 'javascript',
    'spatial': {
        'simpleGeo':
            '''
            function(doc) {
                if (doc.loc) {
                    emit({"type":"Point", "coordinates": doc.loc}, null);
                }
            }
            '''.replace("\n", '')
    }
}

DOCS_JSON = {
    'mountain-view_ca_usa': {
        'locname': ['Oakland', 'CA', 'USA'],
        'loc': [-122, 37]
    },
    'reno_nv_usa': {
        'locname': ['Reno', 'NV', 'USA'],
        'loc': [-119, 39]
    },
    'guayaquil_guayas_ec': {
        'locname': ['Guayaquil', 'Guayas', 'Ecuador'],
        'loc': [-79, -2]
    },
    'banos_tungurahua_ec': {
        'locname': ['Banos', 'Tungurahua', 'Ecuador'],
        'loc': [-78, -1]
    }
}


class SpatialTest(RealServerTestCase):
    def setUp(self):
        super(SpatialTest, self).setUp()
        mgr = self.cb.bucket_manager()
        ret = mgr.design_create('geo', DESIGN_JSON, use_devmode=False)
        self.assertTrue(ret.success)
        self.assertTrue(self.cb.upsert_multi(DOCS_JSON, format=FMT_JSON).all_ok)

    def test_simple_spatial(self):
        spq = SpatialQuery()

        # Get all locations within five degress of the equator
        spq.start_range = [None, -5]
        spq.end_range = [None, 5]
        rows_found = [r for r in self.cb.query('geo', 'simpleGeo', query=spq)]
        self.assertEqual(2, len(rows_found))

        # Get everything on the US west
        spq.start_range = [-130, None]
        spq.end_range  = [-110, None]
        rows_found = [r for r in self.cb.query('geo', 'simpleGeo', query=spq)]
        self.assertEqual(2, len(rows_found))

        # Sanity check: Ensure we actually did filtering earlier on!
        spq = SpatialQuery()
        rows_found = [r for r in self.cb.query('geo', 'simpleGeo', query=spq)]
        self.assertTrue(len(rows_found) > 2)
