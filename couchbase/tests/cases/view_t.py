#
# Copyright 2013, Couchbase, Inc.
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
import json

from couchbase.tests.base import ViewTestCase
from couchbase.user_constants import FMT_JSON
from couchbase.exceptions import HTTPError

DESIGN_JSON = {
    'language' : 'javascript',
    'views' : {
        'recent_posts' : {
            'map' :
            """
            function(doc) {
                if (doc.date && doc.title) {
                    emit(doc.date, doc.title);
                }
            }
            """.replace("\n", '')
        }
    }
}

DOCS_JSON = {
    "bought-a-cat" : {
        "title" : "Bought a Cat",
        "body" : "I went to the pet store earlier and brought home a "
                "little kitty",
        "date" : "2009/01/30 18:04:11"
    },
    "biking" : {
        "title" : "Biking",
        "body" : "My biggest hobby is mountainbiking. The other day..",
        "date" : "2009/01/30 18:04:11"
    },
    "hello-world" : {
        "title" : "Hello World",
        "body" : "Well hello and welcome to my new blog",
        "date" : "2009/01/15 15:52:20"
    }
}

class ViewTest(ViewTestCase):
    def setUp(self):
        super(ViewTest, self).setUp()
        self.skipIfMock()
        mgr = self.cb.bucket_manager()
        ret = mgr.design_create('blog', DESIGN_JSON, use_devmode=False)
        self.assertTrue(ret.success)
        self.assertTrue(self.cb.upsert_multi(DOCS_JSON, format=FMT_JSON).all_ok)

    def test_simple_view(self):
        ret = self.cb._view("blog", "recent_posts",
                            params={ 'stale' : 'false' })
        self.assertTrue(ret.success)
        rows = ret.value
        self.assertIsInstance(rows, dict)
        print(rows)
        self.assertTrue(rows['total_rows']  >= 3)
        self.assertTrue(len(rows['rows']) == rows['total_rows'])

    def test_with_params(self):
        ret = self.cb._view("blog", "recent_posts",
                            params={'limit':1})
        self.assertTrue(ret.success)
        rows = ret.value['rows']
        self.assertEqual(len(rows), 1)

    def test_with_strparam(self):
        ret = self.cb._view("blog", "recent_posts", params='limit=2')
        self.assertTrue(ret.success)
        self.assertEqual(len(ret.value['rows']), 2)

    def test_with_jparams(self):
        jkey_pure = '2009/01/15 15:52:20'

        ret = self.cb._view("blog", "recent_posts",
                            params={
                                'startkey' : jkey_pure,
                                'endkey' : jkey_pure,
                                'inclusive_end' : 'true'
                            })
        print(ret)
        self.assertTrue(ret.success)
        rows = ret.value['rows']
        self.assertTrue(len(rows) == 1)
        single_row = rows[0]
        self.assertEqual(single_row['id'], 'hello-world')
        self.assertEqual(single_row['key'], jkey_pure)


        jkey_pure = []
        for v in DOCS_JSON.values():
            curdate = v['date']
            jkey_pure.append(curdate)

        ret = self.cb._view("blog", "recent_posts",
                            params={
                                'keys' : jkey_pure
                            })
        self.assertTrue(ret.success)
        self.assertTrue(len(ret.value['rows']), 3)
        for row in ret.value['rows']:
            self.assertTrue(row['id'] in DOCS_JSON)
            self.assertTrue(row['key'] in jkey_pure)

    def test_missing_view(self):
        self.assertRaises(HTTPError,
                          self.cb._view,
                          "nonexist", "designdoc")
