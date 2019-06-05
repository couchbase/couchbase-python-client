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

from couchbase_tests.base import ViewTestCase, SkipTest
from couchbase_v2.views.iterator import (
    View, ViewRow, RowProcessor, AlreadyQueriedError)

from couchbase_v2.views.params import Query, UNSPEC
from couchbase_v2.exceptions import ArgumentError, CouchbaseError, HTTPError
from couchbase_core._pyport import xrange
from couchbase_core._bootstrap import MAX_URI_LENGTH

# We'll be using the beer-sample database as it has a sufficiently large
# dataset with well-defined return values

class Brewery(object):
    __ALL_BREWERIES = {}
    __BY_ID = {}

    def __init__(self, id, doc):
        self._id = id
        self.name = doc['name']
        self.city = doc['city']
        self.code = doc['code']
        self.country = doc['country']
        self.phone = doc['phone']
        self.website = doc['website']
        self.type = doc['type']
        self.updated = doc['updated']
        self.description = doc['description']
        self.address = doc['address']

        self.__ALL_BREWERIES[self.name] = self
        self.__BY_ID[self._id] = self

    @classmethod
    def get_brewery(cls, name):
        return cls.__ALL_BREWERIES.get(name)

    @classmethod
    def by_id(cls, name):
        return cls.__BY_ID.get(name)

class Beer(object):
    __ALL_BEERS = {}
    __BY_ID = {}

    @classmethod
    def get_beer(cls, name):
        return cls.__ALL_BEERS.get(name)

    @classmethod
    def by_id(cls, name):
        return cls.__BY_ID.get(name)

    def __init__(self, id, doc):
        self._id = id
        self.name = doc['name']
        self.brewery = Brewery.by_id(doc['brewery_id'])
        self.abv = doc['abv']
        self.updated = doc['updated']
        self.description = doc['description']
        self.category = doc['category']
        self.style = doc['style']

        self.__ALL_BEERS[self.name] = self
        self.__BY_ID[self._id] = self


class BreweryBeerRowProcessor(object):
    """
    This specialized processor will attempt to fetch the name of the beers
    and breweries which it gets, trying to ensure maximum efficiency.

    This only returns beers, skipping over any breweries.
    """
    def __init__(self):
        # Iterates over names of beers. We get them via 'get_beer'.
        self._riter = None

    def handle_rows(self, rows, connection, include_docs):
        """
        This shows an example of an efficient 'include_docs' algorithm
        which fetches the beers and relevant breweries in a single sweep,
        skipping over those that are already cached locally.
        """

        breweries_to_fetch = set()
        beers_to_fetch = set()

        # The order of the keys returned in the result set.
        retkeys = []
        pre_included = {}

        for r in rows:
            if len(r['key']) == 1:
                # It's a brewery
                continue

            brewery_id, beer_id = r['key']
            retkeys.append(beer_id)

            if not Brewery.by_id(brewery_id):
                breweries_to_fetch.add(brewery_id)

            if not Beer.by_id(beer_id):
                beers_to_fetch.add(beer_id)

            if r.get('__DOCRESULT__'):
                pre_included[r['id']] = r['__DOCRESULT__']

        self._riter = iter(retkeys)

        if beers_to_fetch or breweries_to_fetch:
            if not include_docs and not pre_included:
                raise ValueError(
                    "Don't have all documents, but include_docs was set to False")

            keys_to_fetch = list(breweries_to_fetch) + list(beers_to_fetch)
            keys_to_fetch = [x for x in keys_to_fetch if x not in pre_included]
            docs = {}
            docs.update(pre_included)

            if keys_to_fetch:
                docs.update(connection.get_multi(keys_to_fetch))

            for brewery in breweries_to_fetch:
                Brewery(brewery, docs[brewery].value)

            for beer in beers_to_fetch:
                Beer(beer, docs[beer].value)

        return iter(self)

    def __iter__(self):
        if not self._riter:
            return

        for b in self._riter:
            beer = Beer.by_id(b)
            assert beer, "Eh?"

            yield beer

        self._riter = None


class ViewIteratorTest(ViewTestCase):
    def setUp(self):
        try:
            super(ViewIteratorTest, self).setUp(bucket='beer-sample')
        except CouchbaseError:
            raise SkipTest("Need 'beer-sample' bucket for this")
        self.skipIfMock()

    def test_simple_query(self):
        ret = self.cb.query("beer", "brewery_beers", limit=3)
        self.assertIsInstance(ret, View)
        self.assertIsInstance(ret.row_processor, RowProcessor)

        count = 0
        rows = list(ret)
        self.assertEqual(len(rows), 3)
        for r in rows:
            self.assertIsInstance(r, ViewRow)

    def test_include_docs(self):
        ret = self.cb.query("beer", "brewery_beers", limit=10,
                            include_docs=True)
        rows = list(ret)
        self.assertEqual(len(rows), 10)
        for r in rows:
            self.assertIsInstance(r.doc, self.cls_Result)
            doc = r.doc
            mc_doc = self.cb.get(r.docid, quiet=True)
            self.assertEqual(doc.cas, mc_doc.cas)
            self.assertEqual(doc.value, mc_doc.value)
            self.assertTrue(doc.success)

        # Try with reduce
        self.assertRaises(ArgumentError,
                          self.cb.query,
                          "beer", "by_location",
                          reduce=True,
                          include_docs=True)

    def test_bad_view(self):
        ret = self.cb.query("beer", "bad_view")
        self.assertIsInstance(ret, View)
        self.assertRaises(HTTPError,
                          tuple, ret)

    def test_streaming(self):
        ret = self.cb.query("beer", "brewery_beers", streaming=True, limit=100)
        rows = list(ret)
        self.assertEqual(len(rows), 100)

        # Get all the views
        ret = self.cb.query("beer", "brewery_beers", streaming=True)
        rows = list(ret)
        self.assertTrue(len(rows))
        self.assertEqual(len(rows), ret.indexed_rows)

        self.assertTrue(ret.raw.value)
        self.assertIsInstance(ret.raw.value, dict)
        self.assertTrue('total_rows' in ret.raw.value)

    def test_streaming_dtor(self):
        # Ensure that the internal lcb_http_request_t is destroyed if the
        # Python object is destroyed before the results are done.

        ret = self.cb.query("beer", "brewery_beers", streaming=True)
        v = iter(ret)
        try:
            v.next()
        except AttributeError:
            v.__next__()

        del ret

    def test_mixed_query(self):
        self.assertRaises(ArgumentError,
                          self.cb.query,
                          "d", "v",
                          query=Query(),
                          limit=10)

        self.cb.query("d","v", query=Query(limit=5).update(skip=15))

    def test_range_query(self):
        q = Query()

        q.mapkey_range = [
            ["abbaye_de_maredsous"],
            ["abbaye_de_maredsous", Query.STRING_RANGE_END]
        ]

        q.inclusive_end = True

        ret = self.cb.query("beer", "brewery_beers", query=q)
        rows = list(ret)
        self.assertEqual(len(rows), 4)

        q.mapkey_range = [ ["u"], ["v"] ]
        ret = self.cb.query("beer", "brewery_beers", query=q)
        self.assertEqual(len(list(ret)), 88)

        q.mapkey_range = [ ["u"], ["uppper"+Query.STRING_RANGE_END]]
        ret = self.cb.query("beer", "brewery_beers", query=q)
        rows = list(ret)
        self.assertEqual(len(rows), 56)

    def test_key_query(self):
        q = Query()
        q.mapkey_single = ["abbaye_de_maredsous"]
        ret = self.cb.query("beer", "brewery_beers", query=q)
        rows = list(ret)
        self.assertEqual(len(rows), 1)

        q.mapkey_single = UNSPEC
        q.mapkey_multi = [["abbaye_de_maredsous"],
                          ["abbaye_de_maredsous", "abbaye_de_maredsous-8"]]
        ret = self.cb.query("beer", "brewery_beers", query=q)
        rows = list(ret)
        self.assertEqual(len(rows), 2)

    def test_row_processor(self):
        rp = BreweryBeerRowProcessor()
        q = Query(limit=20)

        ret = self.cb.query("beer", "brewery_beers",
                            query=q,
                            row_processor=rp,
                            include_docs=True)

        beers = list(ret)
        for b in beers:
            self.assertIsInstance(b, Beer)
            self.assertIsInstance(b.brewery, Brewery)

        ret = self.cb.query("beer", "brewery_beers",
                            query=q,
                            row_processor=rp,
                            include_docs=False)

        list(ret)

        ret = self.cb.query("beer", "brewery_beers",
                            row_processor=rp,
                            include_docs=False,
                            limit=40)

        self.assertRaises(ValueError, list, ret)

    def test_already_queried(self):
        ret = self.cb.query("beer", "brewery_beers", limit=5)
        list(ret)
        self.assertRaises(AlreadyQueriedError, list, ret)

    def test_no_rows(self):
        ret = self.cb.query("beer", "brewery_beers", limit=0)
        for row in ret:
            raise Exception("...")


    def test_long_uri(self):
        qobj = Query()
        qobj.mapkey_multi = [ str(x) for x in xrange(MAX_URI_LENGTH) ]
        ret = self.cb.query("beer", "brewery_beers", query=qobj)
        # No assertions, just make sure it didn't break
        for row in ret:
            raise Exception("...")

        # Apparently only the "keys" parameter is supposed to be in POST.
        # Let's fetch 100 items now
        keys = [r.key for r in self.cb.query("beer", "brewery_beers", limit=100)]
        self.assertEqual(100, len(keys))

        kslice = keys[90:]
        self.assertEqual(10, len(kslice))
        rows = [x for x in self.cb.query("beer", "brewery_beers", mapkey_multi=kslice, limit=5)]
        self.assertEqual(5, len(rows))
        for row in rows:
            self.assertTrue(row.key in kslice)


    def _verify_data(self, ret):
        list(ret)
        data = ret.raw.value
        self.assertTrue('rows' in data)
        self.assertTrue('total_rows' in data)
        self.assertTrue('debug_info' in data)


    def test_http_data(self):
        q = Query(limit=30, debug=True)
        self._verify_data(self.cb.query("beer", "brewery_beers", streaming=False,
                                        query=q))

    def test_http_data_streaming(self):
        q = Query(limit=30, debug=True)
        self._verify_data(self.cb.query("beer", "brewery_beers", streaming=True,
                                        query=q))

    def test_pycbc_206(self):
        # Set up the view..
        mgr = self.cb.bucket_manager()
        design = mgr.design_get('beer', use_devmode=False).value
        if not 'with_value' in design['views']:

            design['views']['with_value'] = {
                'map': 'function(doc,meta) { emit(meta.id,doc.name); }'
            }

            ret = mgr.design_create('beer', design, use_devmode=0)
            self.assertTrue(ret.success)

        # Streaming with values
        view = self.cb.query("beer", "with_value", streaming=True)
        rows = list(view)
        self.assertTrue(len(rows))

    def test_props_before_rows(self):
        it = self.cb.query('beer', 'brewery_beers', limit=1)

        def get_rowcount():
            return it.indexed_rows

        def get_errors():
            return it.errors

        self.assertRaises(RuntimeError, get_rowcount)
        self.assertRaises(RuntimeError, get_errors)
