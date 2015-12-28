import asyncio
import unittest

from couchbase.experimental import enable; enable()
from fixtures import asynct, AioTestCase, beer_bucket, default_bucket
from couchbase.n1ql import N1QLQuery



class CouchbaseTest(AioTestCase):

    @asynct
    @asyncio.coroutine
    def test_get_data(self):
        yield from (beer_bucket.connect() or asyncio.sleep(0.01))

        data = yield from beer_bucket.get('21st_amendment_brewery_cafe')
        self.assertEqual("21st Amendment Brewery Cafe", data.value["name"])

    @asynct
    @asyncio.coroutine
    def test_query(self):
        yield from (beer_bucket.connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.query("beer", "brewery_beers", limit=10)
        yield from viewiter.future

        count = len(list(viewiter))

        self.assertEqual(count, 10)



    @asynct
    @asyncio.coroutine
    def test_upsert(self):
        yield from (default_bucket.connect() or asyncio.sleep(0.01))

        yield from default_bucket.upsert('hello', {"key": "test"})


    @asynct
    @asyncio.coroutine
    def test_n1ql(self):
        yield from (default_bucket.connect() or asyncio.sleep(0.01))

        q = N1QLQuery("SELECT mockrow")
        it = default_bucket.n1ql_query(q)
        yield from it.future

        data = list(it)
        self.assertEqual('value', data[0]['row'])

