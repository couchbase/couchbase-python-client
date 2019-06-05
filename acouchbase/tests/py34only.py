import asyncio

from couchbase_core.experimental import enable; enable()
from fixtures import asynct, AioTestCase
from couchbase_core.n1ql import N1QLQuery
from couchbase_core.exceptions import CouchbaseError



class CouchbaseBeerTest(AioTestCase):

    def setUp(self):
        try:
            return super(CouchbaseBeerTest,self).setUp(bucket='beer-sample')
        except CouchbaseError:
            raise SkipTest("Need 'beer-sample' bucket for this")

    @asynct
    @asyncio.coroutine
    def test_get_data(self):
        beer_bucket = self.cb
        yield from (beer_bucket.connect() or asyncio.sleep(0.01))

        data = yield from beer_bucket.get('21st_amendment_brewery_cafe')
        self.assertEqual("21st Amendment Brewery Cafe", data.value["name"])

    @asynct
    @asyncio.coroutine
    def test_query(self):
        beer_bucket = self.cb

        yield from (beer_bucket.connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.query("beer", "brewery_beers", limit=10)
        yield from viewiter.future

        count = len(list(viewiter))

        self.assertEqual(count, 10)


class CouchbaseDefaultTest(AioTestCase):

    @asynct
    @asyncio.coroutine
    def test_upsert(self):
        import uuid

        expected = str(uuid.uuid4())

        default_bucket = self.cb
        yield from (default_bucket.connect() or asyncio.sleep(0.01))

        yield from default_bucket.upsert('hello', {"key": expected})

        obtained = yield from default_bucket.get('hello')
        self.assertEqual({"key": expected}, obtained.value)


    @asynct
    @asyncio.coroutine
    def test_n1ql(self):

        default_bucket = self.cb
        yield from (default_bucket.connect() or asyncio.sleep(0.01))

        q = N1QLQuery("SELECT mockrow")
        it = default_bucket.n1ql_query(q)
        yield from it.future

        data = list(it)
        self.assertEqual('value', data[0]['row'])

