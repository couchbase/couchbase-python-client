from fixtures import asynct, AioTestCase
from couchbase_core.exceptions import CouchbaseError
class CouchbasePy35Test(AioTestCase):

    def setUp(self):
        try:
            super(CouchbasePy35Test,self).setUp(bucket='beer-sample')
        except CouchbaseError:
            raise SkipTest("Need 'beer-sample' bucket for this")

    @asynct
    async def test_query_with_async_iterator(self):
        beer_bucket = self.cb
        await (beer_bucket.connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.query("beer", "brewery_beers", limit=10)

        count = 0
        async for row in viewiter:
            count += 1

        self.assertEqual(count, 10)
