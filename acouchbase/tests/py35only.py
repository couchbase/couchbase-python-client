from .fixtures import asynct, AioTestCase
from couchbase_core.exceptions import CouchbaseError
from unittest import SkipTest


class CouchbasePy35Test(AioTestCase):

    def setUp(self):
        try:
            super(CouchbasePy35Test,self).setUp(bucket='beer-sample', type="Bucket")
        except CouchbaseError as e:
            raise SkipTest("Need 'beer-sample' bucket for this")

    @asynct
    async def test_query_with_async_iterator(self):
        beer_bucket = self.cb
        from acouchbase.bucket import asyncio
        await (beer_bucket.on_connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.view_query("beer", "brewery_beers", limit=10)

        count = 0
        async for _ in viewiter:
            count += 1

        self.assertEqual(count, 10)
