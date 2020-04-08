from .fixtures import asynct, AioTestCase
from couchbase.exceptions import CouchbaseException
from unittest import SkipTest


class CouchbasePy35Test(AioTestCase):

    def setUp(self):
        try:
            super(CouchbasePy35Test,self).setUp(bucket='beer-sample', type="Bucket")
        except CouchbaseException as e:
            raise SkipTest("Need 'beer-sample' bucket for this")

    @asynct
    async def test_query_with_async_iterator(self):
        beer_bucket = self.gen_bucket(**self.make_connargs(bucket='beer-sample'))
        from acouchbase.cluster import asyncio
        await (beer_bucket.on_connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.view_query("beer", "brewery_beers", limit=10)

        count = 0
        async for _ in viewiter:
            count += 1

        self.assertEqual(count, 10)
