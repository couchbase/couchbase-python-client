import unittest
from fixtures import asynct, AioTestCase, beer_bucket, default_bucket

class CouchBasePy35Test(unittest.TestCase):

    @asynct
    async def test_query_with_async_iterator(self):
        await (beer_bucket.connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.query("beer", "brewery_beers", limit=10)

        count = 0
        async for row in viewiter:
            count += 1

        self.assertEqual(count, 10)
