import asyncio

from couchbase.asynchronous import AsyncSearchResult
from couchbase.asynchronous import AsyncAnalyticsResult
from .fixtures import asynct, AioTestCase
from couchbase.exceptions import CouchbaseException
from unittest import SkipTest
import couchbase.search as SEARCH


class CouchbaseBeerTest(AioTestCase):
    def setUp(self, **kwargs):
        try:
            return super(CouchbaseBeerTest,self).setUp(bucket='beer-sample', **kwargs)
        except CouchbaseException:
            raise SkipTest("Need 'beer-sample' bucket for this")


class CouchbaseBeerKVTest(CouchbaseBeerTest):
    def setUp(self):
        super(CouchbaseBeerKVTest, self).setUp()

    @asynct
    @asyncio.coroutine
    def test_get_data(self):
        connargs=self.make_connargs(bucket='beer-sample')
        beer_default_collection = self.gen_collection(**connargs)

        yield from (beer_default_collection.on_connect() or asyncio.sleep(0.01))

        data = yield from beer_default_collection.get('21st_amendment_brewery_cafe')
        self.assertEqual("21st Amendment Brewery Cafe", data.content["name"])


class CouchbaseBeerViewTest(CouchbaseBeerTest):
    def setUp(self):
        super(CouchbaseBeerViewTest, self).setUp(type='Bucket')
    @asynct
    @asyncio.coroutine
    def test_query(self):

        beer_bucket = self.gen_cluster(**self.make_connargs()).bucket('beer-sample')

        yield from (beer_bucket.on_connect() or asyncio.sleep(0.01))
        viewiter = beer_bucket.view_query("beer", "brewery_beers", limit=10)
        yield from viewiter.future

        count = len(list(viewiter))

        self.assertEqual(count, 10)


class CouchbaseDefaultTestKV(AioTestCase):
    @asynct
    @asyncio.coroutine
    def test_upsert(self):
        import uuid

        expected = str(uuid.uuid4())

        default_collection = self.gen_collection(**self.make_connargs())
        yield from (default_collection.on_connect() or asyncio.sleep(0.01))

        yield from default_collection.upsert('hello', {"key": expected})

        obtained = yield from default_collection.get('hello')
        self.assertEqual({"key": expected}, obtained.content)


class AIOClusterTest(AioTestCase):
    def setUp(self, **kwargs):
        super(AIOClusterTest, self).setUp(**kwargs)

    @asynct
    @asyncio.coroutine
    def test_n1ql(self):

        cluster = self.gen_cluster(**self.make_connargs())
        yield from (cluster.on_connect() or asyncio.sleep(0.01))

        it = cluster.query(self.query_props.statement)
        yield from it.future

        data = list(it)
        self.assertEqual(self.query_props.rowcount, len(data))

    @asynct
    @asyncio.coroutine
    def test_search(self  # type: Base
                    ):
        if self.is_mock:
            raise SkipTest("No search on mock")
        cluster = self.gen_cluster(**self.make_connargs())
        yield from (cluster.on_connect() or asyncio.sleep(0.01))
        it = cluster.search_query("beer-search", SEARCH.TermQuery("category"),
                                      facets={'fred': SEARCH.TermFacet('category', 10)})
        yield from it.future
        data = list(it)
        self.assertIsInstance(it, AsyncSearchResult)
        self.assertEqual(10, len(data))


class AnalyticsTest(AioTestCase):
    def testBatchedAnalytics(self  # type: Base
                             ):
        if self.is_mock:
            raise SkipTest("No analytics on mock")
        cluster = self.gen_cluster(**self.make_connargs())
        yield from (cluster.on_connect() or asyncio.sleep(0.01))

        it = cluster.analytics_query("SELECT * FROM `{}` LIMIT 1".format(self.dataset_name))
        yield from it.future

        self.assertIsInstance(it, AsyncAnalyticsResult)
        self.assertEqual(1, len(it.rows()))

