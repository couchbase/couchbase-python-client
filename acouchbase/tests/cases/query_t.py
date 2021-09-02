import datetime
from functools import wraps
from nose.tools import nottest
from unittest import SkipTest
from flaky import flaky

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase.asynchronous import AsyncQueryResult
from couchbase.cluster import QueryOptions, QueryProfile
from couchbase.n1ql import QueryMetaData, QueryResult, QueryStatus, QueryWarning, UnsignedInt64, N1QLRequest
from couchbase_tests.async_base import AsyncioTestCase
from couchbase.exceptions import ParsingFailedException


@nottest
def async_test(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return self.loop.run_until_complete(func(self, *args, **kwargs))

    return wrapper


class AcouchbaseQueryTests(AsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseQueryTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseQueryTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseQueryTests, self).setUp()

        if self.is_mock:
            raise SkipTest('Mock does not support queries')

        self.query_bucket = 'beer-sample'

    async def assertRows(self,
                         query_iter,  # type: AsyncQueryResult
                         expected_count):
        count = 0
        self.assertIsNotNone(query_iter)
        async for row in query_iter:
            self.assertIsNotNone(row)
            count += 1
        self.assertEqual(count, expected_count)

    @async_test
    async def test_simple_query(self):
        query_iter = self.cluster.query(
            "SELECT * FROM `{}` LIMIT 2;".format(self.query_bucket))
        await self.assertRows(query_iter, 2)
        self.assertIsNone(query_iter.metadata().profile())
        self.assertTrue(query_iter._params._adhoc)

    @async_test
    async def test_simple_query_prepared(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` LIMIT 2".format(self.query_bucket),
                                        QueryOptions(adhoc=False, metrics=True))  # type: AsyncQueryResult
        await self.assertRows(query_iter, 2)
        self.assertIsNone(query_iter.metadata().profile())
        self.assertFalse(query_iter._params._adhoc)

    @async_test
    async def test_simple_query_with_positional_params(self):
        query_iter = self.cluster.query(
            "SELECT * FROM `{}` WHERE brewery_id LIKE $1 LIMIT 1".format(self.query_bucket), '21st_amendment%')
        await self.assertRows(query_iter, 1)

    @async_test
    async def test_simple_query_with_named_params(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $brewery LIMIT 1".format(self.query_bucket),
                                        brewery='21st_amendment%')
        await self.assertRows(query_iter, 1)

    @async_test
    async def test_simple_query_with_positional_params_in_options(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $1 LIMIT 1".format(self.query_bucket),
                                        QueryOptions(positional_parameters=['21st_amendment%']))
        await self.assertRows(query_iter, 1)

    @async_test
    async def test_simple_query_with_named_params_in_options(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $brewery LIMIT 1".format(self.query_bucket),
                                        QueryOptions(named_parameters={'brewery': '21st_amendment%'}))
        await self.assertRows(query_iter, 1)

    # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # now, kwargs override the corresponding value in the options, only.
    @async_test
    async def test_simple_query_without_options_with_kwargs_positional_params(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $1 LIMIT 1".format(self.query_bucket),
                                        positional_parameters=['21st_amendment%'])
        await self.assertRows(query_iter, 1)

    # NOTE: Ideally I'd notice that a named parameter wasn't an option parameter name, and just _assume_ that it is a
    # named parameter for the query.  However I worry about overlap being confusing, etc...
    @async_test
    async def test_simple_query_without_options_with_kwargs_named_params(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $brewery LIMIT 1".format(self.query_bucket),
                                        named_parameters={'brewery': '21st_amendment%'})
        await self.assertRows(query_iter, 1)

    @async_test
    async def test_query_with_profile(self):
        query_iter = self.cluster.query(
            "SELECT * FROM `{}` LIMIT 1".format(self.query_bucket), QueryOptions(profile=QueryProfile.TIMINGS))
        await self.assertRows(query_iter, 1)
        self.assertIsNotNone(query_iter.metadata().profile())

    @async_test
    async def test_query_with_metrics(self):
        initial = datetime.datetime.now()
        result = self.cluster.query(
            "SELECT * FROM `{}` LIMIT 1".format(self.query_bucket), QueryOptions(metrics=True))
        await self.assertRows(result, 1)
        taken = datetime.datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        metrics = metadata.metrics()
        self.assertIsInstance(metrics.elapsed_time(), datetime.timedelta)
        self.assertLess(metrics.elapsed_time(), taken)
        self.assertGreater(metrics.elapsed_time(),
                           datetime.timedelta(milliseconds=0))
        self.assertIsInstance(metrics.execution_time(), datetime.timedelta)
        self.assertLess(metrics.execution_time(), taken)
        self.assertGreater(metrics.execution_time(),
                           datetime.timedelta(milliseconds=0))

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            self.assertIsInstance(count_result, UnsignedInt64, msg=fail_msg)
            self.assertEqual(UnsignedInt64(expected),
                             count_result, msg=fail_msg)
        self.assertGreater(metrics.result_size(), UnsignedInt64(500))

        self.assertEqual(UnsignedInt64(0), metrics.error_count())
        self.assertIsNone(metadata.profile())

    @async_test
    async def test_query_metadata(self):
        result = self.cluster.query(
            "SELECT * FROM `{}` LIMIT 2".format(self.query_bucket))
        await self.assertRows(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            self.assertIsInstance(id_res, str, msg=fail_msg)
        self.assertEqual(QueryStatus.SUCCESS, metadata.status())
        self.assertIsInstance(metadata.signature(), (str, dict))
        self.assertIsInstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            self.assertIsInstance(warning, QueryWarning)
            self.assertIsInstance(warning.message, str)
            self.assertIsInstance(warning.code, int)

    @async_test
    async def test_mixed_positional_parameters(self):
        # we assume that positional overrides one in the Options
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $1 LIMIT 1".format(self.query_bucket),
                                        QueryOptions(positional_parameters=['xgfflq']), '21st_am%')
        await self.assertRows(query_iter, 1)

    @async_test
    async def test_mixed_named_parameters(self):
        query_iter = self.cluster.query("SELECT * FROM `{}` WHERE brewery_id LIKE $brewery LIMIT 1".format(self.query_bucket),
                                        QueryOptions(named_parameters={'brewery': 'xxffqqlx'}), brewery='21st_am%')
        await self.assertRows(query_iter, 1)

    @async_test
    async def test_bad_query(self):
        with self.assertRaises(ParsingFailedException):
            query_iter = self.cluster.query("I'm not N1QL!")
            await self.assertRows(query_iter, 0)

    @async_test
    async def test_large_result_set(self):
        query_iter = self.cluster.query(
            "SELECT * FROM `{}` LIMIT 1500;".format(self.query_bucket))
        await self.assertRows(query_iter, 1500)