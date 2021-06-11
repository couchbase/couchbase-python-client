# -*- coding:utf-8 -*-
#
# Copyright 2020, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
from unittest import SkipTest, TestCase
import objgraph
from collections import Counter
import gc
import json

from couchbase.n1ql import UnsignedInt64
from couchbase.cluster import QueryOptions, QueryProfile, QueryResult
from couchbase.n1ql import QueryMetaData, QueryStatus, QueryWarning
from couchbase_tests.base import CollectionTestCase, CouchbaseTestCase
from couchbase.exceptions import (KeyspaceNotFoundException, NotSupportedException,
                                  ScopeNotFoundException)
from couchbase.mutation_state import MutationState
from couchbase_core.n1ql import NOT_BOUNDED, REQUEST_PLUS


class QueryTests(CollectionTestCase):
    def setUp(self):
        super(QueryTests, self).setUp()

        if not self.is_realserver:
            raise SkipTest('mock does not mock queries')
        # since we know that the CollectionTestCase loads beers, lets
        # use beer-sample bucket for our query tests.  NOTE: it isn't
        # clear to me that this is a great idea long-term, but we seem
        # to require this, and that bucket has a primary index, lets
        # use it.  Later when the querymgr works, and it can wait for the
        # index to exist, we can make this more isolated
        self.query_bucket = 'beer-sample'

    def assertRows(self,
                   result,  # type: QueryResult
                   expected_count):
        count = 0
        self.assertIsNotNone(result)
        for row in result.rows():
            self.assertIsNotNone(row)
            count += 1
        self.assertEqual(count, expected_count)

    def test_simple_query(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` LIMIT 2")
        self.assertRows(result, 2)
        self.assertIsNone(result.metadata().profile())
        self.assertTrue(result._params._adhoc)

    def test_simple_query_prepared(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` LIMIT 2",
                                    QueryOptions(adhoc=False, metrics=True))  # type: QueryResult
        self.assertRows(result, 2)
        self.assertIsNone(result.metadata().profile())
        self.assertFalse(result._params._adhoc)

    def test_simple_query_with_positional_params(self):
        result = self.cluster.query(
            "SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1", '21st_amendment%')
        self.assertRows(result, 1)

    def test_simple_query_with_named_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1",
                                    brewery='21st_amendment%')
        self.assertRows(result, 1)

    def test_simple_query_with_positional_params_in_options(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=['21st_amendment%']))
        self.assertRows(result, 1)

    def test_simple_query_with_named_params_in_options(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1",
                                    QueryOptions(named_parameters={'brewery': '21st_amendment%'}))
        self.assertRows(result, 1)

    # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # now, kwargs override the corresponding value in the options, only.
    def test_simple_query_without_options_with_kwargs_positional_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1",
                                    positional_parameters=['21st_amendment%'])
        self.assertRows(result, 1)

    # NOTE: Ideally I'd notice that a named parameter wasn't an option parameter name, and just _assume_ that it is a
    # named parameter for the query.  However I worry about overlap being confusing, etc...
    def test_simple_query_without_options_with_kwargs_named_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1",
                                    named_parameters={'brewery': '21st_amendment%'})
        self.assertRows(result, 1)

    def test_query_with_profile(self):
        result = self.cluster.query(
            "SELECT * FROM `beer-sample` LIMIT 1", QueryOptions(profile=QueryProfile.TIMINGS))
        self.assertRows(result, 1)
        self.assertIsNotNone(result.metadata().profile())

    def test_query_with_metrics(self):
        initial = datetime.datetime.now()
        result = self.cluster.query(
            "SELECT * FROM `beer-sample` LIMIT 1", QueryOptions(metrics=True))
        self.assertRows(result, 1)
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

    def test_query_metadata(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` LIMIT 2")
        self.assertRows(result, 2)
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

    def test_mixed_positional_parameters(self):
        # we assume that positional overrides one in the Options
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=['xgfflq']), '21st_am%')
        self.assertRows(result, 1)

    def test_mixed_named_parameters(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1",
                                    QueryOptions(named_parameters={'brewery': 'xxffqqlx'}), brewery='21st_am%')
        self.assertRows(result, 1)


class QueryStringTests(TestCase):

    def test_encoded_consistency(self):
        qstr = 'SELECT * FROM default'
        qopts = QueryOptions()
        q = qopts.to_query_object(qstr)
        q.consistency = REQUEST_PLUS
        dval = json.loads(q.encoded)
        self.assertEqual('request_plus', dval['scan_consistency'])

        q.consistency = NOT_BOUNDED
        dval = json.loads(q.encoded)
        self.assertEqual('not_bounded', dval['scan_consistency'])

    def test_encode_scanvec(self):
        # The value is a vbucket's sequence number,
        # and guard is a vbucket's UUID.

        qstr = 'SELECT * FROM default'
        qopts = QueryOptions()
        q = qopts.to_query_object(qstr)
        ms = MutationState()
        ms._add_scanvec((42, 3004, 3, 'default'))
        q.consistent_with = ms

        dval = json.loads(q.encoded)
        sv_exp = {
            'default': {'42': [3, '3004']}
        }

        self.assertEqual('at_plus', dval['scan_consistency'])
        self.assertEqual(sv_exp, dval['scan_vectors'])

        # Ensure the vb field gets updated. No duplicates!
        ms._add_scanvec((42, 3004, 4, 'default'))
        sv_exp['default']['42'] = [4, '3004']
        dval = json.loads(q.encoded)
        self.assertEqual(sv_exp, dval['scan_vectors'])

        ms._add_scanvec((91, 7779, 23, 'default'))
        dval = json.loads(q.encoded)
        sv_exp['default']['91'] = [23, '7779']
        self.assertEqual(sv_exp, dval['scan_vectors'])

        # Try with a second bucket
        sv_exp['other'] = {'666': [99, '5551212']}
        ms._add_scanvec((666, 5551212, 99, 'other'))
        dval = json.loads(q.encoded)
        self.assertEqual(sv_exp, dval['scan_vectors'])


class QueryLeakTest(CollectionTestCase):
    def setUp(self, default_collections=None, real_collections=None, **kwargs):
        super(QueryLeakTest, self).setUp()

    def test_no_leak(self):
        import tracemalloc
        tracemalloc.start(25)
        snapshot = tracemalloc.take_snapshot()
        doc = {'field1': "value1"}
        for i in range(100):
            key = str(i)
            self.bucket.default_collection().upsert(key, doc)

        if self.is_realserver:
            statement = "SELECT * FROM default:`default` USE KEYS[$1];".format(self.cluster_info.bucket_name,
                                                                               self.coll._self_scope.name,
                                                                               self.coll._self_name)
        else:
            statement = "'SELECT mockrow'"
        counts = Counter({"builtins.dict": 1, "builtins.list": 2})

        objgraph.growth(shortnames=False)

        for i in range(5):
            args = [str(i)] if self.is_realserver else []
            print("PRE: key: {}".format(i))
            result = self.cluster.query(statement, *args)
            try:
                stuff = list(result)
                metadata = result.meta
                del stuff
                del result
                del metadata
                gc.collect()
                print("POST: key: {}".format(i))
            except:
                pass
            growth = objgraph.growth(shortnames=False)
            print("growth is {}".format(growth))
            if i > 0:
                for entry in growth:
                    key = entry[0]
                    if key in ('builtins.dict', 'builtins.list'):
                        self.assertLessEqual(entry[2], counts[key],
                                             "{} count should not grow more than {}".format(key, counts[key]))
            print("\n")
            del growth
            gc.collect()
        snapshot2 = tracemalloc.take_snapshot()

        top_stats = snapshot2.compare_to(snapshot, 'lineno')
        import logging
        logging.error("[ Top 10 differences ]")
        for stat in top_stats[:10]:
            logging.error(stat)
        # pick the biggest memory block
        top_stats = snapshot2.statistics('traceback')
        stat = top_stats[0]
        logging.error("%s memory blocks: %.1f KiB" %
                      (stat.count, stat.size / 1024))
        for line in stat.traceback.format():
            logging.error(line)


class QueryCollectionTests(CollectionTestCase):
    def setUp(self):
        super(QueryCollectionTests, self).setUp(bucket='beer-sample')

        if not self.is_realserver:
            raise SkipTest('mock does not mock queries')

        # SkipTest if collections not supported
        try:
            self.bucket.collections().get_all_scopes()
        except NotSupportedException:
            raise SkipTest('cluster does not support collections')

        self.cm = self.bucket.collections()
        self.create_beer_sample_collections()

    @classmethod
    def setUpClass(cls) -> None:
        super(QueryCollectionTests, cls).setUpClass(True)

    @classmethod
    def tearDownClass(cls) -> None:
        super(QueryCollectionTests, cls).tearDownClass()

    def assertRows(self,
                   result,  # type: QueryResult
                   expected_count):
        count = 0
        self.assertIsNotNone(result)
        for row in result.rows():
            self.assertIsNotNone(row)
            count += 1
        self.assertEqual(count, expected_count)

    def test_query_fully_qualified(self):
        beers_fqdn = '`{}`.`{}`.beers'.format(
            self.bucket_name, self.beer_sample_collections.scope)
        result = self.cluster.query(
            "SELECT * FROM {} LIMIT 2".format(beers_fqdn))
        self.assertRows(result, 2)
        self.assertIsNone(result.metadata().profile())
        self.assertTrue(result._params._adhoc)

    def test_cluster_query_context(self):
        q_context = '{}.{}'.format(
            self.bucket_name, self.beer_sample_collections.scope)
        # test with QueryOptions
        q_opts = QueryOptions(query_context=q_context, adhoc=True)
        result = self.cluster.query("SELECT * FROM beers LIMIT 2", q_opts)
        self.assertRows(result, 2)

        # test with kwargs
        result = self.cluster.query(
            "SELECT * FROM beers LIMIT 2", query_context=q_context)
        self.assertRows(result, 2)

    def test_bad_query_context(self):
        # test w/ no context
        result = self.cluster.query("SELECT * FROM beers LIMIT 2")
        with self.assertRaises(KeyspaceNotFoundException):
            result.rows()

        # test w/ bad scope
        q_context = '{}.{}'.format(self.bucket_name, 'fake-scope')
        result = self.cluster.query(
            "SELECT * FROM beers LIMIT 2", QueryOptions(query_context=q_context))
        with self.assertRaises(ScopeNotFoundException):
            result.rows()

    def test_scope_query(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.query("SELECT * FROM beers LIMIT 2")
        self.assertRows(result, 2)
        fully_qualified = '{}.{}.beers'.format(
            self.bucket_name, self.beer_sample_collections.scope)
        result = scope.query(
            "SELECT * FROM {} LIMIT 2".format(fully_qualified))

    def test_bad_scope_query(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        q_context = '{}.{}'.format(self.bucket_name, 'fake-scope')
        result = scope.query("SELECT * FROM beers LIMIT 2",
                             QueryOptions(query_context=q_context))
        with self.assertRaises(ScopeNotFoundException):
            result.rows()

        q_context = '{}.{}'.format(
            'fake-bucket', self.beer_sample_collections.scope)
        result = scope.query("SELECT * FROM beers LIMIT 2",
                             query_context=q_context)
        with self.assertRaises(KeyspaceNotFoundException):
            result.rows()

    def test_scope_query_with_positional_params_in_options(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.query("SELECT * FROM breweries WHERE META().id LIKE $1 LIMIT 1",
                             QueryOptions(positional_parameters=['21st_amendment%']))
        self.assertRows(result, 1)

    def test_scope_query_with_named_params_in_options(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.query("SELECT * FROM breweries WHERE META().id LIKE $brewery LIMIT 1",
                             QueryOptions(named_parameters={'brewery': '21st_amendment%'}))
        self.assertRows(result, 1)

    def test_scope_query_with_metrics(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        initial = datetime.datetime.now()
        result = scope.query(
            "SELECT * FROM breweries LIMIT 1", QueryOptions(metrics=True))
        self.assertRows(result, 1)
        taken = datetime.datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        metrics = metadata.metrics()
        self.assertIsInstance(metrics.elapsed_time(), datetime.timedelta)
        self.assertLess(metrics.elapsed_time(), taken)
        self.assertGreater(metrics.elapsed_time(),
                           datetime.timedelta(milliseconds=0))
        self.assertLess(metrics.elapsed_time(), taken)
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

    def test_scope_query_metadata(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.query("SELECT * FROM beers LIMIT 2")
        self.assertRows(result, 2)
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
