#
# Copyright 2018, Couchbase, Inc.
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

from __future__ import print_function

import logging

import couchbase
import couchbase.analytics
import couchbase.exceptions
from couchbase.tests.base import RealServerTestCase, ConnectionTestCase
from parameterized import parameterized
import json
import os
import time
import copy
import typing
from couchbase import JSON
from couchbase.analytics_ingester import AnalyticsIngester
import couchbase.bucket
from typing import *
from couchbase.analytics_ingester import BucketOperators

analytics_dir = os.path.join("couchbase", "tests", "cases", "analytics")
response_dir = analytics_dir


class CBASTestBase(RealServerTestCase):
    initialised = False
    lastcb = None
    datasets = {}

    @classmethod
    def tearDownClass(cls):
        if CBASTestBase.lastcb:
            CBASTestBase.lastcb.cleanUp()
            CBASTestBase.lastcb.tearDown()
            del CBASTestBase.lastcb
        super(CBASTestBase, cls).tearDownClass()

    @classmethod
    def setUpClass(cls):
        CBASTestBase.lastcb = None
        CBASTestBase.initialised = False
        CBASTestBase.datasets = {'beers': ['beer', '21A IPA'], 'breweries': ['brewery', 'Kona Brewing']}

    def setUp(self):
        super(CBASTestBase, self).setUp()
        if not CBASTestBase.lastcb:
            CBASTestBase.lastcb = copy.copy(self)

    def init_if_not_setup(self, is_setup_test=False):
        if is_setup_test:
            self.cleanUp()
        else:
            self.initialise()

    def initialise(self):
        if not self.is_mock:
            if not CBASTestBase.initialised:
                logging.error("initialising dataset")
                self.cleanUp()
                for dataset, source in CBASTestBase.datasets.items():
                    self.perform_query(
                        "CREATE DATASET {} ON `beer-sample` WHERE `type` = '{}';".format(dataset, source[0]),
                        quiet=False)

                self.perform_query("CONNECT LINK Local;", quiet=False)
                for dataset, source in CBASTestBase.datasets.items():
                    self.poll_for_response(dataset, source[1])
                time.sleep(10)

        CBASTestBase.initialised = True

    def cleanUp(self):
        if not self.is_mock:
            logging.error("cleaning up")
            self.perform_query("DISCONNECT LINK Local;", quiet=False)
            for dataset, source in CBASTestBase.datasets.items():
                self.perform_query("DROP DATASET {} IF EXISTS;".format(dataset), quiet=False)
            time.sleep(10)
        CBASTestBase.initialised = False

    def poll_for_response(self, dataset, marker):
        if not self.is_mock:
            while not self.perform_query("SELECT VALUE bw FROM {} bw WHERE bw.name = '{}'".format(dataset, marker),
                                         quiet=False,
                                         wait=0.1):
                pass

    def initiate_query(self, statement, *args, **kwargs):
        logging.info("initiating query {} : {}, {}".format(statement,args,kwargs))
        return self.cb.analytics_query(statement, self.cluster_info.host, *args, **kwargs)

    def perform_query(self, statement, *args, **kwargs):
        quiet = kwargs.pop("quiet", False)
        query = None
        metrics = None
        encoded = "{}"
        try:
            waittime = kwargs.pop("wait", 0)
            query = self.initiate_query(statement,*args,**kwargs)
            encoded = query._params.encoded
            if self.is_mock:
                result = None
            else:
                time.sleep(waittime)
                result = list(query)
                if len(result) == 1:
                    result = result[0]
            metrics = query.metrics
        except Exception as e:
            result = e
        if not quiet:
            logging.error(
                "statement {}(   args={}  kwargs={}   )\nyields\n{}\n with metrics {}".format(statement, args, kwargs,
                                                                                              result, metrics))
            if isinstance(result, Exception):
                raise result

        return result, encoded


gen_reference = os.getenv("PYCBC_GEN_CBAS_REF")
with open(os.path.join(response_dir, "queries.json")) as resp_file:
    cbas_response = json.load(resp_file)


class CBASTestQueries(CBASTestBase):
    @classmethod
    def tearDownClass(cls):
        if gen_reference:
            with open(os.path.join(response_dir, "queries.json"), "w+") as resp_file:
                json.dump(cbas_response, resp_file, sort_keys=True, indent=4, separators=(',', ': '))

    @parameterized.expand(
        x for x in sorted(cbas_response.keys())
    )
    def test_query(self, query_file):
        args, query, statement = self.gen_query_params(query_file)
        result, encoded = self.perform_query(statement, *args, **(query.get("options", {})))
        if gen_reference:
            decoded_encoded = json.loads(encoded)
            cbas_response[query_file] = {'query': {'statement': statement, 'options': query}, 'response': result,
                                         'encoded': decoded_encoded}
        else:
            self._check_response(encoded, query_file, result)

    def gen_query_params(self, query_file):
        logging.info("test={}".format(query_file))
        self.init_if_not_setup("setup-dataset" in query_file)
        query = copy.deepcopy(cbas_response[query_file]['query'])
        statement = query.pop("statement")
        args = query.pop("args", [])
        for key, value in query.items():
            if key.startswith("$"):
                query.pop(key)
                query[key[1:]] = value
        return args, query, statement

    def _check_response(self, encoded, query_file, result):
        logging.error("encoded is {}".format(encoded))
        decoded_encoded = json.loads(encoded)

        expected_query = cbas_response[query_file]['query']
        if not expected_query['statement'].endswith(';'):
            expected_query['statement'] += ';'
        options = expected_query.pop('options')
        if len(options):
            expected_query.update({"${}".format(k): v for k, v in options.items()})
        actual = (result)
        expected = (cbas_response[query_file]['response'])
        self.assertSanitizedEqual(actual, expected, {u'meta': 'cas', 'Dataverse': 'Timestamp'})


class CBASTestSpecific(CBASTestBase):
    def test_importworks(self):
        self.init_if_not_setup()
        try:
            row = self.cb.analytics_query("SELECT VALUE bw FROM breweries bw WHERE bw.name = 'Kona Brewing'",
                                           self.cluster_info.analytics_host).get_single_result()
        except ImportError:
            raise
        except:
            pass

    def test_parameterised_positional(self):
        self.init_if_not_setup()
        result, metrics = self.perform_query('SELECT VALUE bw FROM breweries bw WHERE bw.name = ?',
                                             "Kona Brewing")
        self.assertEqual(result,
                         {'address': ['75-5629 Kuakini Highway'], 'city': 'Kailua-Kona', 'code': '96740',
                          'country': 'United States',
                          'description': '', 'geo': {'accuracy': 'RANGE_INTERPOLATED', 'lat': 19.642, 'lon': -155.996},
                          'name': 'Kona Brewing', 'phone': '1-808-334-1133', 'state': 'Hawaii', 'type': 'brewery',
                          'updated': '2010-07-22 20:00:20', 'website': 'http://www.konabrewingco.com'})

    def test_dataverse(self):
        self.init_if_not_setup()
        query = self.cb.analytics_query('SELECT * FROM Metadata.`Dataverse`', self.cluster_info.analytics_host)
        logging.info("Got query {}".format(repr(query._params)))
        result = list(query)
        logging.info("got result [{}]".format(result))

    def test_hasmetrics(self):
        self.init_if_not_setup()
        query = self.cb._analytics_query('SELECT * FROM Metadata.`Dataverse`', self.cluster_info.analytics_host)
        logging.info("Got query {}".format(repr(query._params)))
        self.assertNotEqual(query.metrics,{})
        result = list(query)
        logging.info("got result [{}]".format(result))

    def test_cbas_alias(self):
        import couchbase.cbas
        query = couchbase.cbas.AnalyticsQuery('SELECT * FROM Metadata.`Dataverse`')


class TestIdGenerator:
    def __init__(self):
        self._counter = 0

    def __call__(self, document):
        # type: (JSON) -> str
        self._counter += 1
        return "DataverseResult_"+str(self._counter)


def data_converter(document):
    # type: (JSON) -> JSON
    return {'results':document}


class AnalyticsIngestTest(CBASTestBase):
    def test_ingest_basic(self):
        x = AnalyticsIngester(TestIdGenerator(), data_converter, BucketOperators.UPSERT)
        x(self.cb,'SELECT * FROM Metadata.`Dataverse`',self.cluster_info.host)
        result=self.cb.get("DataverseResult_1")
        self.assertIsNotNone(result)

    def test_ingest_defaults(self):
        x=AnalyticsIngester()
        x(self.cb,'SELECT * FROM Metadata.`Dataverse`',self.cluster_info.host)
        result=self.cb.get("DataverseResult_1")
        self.assertIsNotNone(result)

    def test_ingest_ignore_errors(self):
        x = AnalyticsIngester(TestIdGenerator(), lambda json: 1/0, BucketOperators.UPSERT)
        x(self.cb,'SELECT * FROM Metadata.`Dataverse`',self.cluster_info.host, ignore_ingest_error=True)
