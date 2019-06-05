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
from unittest import SkipTest

from couchbase_tests.base import RealServerTestCase, version_to_tuple
from parameterized import parameterized
import json
import time
import copy
from couchbase_core import JSON
from couchbase_core.analytics_ingester import AnalyticsIngester
from couchbase_core.analytics_ingester import BucketOperators
import traceback
from couchbase_tests.base import PYCBC_SERVER_VERSION
import os


analytics_dir = os.path.join(os.path.dirname(__file__), "analytics")
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
        pycbc_min_analytics = version_to_tuple(os.environ.get("PYCBC_MIN_ANALYTICS"))

        # analytics tests seem to fail on 5.5.2, seemingly because dataset creation semantics have changed
        # we get "cannot find dataset 'Breweries'"
        # offer the option to skip these tests pending further investigation

        old_analytics = PYCBC_SERVER_VERSION and pycbc_min_analytics and PYCBC_SERVER_VERSION < pycbc_min_analytics
        if old_analytics and not os.environ.get("PYCBC_TEST_OLD_ANALYTICS"):
            raise SkipTest("Skipping analytics tests on version {} as older than {}".format(PYCBC_SERVER_VERSION,
                                                                                            pycbc_min_analytics))

        logging.error("Testing against server version {}".format(PYCBC_SERVER_VERSION))
        self.override_quiet = old_analytics

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
                        quiet=True)

                self.perform_query("CONNECT LINK Local;", quiet=True)
                for dataset, source in CBASTestBase.datasets.items():
                    self.poll_for_response(dataset, source[1])
                time.sleep(10)

        CBASTestBase.initialised = True

    def cleanUp(self):
        if not self.is_mock:
            logging.error("cleaning up")
            self.perform_query("DISCONNECT LINK Local;", quiet=True)
            for dataset, source in CBASTestBase.datasets.items():
                self.perform_query("DROP DATASET {} IF EXISTS;".format(dataset), quiet=True)
            time.sleep(10)
        CBASTestBase.initialised = False

    def poll_for_response(self, dataset, marker):
        if not self.is_mock:
            while not self.perform_query("SELECT VALUE bw FROM {} bw WHERE bw.name = '{}'".format(dataset, marker),
                                         quiet=True,
                                         wait=0.1):
                pass

    def initiate_query(self, statement, options, *args, **kwargs):
        logging.info("initiating query {} : {}, {}".format(statement,args,kwargs))
        query=couchbase_v2.analytics.AnalyticsQuery(statement, *args, **kwargs)
        for k,v in options.items():
            query.set_option(k, v)
        return self.cb.analytics_query(query, self.cluster_info.analytics_host)

    def perform_query(self, statement, *args, **kwargs):
        return self.perform_query_with_options(statement, {}, *args,**kwargs)

    def perform_query_with_options(self, statement, options, *args, **kwargs):
        quiet = kwargs.pop("quiet", False)
        query = None
        metrics = None
        encoded = "{}"
        try:
            waittime = kwargs.pop("wait", 0)
            query = self.initiate_query(statement,options,*args,**kwargs)
            encoded = query._params.encoded
            if self.is_mock:
                result = None
            else:
                time.sleep(waittime)
                result = self.extract_results(query)
            metrics = query.metrics
        except Exception as e:
            result = e
        if not quiet or self.override_quiet:
            logging.error(
                "statement {}(   args={}  kwargs={}   )\nyields\n{}\n with metrics {}".format(statement, args, kwargs,
                                                                                              result, metrics))
        if not quiet:
            if isinstance(result, Exception):
                raise result

        return result, encoded

    def extract_results(self, query):
        result = list(query)
        if len(result) == 1:
            result = result[0]
        return result


gen_reference = os.getenv("PYCBC_GEN_CBAS_REF")
with open(os.path.join(response_dir, "queries.json")) as resp_file:
    cbas_response = json.load(resp_file)

def build_expected_query_opts(expected_query, kwarg_type, prefix=""):
    kwargs = expected_query.pop(kwarg_type,{})
    if len(kwargs):
        expected_query.update({prefix+"{}".format(k): v for k, v in kwargs.items()})


def get_expected_query(expected_query):
    expected_query_copy=copy.deepcopy(expected_query)
    build_expected_query_opts(expected_query_copy, "options")
    build_expected_query_opts(expected_query_copy, "kwargs", "$")
    return expected_query_copy

class CBASTestQueriesBase(CBASTestBase):
    def gen_query_params(self, query_file, responsedict):
        logging.info("test={}".format(query_file))
        entry = copy.deepcopy(responsedict[query_file])
        query = copy.deepcopy(entry['query'])
        statement = query.get("statement")
        args = query.pop("args", [])
        kwargs = query.get('kwargs',{})
        options = query.get('options',{})
        return args, kwargs, statement, options



    def _check_response(self, encoded, query_file, result, responsedict):
        logging.error("encoded is {}".format(encoded))
        decoded_encoded = json.loads(encoded)

        expected_query = responsedict[query_file]['query']
        if not expected_query['statement'].endswith(';'):
            expected_query['statement'] += ';'
        expected_query = get_expected_query(expected_query)
        actual = (result)
        expected = (responsedict[query_file]['response'])
        self.assertSanitizedEqual(actual, expected, {u'meta': 'cas', 'Dataverse': 'Timestamp'})



class CBASTestQueries(CBASTestQueriesBase):
    @classmethod
    def tearDownClass(cls):
        if gen_reference:
            with open(os.path.join(response_dir, "queries.json"), "w+") as resp_file:
                json.dump(cbas_response, resp_file, sort_keys=True, indent=4, separators=(',', ': '))

    @parameterized.expand(
        x for x in sorted(cbas_response.keys())
    )
    def test_query(self, query_file):
        self.init_if_not_setup("setup-dataset" in query_file)
        args, kwargs, statement, options= self.gen_query_params(query_file, cbas_response)
        result, encoded = self.perform_query_with_options(statement, options, *args, **kwargs)
        if gen_reference:
            decoded_encoded = json.loads(encoded)
            cbas_response[query_file] = {'query': {'statement': statement, 'options': options, 'kwargs': kwargs}, 'response': result,
                                         'encoded': decoded_encoded}
        else:
            self._check_response(encoded, query_file, result, cbas_response)

class DeferredAnalyticsTest(CBASTestQueriesBase):
    _responses = None

    @property
    def _handles(self):
        if not DeferredAnalyticsTest._responses:
            self.init_if_not_setup()
            logging.error("setting up deferred queries")
            DeferredAnalyticsTest._responses = {}
            for query_file in cbas_response.keys():
                if 'setup-dataset' in query_file or 'initiate-shadow' in query_file:
                    continue
                args, kwargs, statement, options = self.gen_query_params(query_file, cbas_response)
                real_statement = couchbase_v2.analytics.DeferredAnalyticsQuery(statement,*args, **kwargs)
                real_statement.timeout = 100
                logging.error("scheduling query {} with args{} kwargs {} and options {}".format(real_statement, args, kwargs, options))
                logging.error("query content; {}, body: {}".format(real_statement,real_statement._body))
                deferred_query = self.cb.analytics_query(real_statement, self.cluster_info.analytics_host)
                logging.error("scheduled query {}, got response {}".format(real_statement,deferred_query))
                DeferredAnalyticsTest._responses[query_file]=deferred_query, real_statement.encoded
        logging.error("finished scheduling")
        return DeferredAnalyticsTest._responses

    def test_deferred(self):
        exceptions = []
        for query_file, response in self._handles.items():
            pre_result, encoded = self._handles[query_file]
            result=self.extract_results(pre_result)
            logging.error("checking response for {}: {}".format(query_file,result))
            try:
                self._check_response(encoded, query_file, result,cbas_response)
            except Exception as e:
                exceptions.append(traceback.format_exc())

        self.assertListEqual([],exceptions)

    def test_single(self):
        self.init_if_not_setup()
        x=couchbase_v2.analytics.DeferredAnalyticsQuery("SELECT VALUE bw FROM breweries bw WHERE bw.name = 'Kona Brewing'")
        x.timeout = 100
        response=self.cb.analytics_query(x,self.cluster_info.analytics_host)
        list_resp = list(response)
        expected = [{"address": ["75-5629 Kuakini Highway"], "city": "Kailua-Kona", "code": "96740",
                    "country": "United States",
                    "description": "", "geo": {"accuracy": "RANGE_INTERPOLATED", "lat": 19.642, "lon": -155.996},
                    "name": "Kona Brewing", "phone": "1-808-334-1133", "state": "Hawaii", "type": "brewery",
                    "updated": "2010-07-22 20:00:20", "website": "http://www.konabrewingco.com"}]

        self.assertSanitizedEqual(expected,list_resp)

    def test_correct_timeout_via_query_property(self):
        self.init_if_not_setup()
        x = couchbase_v2.analytics.DeferredAnalyticsQuery(
            "SELECT VALUE bw FROM breweries bw WHERE bw.name = 'Kona Brewing'")

        def creator(query, host, timeout):
            query.timeout = timeout
            return self.cb.analytics_query(query, host)

        self._check_finish_time_in_bounds(x, creator, 100)

    def test_correct_timeout_in_constructor(self):
        self.init_if_not_setup()
        x = couchbase_v2.analytics.DeferredAnalyticsQuery(
            "SELECT VALUE bw FROM breweries bw WHERE bw.name = 'Kona Brewing'")
        creator = lambda query, host, timeout: couchbase_v2.analytics.DeferredAnalyticsRequest(query, host, self.cb,
                                                                                            timeout=timeout)
        self._check_finish_time_in_bounds(x, creator, 500)

    def _check_finish_time_in_bounds(self, x, response_creator, expected_timeout):
        orig_time = time.time()
        response = response_creator(x, self.cluster_info.analytics_host, expected_timeout)
        self.assertGreater(response.finish_time, orig_time + expected_timeout)
        self.assertLess(response.finish_time, time.time() + expected_timeout)
        # consume the response, just to be safe
        try:
            list(response)
        except:
            pass


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
