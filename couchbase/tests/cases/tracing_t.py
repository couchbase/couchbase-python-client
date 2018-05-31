#
# Copyright 2013, Couchbase, Inc.
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
import os
from time import sleep
from unittest import SkipTest

from nose.plugins.attrib import attr

from couchbase import FMT_JSON, FMT_UTF8

from couchbase.exceptions import (
    CouchbaseError, ValueFormatError, NotFoundError)

from couchbase.tests.base import TracedCase, ConnectionTestCase
import logging
import couchbase._libcouchbase
import couchbase._logutil

class TracingTest(TracedCase):

    def setUp(self, *args, **kwargs):
        super(TracingTest,self).setUp(trace_all=True, flushcount=400)

    class BogusHandler:
        import couchbase.exceptions
        couchbase.exceptions.TimeoutError
        def __init__(self):
            self.records=[]

        def handler(self,**kwargs):
            couchbase._logutil.pylog_log_handler(**kwargs)
            self.records.append(kwargs)

    def test_threshold_multi_get(self):
        raise SkipTest("temporarily disabling - fix pending")
        handler = TracingTest.BogusHandler()
        couchbase._libcouchbase.lcb_logging(handler.handler)

        kv = self.gen_kv_dict(amount=3, prefix='get_multi')
        for i in range(0,50):
            rvs = self.cb.upsert_multi(kv)
            self.assertTrue(rvs.all_ok)

            k_subset = list(kv.keys())[:2]

            rvs1 = self.cb.get_multi(k_subset)
            self.assertEqual(len(rvs1), 2)
            self.assertEqual(rvs1[k_subset[0]].value, kv[k_subset[0]])
            self.assertEqual(rvs1[k_subset[1]].value, kv[k_subset[1]])

            rv2 = self.cb.get_multi(kv.keys())
            self.assertEqual(rv2.keys(), kv.keys())
        self.flush_tracer()

        logging.error("finished test")
        self.assertRegex(str(handler.records),r'.*Operations over threshold:.*')

if __name__ == '__main__':
    unittest.main()
