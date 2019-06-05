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

from couchbase_v2.exceptions import PipelineError, NotFoundError, ArgumentError
from couchbase_tests.base import ConnectionTestCase
from couchbase_core import FMT_UTF8

class PipelineTest(ConnectionTestCase):

    def test_simple_pipeline(self):
        k = self.gen_key("pipeline_test")
        with self.cb.pipeline():
            self.cb.remove(k, quiet=True)
            self.cb.insert(k, "MIDDLE", format=FMT_UTF8)
            self.cb.prepend(k, "BEGIN_")
            self.cb.append(k, "_END")

        # No errors
        rv = self.cb.get(k)
        self.assertEqual(rv.value, "BEGIN_MIDDLE_END")

    def test_empty_pipeline(self):
        k = self.gen_key("empty_pipeline")

        with self.cb.pipeline():
            pass

        self.cb.upsert(k, "a value")
        rv = self.cb.get(k)
        self.assertEqual(rv.value, "a value")

    def test_pipeline_results(self):
        k = self.gen_key("pipeline_results")
        pipeline = self.cb.pipeline()
        with pipeline:
            self.cb.remove(k, quiet=True)
            self.cb.upsert(k, "blah")
            self.cb.get(k)
            self.cb.remove(k)

        results = pipeline.results
        self.assertEqual(len(results), 4)

        self.assertTrue(results[1].success)
        self.assertEqual(results[1].key, k)

        self.assertTrue(results[2].success)
        self.assertEqual(results[2].key, k)
        self.assertEqual(results[2].value, "blah")

        self.assertTrue(results[3].success)

    def test_pipeline_operrors(self):
        k = self.gen_key("pipeline_errors")
        v = "hahahaha"
        self.cb.remove(k, quiet=True)

        def run_pipeline():
            with self.cb.pipeline():
                self.cb.get(k, quiet=False)
                self.cb.upsert(k, v)
        self.assertRaises(NotFoundError, run_pipeline)

        rv = self.cb.upsert("foo", "bar")
        self.assertTrue(rv.success)

    def test_pipeline_state_errors(self):
        def fun():
            with self.cb.pipeline():
                with self.cb.pipeline():
                    pass

        self.assertRaises(PipelineError, fun)

        def fun():
            with self.cb.pipeline():
                list(self.cb.query("design", "view"))

        self.assertRaises(PipelineError, fun)

    def test_pipeline_argerrors(self):
        k = self.gen_key("pipeline_argerrors")
        self.cb.remove(k, quiet=True)

        pipeline = self.cb.pipeline()

        def fun():
            with pipeline:
                self.cb.upsert(k, "foo")
                self.cb.get("foo", "bar")
                self.cb.get(k)

        self.assertRaises(ArgumentError, fun)
        self.assertEqual(len(pipeline.results), 1)
        self.assertEqual(self.cb.get(k).value, "foo")

    def test_multi_pipeline(self):
        kvs = self.gen_kv_dict(prefix="multi_pipeline")

        pipeline = self.cb.pipeline()
        with pipeline:
            self.cb.upsert_multi(kvs)
            self.cb.get_multi(kvs.keys())

        self.assertEqual(len(pipeline.results), 2)
        for mres in pipeline.results:
            for k in kvs:
                self.assertTrue(k in mres)
                self.assertTrue(mres[k].success)


        for k, v in pipeline.results[1].items():
            self.assertEqual(v.value, kvs[k])
