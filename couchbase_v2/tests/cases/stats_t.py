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

from couchbase_tests.base import ConnectionTestCase, RealServerTestCase


# For Python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str


class StatsTest(ConnectionTestCase):

    def test_trivial_stats_without_argument(self):
        stats = self.cb.stats()
        self.assertIsInstance(stats, dict)
        self.assertTrue('curr_connections' in stats)
        val = list(stats['curr_connections'].values())[0]
        self.assertIsInstance(val, (float,int))
        key, info = list(stats.items())[0]
        self.assertIsInstance(key, basestring)
        self.assertIsInstance(info, dict)

    def test_stats_with_argument(self):
        stats = self.cb.stats('memory')
        self.assertIsInstance(stats, dict)
        self.assertTrue('mem_used' in stats)
        self.assertFalse('ep_tap_count' in stats)
        key, info = list(stats.items())[0]
        self.assertIsInstance(key, basestring)
        self.assertIsInstance(info, dict)

    def test_stats_with_argument_list(self):
        second_entry = {True: {'tap': "ep_tap_count"}, False: {'config': "ep_dcp_conn_buffer_size"}}[self.is_mock]
        stats = self.cb.stats(['memory'] + list(second_entry.keys()))
        self.assertIsInstance(stats, dict)
        self.assertTrue('mem_used' in stats)
        self.assertSetEqual(set(), set(second_entry.values()).difference(stats.keys()))
        key, info = list(stats.items())[0]
        self.assertIsInstance(key, basestring)
        self.assertIsInstance(info, dict)


if __name__ == '__main__':
    unittest.main()
