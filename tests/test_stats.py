from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


# For Python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str


class ConnectionStatsTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionStatsTest, self).setUp()
        self.cb = self.make_connection()

    def test_trivial_stats_without_argument(self):
        stats = self.cb.stats()
        self.assertIsInstance(stats, dict)
        self.assertTrue('curr_connections' in stats)
        val = list(stats['curr_connections'].values())[0]
        self.assertIsInstance(val, int)
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
        stats = self.cb.stats(['memory', 'tap'])
        self.assertIsInstance(stats, dict)
        self.assertTrue('mem_used' in stats)
        self.assertTrue('ep_tap_count' in stats)
        key, info = list(stats.items())[0]
        self.assertIsInstance(key, basestring)
        self.assertIsInstance(info, dict)


if __name__ == '__main__':
    unittest.main()
