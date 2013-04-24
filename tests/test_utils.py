import unittest

from couchbase.libcouchbase import Utils

class UtilsTest(unittest.TestCase):
    def test_string_to_num(self):
        string = 'foo'
        val = Utils.string_to_num(string)
        self.assertEqual(val, 'foo')

        string = '1'
        val = Utils.string_to_num(string)
        self.assertEqual(val, 1)

        string = '4723467'
        val = Utils.string_to_num(string)
        self.assertEqual(val, 4723467)

        string = '274.9576'
        val = Utils.string_to_num(string)
        self.assertEqual(val, 274.9576)

        string = '0.582'
        val = Utils.string_to_num(string)
        self.assertEqual(val, 0.582)

        string = '8foo4'
        val = Utils.string_to_num(string)
        self.assertEqual(val, '8foo4')

        string = 'foo45.593834'
        val = Utils.string_to_num(string)
        self.assertEqual(val, 'foo45.593834')


if __name__ == '__main__':
    unittest.main()
