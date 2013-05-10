from time import sleep

from couchbase.exceptions import (ValueFormatError,
                                  ArgumentError, NotFoundError)

from tests.base import CouchbaseTestCase


class ConnectionItemSyntaxTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionItemSyntaxTest, self).setUp()
        self.cb = self.make_connection()

    def test_simple_accessors(self):
        cb = self.cb
        cb.quiet = True

        del cb['foo']
        cb['foo'] = "bar"
        self.assertEqual(cb['foo'].value, 'bar')

        del cb['blah']


if __name__ == '__main__':
    unittest.main()
