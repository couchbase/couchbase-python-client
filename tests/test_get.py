import pickle

from couchbase import FMT_JSON, FMT_PICKLE, FMT_PLAIN
from couchbase.exceptions import ValueFormatError, NotFoundError
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


class ConnectionGetTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionGetTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_trivial_get(self):
        self.cb.set('key_trivial1', 'value1')
        val = self.cb.get('key_trivial1')
        self.assertEqual(val, 'value1')

    def test_get_missing_key(self):
        val = self.cb.get('key_missing_1')
        self.assertIsNone(val)
        # Get with quiet=False
        self.assertRaises(NotFoundError, self.cb.get, 'key_missing_1', quiet=False)

    def test_multi_get(self):
        self.cb.set({'key_multi1': 'value1', 'key_multi3': 'value3',
                          'key_multi2': 'value2'})
        val1 = self.cb.get(['key_multi2', 'key_multi3'])
        self.assertEqual(val1, ['value2', 'value3'])
        val2 = self.cb.get(['key_multi3', 'key_multi1', 'key_multi2'])
        self.assertEqual(val2, ['value3', 'value1', 'value2'])


    def test_get_format(self):
        self.cb.set('key_format1', {'some': 'value1'}, format=FMT_JSON)
        val1 = self.cb.get('key_format1')
        self.assertEqual(val1, {'some': 'value1'})

        self.cb.set('key_format2', {'some': 'value2'}, format=FMT_PICKLE)
        val2 = self.cb.get('key_format2')
        self.assertEqual(val2, {'some': 'value2'})

        self.cb.set('key_format3', b'some value3', format=FMT_PLAIN)
        val3 = self.cb.get('key_format3')
        self.assertEqual(val3, b'some value3')


        self.cb.set('key_format4', {'some': 'value4'}, format=FMT_JSON)
        val4 = self.cb.get('key_format4', format=FMT_PLAIN)
        self.assertEqual(val4, b'{"some": "value4"}')

        self.cb.set('key_format5', {'some': 'value5'}, format=FMT_PICKLE)
        val5 = self.cb.get('key_format5', format=FMT_PLAIN)
        self.assertEqual(pickle.loads(val5), {'some': 'value5'})


        self.cb.set('key_format6', {'some': 'value6'}, format=FMT_JSON)
        self.assertRaises(ValueFormatError, self.cb.get, 'key_format6',
                          format=FMT_PICKLE)

        self.cb.set('key_format7', {'some': 'value7'}, format=FMT_PICKLE)
        self.assertRaises(ValueFormatError, self.cb.get, 'key_format7',
                          format=FMT_JSON)

    def test_extended_get(self):
        orig_cas1 = self.cb.set('key_extended1', 'value1')
        val1, flags1, cas1 = self.cb.get('key_extended1', extended=True)
        self.assertEqual(val1, 'value1')
        self.assertEqual(flags1, 0x0)
        self.assertEqual(cas1, orig_cas1)

        cas2 = self.cb.set('key_extended2', 'value2')
        cas3 = self.cb.set('key_extended3', 'value3')
        results = self.cb.get(['key_extended2', 'key_extended3'],
                              extended=True)
        self.assertEqual(results['key_extended2'], ('value2', 0x0, cas2))
        self.assertEqual(results['key_extended3'], ('value3', 0x0, cas3))


if __name__ == '__main__':
    unittest.main()
