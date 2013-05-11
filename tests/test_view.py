import time

from couchbase.libcouchbase import Connection
from couchbase import exceptions

from tests.base import CouchbaseTestCase


class ConnectionViewTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionViewTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_http_view(self):
        '''
        Simple test for retrieving view results via HTTP -- based on the
        tests from pylcb. First creates a simple view using the lower-level
        _make_http_request API, then queries it.
        '''
        design_doc = {
            'views': {
                'test': {
                    'map': "function(doc, meta) { emit(doc.key, doc.data); }",
                    }
                }
            }
        result = self.cb._http_view("_design/dev_test", body=design_doc,
                                    method="PUT")
        self.assertEquals(result["id"], "_design/dev_test")

        self.cb.set("testViewKey1", {"key": "key1", "data": "somedata"})
        self.cb.set("testViewKey2", {"key": "key2", "data": "somedata"})
        self.cb.set("testViewKey3", {"key": "key3", "data": "somedata"})
        params = dict(
            stale="false",
            startkey="key1",
            endkey="key3"
        )

        time.sleep(1)
        result = self.cb._http_view("_design/dev_test/_view/test", **params)

        self.assertNotEqual(result['total_rows'], 0)

        with self.assertRaises(exceptions.HTTPError):
            self.cb._http_view("not_a_design_document")
