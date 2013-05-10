import json

from couchbase.libcouchbase import Connection
from couchbase import exceptions

from tests.base import CouchbaseTestCase


class ConnectionHTTPTest(CouchbaseTestCase):
    '''
    Quick tests for the Couchbase HTTP methods.
    '''
    def setUp(self):
        '''
        Initialize the connection to the test Couchbase bucket.
        '''
        super(ConnectionHTTPTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_make_simple_http_request(self):
        '''
        Verify that a simple GET request to the root of the bucket works.
        '''
        result = self.cb._make_http_request(
            0, "GET", "/", None,
            "application/x-www-form-urlencoded")
        self.assertEquals(result['status'], 200)
        self.assertEquals(result['path'], "/")
        self.assertNotEqual(result['content'], None)

    def test_make_http_request_404(self):
        '''
        Verify that a request to a presumably-nonexistent URL raises the right
        kind of exception.
        '''
        self.assertRaises(exceptions.HTTPError,
                          self.cb._make_http_request,
                          0, "GET", "/this_had_better_not_exist", None,
                          "application/x-www-form-urlencoded")

    def test_make_http_request_invalid_method(self):
        '''
        Verify that the server won't accept bizarre methods, and the client
        respects that.
        '''
        self.assertRaises(ValueError,
                          self.cb._make_http_request,
                          0, "QUERY", "/", None,
                          "application/x-www-form-urlencoded")

    def test_make_empty_post_request(self):
        '''
        Verify that the POST HTTP request method works.

        Since there isn't really anything to send, simply verify that it raises
        an appropriate error when made against an endpoint that shouldn't
        accept it.
        '''
        with self.assertRaises(exceptions.HTTPError) as cm:
            self.cb._make_http_request(0, "POST", "/", None, None)
        self.assertEquals(cm.exception.status, 400)

    def test_make_management_request(self):
        '''
        Verify that a request of LCB_HTTP_TYPE_MANAGEMENT also works.

        This should return a JSON response, as indicated by the HTTP headers.
        '''
        result = self.cb._make_http_request(
            1, "GET",  "/pools/" + self.bucket_prefix, None, None)
        self.assertEquals(result['status'], 200)
        self.assertEquals(result['headers']['Content-Type'],
                          "application/json")

    def test_make_put_request(self):
        '''
        Verify that HTTP PUTs work with a JSON body.

        This uploads a simple design document to the bucket.
        '''
        ddoc = json.dumps({
                "views": {
                    "by_id": {
                        "map": "function(doc, meta) { emit(doc.type, null); }",
                        "reduce": "_count"
                        }
                    },
                "language": "javascript",
                })

        result = self.cb._make_http_request(0, "PUT", "_design/dev_test",
                                            ddoc, "application/json")

        self.assertEquals(result['status'], 201)
        data = json.loads(result['content'])
        self.assertEquals(data['id'], "_design/dev_test")

    def test_make_zdelete_request(self):
        '''
        Verify that HTTP DELETEs work correctly.


        This deletes the previously uploaded design document, and is named with
        a leading 'z' to ensure it runs after the document-upload test.
        '''
        result = self.cb._make_http_request(0, "DELETE", "_design/dev_test",
                                            None, None)
        self.assertEquals(result['status'], 200)
