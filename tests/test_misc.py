from tests.base import CouchbaseTestCase

class ConnectionMiscTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionMiscTest, self).setUp()
        self.cb = self.make_connection()

    def test_server_nodes(self):
        nodes = self.cb.server_nodes
        self.assertIsInstance(nodes, (list, tuple))
        self.assertTrue(len(nodes) > 0)
        for n in nodes:
            self.assertIsInstance(n, str)

        def _set_nodes():
            self.cb.server_nodes = 'sdf'
        self.assertRaises(AttributeError, _set_nodes)
