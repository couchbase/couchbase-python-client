import unittest
import asyncio
from unittest.mock import patch


from acouchbase.cluster import Cluster, ABucket, get_event_loop, close_event_loop
from acouchbase.asyncio_iops import IOPS
from couchbase.cluster import ClusterOptions
from couchbase.auth import PasswordAuthenticator


class TestAcouchbaseConnection(unittest.TestCase):

    # TODO: possible normalize how to validate cluster __init__ args?

    @patch('couchbase.cluster.Cluster.__init__')
    def test_connection_basic(self, mock_cluster_init):
        mock_cluster_init.return_value = None
        conn_string = "couchbaes://fake-host"
        _ = Cluster(conn_string, ClusterOptions(
            PasswordAuthenticator("Administrator", "password")))

        args = mock_cluster_init.call_args[0]
        kwargs = mock_cluster_init.call_args[1]

        # validate *args
        self.assertEqual(conn_string, args[0])
        self.assertIsInstance(args[1], ClusterOptions)
        self.assertIn('authenticator', args[1])
        self.assertIsInstance(args[1]['authenticator'], PasswordAuthenticator)

        # validate **kwargs
        self.assertIn('bucket_factory', kwargs)
        # bucket_factory has not been instantiated at this moment
        self.assertEqual(kwargs['bucket_factory'].__name__, ABucket.__name__)
        self.assertIn('_flags', kwargs)
        self.assertEqual(40, kwargs['_flags'])
        self.assertIn('_iops', kwargs)
        self.assertIsInstance(kwargs['_iops'], IOPS)

    @patch('couchbase.cluster.Cluster.__init__')
    def test_connection_kwargs(self, mock_cluster_init):
        mock_cluster_init.return_value = None
        conn_string = "couchbaes://fake-host"
        _ = Cluster(connection_string=conn_string, authenticator=PasswordAuthenticator(
            "Administrator", "password"))

        args = mock_cluster_init.call_args[0]
        kwargs = mock_cluster_init.call_args[1]

        # validate *args
        self.assertEqual(conn_string, args[0])

        # validate **kwargs
        self.assertIn('authenticator', kwargs)
        self.assertIsInstance(kwargs['authenticator'], PasswordAuthenticator)
        self.assertIn('bucket_factory', kwargs)
        # bucket_factory has not been instantiated at this moment
        self.assertEqual(kwargs['bucket_factory'].__name__, ABucket.__name__)
        self.assertIn('_flags', kwargs)
        self.assertEqual(40, kwargs['_flags'])
        self.assertIn('_iops', kwargs)
        self.assertIsInstance(kwargs['_iops'], IOPS)

        _ = Cluster(conn_string, ClusterOptions(
            authenticator=PasswordAuthenticator("Administrator", "password")))

        args = mock_cluster_init.call_args[0]
        kwargs = mock_cluster_init.call_args[1]

        # validate *args
        self.assertEqual(conn_string, args[0])
        self.assertIsInstance(args[1], ClusterOptions)
        self.assertIn('authenticator', args[1])
        self.assertIsInstance(args[1]['authenticator'], PasswordAuthenticator)

        # validate **kwargs
        self.assertIn('bucket_factory', kwargs)
        # bucket_factory has not been instantiated at this moment
        self.assertEqual(kwargs['bucket_factory'].__name__, ABucket.__name__)
        self.assertIn('_flags', kwargs)
        self.assertEqual(40, kwargs['_flags'])
        self.assertIn('_iops', kwargs)
        self.assertIsInstance(kwargs['_iops'], IOPS)

    def test_loop_open_close(self):
        loop = get_event_loop()
        self.assertIsNotNone(IOPS._working_loop)
        # verify IOPS and asyncio event loops are the same
        self.assertEqual(id(loop), id(asyncio.get_event_loop()))

        close_event_loop()
        self.assertIsNone(IOPS._working_loop)
        new_loop = get_event_loop()
        # verify a new loop is not the same as the old
        self.assertNotEqual(id(loop), id(new_loop))

        # verify that after closing and recreating another loop, IOPS and asyncio event
        # loops are the same
        self.assertEqual(id(new_loop), id(asyncio.get_event_loop()))
        close_event_loop()
