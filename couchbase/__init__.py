from couchbase import libcouchbase

# Import a few things into the root of the module
from couchbase.libcouchbase import CB_FMT_JSON, CB_FMT_PICKLE, CB_FMT_PLAIN


class Couchbase:
    """The base class for interacting with Couchbase"""
    @staticmethod
    def connect(host='localhost', port=8091):
        """Connect to a bucket

        :param string host: the hostname or IP address of the node
        :param number port: port of the management API

        :raise: :exc:`couchbase.exceptions.BucketNotFoundError` if there is
                no such bucket to connect to

                :exc:`couchbase.exceptions.ConnectionError` if the socket
                wasn't accessible (doesn't accept connections or doesn't
                respond in time)

        :return: instance of :class:`couchbase.libcouchbase.Connection`


        Initialize connection using default options::

            from couchbase import Couchbase
            cb = Couchbase.connect()

        Select custom bucket::

            cb = Couchbase.connect(bucket = 'foo')

        Connect to protected bucket::

            cb = Couchbase.connect('localhost', 8091, 'protected', 'secret',
                                   'protected')

        """
        return libcouchbase.Connection(host, port)
