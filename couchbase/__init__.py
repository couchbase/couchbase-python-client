import couchbase

# Import a few things into the root of the module
from couchbase.libcouchbase import FMT_JSON, FMT_PICKLE, FMT_PLAIN


class Couchbase:
    """The base class for interacting with Couchbase"""
    @staticmethod
    def connect(host='localhost', port=8091, username=None, password=None,
                bucket=None, quiet=False, conncache=None):
        """Connect to a bucket.

        If `username` is not given but `password` is specified,
        it will automatically set to the bucket name, as it is
        expected that you try to connect to a SASL
        protected bucket, where the username is equal to the bucket
        name.

        :param host: the hostname or IP address of the node.
          This can be a list or tuple of multiple nodes; the nodes can either
          be simple strings, or (host, port) tuples (in which case the `port`
          parameter from the method arguments is ignored).
        :type host: string or list
        :param number port: port of the management API
        :param string username: the user name to connect to the cluster.
                                It's the username of the management API.
                                The username could be skipped for
                                protected buckets, the bucket name will
                                be used instead.
        :param string password: the password of the user or bucket
        :param string bucket: the bucket name
        :param boolean quiet: the flag controlling whether to raise an
          exception when the client executes operations on non-existent
          keys. If it is `False` it will raise
          :exc:`couchbase.exceptions.NotFoundError` exceptions. When set
          to `True` the operations will return `None` silently.
        :param string conncache: If set, this will refer to a path on the
          filesystem where cached "bootstrap" information may be stored. This
          path may be shared among multiple instance of the Couchbase client.
          Using this option may reduce overhead when using many short-lived
          instances of the client.

        :raise: :exc:`couchbase.exceptions.BucketNotFoundError` if there
                is no such bucket to connect to

                :exc:`couchbase.exceptions.ConnectError` if the socket
                wasn't accessible (doesn't accept connections or doesn't
                respond in time)

                :exc:`couchbase.exceptions.ArgumentError`
                if the bucket wasn't specified

        :return: instance of :class:`couchbase.libcouchbase.Connection`


        Initialize connection using default options::

            from couchbase import Couchbase
            cb = Couchbase.connect(bucket='mybucket')

        Connect to protected bucket::

            cb = Couchbase.connect(password='secret', bucket='protected')

        Connect to a different server on the default port 8091::

            cb = Couchbase.connect('example.com', username='admin',
                                   password='secret', bucket='mybucket')

        """
        return couchbase.libcouchbase.Connection(host, port, username,
                                                 password, bucket,
                                                 conncache=conncache)
