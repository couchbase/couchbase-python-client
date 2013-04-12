import couchbase

# Import a few things into the root of the module
from couchbase.libcouchbase import FMT_JSON, FMT_PICKLE, FMT_PLAIN


class Couchbase:
    """The base class for interacting with Couchbase"""
    @staticmethod
    def connect(host='localhost', port=8091, username=None, password=None,
                bucket=None):
        """Connect to a bucket

        The parameters `password` and `bucket` are **not** optional and
        will cause a :exc:`couchbase.excpetions.ArgumentError`
        exception if they are not specified.
        If `username` is not given, it will automatically set to the
        bucket name, as it is expected that you try to connect to a SASL
        protected bucket, where the username is equal to the bucket
        name.

        :param string host: the hostname or IP address of the node
        :param number port: port of the management API
        :param string username: the user name to connect to the cluster.
                                It's the username of the management API.
                                The username could be skipped for
                                protected buckets, the bucket name will
                                be used instead.
        :param string password: the password of the user or bucket
        :param string bucket: the bucket name

        :raise: :exc:`couchbase.exceptions.BucketNotFoundError` if there
                is no such bucket to connect to

                :exc:`couchbase.exceptions.ConnectError` if the socket
                wasn't accessible (doesn't accept connections or doesn't
                respond in time)

                :exc:`couchbase.exceptions.ArgumentError` if either the
                password or the bucket wasn't specified

        :return: instance of :class:`couchbase.libcouchbase.Connection`


        Initialize connection using default options::

            from couchbase import Couchbase
            cb = Couchbase.connect(username='admin', password='secret',
                                   bucket='mybucket')

        Connect to protected bucket::

            cb = Couchbase.connect(password='secret', bucket='protected')

        Connect to a different server on the default port 8091::

            cb = Couchbase.connect('example.com', username='admin',
                                   password='secret', bucket='mybucket')
        """
        return couchbase.libcouchbase.Connection(host, port, username,
                                                 password, bucket)
