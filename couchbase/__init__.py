#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from couchbase.connection import Connection
from couchbase.user_constants import *
import couchbase._libcouchbase as _LCB


def set_json_converters(encode, decode):
    """
    Modify the default JSON conversion functions. This affects all
    :class:`~couchbase.connection.Connection` instances.

    These functions will called instead of the default ones (``json.dumps``
    and ``json.loads``) to encode and decode JSON (when :const:`FMT_JSON` is
    used).

    :param callable encode: Callable to invoke when encoding an object to JSON.
        This should have the same prototype as ``json.dumps``, with the
        exception that it is only ever passed a single argument.

    :param callable decode: Callable to invoke when decoding an object to JSON.
        This should have the same prototype and behavior
        as ``json.loads`` with the exception that it is only ever
        passed a single argument.

    :return: A tuple of ``(old encoder, old decoder)``

    No exceptions are raised, and it is the responsibility of the caller to
    ensure that the provided functions operate correctly, otherwise exceptions
    may be thrown randomly when encoding and decoding values
    """
    ret = _LCB._modify_helpers(json_encode=encode, json_decode=decode)
    return (ret['json_encode'], ret['json_decode'])


def set_pickle_converters(encode, decode):
    """
    Modify the default Pickle conversion functions. This affects all
    :class:`~couchbase.connection.Connection` instances.

    These functions will be called instead of the default ones
    (``pickle.dumps`` and ``pickle.loads``) to encode and decode values to and
    from the Pickle format (when :const:`FMT_PICKLE` is used).

    :param callable encode: Callable to invoke when encoding an object to
        Pickle. This should have the same prototype as ``pickle.dumps`` with
        the exception that it is only ever called with a single argument

    :param callable decode: Callable to invoke when decoding a Pickle encoded
        object to a Python object. Should have the same prototype as
        ``pickle.loads`` with the exception that it is only ever passed a
        single argument

    :return: A tuple of ``(old encoder, old decoder)``

    No exceptions are raised and it is the responsibility of the caller to
    ensure that the provided functions operate correctly.
    """
    ret = _LCB._modify_helpers(pickle_encode=encode, pickle_decode=decode)
    return (ret['pickle_encode'], ret['pickle_decode'])


class Couchbase:
    """The base class for interacting with Couchbase"""
    @staticmethod
    def connect(bucket=None,
                host='localhost',
                port=8091,
                username=None,
                password=None,
                quiet=False,
                conncache=None,
                unlock_gil=True,
                timeout=2.5,
                transcoder=None,
                lockmode=LOCKMODE_EXC,
                **kwargs):
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

        :param boolean unlock_gil: If set (which is the default), the
          connection object will release the python GIL when possible, allowing
          other (Python) threads to function in the background. This should be
          set to true if you are using threads in your application (and is the
          default), as otherwise all threads will be blocked while couchbase
          functions execute.

          You may turn this off for some performance boost and you are certain
          your application is not using threads

        :param float timeout:
          Set the timeout in seconds. If an operation takes longer than this
          many seconds, the method will return with an error. You may set this
          higher if you have slow network conditions.

        :param transcoder:
          (*EXPERIMENTAL*)
          Set the transcoder object to use. This should conform to the
          interface in the documentation (it need not actually be a subclass)
        :type transcoder: :class:`couchbase.transcoder.Transcoder`

        :param lockmode:
          The *lockmode* for threaded access. See :ref:`multiple_threads`
          for more information.

        :raise: :exc:`couchbase.exceptions.BucketNotFoundError` if there
                is no such bucket to connect to

                :exc:`couchbase.exceptions.ConnectError` if the socket
                wasn't accessible (doesn't accept connections or doesn't
                respond in time)

                :exc:`couchbase.exceptions.ArgumentError`
                if the bucket wasn't specified

        :return: instance of :class:`couchbase.connection.Connection`


        Initialize connection using default options::

            from couchbase import Couchbase
            cb = Couchbase.connect(bucket='mybucket')

        Connect to protected bucket::

            cb = Couchbase.connect(password='secret', bucket='protected')

        Connect to a different server on the default port 8091::

            cb = Couchbase.connect(host='example.com', username='admin',
                                   password='secret', bucket='mybucket')

        """
        return Connection(host=host,
                          port=port,
                          username=username,
                          password=password,
                          bucket=bucket,
                          conncache=conncache,
                          unlock_gil=unlock_gil,
                          timeout=timeout,
                          transcoder=transcoder,
                          quiet=quiet,
                          **kwargs)
