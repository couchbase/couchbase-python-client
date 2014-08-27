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

try:
    from couchbase._version import __version__

except ImportError:
    __version__ = "0.0.0-could-not-find-git"


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
    def connect(connection_string, password=None, quiet=False, unlock_gil=True,
                transcoder=None, lockmode=LOCKMODE_EXC, **kwargs):
        """Connect to a bucket.

        :param string connection_string:
          The connection string to use for connecting to the bucket. The
          connection string is a URI-like string allowing specifying multiple
          hosts and a bucket name.

          The format of the connection string is the *scheme* (``couchbase``
          for normal connections, ``couchbases`` for SSL enabled connections);
          a list of one or more *hostnames* delimited by commas; a *bucket*
          and a set of options.

          like so::

            couchbase://host1,host2,host3/bucketname?option1=value1&option2=value2


          If using the SSL scheme (``couchbases``), ensure to specify the
          ``certpath`` option to point to the location of the certificate on the
          client's filesystem; otherwise connection may fail with an error code
          indicating the server's certificate could not be trusted.

          See :ref:`connopts` for additional connection options.


        :param string password: the password of the bucket

        :param boolean quiet: the flag controlling whether to raise an
          exception when the client executes operations on non-existent
          keys. If it is `False` it will raise
          :exc:`couchbase.exceptions.NotFoundError` exceptions. When set
          to `True` the operations will return `None` silently.

        :param boolean unlock_gil: If set (which is the default), the
          connection object will release the python GIL when possible, allowing
          other (Python) threads to function in the background. This should be
          set to true if you are using threads in your application (and is the
          default), as otherwise all threads will be blocked while couchbase
          functions execute.

          You may turn this off for some performance boost and you are certain
          your application is not using threads

        :param transcoder:
          Set the transcoder object to use. This should conform to the
          interface in the documentation (it need not actually be a subclass).
          This can be either a class type to instantiate, or an initialized
          instance.
        :type transcoder: :class:`couchbase.transcoder.Transcoder`

        :param lockmode:
          The *lockmode* for threaded access. See :ref:`multiple_threads`
          for more information.

        :param boolean experimental_gevent_support:
          This boolean value specifies whether *experimental*
          support for `gevent` should be used. Experimental support is supplied
          by substituting the built-in libcouchbase I/O functions with their
          monkey-patched `gevent` equivalents. Note that
          `gevent.monkey_patch_all` (or similar) must have already been called
          in order to ensure that the cooperative socket methods are called.

          .. warning::

            As the parameter name implies, this feature is experimental. This
            means it may crash or hang your application. While no known issues
            have been discovered at the time of writing, it has not been
            sufficiently tested and as such is marked as experimental.

            API and implementation of this feature are subject to change.

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
            cb = Couchbase.connect('couchbase:///mybucket')

        Connect to protected bucket::

            cb = Couchbase.connect('couchbase:///protected', password='secret')

        Connect using a list of servers::

            cb = Couchbase.connect('couchbase://host1,host2,host3/mybucket')

        Connect using SSL::

            cb = Couchbase.connect('couchbases://securehost/bucketname?certpath=/var/cb-cert.pem')

        """
        return Connection(connection_string=connection_string, password=password,
                          unlock_gil=unlock_gil, transcoder=transcoder,
                          quiet=quiet, lockmode=lockmode, **kwargs)
