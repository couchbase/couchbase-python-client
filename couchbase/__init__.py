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
import warnings

# Pythons > (2.7||3.2) silence deprecation warnings by default.
# Many folks are not happy about this, as it avoids letting them
# know about potential upcoming breaking changes in their code.
# Here we add a warning filter for any deprecation warning thrown
# by Couchbase
warnings.filterwarnings(action='default',
                        category=DeprecationWarning,
                        module=r"^couchbase($|\..*)")

from couchbase.user_constants import *
import couchbase._libcouchbase as _LCB

try:
    from couchbase._version import __version__

except ImportError:
    __version__ = "0.0.0-could-not-find-git"


def set_json_converters(encode, decode):
    """
    Modify the default JSON conversion functions. This affects all
    :class:`~couchbase.bucket.Bucket` instances.

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
    :class:`~couchbase.bucket.Bucket` instances.

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


def _to_json(*args):
    """
    Utility function to encode an object to json using the user-defined
    JSON encoder (see :meth:`set_json_converters`).

    :param args: Arguments passed to the encoder
    :return: Serialized JSON string
    """
    return _LCB._get_helper('json_encode')(*args)


def _from_json(*args):
    """
    Utility function to decode a JSON string to a Python object using
    the user-defined JSON decoder (see :meth:`set_json_converters`).

    :param args: Arguments passed to the decoder
    :return: Python object converted from JSON
    """
    return _LCB._get_helper('json_decode')(*args)


def enable_logging():
    """
    Enables integration with Python's `logging` module.

    This function enables the C library's logging to be propagated to
    the Python standard `logging` module.

    Calling this function affects any :class:`~.Bucket` objects created
    afterwards (but not before). Note that currently this will also
    override any ``LCB_LOGLEVEL`` directive inside the environment as
    well.

    The "root" couchbase logger is ``couchbase``.
    """
    import couchbase._logutil
    couchbase._logutil.configure(True)


def disable_logging():
    import couchbase._logutil
    couchbase._logutil.configure(False)


class Couchbase(object):
    @classmethod
    def connect(self, bucket, **kwargs):
        from couchbase.bucket import _depr
        from couchbase.connection import Connection
        _depr('Couchbase.connect()', 'Bucket()')
        return Connection(bucket, **kwargs)

