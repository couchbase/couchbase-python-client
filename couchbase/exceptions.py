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

import couchbase._libcouchbase as C

class CouchbaseError(Exception):
    """Base exception for Couchbase errors

    This is the base class for all exceptions thrown by Couchbase

    **Exception Attributes**

      .. py:attribute:: rc

      The return code which caused the error

        A :class:`~couchbase.result.MultiResult` object, if this
        exception was thrown as part of a multi-operation. This contains
        all the operations (including ones which may not have failed)

      .. py:attribute:: inner_cause

        If this exception was triggered by another exception, it is
        present here.

      .. py:attribute:: key

        If applicable, this is the key which failed.

      .. py:attribute:: csrc_info

        A tuple of (`file`, `line`) pointing to a location in the C
        source code where the exception was thrown (if applicable)

      .. py:attribute:: categories

        An integer representing a set of bits representing various error
        categories for the specific error as returned by libcouchbase.

      .. py:attribute:: is_data

        True if this error is a negative reply from the server
        (see :exc:`CouchbaseDataError`)

      .. py:attribute:: is_transient

        True if this error was likely caused by a transient condition
        (see :exc:`CouchbaseTransientError`)

      .. py:attribute:: is_fatal

        True if this error indicates a likely fatal condition for the client.
        See :exc:`CouchbaseFatalError`

      .. py:attribute:: is_network

        True if errors were received during TCP transport.
        See :exc:`CouchbaseNetworkError`

      .. py:attribute:: CODE

        This is a _class_ level attribute which contains the equivalent
        libcouchbase error code which is mapped to this exception class.

        This is usually the :attr:`rc` value for an exception instance. Unlike
        :attr:`rc` however, it may be used without an instantiated object,
        possibly helping performance.

    """

    CODE = 0

    @classmethod
    def rc_to_exctype(cls, rc):
        """
        Map an error code to an exception

        :param int rc: The error code received for an operation

        :return: a subclass of :class:`CouchbaseError`
        """
        try:
            return _LCB_ERRNO_MAP[rc]
        except KeyError:
            newcls = _mk_lcberr(rc)
            _LCB_ERRNO_MAP[rc] = newcls
            return newcls

    @classmethod
    def _can_derive(cls, rc):
        """
        Determines if the given error code is logically derived from this class
        :param int rc: the error code to check
        :return: a boolean indicating if the code is derived from this exception
        """
        return issubclass(cls.rc_to_exctype(rc), cls)

    def __init__(self, params=None):
        if isinstance(params, str):
            params = {'message': params}
        elif isinstance(params, CouchbaseError):
            self.__dict__.update(params.__dict__)
            return

        self.rc = params.get('rc', self.CODE)
        self.all_results = params.get('all_results', {})
        self.result = params.get('result', None)
        self.inner_cause = params.get('inner_cause', None)
        self.csrc_info = params.get('csrc_info', ())
        self.key = params.get('key', None)
        self.objextra = params.get('objextra', None)
        self.message = params.get('message', None)

    @classmethod
    def pyexc(cls, message=None, obj=None, inner=None):
        return cls({'message': message,
                    'objextra': obj,
                    'inner_cause': inner})

    @property
    def categories(self):
        """
        Gets the exception categories (as a set of bits)
        """
        return C._get_errtype(self.rc)

    @property
    def is_transient(self):
        return self.categories & C.LCB_ERRTYPE_TRANSIENT

    @property
    def is_fatal(self):
        return self.categories & C.LCB_ERRTYPE_FATAL

    @property
    def is_network(self):
        return self.categories & C.LCB_ERRTYPE_NETWORK

    @property
    def is_data(self):
        return self.categories & C.LCB_ERRTYPE_DATAOP

    def split_results(self):
        """
        Convenience method to separate failed and successful results.

        .. versionadded:: 2.0.0

        This function will split the results of the failed operation
        (see :attr:`.all_results`) into "good" and "bad" dictionaries.

        The intent is for the application to handle any successful
        results in a success code path, and handle any failed results
        in a "retry" code path. For example

        .. code-block:: python

            try:
                cb.add_multi(docs)
            except CouchbaseTransientError as e:
                # Temporary failure or server OOM
                _, fail = e.split_results()

                # Sleep for a bit to reduce the load on the server
                time.sleep(0.5)

                # Try to add only the failed results again
                cb.add_multi(fail)

        Of course, in the example above, the second retry may fail as
        well, and a more robust implementation is left as an exercise
        to the reader.

        :return: A tuple of ( `ok`, `bad` ) dictionaries.
        """

        ret_ok, ret_fail = {}, {}
        for v in self.all_results.values():
            if v.success:
                ret_ok[v.key] = v
            else:
                ret_fail[v.key] = v

        return ret_ok, ret_fail

    def __str__(self):
        details = []

        if self.key:
            details.append("Key={0}".format(repr(self.key)))

        if self.rc:
            details.append("RC=0x{0:X}[{1}]".format(
                self.rc, C._strerror(self.rc)))
        if self.message:
            details.append(self.message)
        if self.all_results:
            details.append("Results={0}".format(len(self.all_results)))

        if self.inner_cause:
            details.append("inner_cause={0}".format(self.inner_cause))

        if self.csrc_info:
            details.append("C Source=({0},{1})".format(*self.csrc_info))

        if self.objextra:
            details.append("OBJ={0}".format(repr(self.objextra)))

        s = "<{0}>".format(", ".join(details))
        return s


class InternalSDKError(CouchbaseError):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should
    not be seeing this message)
    """

class CouchbaseInternalError(InternalSDKError):
    pass

class CouchbaseNetworkError(CouchbaseError):
    """
    Base class for network-related errors. These indicate issues in the low
    level connectivity
    """

class CouchbaseInputError(CouchbaseError):
    """
    Base class for errors possibly caused by malformed input
    """

class CouchbaseTransientError(CouchbaseError):
    """
    Base class for errors which are likely to go away with time
    """

class CouchbaseFatalError(CouchbaseError):
    """
    Base class for errors which are likely fatal and require reinitialization
    of the instance
    """

class CouchbaseDataError(CouchbaseError):
    """
    Base class for negative replies received from the server. These errors
    indicate that the server could not satisfy the request because of certain
    data constraints (such as an item not being present, or a CAS mismatch)
    """


class ArgumentError(CouchbaseError):
    """Invalid argument

    A given argument is invalid or must be set
    """


class ValueFormatError(CouchbaseError):
    """Failed to decode or encode value"""


# The following exceptions are derived from libcouchbase
class AuthError(CouchbaseError):
    """Authentication failed

    You provided an invalid username/password combination.
    """


class DeltaBadvalError(CouchbaseError):
    """The given value is not a number

    The server detected that operation cannot be executed with
    requested arguments. For example, when incrementing not a number.
    """


class TooBigError(CouchbaseError):
    """Object too big

    The server reported that this object is too big
    """


class BusyError(CouchbaseError):
    """The cluster is too busy

    The server is too busy to handle your request right now.
    please back off and try again at a later time.
    """


class InternalError(CouchbaseError):
    """Internal Error

    Internal error inside the library. You would have
    to destroy the instance and create a new one to recover.
    """


class InvalidError(CouchbaseError):
    """Invalid arguments specified"""


class NoMemoryError(CouchbaseError):
    """The server ran out of memory"""


class RangeError(CouchbaseError):
    """An invalid range specified"""


class LibcouchbaseError(CouchbaseError):
    """A generic error"""


class TemporaryFailError(CouchbaseError):
    """Temporary failure (on server)

    The server tried to perform the requested operation, but failed
    due to a temporary constraint. Retrying the operation may work.

    This error may also be delivered if the key being accessed was
    locked.

    .. seealso::

        :meth:`couchbase.bucket.Bucket.lock`
        :meth:`couchbase.bucket.Bucket.unlock`
    """


class KeyExistsError(CouchbaseError):
    """The key already exists (with another CAS value)

    This exception may be thrown during an ``add()`` operation
    (if the key already exists), or when a CAS is supplied
    and the server-side CAS differs.
    """


class NotFoundError(CouchbaseError):
    """The key does not exist"""


class DlopenFailedError(CouchbaseError):
    """Failed to open shared object"""


class DlsymFailedError(CouchbaseError):
    """Failed to locate the requested symbol in the shared object"""


class NetworkError(CouchbaseNetworkError):
    """Network error

    A network related problem occured (name lookup,
    read/write/connect etc)
    """


class NotMyVbucketError(CouchbaseError):
    """The vbucket is not located on this server

    The server who received the request is not responsible for the
    object anymore. (This happens during changes in the cluster
    topology)
    """


class NotStoredError(CouchbaseError):
    """The object was not stored on the server"""


class NotSupportedError(CouchbaseError):
    """Not supported

    The server doesn't support the requested command. This error
    differs from :exc:`couchbase.exceptions.UnknownCommandError` by
    that the server knows about the command, but for some reason
    decided to not support it.
    """


class UnknownCommandError(CouchbaseError):
    """The server doesn't know what that command is"""


class UnknownHostError(CouchbaseNetworkError):
    """The server failed to resolve the requested hostname"""


class ProtocolError(CouchbaseNetworkError):
    """Protocol error

    There is something wrong with the datastream received from
    the server
    """


class TimeoutError(CouchbaseError):
    """The operation timed out"""


class ConnectError(CouchbaseNetworkError):
    """Failed to connect to the requested server"""


class BucketNotFoundError(CouchbaseError):
    """The requested bucket does not exist"""


class ClientNoMemoryError(CouchbaseError):
    """The client ran out of memory"""


class ClientTemporaryFailError(CouchbaseError):
    """Temporary failure (on client)

    The client encountered a temporary error (retry might resolve
    the problem)
    """


class BadHandleError(CouchbaseError):
    """Invalid handle type

    The requested operation isn't allowed for given type.
    """


class HTTPError(CouchbaseError):
    """HTTP error"""


class ObjectThreadError(CouchbaseError):
    """Thrown when access from multiple threads is detected"""


class ViewEngineError(CouchbaseError):
    """Thrown for inline errors during view queries"""

class ObjectDestroyedError(CouchbaseError):
    """Object has been destroyed. Pending events are invalidated"""


class PipelineError(CouchbaseError):
    """Illegal operation within pipeline state"""


class SubdocPathNotFoundError(CouchbaseError):
    """Subdocument path does not exist"""


class SubdocPathExistsError(CouchbaseError):
    """Subdocument path already exists (and shouldn't)"""


class SubdocPathInvalidError(CouchbaseError):
    """Subdocument path is invalid"""


class DocumentNotJsonError(CouchbaseError):
    """Document is not JSON and cannot be used for subdoc operations"""

class SubdocPathMismatchError(CouchbaseError):
    """Subdocument path conflicts with actual document structure"""


class DocumentTooDeepError(CouchbaseError):
    """Document is too deep to be used for subdocument operations"""


class SubdocNumberTooBigError(CouchbaseError):
    """Existing number is too big to be used for subdocument operations"""


class SubdocValueTooDeepError(CouchbaseError):
    """Value is too deep to insert into document, or would cause the document
    to be too deep"""


class SubdocCantInsertValueError(CouchbaseError):
    """Cannot insert value for given operation"""


class SubdocBadDeltaError(CouchbaseError):
    """Bad delta supplied for counter command"""


class SubdocMultipleErrors(CouchbaseError):
    """One or more subcommands failed. Inspect the individual operation"""
    CODE = C.LCB_SUBDOC_MULTI_FAILURE


class SubdocEmptyPathError(CouchbaseError):
    """Empty path passed as subdoc spec"""

_LCB_ERRCAT_MAP = {
    C.LCB_ERRTYPE_NETWORK:      CouchbaseNetworkError,
    C.LCB_ERRTYPE_INPUT:        CouchbaseInputError,
    C.LCB_ERRTYPE_TRANSIENT:    CouchbaseTransientError,
    C.LCB_ERRTYPE_FATAL:        CouchbaseFatalError,
    C.LCB_ERRTYPE_DATAOP:       CouchbaseDataError,
    C.LCB_ERRTYPE_INTERNAL:     CouchbaseInternalError
}

_LCB_ERRNO_MAP = {
    C.LCB_AUTH_ERROR:       AuthError,
    C.LCB_DELTA_BADVAL:     DeltaBadvalError,
    C.LCB_E2BIG:            TooBigError,
    C.LCB_EBUSY:            BusyError,
    C.LCB_ENOMEM:           NoMemoryError,
    C.LCB_ETMPFAIL:         TemporaryFailError,
    C.LCB_KEY_EEXISTS:      KeyExistsError,
    C.LCB_KEY_ENOENT:       NotFoundError,
    C.LCB_DLOPEN_FAILED:    DlopenFailedError,
    C.LCB_DLSYM_FAILED:     DlsymFailedError,
    C.LCB_NETWORK_ERROR:    NetworkError,
    C.LCB_NOT_MY_VBUCKET:   NotMyVbucketError,
    C.LCB_NOT_STORED:       NotStoredError,
    C.LCB_NOT_SUPPORTED:    NotSupportedError,
    C.LCB_UNKNOWN_HOST:     UnknownHostError,
    C.LCB_PROTOCOL_ERROR:   ProtocolError,
    C.LCB_ETIMEDOUT:        TimeoutError,
    C.LCB_CONNECT_ERROR:    ConnectError,
    C.LCB_BUCKET_ENOENT:    BucketNotFoundError,
    C.LCB_EBADHANDLE:       BadHandleError,
    C.LCB_INVALID_HOST_FORMAT: InvalidError,
    C.LCB_INVALID_CHAR:     InvalidError,
    C.LCB_EINVAL:           InvalidError,
    C.LCB_DURABILITY_ETOOMANY: ArgumentError,
    C.LCB_DUPLICATE_COMMANDS: ArgumentError,
    C.LCB_CLIENT_ETMPFAIL:  ClientTemporaryFailError,
    C.LCB_HTTP_ERROR:       HTTPError,
    C.LCB_SUBDOC_PATH_ENOENT: SubdocPathNotFoundError,
    C.LCB_SUBDOC_PATH_EEXISTS: SubdocPathExistsError,
    C.LCB_SUBDOC_PATH_EINVAL: SubdocPathInvalidError,
    C.LCB_SUBDOC_DOC_E2DEEP: DocumentTooDeepError,
    C.LCB_SUBDOC_DOC_NOTJSON: DocumentNotJsonError,
    C.LCB_SUBDOC_VALUE_E2DEEP: SubdocValueTooDeepError,
    C.LCB_SUBDOC_PATH_MISMATCH: SubdocPathMismatchError,
    C.LCB_SUBDOC_VALUE_CANTINSERT: SubdocCantInsertValueError,
    C.LCB_SUBDOC_BAD_DELTA: SubdocBadDeltaError,
    C.LCB_SUBDOC_NUM_ERANGE: SubdocNumberTooBigError,
    C.LCB_EMPTY_PATH: SubdocEmptyPathError
}


def _set_default_codes():
    for k, v in _LCB_ERRNO_MAP.items():
        v.CODE = k

    ArgumentError.CODE = 0

_set_default_codes()


def _mk_lcberr(rc, name=None, default=CouchbaseError, docstr="", extrabase=[]):
    """
    Create a new error class derived from the appropriate exceptions.
    :param int rc: libcouchbase error code to map
    :param str name: The name of the new exception
    :param class default: Default exception to return if no categories are found
    :return: a new exception derived from the appropriate categories, or the
             value supplied for `default`
    """
    categories = C._get_errtype(rc)
    if not categories:
        return default

    bases = extrabase[::]

    for cat, base in _LCB_ERRCAT_MAP.items():
        if cat & categories:
            bases.append(base)

    if name is None:
        name = "LCB_0x{0:0X} (generated, catch: {1})".format(
            rc, ", ".join(x.__name__ for x in bases))

    d = { '__doc__' : docstr }

    if not bases:
        bases = [CouchbaseError]

    return type(name, tuple(bases), d)

# Reinitialize the exception classes again.
for rc, oldcls in _LCB_ERRNO_MAP.items():
    # Determine the new reparented error category for this
    newname = "_{0}_0x{1:0X} (generated, catch {0})".format(oldcls.__name__, rc)
    newcls = _mk_lcberr(rc, name=newname, default=None, docstr=oldcls.__doc__,
                        extrabase=[oldcls])
    if not newcls:
        # No categories for this type, fall back to existing one
        continue

    _LCB_ERRNO_MAP[rc] = newcls

    del newcls
    del oldcls

_EXCTYPE_MAP = {
    C.PYCBC_EXC_ARGUMENTS:  ArgumentError,
    C.PYCBC_EXC_ENCODING:   ValueFormatError,
    C.PYCBC_EXC_INTERNAL:   InternalSDKError,
    C.PYCBC_EXC_HTTP:       HTTPError,
    C.PYCBC_EXC_THREADING:  ObjectThreadError,
    C.PYCBC_EXC_DESTROYED:  ObjectDestroyedError,
    C.PYCBC_EXC_PIPELINE:   PipelineError
}


def exc_from_rc(rc, msg=None, obj=None):
    """
    .. warning:: INTERNAL

    For those rare cases when an exception needs to be thrown from
    Python using a libcouchbase error code.

    :param rc: The error code
    :param msg: Message (description)
    :param obj: Context
    :return: a raisable exception
    """
    newcls = CouchbaseError.rc_to_exctype(rc)
    return newcls(params={'rc': rc, 'objextra': obj, 'message': msg})


class QueueEmpty(Exception):
    """
    Thrown if a datastructure queue is empty
    """