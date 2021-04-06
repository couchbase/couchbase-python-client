# coding=utf-8
# Copyright 2019, Couchbase, Inc.
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
import couchbase_core._libcouchbase as C
from typing import *
import json
import sys
import inspect
import re
from couchbase_core import CompatibilityEnum
from string import Template
from collections import defaultdict
from functools import wraps
try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from couchbase_core.supportability import uncommitted


class CouchbaseException(Exception):
    """Base exception for Couchbase errors

    This is the base class for all exceptions thrown by Couchbase

    **Exception Attributes**

      .. py:attribute:: rc

      The return code which caused the error

        A :class:`~couchbase_core.result.MultiResult` object, if this
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
        (see :exc:`CouchbaseDataException`)

      .. py:attribute:: is_transient

        True if this error was likely caused by a transient condition
        (see :exc:`CouchbaseTransientException`)

      .. py:attribute:: is_fatal

        True if this error indicates a likely fatal condition for the client.
        See :exc:`CouchbaseFatalException`

      .. py:attribute:: is_network

        True if errors were received during TCP transport.
        See :exc:`CouchbaseNetworkException`

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

        :return: a subclass of :class:`CouchbaseException`
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

    ParamType = TypedDict('ParamType',
                          {'rc': int,
                           'all_results': Mapping,
                           'result': Any,
                           'inner_cause': Exception,
                           'csrc_info': Any,
                           'key': str,
                           'objextra': Any,
                           'message': str,
                           'context': Any})

    def __init__(self,  # type: CouchbaseException
                 params=None  # type: Union[CouchbaseException.ParamType,str]
                 ):
        if isinstance(params, str):
            params = {'message': params}
        elif isinstance(params, CouchbaseException):
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
        self.context = ErrorContext.from_dict(**params.get('error_context', dict()))


    @classmethod
    def pyexc(cls, message=None, obj=None, inner=None):
        context = dict()
        if inner and isinstance(inner, CouchbaseException):
            context = inner.context

        return cls({'message': message,
                    'objextra': obj,
                    'inner_cause': inner,
                    'error_context': context})

    @property
    def categories(self):
        """
        Gets the exception categories (as a set of bits)
        """
        return C._get_errtype(self.rc)

    @property
    def is_base(self):
        return self.categories & C.LCB_ERROR_TYPE_BASE

    @property
    def is_shared(self):
        return self.categories & C.LCB_ERROR_TYPE_SHARED

    @property
    def is_keyvalue(self):
        return self.categories & C.LCB_ERROR_TYPE_KEYVALUE

    @property
    def is_query(self):
        return self.categories & C.LCB_ERROR_TYPE_QUERY

    @property
    def is_analytics(self):
        return self.categories & C.LCB_ERROR_TYPE_ANALYTICS

    @property
    def is_search(self):
        return self.categories & C.LCB_ERROR_TYPE_SEARCH

    @property
    def is_view(self):
        return self.categories & C.LCB_ERROR_TYPE_VIEW

    @property
    def is_sdk(self):
        return self.categories & C.LCB_ERROR_TYPE_SDK

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
            except CouchbaseTransientException as e:
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
        count = 0
        nokey_prefix = ([""] + sorted(filter(bool, self.all_results.keys())))[-1]
        for key, v in self.all_results.items():
            if not key:
                key = nokey_prefix + ":nokey:" + str(count)
                count += 1
            success = getattr(v,'success', True)
            if success:
                ret_ok[key] = v
            else:
                ret_fail[key] = v

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

        if self.context:
            details.append("Context={0}".format(self.context))

        success, fail = self.split_results()
        if len(fail)>0:
            summary = {key: value.tracing_output for key, value in fail.items() if hasattr(value,"tracing_output")}
            details.append("Tracing Output={}".format(json.dumps(summary)))

        s = "<{0}>".format(", ".join(details))
        return s


"""
Service Exceptions
A Service level exception is any error or exception thrown or handled by one of the specific Couchbase Services: Query/N1QL, F.T.S., Analytics, View and Key/Value (Memcached). The exception or error names for each service are:

QueryException
SearchException
ViewException
KeyValueException
AnalyticsException
SDKException
BaseException

All Service exceptions derived from the base CouchbaseException and have an internal exception which can be either a system error/exception raised by the platform or a generic or shared error/exception across all services.

"""
class QueryException(CouchbaseException):
    """
    A server error occurred while executing a N1QL query. Assumes that that the service has returned a response.
    Message
    The error message returned by the Query service
    Properties
    The error(s) returned by response from the server by the Query/N1QL service
    Any additional information returned by the server, the node it executed on, payload, HTTP status
    """
    pass

class SearchException(CouchbaseException):
    pass

    """Message
    The error message returned by the Search service
    Properties
    The error(s) returned by response from the server by the F.T.S. Service
    Any additional information returned by the server, the node it executed on, payload, HTTP status
    """

"""Derived Exceptions
TBD? May be nothing to extend...
"""
class AnalyticsException(CouchbaseException):
    pass
    """A server error occurred while executing an Analytics query. Assumes that that the service has returned a response
    Message
    The error message returned by the Analytics service
    Properties
    The error(s) returned by response from the server, contextId, any additional information returned by the server, the node it executed on, payload, HTTP status.
    """
"""
Derived Exceptions
TBD? May be nothing to extend...
"""
class ViewException(CouchbaseException):
    """A server error occurred while executing a View query.  Assumes that that the service has returned a response.
    Message
    The error message returned by the View service
    Properties
    The error(s) returned by response from the server, contextId, any additional information returned by the server, the node it executed on, payload, HTTP status.
    """
    pass

class KeyValueException(CouchbaseException):
    """
    A server error occurred while executing a K/V operation. Assumes that the service has returned a response.
    Message
    The XError message returned by the memcached server
    Properties
    The memcached response status
    XError and Enhanced error message information
    The document id
    The opaque used in the request"""
    pass

class SDKException(CouchbaseException):
    """
    An error occured within the SDK, while executing a command.
    Message
    The error message returned from the SDK itself
    Properties
    """
    pass

class SharedException(CouchbaseException):
    """
    A server error occured, and it is of a sort that several services would all raise.
    Message
    The error message returned by the server
    Properties
    """

class BaseException(CouchbaseException):
    """
    An error occured which doesn't  fit into any of the other categories
    Message
    The error message describing the error
    Properties
    """


"""Specific Exceptions (and Internal Exceptions)
Specific errors are always returned from Couchbase server itself and are specific to the service which generated them. Specific exceptions may also be Internal Exceptions, meaning that they are handled internally by the SDK and not propagated to the application.

Examples of specific exceptions:

DocumentNotFoundException
NotMyVBucketException
IndexNotFoundException
Etc.
"""

"""Derived Exceptions
Expected to be handled specifically by the application to perform an additional action such as retrying to check if the key has become unlocked.
"""

class SearchIndexNotFoundException(SearchException):
  pass


class DocumentLockedException(KeyValueException):
  pass


class DocumentNotFoundException(KeyValueException):
    pass


class DocumentExistsException(KeyValueException):
    pass


class ValueTooBigException(KeyValueException):
    pass


class KeyLockedException(KeyValueException):
    pass


class DocumentUnretrievableException(KeyValueException):
  pass


class PathNotFoundException(KeyValueException):
    pass


class PathExistsException(KeyValueException):
    pass


class InvalidRangeException(KeyValueException):
    pass


class KeyDeletedException(KeyValueException):
    pass


class CollectionAlreadyExistsException(KeyValueException):
  pass


class CollectionNotFoundException(KeyValueException):
  pass


class ScopeAlreadyExistsException(KeyValueException):
  pass


class ScopeNotFoundException(KeyValueException):
  pass


class BucketAlreadyExistsException(KeyValueException):
  pass


class BucketDoesNotExistException(KeyValueException):
  pass

class BucketNotFlushableException(KeyValueException):
  pass

class PartialViewResultException(ViewException):
    #? (returns rows that it did get)
    pass

"""
Generic/Shared Exceptions
A Generic or Shared Exception is common across all services and specific to known. Examples include:

TemporaryFailureException
TimeoutException
AuthenticationException
Etc.

These may or may not be exceptions defined by the underlying platform. The inner exception should be specific to the Service that threw or raised the exception or error."""


class InvalidConfigurationException(CouchbaseException):
    """An invalid configuration was supplied to the client.
    Message
    "A configuration error has occurred." details and inner exceptions, any stacktrace info.
    Properties
    TBD
    """
    pass


class BootstrappingException(CouchbaseException):
    """The client cannot initiate or fails while performing the bootstrap process.
    Message
    "A bootstrapping error has occurred." details and inner exceptions, any stacktrace info.
    Properties
    TBD"""
    pass


class ServiceNotFoundException(CouchbaseException):
    """The client requests or queries a service that is not enabled or available in the cluster.
    Message
    "The service requested is not enabled or cannot be found on the node requested.." details and inner exceptions, any stacktrace info.
    Properties
    TBD"""


class TimeoutException(SharedException):
    """---
    Message
    Properties
    Reason: (Exception) Explains the underlying reason we expect this was caused.
    """
    pass


class NetworkException(CouchbaseException):
    """A generic network error"""
    pass


class NodeUnavailableException(CouchbaseException):
    """The client attempts to use a node which is either offline or cannot fulfill a request.
    Message
    "The node that the operation has been requested on is down or not available". details and inner exceptions, any stacktrace info.
    Properties
    TBD"""
    pass


class CollectionMissingException(CouchbaseException):
    """The application attempts to open or use a collection which does not exist or is not available at that time.
    Message
    "The requested collection '{collectionname}' cannot be found."
    Properties
    TBD
    """
    pass


class AuthenticationException(CouchbaseException):
    """An authorization failure is returned by the server for given resource and credentials.
    Message
    "An authorization error has occurred"
    Properties
    TBD"""


class AccessDeniedException(CouchbaseException):
    pass


class DiagnosticsException(CouchbaseException):
    pass


class AlreadyShutdownException(CouchbaseException):
    pass


class CASMismatchException(CouchbaseException):
    pass


class ReplicaNotConfiguredException(CouchbaseException):
    pass


class DocumentConcurrentlyModifiedException(CouchbaseException):
    pass


class DocumentMutationLostException(CouchbaseException):
    pass


class ReplicaNotAvailableException(CouchbaseException):
    pass




# TODO: make types to match.  This is just to get it to compile and run...
_LCB_ERRCAT_MAP = {
    C.LCB_ERROR_TYPE_BASE: BaseException,
    C.LCB_ERROR_TYPE_SHARED: SharedException,
    C.LCB_ERROR_TYPE_KEYVALUE: KeyValueException,
    C.LCB_ERROR_TYPE_QUERY: QueryException,
    C.LCB_ERROR_TYPE_ANALYTICS: AnalyticsException,
    C.LCB_ERROR_TYPE_SEARCH: SearchException,
    C.LCB_ERROR_TYPE_VIEW: ViewException,
    C.LCB_ERROR_TYPE_SDK: SDKException
}


class ErrorContext(dict):
    @staticmethod
    def from_dict(**kwargs):
        # type: (...) -> ErrorContext
        klass = kwargs.get('type', "ErrorContext")
        cl = getattr(sys.modules[__name__], klass)
        return cl(**kwargs)

    @property
    @uncommitted
    def endpoint(self):
        # type: (...) -> str
        return self.get('endpoint', None)

    @property
    @uncommitted
    def extended_context(self):
        # type: (...) -> str
        return self.get('extended_context', None)

    @property
    @uncommitted
    def extended_ref(self):
        # type: (...) -> str
        return self.get('extended_ref', None)


class HTTPErrorContextBase(ErrorContext):
    @property
    @uncommitted
    def http_response_code(self):
        # type: (...) -> int
        return self.get('response_code', None)

    @property
    @uncommitted
    def http_response_body(self):
        # type: (...) -> str
        return self.get('response_body', None)


class ViewErrorContext(HTTPErrorContextBase):
    @property
    @uncommitted
    def first_error_message(self):
        # type: (...) -> str
        return self.get('first_error_message', None)

    @property
    @uncommitted
    def first_error_code(self):
        # type: (...) -> str
        return self.get('first_error_code', None)

    @property
    @uncommitted
    def design_document(self):
        # type: (...) -> str
        return self.get('design_document', None)

    @property
    @uncommitted
    def view(self):
        # type: (...) -> str
        return self.get('view', None)

    @property
    @uncommitted
    def query_params(self):
        # type: (...) -> str
        return self.get('query_params', None)


class SearchErrorContext(HTTPErrorContextBase):
    @property
    @uncommitted
    def error_message(self):
        # type: (...) -> str
        return self.get('error_message', None)

    @property
    @uncommitted
    def index_name(self):
        # type: (...) -> str
        return self.get('index_name', None)

    @property
    @uncommitted
    def query(self):
        # type: (...) -> str
        return self.get('query', None)

    @property
    @uncommitted
    def params(self):
        # type: (...) -> str
        return self.get('params', None)


class QueryErrorContext(HTTPErrorContextBase):
    @property
    @uncommitted
    def first_error_code(self):
        # type: (...) -> int
        return self.get('first_error_code', None)

    @property
    @uncommitted
    def first_error_message(self):
        # type: (...) -> str
        return self.get('first_error_message', None)

    @property
    @uncommitted
    def statement(self):
        # type: (...) -> str
        return self.get('statement', None)

    @property
    @uncommitted
    def client_context_id(self):
        # type: (...) -> str
        return self.get('client_context_id', None)

    @property
    @uncommitted
    def query_params(self):
        # type: (...) -> str
        return self.get('query_params', None)


class AnalyticsErrorContext(QueryErrorContext):
    pass


class HTTPErrorContext(ErrorContext):
    @property
    @uncommitted
    def response_code(self):
        # type: (...) -> int
        return self.get('response_code', None)

    @property
    @uncommitted
    def path(self):
        # type: (...) -> str
        return self.get('path', None)

    @property
    @uncommitted
    def response_body(self):
        # type: (...) -> str
        return self.get('response_body', None)


class KVErrorContext(ErrorContext):
    @property
    @uncommitted
    def status_code(self):
        # type: (...) -> int
        return self.get('status_code', None)

    @property
    @uncommitted
    def cas(self):
        # type: (...) -> int
        return self.get('cas', None)

    @property
    @uncommitted
    def opaque(self):
        # type: (...) -> int
        return self.get('opaque', None)

    @property
    @uncommitted
    def key(self):
        # type: (...) -> str
        return self.get('key', None)

    @property
    @uncommitted
    def bucket(self):
        # type: (...) -> str
        return self.get('bucket', None)

    @property
    @uncommitted
    def scope(self):
        # type: (...) -> str
        return self.get('scope', '_default')

    @property
    @uncommitted
    def collection(self):
        # type: (...) -> str
        return self.get('collection', '_default')

    @property
    @uncommitted
    def context(self):
        # type: (...) -> str
        return self.get('context', None)

    @property
    @uncommitted
    def ref(self):
        # type: (...) -> str
        return self.get('ref', None)


# v2 exception types -- needs to go!
class CouchbaseInputException(CouchbaseException):
    """
    Base class for errors possibly caused by malformed input
    """


class CouchbaseTransientException(CouchbaseException):
    """
    Base class for errors which are likely to go away with time
    """


class CouchbaseFatalException(CouchbaseException):
    """
    Base class for errors which are likely fatal and require reinitialization
    of the instance
    """


class CouchbaseDataException(CouchbaseException):
    """
    Base class for negative replies received from the server. These errors
    indicate that the server could not satisfy the request because of certain
    data constraints (such as an item not being present, or a CAS mismatch)
    """
# END V2 exception types -- needs to go eventually


class InternalSDKException(CouchbaseException):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should
    not be seeing this message)
    """


class CouchbaseInternalException(InternalSDKException):
    pass


class CouchbaseDurabilityException(InternalSDKException):
    pass


class InvalidArgumentException(CouchbaseException):
    """
    Raised when It is unambiguously determined that the error was caused because of invalid arguments from the user
    Usually only thrown directly when doing request arg validation.
    Also commonly used as a parent class for many service-specific exceptions (see below)"""


class ValueFormatException(CouchbaseException):
    """Failed to decode or encode value"""


# The following exceptions are derived from libcouchbase

class DeltaBadvalException(CouchbaseException):
    """The given value is not a number

    The server detected that operation cannot be executed with
    requested arguments. For example, when incrementing not a number.
    """


class TooBigException(CouchbaseException):
    """Object too big

    The server reported that this object is too big
    """


class BusyException(CouchbaseException):
    """The cluster is too busy

    The server is too busy to handle your request right now.
    please back off and try again at a later time.
    """


class InternalException(CouchbaseException):
    """Internal Error

    Internal error inside the library. You would have
    to destroy the instance and create a new one to recover.
    """


class InvalidException(CouchbaseException):
    """Invalid arguments specified"""


class NoMemoryException(CouchbaseException):
    """The server ran out of memory"""


class RangeException(CouchbaseException):
    """An invalid range specified"""


class LibcouchbaseException(CouchbaseException):
    """A generic error"""


class TemporaryFailException(SharedException):
    """Temporary failure (on server)

    The server tried to perform the requested operation, but failed
    due to a temporary constraint. Retrying the operation may work.

    This error may also be delivered if the key being accessed was
    locked.

    .. seealso::

        :meth:`couchbase_core.client.Client.lock`
        :meth:`couchbase_core.client.Client.unlock`
    """


class DlopenFailedException(CouchbaseException):
    """Failed to open shared object"""


class DlsymFailedException(CouchbaseException):
    """Failed to locate the requested symbol in the shared object"""


class NotMyVbucketException(CouchbaseException):
    """The vbucket is not located on this server

    The server who received the request is not responsible for the
    object anymore. (This happens during changes in the cluster
    topology)
    """


class NotStoredException(CouchbaseException):
    """The object was not stored on the server"""


class NotSupportedException(CouchbaseException):
    """Not supported

    The server doesn't support the requested command. This error
    differs from :exc:`UnknownCommandException` by
    that the server knows about the command, but for some reason
    decided to not support it.
    """


class UnknownCommandException(CouchbaseException):
    """The server doesn't know what that command is"""


class UnknownHostException(NetworkException):
    """The server failed to resolve the requested hostname"""


class ProtocolException(NetworkException):
    """Protocol error

    There is something wrong with the datastream received from
    the server
    """


class ConnectException(NetworkException):
    """Failed to connect to the requested server"""


class BucketNotFoundException(CouchbaseException):
    """The requested bucket does not exist"""


class QueryIndexNotFoundException(CouchbaseException):
    """The requested index does not exist"""


class QueryIndexAlreadyExistsException(CouchbaseException):
    """The requested index already exists"""


class ClientNoMemoryException(CouchbaseException):
    """The client ran out of memory"""


class ClientTemporaryFailException(CouchbaseException):
    """Temporary failure (on client)

    The client encountered a temporary error (retry might resolve
    the problem)
    """


class BadHandleException(CouchbaseException):
    """Invalid handle type

    The requested operation isn't allowed for given type.
    """


class HTTPException(CouchbaseException):
    """HTTP error"""

class FeatureNotFoundException(HTTPException):
    """Thrown when feature is not supported by server version."""

class ObjectThreadException(CouchbaseException):
    """Thrown when access from multiple threads is detected"""


class ViewEngineException(CouchbaseException):
    """Thrown for inline errors during view queries"""


class ObjectDestroyedException(CouchbaseException):
    """Object has been destroyed. Pending events are invalidated"""


class PipelineException(CouchbaseException):
    """Illegal operation within pipeline state"""


class SubdocPathInvalidException(CouchbaseException):
    """Subdocument path is invalid"""


class DocumentNotJsonException(CouchbaseException):
    """Document is not JSON and cannot be used for subdoc operations"""


class SubdocPathMismatchException(CouchbaseException):
    """Subdocument path conflicts with actual document structure"""


class DocumentTooDeepException(CouchbaseException):
    """Document is too deep to be used for subdocument operations"""


class SubdocNumberTooBigException(CouchbaseException):
    """Existing number is too big to be used for subdocument operations"""


class SubdocValueTooDeepException(CouchbaseException):
    """Value is too deep to insert into document, or would cause the document
    to be too deep"""


class SubdocCantInsertValueException(CouchbaseException):
    """Cannot insert value for given operation"""


class SubdocBadDeltaException(CouchbaseException):
    """Bad delta supplied for counter command"""


class SubdocEmptyPathException(CouchbaseException):
    """Empty path passed as subdoc spec"""


class CryptoException(CouchbaseException):
    def __init__(self, params=None, message="Generic Cryptography Error for alias:$alias", **kwargs):
        params = params or {}
        param_dict = params.get('objextra') or defaultdict(lambda: "unknown")
        params['message'] = Template(message).safe_substitute(**param_dict)
        super(CryptoException, self).__init__(params=params)


class CryptoConfigException(CryptoException):
    """Generic Crypto Config Error"""

    def __init__(self, params=None, message="Generic Cryptography Configuration Error for alias:$alias", **kwargs):
        super(CryptoConfigException, self).__init__(params=params, message=message, **kwargs)


class CryptoExecutionException(CryptoException):
    """Generic Crypto Execution Error"""

    def __init__(self, params=None, message="Generic Cryptography Execution Error for alias:$alias", **kwargs):
        super(CryptoExecutionException, self).__init__(params=params, message=message, **kwargs)


class CryptoProviderNotFoundException(CryptoConfigException):
    """No crypto provider can be found for a given alias."""

    def __init__(self, params=None):
        super(CryptoProviderNotFoundException, self).__init__(params=params,
                                                              message="The cryptographic provider could not be found for the alias:$alias")


class CryptoProviderAliasNullException(CryptoConfigException):
    """The annotation has no associated alias or is null or and empty string."""

    def __init__(self, params=None):
        super(CryptoProviderAliasNullException, self).__init__(params=params,
                                                               message="Cryptographic providers require a non-null, empty alias be configured.")


class CryptoProviderMissingPublicKeyException(CryptoConfigException):
    """The PublicKeyName field has not been set in the crypto provider configuration or is null or and empty string"""
    def __init__(self, params = None):
        super(CryptoProviderMissingPublicKeyException,self).__init__(params=params, message="Cryptographic providers require a non-null, empty public and key identifier (kid) be configured for the alias:$alias")


class CryptoProviderMissingSigningKeyException(CryptoConfigException):
    """The SigningKeyName field has not been set in the crypto provider configuration or is null or and empty string. Required for symmetric algos."""
    def __init__(self, params = None):
        super(CryptoProviderMissingSigningKeyException,self).__init__(params=params, message="Symmetric key cryptographic providers require a non-null, empty signing key be configured for the alias:$alias")


class CryptoProviderMissingPrivateKeyException(CryptoConfigException):
    """The PrivateKeyName field has not been set in the crypto provider configuration or is null or and empty string. Required for asymmetric algos."""
    def __init__(self, params = None):
        super(CryptoProviderMissingPrivateKeyException,self).__init__(params=params, message="Asymmetric key cryptographic providers require a non-null, empty private key be configured for the alias:$alias")


class CryptoProviderSigningFailedException(CryptoExecutionException):
    """Thrown if the authentication check fails on the decryption side."""
    def __init__(self, params = None):
        super(CryptoProviderSigningFailedException,self).__init__(params=params, message="The authentication failed while checking the signature of the message payload for the alias:$alias")


class CryptoProviderEncryptFailedException(CryptoExecutionException):
    """Thrown if an error occurs during encryption."""
    def __init__(self, params = None):
        super(CryptoProviderEncryptFailedException,self).__init__(params=params, message="The encryption of the field failed for the alias:$alias")


class CryptoProviderDecryptFailedException(CryptoExecutionException):
    """Thrown if an error occurs during decryption."""
    def __init__(self, params = None):
        super(CryptoProviderDecryptFailedException,self).__init__(params=params, message="The decryption of the field failed for the alias:$alias")


class CryptoProviderKeySizeException(CryptoException):
    def __init__(self, params = None):
        super(CryptoProviderKeySizeException,self).__init__(params=params, message=
        "The key found does not match the size of the key that the algorithm expects for the alias: $alias. Expected key size was $expected_keysize and configured key size is $configured_keysize")


class NotImplementedInV3(CouchbaseException):
    """Not available on PYCBC>=3.0.0-alpha1"""
    pass


class DataverseAlreadyExistsException(AnalyticsException):
    """Raised when attempting to create dataverse when it already exists"""
    pass


class DataverseNotFoundException(AnalyticsException):
    """Raised when attempting to drop a dataverse which does not exist"""
    pass


class DatasetNotFoundException(AnalyticsException):
    """Raised when attempting to drop a dataset which does not exist."""
    pass


class DatasetAlreadyExistsException(AnalyticsException):
    """Raised when attempting to create a dataset which already exists"""

_PYCBC_CRYPTO_ERR_MAP ={
    C.PYCBC_CRYPTO_PROVIDER_NOT_FOUND: CryptoProviderNotFoundException,
    C.PYCBC_CRYPTO_PROVIDER_ALIAS_NULL: CryptoProviderAliasNullException,
    C.PYCBC_CRYPTO_PROVIDER_MISSING_PUBLIC_KEY: CryptoProviderMissingPublicKeyException,
    C.PYCBC_CRYPTO_PROVIDER_MISSING_SIGNING_KEY: CryptoProviderMissingSigningKeyException,
    C.PYCBC_CRYPTO_PROVIDER_MISSING_PRIVATE_KEY: CryptoProviderMissingPrivateKeyException,
    C.PYCBC_CRYPTO_PROVIDER_SIGNING_FAILED: CryptoProviderSigningFailedException,
    C.PYCBC_CRYPTO_PROVIDER_ENCRYPT_FAILED: CryptoProviderEncryptFailedException,
    C.PYCBC_CRYPTO_PROVIDER_DECRYPT_FAILED: CryptoProviderDecryptFailedException,
    C.PYCBC_CRYPTO_CONFIG_ERROR: CryptoConfigException,
    C.PYCBC_CRYPTO_EXECUTION_ERROR: CryptoExecutionException,
    C.PYCBC_CRYPTO_ERROR: CryptoException,
    C.PYCBC_CRYPTO_PROVIDER_KEY_SIZE_EXCEPTION: CryptoProviderKeySizeException
}

_LCB_ERRCAT_MAP = {
    C.LCB_ERROR_TYPE_BASE: BaseException,
    C.LCB_ERROR_TYPE_SHARED: SharedException,
    C.LCB_ERROR_TYPE_KEYVALUE: KeyValueException,
    C.LCB_ERROR_TYPE_QUERY: QueryException,
    C.LCB_ERROR_TYPE_ANALYTICS: AnalyticsException,
    C.LCB_ERROR_TYPE_SEARCH: SearchException,
    C.LCB_ERROR_TYPE_VIEW: ViewException,
    C.LCB_ERROR_TYPE_SDK: SDKException
}

class DurabilityInvalidLevelException(CouchbaseDurabilityException):
    """Given durability level is invalid"""


class DurabilityImpossibleException(CouchbaseDurabilityException):
    """Given durability requirements are impossible to achieve"""


class DurabilitySyncWriteInProgressException(CouchbaseDurabilityException):
    """Returned if an attempt is made to mutate a key which already has a
    SyncWrite pending. Client would typically retry (possibly with backoff).
    Similar to ELOCKED"""


class DurabilitySyncWriteAmbiguousException(CouchbaseDurabilityException):
    """There is a synchronous mutation pending for given key
    The SyncWrite request has not completed in the specified time and has ambiguous
    result - it may Succeed or Fail; but the final value is not yet known"""


class DurabilityErrorCode(CompatibilityEnum):
    @classmethod
    def prefix(cls):
        return "LCB_DURABILITY_"
    INVALID_LEVEL = DurabilityInvalidLevelException
    IMPOSSIBLE = DurabilityImpossibleException
    SYNC_WRITE_IN_PROGRESS = DurabilitySyncWriteInProgressException
    SYNC_WRITE_AMBIGUOUS = DurabilitySyncWriteAmbiguousException


_LCB_SYNCREP_MAP = {item.value:item.orig_value for item in DurabilityErrorCode}


_LCB_ERRNO_MAP = dict(list({
                        C.LCB_ERR_AUTHENTICATION_FAILURE:        AuthenticationException,
                        C.LCB_ERR_INVALID_DELTA:                 DeltaBadvalException,
                        C.LCB_ERR_VALUE_TOO_LARGE:               ValueTooBigException,
                        C.LCB_ERR_NO_MEMORY:                     NoMemoryException,
                        C.LCB_ERR_TEMPORARY_FAILURE:             TemporaryFailException,
                        C.LCB_ERR_DOCUMENT_EXISTS:               DocumentExistsException,
                        C.LCB_ERR_DOCUMENT_NOT_FOUND:            DocumentNotFoundException,
                        C.LCB_ERR_DOCUMENT_LOCKED:               DocumentLockedException,
                        C.LCB_ERR_CAS_MISMATCH:                  CASMismatchException,
                        C.LCB_ERR_DLOPEN_FAILED:                 DlopenFailedException,
                        C.LCB_ERR_DLSYM_FAILED:                  DlsymFailedException,
                        C.LCB_ERR_NETWORK:                       NetworkException,
                        C.LCB_ERR_NOT_MY_VBUCKET:                NotMyVbucketException,
                        C.LCB_ERR_NOT_STORED:                    NotStoredException,
                        C.LCB_ERR_UNSUPPORTED_OPERATION:    NotSupportedException,
                        C.LCB_ERR_UNKNOWN_HOST:             UnknownHostException,
                        C.LCB_ERR_PROTOCOL_ERROR:           ProtocolException,
                        C.LCB_ERR_TIMEOUT:                  TimeoutException,
                        C.LCB_ERR_CONNECT_ERROR:            ConnectException,
                        C.LCB_ERR_BUCKET_NOT_FOUND:         BucketNotFoundException,
                        C.LCB_ERR_QUERY:                    QueryException,
                        C.LCB_ERR_NO_MATCHING_SERVER:       DocumentUnretrievableException,
                        C.LCB_ERR_INVALID_HOST_FORMAT:      InvalidException,
                        C.LCB_ERR_INVALID_CHAR:             InvalidException,
                        C.LCB_ERR_INVALID_ARGUMENT:         InvalidArgumentException,
                        C.LCB_ERR_DURABILITY_TOO_MANY:      DurabilityImpossibleException,
                        C.LCB_ERR_DUPLICATE_COMMANDS:       InvalidArgumentException,
                        C.LCB_ERR_NO_CONFIGURATION:         ClientTemporaryFailException,
                        C.LCB_ERR_HTTP:                     HTTPException,
                        C.LCB_ERR_SUBDOC_PATH_NOT_FOUND:    PathNotFoundException,
                        C.LCB_ERR_SUBDOC_PATH_EXISTS:       PathExistsException,
                        C.LCB_ERR_SUBDOC_PATH_INVALID:      SubdocPathInvalidException,
                        C.LCB_ERR_SUBDOC_PATH_TOO_DEEP:     DocumentTooDeepException,
                        C.LCB_ERR_SUBDOC_DOCUMENT_NOT_JSON: DocumentNotJsonException,
                        C.LCB_ERR_SUBDOC_VALUE_TOO_DEEP:    SubdocValueTooDeepException,
                        C.LCB_ERR_SUBDOC_PATH_MISMATCH:          SubdocPathMismatchException,
                        C.LCB_ERR_SUBDOC_VALUE_INVALID:          SubdocCantInsertValueException,
                        C.LCB_ERR_SUBDOC_DELTA_INVALID:          SubdocBadDeltaException,
                        C.LCB_ERR_INDEX_NOT_FOUND:               QueryIndexNotFoundException,
                        C.LCB_ERR_INDEX_EXISTS:                  QueryIndexAlreadyExistsException,
                        C.LCB_ERR_SUBDOC_NUMBER_TOO_BIG:         SubdocNumberTooBigException,
                        C.LCB_ERR_DATAVERSE_EXISTS:              DataverseAlreadyExistsException,
                        C.LCB_ERR_DATAVERSE_NOT_FOUND:           DataverseNotFoundException,
                        C.LCB_ERR_DATASET_NOT_FOUND:             DatasetNotFoundException,
                        C.LCB_ERR_DATASET_EXISTS:                DatasetAlreadyExistsException
                    }.items()) + list(_PYCBC_CRYPTO_ERR_MAP.items()) + list(_LCB_SYNCREP_MAP.items()))


def _set_default_codes():
    for k, v in _LCB_ERRNO_MAP.items():
        v.CODE = k

    InvalidArgumentException.CODE = 0


_set_default_codes()


def _mk_lcberr(rc, name=None, default=CouchbaseException, docstr="", extrabase=[]):
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
        bases = [CouchbaseException]

    return type(name, tuple(bases), d)


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
    newcls = CouchbaseException.rc_to_exctype(rc)
    return newcls(params={'rc': rc, 'objextra': obj, 'message': msg})


class QueueEmpty(Exception):
    """
    Thrown if a datastructure queue is empty
    """


CBErrorType = TypeVar('CBErrorType', bound=CouchbaseException)


class AnyPattern(object):
    def match(self, *args, **kwargs):
        return True

    def __hash__(self):
        return hash(True)

    def __eq__(self, other):
        return isinstance(other, AnyPattern)


class NotSupportedWrapper(object):
    @classmethod
    def a_404_means_not_supported(cls, func):
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except HTTPException as e:
                extra = getattr(e, 'objextra', None)
                status = getattr(extra, 'http_status', None)
                if status == 404:
                    raise NotSupportedException('Server does not support this api call')
                raise
        return wrapped

    @classmethod
    def a_400_or_404_means_not_supported(cls, func):
        # some functions 404 if < 6.5, but 400 if 6.5 with
        # developer preview off.  <Sigh>
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except HTTPException as e:
                extra = getattr(e, 'objextra', None)
                status = getattr(extra, 'http_status', None)
                if status == 404 or status == 400:
                    raise NotSupportedException('Server does not support this api call')
                raise

        return wrapped


class DictMatcher(object):
    def __init__(self, **kwargs):
        self._pattern=tuple(kwargs.items())

    def match(self, dict):
        for k, v in self._pattern:
            if not k in dict or not v.match(dict[k]):
                return False
        return True

    def __hash__(self):
        return hash(self._pattern)

    def __eq__(self, other):
        return isinstance(other, DictMatcher) and other._pattern == self._pattern


class ErrorMapper(object):
    @classmethod
    def mgmt_exc_wrap(cls, func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except CouchbaseException as e:
                for orig_exc, text_to_final_exc in cls._compiled_mapping().items():
                    if isinstance(e, orig_exc):
                        extra = getattr(e, 'objextra', None)
                        # TODO: this parsing is fragile, lets ponder a better approach, if any
                        if e.context:
                            value = e.context.response_body
                            if isinstance(value, bytearray) or isinstance(value, bytes):
                                value = value.decode("utf-8")
                            for pattern, exc in text_to_final_exc.items():
                                matches = False
                                try:
                                    matches = pattern.match(value)
                                except Exception as f:
                                    pass
                                if matches:
                                    raise exc.pyexc(e.message, extra, e)
                        # fallback to old way
                        if extra:
                            value = getattr(extra, 'value', "")

                            # this value could be a string or a json-encoded string...
                            if isinstance(value, dict):
                                # there should be a key with the error
                                # can be error or errors :(
                                if 'error' in value:
                                    value = value.get('error', None)
                                elif 'errors' in value:
                                    value = value.get('errors', None)
                                elif '_' in value:
                                    value = value.get('_', None)
                                if value and isinstance(value, dict):
                                    # sometimes it is still a dict, so use the name field
                                    value = value.get('name', None)
                            if isinstance(value, bytearray) or isinstance(value, bytes):
                                value = value.decode("utf-8")
                            for pattern, exc in text_to_final_exc.items():
                                matches=False
                                try:
                                    matches=pattern.match(value)
                                except Exception as f:
                                    pass
                                if matches:
                                    raise exc.pyexc(e.message, extra, e)
                raise

        return wrapped

    @classmethod
    def _compiled_mapping(cls):
        if not getattr(cls, '_cm', None):
            cls._cm = {
                orig_exc: {{str: re.compile}.get(type(k), lambda x: x)(k): v for k, v in mapping.items()} for
                orig_exc, mapping in cls.mapping().items()
            }
        return cls._cm

    @staticmethod
    def mapping():
        # type (...)->Mapping[CBErrorType, Mapping[str, CBErrorType]]
        return None

    @classmethod
    def wrap(cls, dest):
        for name, method in inspect.getmembers(dest, inspect.isfunction):
            if not name.startswith('_'):
                setattr(dest, name, cls.mgmt_exc_wrap(method))
        return dest


_EXCTYPE_MAP = {
    C.PYCBC_EXC_ARGUMENTS:  InvalidArgumentException,
    C.PYCBC_EXC_ENCODING:   ValueFormatException,
    C.PYCBC_EXC_INTERNAL:   InternalSDKException,
    C.PYCBC_EXC_HTTP:       HTTPException,
    C.PYCBC_EXC_THREADING:  ObjectThreadException,
    C.PYCBC_EXC_DESTROYED:  ObjectDestroyedException,
    C.PYCBC_EXC_PIPELINE:   PipelineException
}
