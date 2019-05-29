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
import couchbase_core.exceptions
import copy
import couchbase_core._libcouchbase as C
from couchbase_core.exceptions import *


class KeyValueException(CouchbaseError):
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


"""Specific Exceptions (and Internal Exceptions)
Specific errors are always returned from Couchbase server itself and are specific to the service which generated them. Specific exceptions may also be Internal Exceptions, meaning that they are handled internally by the SDK and not propagated to the application.

Examples of specific exceptions:

KeyNotFoundException
NotMyVBucketException
IndexNotFoundException
Etc.
"""

"""Derived Exceptions
Expected to be handled specifically by the application to perform an additional action such as retrying to check if the key has become unlocked.
"""


class KeyNotFoundException(KeyValueException, couchbase_core.exceptions.NotFoundError):
    pass


class KeyExistsException(KeyValueException, couchbase_core.exceptions.KeyExistsError):
    pass


class ValueTooBigException(KeyValueException, couchbase_core.exceptions.TooBigError):
    pass


class KeyLockedException(KeyValueException):
    pass


class TempFailException(KeyValueException, couchbase_core.exceptions.TemporaryFailError):
    pass


class PathNotFoundException(KeyValueException, couchbase_core.exceptions.SubdocPathNotFoundError):
    pass


class PathExistsException(KeyValueException, couchbase_core.exceptions.SubdocPathExistsError):
    pass


class InvalidRangeException(KeyValueException, couchbase_core.exceptions.RangeError):
    pass


class KeyDeletedException(KeyValueException):
    pass


class QueryException(KeyValueException):
    """
    A server error occurred while executing a N1QL query. Assumes that that the service has returned a response.
    Message
    The error message returned by the Query service
    Properties
    The error(s) returned by response from the server by the Query/N1QL service
    Any additional information returned by the server, the node it executed on, payload, HTTP status
    """
    pass

"""
Service Exceptions
A Service level exception is any error or exception thrown or handled by one of the specific Couchbase Services: Query/N1QL, FTS, Analytics, View and Key/Value (Memcached). The exception or error names for each service are:

QueryException
SearchException
ViewException
KeyValueException
AnalyticsException

All Service exceptions derived from the base CouchbaseException and have an internal exception which can be either a system error/exception raised by the platform or a generic or shared error/exception across all services.

"""
class SearchException(CouchbaseError):
    pass

    """Message
    The error message returned by the Search service
    Properties
    The error(s) returned by response from the server by the FTS Service
    Any additional information returned by the server, the node it executed on, payload, HTTP status
    """

"""Derived Exceptions
TBD? May be nothing to extend...
"""
class AnalyticsException(CouchbaseError):
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
class ViewException(CouchbaseError):
    """A server error occurred while executing a View query.  Assumes that that the service has returned a response.
    Message
    The error message returned by the View service
    Properties
    The error(s) returned by response from the server, contextId, any additional information returned by the server, the node it executed on, payload, HTTP status.
    """
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

class InvalidConfigurationException(CouchbaseError):
    """An invalid configuration was supplied to the client.
    Message
    "A configuration error has occurred." details and inner exceptions, any stacktrace info.
    Properties
    TBD
    """
    pass
class BootstrappingException(CouchbaseError):
    """The client cannot initiate or fails while performing the bootstrap process.
    Message
    "A bootstrapping error has occurred." details and inner exceptions, any stacktrace info.
    Properties
    TBD"""
    pass
class ServiceNotFoundException(CouchbaseError):
    """The client requests or queries a service that is not enabled or available in the cluster.
    Message
    "The service requested is not enabled or cannot be found on the node requested.." details and inner exceptions, any stacktrace info.
    Properties
    TBD"""

class TimeoutException(couchbase_core.exceptions.TimeoutError):
    """---
    Message
    Properties
    Reason: (Exception) Explains the underlying reason we expect this was caused.
    """
    pass
class NetworkException(couchbase_core.exceptions.NetworkError):
    """A generic network error"""
    pass
class CouchbaseNetworkException(couchbase_core.exceptions.CouchbaseNetworkError):
    """A network error happens while executing a query or performing the K/V operation; the query or operation does not succeed with a valid service response.
    Message
    Properties
    Bubble up any platform idiomatic information."""
    pass
class NodeUnavailableException(CouchbaseError):
    """The client attempts to use a node which is either offline or cannot fulfill a request.
    Message
    "The node that the operation has been requested on is down or not available". details and inner exceptions, any stacktrace info.
    Properties
    TBD"""
    pass
class BucketMissingException(couchbase_core.exceptions.BucketNotFoundError):
    """The application attempts to open or use a bucket which does exist or is not available at that time.
    Message
    "The requested bucket '{bucketname}' cannot be found."
    Properties
    TBD"""
    pass
class CollectionMissingException(CouchbaseError):
    """The application attempts to open or use a collection which does not exist or is not available at that time.
    Message
    "The requested collection '{collectionname}' cannot be found."
    Properties
    TBD
    """
    pass

class AuthenticationException(couchbase_core.exceptions.AuthError):
    """An authorization failure is returned by the server for given resource and credentials.
    Message
    "An authorization error has occurred"
    Properties
    TBD"""

class AccessDeniedException(CouchbaseError):
    pass

class DiagnosticsException(CouchbaseError):
    pass

# previous exceptions


class CASMismatchException(CouchbaseError):
    pass


class ReplicaNotConfiguredException(CouchbaseError):
    pass


class DocumentConcurrentlyModifiedException(CouchbaseError):
    pass


class DocumentMutationLostException(CouchbaseError):
    pass


class ReplicaNotAvailableException(CouchbaseError):
    pass



_PYCBC_CRYPTO_ERR_MAP ={
    C.PYCBC_CRYPTO_PROVIDER_NOT_FOUND: couchbase_core.exceptions.CryptoProviderNotFoundException,
    C.PYCBC_CRYPTO_PROVIDER_ALIAS_NULL: couchbase_core.exceptions.CryptoProviderAliasNullException,
    C.PYCBC_CRYPTO_PROVIDER_MISSING_PUBLIC_KEY: couchbase_core.exceptions.CryptoProviderMissingPublicKeyException,
    C.PYCBC_CRYPTO_PROVIDER_MISSING_SIGNING_KEY: couchbase_core.exceptions.CryptoProviderMissingSigningKeyException,
    C.PYCBC_CRYPTO_PROVIDER_MISSING_PRIVATE_KEY: couchbase_core.exceptions.CryptoProviderMissingPrivateKeyException,
    C.PYCBC_CRYPTO_PROVIDER_SIGNING_FAILED: couchbase_core.exceptions.CryptoProviderSigningFailedException,
    C.PYCBC_CRYPTO_PROVIDER_ENCRYPT_FAILED: couchbase_core.exceptions.CryptoProviderEncryptFailedException,
    C.PYCBC_CRYPTO_PROVIDER_DECRYPT_FAILED: couchbase_core.exceptions.CryptoProviderDecryptFailedException,
    C.PYCBC_CRYPTO_CONFIG_ERROR: couchbase_core.exceptions.CryptoConfigError,
    C.PYCBC_CRYPTO_EXECUTION_ERROR: couchbase_core.exceptions.CryptoExecutionError,
    C.PYCBC_CRYPTO_ERROR: couchbase_core.exceptions.CryptoError,
    C.PYCBC_CRYPTO_PROVIDER_KEY_SIZE_EXCEPTION: couchbase_core.exceptions.CryptoProviderKeySizeException
}


_LCB_ERRCAT_MAP = {
    C.LCB_ERRTYPE_NETWORK:      couchbase_core.exceptions.CouchbaseNetworkError,
    C.LCB_ERRTYPE_INPUT: couchbase_core.exceptions.CouchbaseInputError,
    C.LCB_ERRTYPE_TRANSIENT: couchbase_core.exceptions.CouchbaseTransientError,
    C.LCB_ERRTYPE_FATAL: couchbase_core.exceptions.CouchbaseFatalError,
    C.LCB_ERRTYPE_DATAOP: couchbase_core.exceptions.CouchbaseDataError,
    C.LCB_ERRTYPE_INTERNAL:     couchbase_core.exceptions.CouchbaseInternalError
}

_LCB_SYNCREP_MAP = {
    C.LCB_DURABILITY_INVALID_LEVEL: DurabilityInvalidLevelException,
    C.LCB_DURABILITY_IMPOSSIBLE: DurabilityImpossibleException,
    C.LCB_DURABILITY_SYNC_WRITE_IN_PROGRESS: DurabilitySyncWriteInProgressException,
    C.LCB_DURABILITY_SYNC_WRITE_AMBIGUOUS: DurabilitySyncWriteAmbiguousException
}

V3Mapping=dict(list({
           C.LCB_AUTH_ERROR:       AuthenticationException,
           C.LCB_DELTA_BADVAL: couchbase_core.exceptions.DeltaBadvalError,
           C.LCB_E2BIG:            ValueTooBigException,
           C.LCB_EBUSY: couchbase_core.exceptions.BusyError,
           C.LCB_ENOMEM: couchbase_core.exceptions.NoMemoryError,
           C.LCB_ETMPFAIL:         TempFailException,
           C.LCB_KEY_EEXISTS:      KeyExistsException,
           C.LCB_KEY_ENOENT:       KeyNotFoundException,
           C.LCB_DLOPEN_FAILED: couchbase_core.exceptions.DlopenFailedError,
           C.LCB_DLSYM_FAILED: couchbase_core.exceptions.DlsymFailedError,
           C.LCB_NETWORK_ERROR:    couchbase_core.exceptions.NetworkError,
           C.LCB_NOT_MY_VBUCKET:   couchbase_core.exceptions.NotMyVbucketError,
           C.LCB_NOT_STORED:       couchbase_core.exceptions.NotStoredError,
           C.LCB_NOT_SUPPORTED:    couchbase_core.exceptions.NotSupportedError,
           C.LCB_UNKNOWN_HOST:     couchbase_core.exceptions.UnknownHostError,
           C.LCB_PROTOCOL_ERROR:   couchbase_core.exceptions.ProtocolError,
           C.LCB_ETIMEDOUT:        couchbase_core.exceptions.TimeoutError,
           C.LCB_CONNECT_ERROR:    couchbase_core.exceptions.ConnectError,
           C.LCB_BUCKET_ENOENT:    BucketMissingException,
           C.LCB_EBADHANDLE:       couchbase_core.exceptions.BadHandleError,
           C.LCB_INVALID_HOST_FORMAT: couchbase_core.exceptions.InvalidError,
           C.LCB_INVALID_CHAR:     couchbase_core.exceptions.InvalidError,
           C.LCB_EINVAL:           couchbase_core.exceptions.InvalidError,
           C.LCB_DURABILITY_ETOOMANY: couchbase_core.exceptions.ArgumentError,
           C.LCB_DUPLICATE_COMMANDS: couchbase_core.exceptions.ArgumentError,
           C.LCB_CLIENT_ETMPFAIL:  couchbase_core.exceptions.ClientTemporaryFailError,
           C.LCB_HTTP_ERROR:       couchbase_core.exceptions.HTTPError,
           C.LCB_SUBDOC_PATH_ENOENT: PathNotFoundException,
           C.LCB_SUBDOC_PATH_EEXISTS: PathExistsException,
           C.LCB_SUBDOC_PATH_EINVAL: couchbase_core.exceptions.SubdocPathInvalidError,
           C.LCB_SUBDOC_DOC_E2DEEP: couchbase_core.exceptions.DocumentTooDeepError,
           C.LCB_SUBDOC_DOC_NOTJSON: couchbase_core.exceptions.DocumentNotJsonError,
           C.LCB_SUBDOC_VALUE_E2DEEP: couchbase_core.exceptions.SubdocValueTooDeepError,
           C.LCB_SUBDOC_PATH_MISMATCH: couchbase_core.exceptions.SubdocPathMismatchError,
           C.LCB_SUBDOC_VALUE_CANTINSERT: couchbase_core.exceptions.SubdocCantInsertValueError,
           C.LCB_SUBDOC_BAD_DELTA: couchbase_core.exceptions.SubdocBadDeltaError,
           C.LCB_SUBDOC_NUM_ERANGE: couchbase_core.exceptions.SubdocNumberTooBigError,
           C.LCB_EMPTY_PATH: couchbase_core.exceptions.SubdocEmptyPathError
}.items()) + list(_PYCBC_CRYPTO_ERR_MAP.items()) + list(_LCB_SYNCREP_MAP.items()))


_EXCTYPE_MAP = {
    C.PYCBC_EXC_ARGUMENTS:  couchbase_core.exceptions.ArgumentError,
    C.PYCBC_EXC_ENCODING:   couchbase_core.exceptions.ValueFormatError,
    C.PYCBC_EXC_INTERNAL:   couchbase_core.exceptions.InternalSDKError,
    C.PYCBC_EXC_HTTP: couchbase_core.exceptions.HTTPError,
    C.PYCBC_EXC_THREADING: couchbase_core.exceptions.ObjectThreadError,
    C.PYCBC_EXC_DESTROYED: couchbase_core.exceptions.ObjectDestroyedError,
    C.PYCBC_EXC_PIPELINE: couchbase_core.exceptions.PipelineError
}

V2ERRORMAP = copy.copy(couchbase_core.exceptions._LCB_ERRNO_MAP)
V2ERRCATMAP = copy.copy(couchbase_core.exceptions._LCB_ERRCAT_MAP)


def patch_errnomap():
    couchbase_core.exceptions._LCB_ERRNO_MAP.clear()
    couchbase_core.exceptions._LCB_ERRNO_MAP.update({k:v for k,v in V3Mapping.items() if k not in []})
    couchbase_core.exceptions._LCB_ERRCAT_MAP.clear()
    couchbase_core.exceptions._LCB_ERRCAT_MAP.update(_LCB_ERRCAT_MAP)
    couchbase_core.exceptions.reparent()

def unpatch_errnomap():
    couchbase_core.exceptions._LCB_ERRNO_MAP.clear()
    couchbase_core.exceptions._LCB_ERRNO_MAP.update(V2ERRORMAP)
    couchbase_core.exceptions._LCB_ERRCAT_MAP.update(V2ERRCATMAP)
    couchbase_core.exceptions.reparent()


patch_errnomap()
