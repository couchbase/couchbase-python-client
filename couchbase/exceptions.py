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

# TODO: make types to match.  This is just to get it to compile and run...
_LCB_ERRCAT_MAP = {
    C.LCB_ERROR_TYPE_BASE: couchbase_core.exceptions.BaseException,
    C.LCB_ERROR_TYPE_SHARED: couchbase_core.exceptions.SharedException,
    C.LCB_ERROR_TYPE_KEYVALUE: couchbase_core.exceptions.KeyValueException,
    C.LCB_ERROR_TYPE_QUERY: couchbase_core.exceptions.QueryException,
    C.LCB_ERROR_TYPE_ANALYTICS: couchbase_core.exceptions.AnalyticsException,
    C.LCB_ERROR_TYPE_SEARCH: couchbase_core.exceptions.SearchException,
    C.LCB_ERROR_TYPE_VIEW: couchbase_core.exceptions.ViewException,
    C.LCB_ERROR_TYPE_SDK: couchbase_core.exceptions.SDKException
}

_LCB_SYNCREP_MAP = {
    C.LCB_ERR_DURABILITY_LEVEL_NOT_AVAILABLE: DurabilityInvalidLevelException,
    C.LCB_ERR_DURABILITY_IMPOSSIBLE: DurabilityImpossibleException,
    C.LCB_ERR_DURABLE_WRITE_IN_PROGRESS: DurabilitySyncWriteInProgressException,
    C.LCB_ERR_DURABILITY_AMBIGUOUS: DurabilitySyncWriteAmbiguousException
}

V3Mapping=dict(list({
           C.LCB_ERR_AUTHENTICATION_FAILURE:       AuthenticationException,
           C.LCB_ERR_INVALID_DELTA: couchbase_core.exceptions.DeltaBadvalError,
           C.LCB_ERR_VALUE_TOO_LARGE:            ValueTooBigException,
           C.LCB_ERR_TEMPORARY_FAILURE: couchbase_core.exceptions.BusyError,
           C.LCB_ERR_NO_MEMORY: couchbase_core.exceptions.NoMemoryError,
           C.LCB_ERR_TEMPORARY_FAILURE:         TempFailException,
           C.LCB_ERR_DOCUMENT_EXISTS:      KeyExistsException,
           C.LCB_ERR_DOCUMENT_NOT_FOUND:       KeyNotFoundException,
           C.LCB_ERR_INDEX_NOT_FOUND: QueryException,
           C.LCB_ERR_DLOPEN_FAILED: couchbase_core.exceptions.DlopenFailedError,
           C.LCB_ERR_DLSYM_FAILED: couchbase_core.exceptions.DlsymFailedError,
           C.LCB_ERR_NETWORK:    couchbase_core.exceptions.NetworkError,
           C.LCB_ERR_NOT_MY_VBUCKET:   couchbase_core.exceptions.NotMyVbucketError,
           C.LCB_ERR_NOT_STORED:       couchbase_core.exceptions.NotStoredError,
           C.LCB_ERR_UNSUPPORTED_OPERATION:    couchbase_core.exceptions.NotSupportedError,
           C.LCB_ERR_UNKNOWN_HOST:     couchbase_core.exceptions.UnknownHostError,
           C.LCB_ERR_PROTOCOL_ERROR:   couchbase_core.exceptions.ProtocolError,
           C.LCB_ERR_TIMEOUT:        couchbase_core.exceptions.TimeoutError,
           C.LCB_ERR_CONNECT_ERROR:    couchbase_core.exceptions.ConnectError,
           C.LCB_ERR_BUCKET_NOT_FOUND:    BucketMissingException,
           #C.LCB_EBADHANDLE:       couchbase_core.exceptions.BadHandleError,
           C.LCB_ERR_INVALID_HOST_FORMAT: couchbase_core.exceptions.InvalidError,
           C.LCB_ERR_INVALID_CHAR:     couchbase_core.exceptions.InvalidError,
           C.LCB_ERR_INVALID_ARGUMENT:           couchbase_core.exceptions.InvalidError,
           C.LCB_ERR_DURABILITY_TOO_MANY: couchbase_core.exceptions.ArgumentError,
           C.LCB_ERR_DUPLICATE_COMMANDS: couchbase_core.exceptions.ArgumentError,
           C.LCB_ERR_NO_CONFIGURATION:  couchbase_core.exceptions.ClientTemporaryFailError,
           C.LCB_ERR_HTTP:       couchbase_core.exceptions.HTTPError,
           C.LCB_ERR_SUBDOC_PATH_NOT_FOUND: PathNotFoundException,
           C.LCB_ERR_SUBDOC_PATH_EXISTS: PathExistsException,
           C.LCB_ERR_SUBDOC_PATH_INVALID: couchbase_core.exceptions.SubdocPathInvalidError,
           C.LCB_ERR_SUBDOC_PATH_TOO_DEEP: couchbase_core.exceptions.DocumentTooDeepError,
           C.LCB_ERR_SUBDOC_DOCUMENT_NOT_JSON: couchbase_core.exceptions.DocumentNotJsonError,
           C.LCB_ERR_SUBDOC_VALUE_TOO_DEEP: couchbase_core.exceptions.SubdocValueTooDeepError,
           C.LCB_ERR_SUBDOC_PATH_MISMATCH: couchbase_core.exceptions.SubdocPathMismatchError,
           C.LCB_ERR_SUBDOC_VALUE_INVALID: couchbase_core.exceptions.SubdocCantInsertValueError,
           C.LCB_ERR_SUBDOC_DELTA_INVALID: couchbase_core.exceptions.SubdocBadDeltaError,
           C.LCB_ERR_SUBDOC_NUMBER_TOO_BIG: couchbase_core.exceptions.SubdocNumberTooBigError
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
