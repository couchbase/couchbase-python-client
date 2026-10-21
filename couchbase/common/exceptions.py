#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

__all__ = [
    'AlreadyQueriedException',
    'AmbiguousTimeoutException',
    'AnalyticsErrorContext',
    'AnalyticsLinkExistsException',
    'AnalyticsLinkNotFoundException',
    'AuthenticationException',
    'BucketAlreadyExistsException',
    'BucketDoesNotExistException',
    'BucketNotFlushableException',
    'BucketNotFoundException',
    'CASMismatchException',
    'CannotReviveLivingDocumentException',
    'CasMismatchException',
    'CollectionAlreadyExistsException',
    'CollectionNotFoundException',
    'CouchbaseErrorCategory',
    'CouchbaseException',
    'CryptoException',
    'CryptoKeyNotFoundException',
    'DatasetAlreadyExistsException',
    'DatasetNotFoundException',
    'DataverseAlreadyExistsException',
    'DataverseNotFoundException',
    'DecrypterAlreadyExistsException',
    'DecrypterNotFoundException',
    'DecryptionFailureException',
    'DeltaInvalidException',
    'DesignDocumentNotFoundException',
    'DocumentExistsException',
    'DocumentLockedException',
    'DocumentNotFoundException',
    'DocumentNotJsonException',
    'DocumentNotLockedException',
    'DocumentUnretrievableException',
    'DurabilityImpossibleException',
    'DurabilityInvalidLevelException',
    'DurabilitySyncWriteAmbiguousException',
    'DurabilitySyncWriteInProgressException',
    'DurableWriteReCommitInProgressException',
    'EncrypterAlreadyExistsException',
    'EncrypterNotFoundException',
    'EncryptionFailureException',
    'ErrorContext',
    'ErrorContextType',
    'EventingFunctionAlreadyDeployedException',
    'EventingFunctionCollectionNotFoundException',
    'EventingFunctionCompilationFailureException',
    'EventingFunctionIdenticalKeyspaceException',
    'EventingFunctionNotBootstrappedException',
    'EventingFunctionNotDeployedException',
    'EventingFunctionNotFoundException',
    'EventingFunctionNotUnDeployedException',
    'FeatureNotFoundException',
    'FeatureUnavailableException',
    'GenericErrorContext',
    'GroupNotFoundException',
    'HTTPErrorContext',
    'HTTPException',
    'InternalSDKException',
    'InternalServerFailureException',
    'InvalidArgumentException',
    'InvalidCipherTextException',
    'InvalidCryptoKeyException',
    'InvalidIndexException',
    'InvalidValueException',
    'KeyValueErrorContext',
    'KeyspaceNotFoundException',
    'ManagementErrorContext',
    'MissingConnectionException',
    'MissingTokenException',
    'MutationTokenOutdatedException',
    'NoChildrenException',
    'NumberTooBigException',
    'ParsingFailedException',
    'PathExistsException',
    'PathInvalidException',
    'PathMismatchException',
    'PathNotFoundException',
    'PathTooBigException',
    'PathTooDeepException',
    'QueryErrorContext',
    'QueryIndexAlreadyExistsException',
    'QueryIndexNotFoundException',
    'QueueEmpty',
    'QuotaLimitedException',
    'RangeScanCompletedException',
    'RateLimitedException',
    'RequestCanceledException',
    'ScopeAlreadyExistsException',
    'ScopeNotFoundException',
    'SearchErrorContext',
    'SearchIndexNotFoundException',
    'ServiceUnavailableException',
    'SubdocCantInsertValueException',
    'SubdocPathMismatchException',
    'SubdocumentErrorContext',
    'TemporaryFailException',
    'TimeoutException',
    'TransactionCommitAmbiguous',
    'TransactionException',
    'TransactionExpired',
    'TransactionFailed',
    'TransactionOperationFailed',
    'TransactionsErrorContext',
    'UnAmbiguousTimeoutException',
    'UnsuccessfulOperationException',
    'UnsupportedOperation',
    'UserNotFoundException',
    'ValueFormatException',
    'ValueTooDeepException',
    'ValueTooLargeException',
    'ViewErrorContext',
    'WatchQueryIndexTimeoutException',
    'XattrCannotModifyVirtualAttributeException',
    'XattrInvalidKeyComboException',
    'XattrNoAccessException',
    'XattrUnknownMacroException',
    'XattrUnknownVirtualAttributeException',
]

from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    TypedDict,
                    get_args)

if TYPE_CHECKING:
    from typing_extensions import Unpack


class CouchbaseErrorCategory(Enum):
    common = "couchbase.common"
    key_value = "couchbase.key_value"
    query = "couchbase.query"
    search = "couchbase.search"
    view = "couchbase.view"
    analytics = "couchbase.analytics"
    management = "couchbase.management"
    network = "couchbase.network"
    other = "other"


class ErrorContext:
    def __init__(self, **kwargs: Any) -> None:
        self._base = kwargs

    @property
    def last_dispatched_to(self) -> str | None:
        return self._base.get("last_dispatched_to", None)

    @property
    def last_dispatched_from(self) -> str | None:
        return self._base.get("last_dispatched_from", None)

    @property
    def retry_attempts(self) -> int | None:
        return self._base.get("retry_attempts", None)

    @property
    def retry_reasons(self) -> set[str] | None:
        return self._base.get("retry_reasons", None)

    @property
    def last_exception(self) -> str | None:
        return self._base.get("last_exception", None)

    @staticmethod
    def from_dict(**kwargs: Any) -> ErrorContext:
        """Build an error context from its fields.

        .. deprecated:: 4.7.0
            Construct the context class directly instead.
        """
        from couchbase.common._supportability import Supportability
        Supportability.method_deprecated('ErrorContext.from_dict', 'the context class directly')
        # A context type this module does not know yet, such as one a newer core sends, still has
        # the fields every context carries, so it degrades to the base class.
        klass = _ERROR_CONTEXT_TYPES.get(kwargs.get("context_type", ""), ErrorContext)
        return klass(**kwargs)

    def _get_base(self) -> dict[str, Any]:
        return self._base

    def __repr__(self) -> str:
        return f'ErrorContext({self._base})'


class HTTPErrorContext(ErrorContext):
    _HTTP_EC_KEYS = ["client_context_id", "method", "path", "http_status",
                     "http_body"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._http_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._HTTP_EC_KEYS}

    @property
    def method(self) -> str | None:
        return self._http_err_ctx.get("method", None)

    @property
    def response_code(self) -> int | None:
        return self._http_err_ctx.get("http_status", None)

    @property
    def path(self) -> str | None:
        return self._http_err_ctx.get("path", None)

    @property
    def response_body(self) -> str | None:
        return self._http_err_ctx.get("http_body", None)

    @property
    def client_context_id(self) -> str | None:
        return self._http_err_ctx.get("client_context_id", None)

    def __repr__(self) -> str:
        return f'HTTPErrorContext({self._http_err_ctx})'


class KeyValueErrorContext(ErrorContext):
    _KV_EC_KEYS = ['key', 'bucket_name', 'scope_name', 'collection_name',
                   'opaque', 'status_code', 'error_map_info', 'extended_error_info',
                   'retry_attempts', 'retry_reasons']

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._kv_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._KV_EC_KEYS}

    @property
    def key(self) -> str | None:
        return self._kv_err_ctx.get("key", None)

    @property
    def bucket_name(self) -> str | None:
        return self._kv_err_ctx.get("bucket_name", None)

    @property
    def scope_name(self) -> str | None:
        return self._kv_err_ctx.get("scope_name", None)

    @property
    def collection_name(self) -> str | None:
        return self._kv_err_ctx.get("collection_name", None)

    def __repr__(self) -> str:
        return "KeyValueErrorContext:{}".format(self._kv_err_ctx)


class ManagementErrorContext(ErrorContext):
    _MGMT_EC_KEYS = ["client_context_id", "content", "path", "http_status"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._mgmt_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._MGMT_EC_KEYS}

    @property
    def response_code(self) -> int | None:
        return self._mgmt_err_ctx.get("http_status", None)

    @property
    def path(self) -> str | None:
        return self._mgmt_err_ctx.get("path", None)

    @property
    def content(self) -> str | None:
        return self._mgmt_err_ctx.get("content", None)

    @property
    def client_context_id(self) -> str | None:
        return self._mgmt_err_ctx.get("client_context_id", None)

    def __repr__(self) -> str:
        return f'ManagementErrorContext({self._mgmt_err_ctx})'


class TransactionsErrorContext(ErrorContext):
    _TXN_EC_KEYS = ["failure_type"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.txn_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._TXN_EC_KEYS}

    @property
    def failure_type(self) -> str | None:
        return self.txn_err_ctx.get("failure_type", None)

    @property
    def last_dispatched_to(self) -> str | None:
        return None

    @property
    def last_dispatched_from(self) -> str | None:
        return None

    @property
    def retry_attempts(self) -> int | None:
        return None

    @property
    def retry_reasons(self) -> set[str] | None:
        return None

    def __str__(self) -> str:
        return f'TransactionsErrorContext{{{self.txn_err_ctx}}}'


class AnalyticsErrorContext(HTTPErrorContext):
    _ANALYTICS_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._analytics_err_ctx = {k: v for k,
                                   v in kwargs.items() if k in self._ANALYTICS_EC_KEYS}

    @property
    def first_error_code(self) -> int | None:
        return self._analytics_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> str | None:
        return self._analytics_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> str | None:
        return self._analytics_err_ctx.get("statement", None)

    @property
    def parameters(self) -> str | None:
        return self._analytics_err_ctx.get("parameters", None)

    def __repr__(self) -> str:
        return f'AnalyticsErrorContext({self._get_base()})'


class QueryErrorContext(HTTPErrorContext):
    _QUERY_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._query_err_ctx = {k: v for k,
                               v in kwargs.items() if k in self._QUERY_EC_KEYS}

    @property
    def first_error_code(self) -> int | None:
        return self._query_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> str | None:
        return self._query_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> str | None:
        return self._query_err_ctx.get("statement", None)

    @property
    def parameters(self) -> str | None:
        return self._query_err_ctx.get("parameters", None)

    def __repr__(self) -> str:
        return f'QueryErrorContext({self._get_base()})'


class SearchErrorContext(HTTPErrorContext):
    _SEARCH_EC_KEYS = ["index_name", "query", "parameters"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._search_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SEARCH_EC_KEYS}

    @property
    def index_name(self) -> str | None:
        return self._search_err_ctx.get("index_name", None)

    @property
    def query(self) -> str | None:
        return self._search_err_ctx.get("query", None)

    @property
    def parameters(self) -> str | None:
        return self._search_err_ctx.get("parameters", None)

    def __repr__(self) -> str:
        return f'SearchErrorContext({self._get_base()})'


class ViewErrorContext(HTTPErrorContext):
    _VIEW_EC_KEYS = ["design_document_name", "view_name", "query_string"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._view_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._VIEW_EC_KEYS}

    @property
    def design_document_name(self) -> str | None:
        return self._view_err_ctx.get("design_document_name", None)

    @property
    def view_name(self) -> str | None:
        return self._view_err_ctx.get("view_name", None)

    @property
    def query_string(self) -> str | None:
        return self._view_err_ctx.get("query_string", None)

    def __repr__(self) -> str:
        return f'ViewErrorContext({self._get_base()})'


class SubdocumentErrorContext(KeyValueErrorContext):
    _SUBDOC_EC_KEYS = ["first_error_path", "first_error_index", "deleted"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._subdoc_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SUBDOC_EC_KEYS}

    @property
    def first_error_path(self) -> str | None:
        return self._subdoc_err_ctx.get("first_error_path", None)

    @property
    def first_error_index(self) -> int | None:
        return self._subdoc_err_ctx.get("first_error_index", None)

    @property
    def deleted(self) -> bool:
        return self._subdoc_err_ctx.get("deleted", False)

    def __repr__(self) -> str:
        return f'SubdocumentErrorContext({self._get_base()})'


class GenericErrorContext(ErrorContext):
    """
    A context for a failure that belongs to no one service, with no fields beyond the ones every
    context carries.
    """

    def __repr__(self) -> str:
        return f'GenericErrorContext({self._get_base()})'


ErrorContextType = (AnalyticsErrorContext
                    | ErrorContext
                    | GenericErrorContext
                    | HTTPErrorContext
                    | KeyValueErrorContext
                    | ManagementErrorContext
                    | QueryErrorContext
                    | SearchErrorContext
                    | SubdocumentErrorContext
                    | TransactionsErrorContext
                    | ViewErrorContext)

_ERROR_CONTEXT_TYPES: dict[str, type[ErrorContext]] = {klass.__name__: klass for klass in get_args(ErrorContextType)}


class _ExceptionInfo(TypedDict, total=False):
    """
    ``cinfo`` and ``message`` are what the classic binding writes, and ``inner_cause`` is the SDK's
    own. ``error_message`` is the server's explanation, which the error mapping reads; the binding
    does not write it at present.
    """
    cinfo: tuple[str, int]
    message: str
    error_message: str
    inner_cause: Exception


class _ExceptionKwargs(TypedDict, total=False):
    """The keyword arguments :class:`CouchbaseException` takes after ``message``."""
    context: ErrorContextType | None
    error_code: int | None
    category: str | None
    exc_info: _ExceptionInfo | None


class CouchbaseException(Exception):
    def __init__(self,
                 message: str | None = None,
                 *,
                 context: ErrorContextType | None = None,
                 error_code: int | None = None,
                 category: str | None = None,
                 exc_info: _ExceptionInfo | None = None
                 ) -> None:
        """
        The protocol that raises an exception resolves everything it knows at construction, so an
        exception holds plain values and no protocol object.
        """
        self._message = message
        self._context = context if context is not None else ErrorContext()
        self._error_code = error_code
        self._category = category
        self._exc_info = exc_info
        super().__init__(message)

    @property
    def error_code(self) -> int | None:
        return self._error_code

    @property
    def context(self) -> ErrorContextType:
        """The exception's error context.

        .. deprecated:: 4.0.0
            Use :attr:`error_context` instead.

        Returns:
            ErrorContext | HTTPErrorContext | KeyValueErrorContext: Exception's error context object
        """
        from couchbase.common._supportability import Supportability
        Supportability.class_property_deprecated('CouchbaseException.context', 'CouchbaseException.error_context')
        return self.error_context

    @property
    def error_context(self) -> ErrorContextType:
        return self._context

    @property
    def message(self) -> str | None:
        """
        **VOLATILE** This API is subject to change at any time.

        Returns:
            str | None: Exception's error message, if it exists.
        """
        return self._message or None

    @property
    def inner_cause(self) -> Exception | None:
        """
        **VOLATILE** This API is subject to change at any time.

        Returns:
            Exception | None: Exception's inner cause, if it exists.
        """
        if not self._exc_info:
            return None
        return self._exc_info.get('inner_cause', None)

    def __repr__(self) -> str:
        from couchbase.common._utils import is_null_or_empty
        details = []
        if self._error_code is not None:
            details.append(f'ec={self._error_code}, category={self._category}')
        if not is_null_or_empty(self._message):
            details.append(f'message={self._message}')
        if self._context._get_base():
            details.append(f'context={self._context}')
        if self._exc_info and 'cinfo' in self._exc_info:
            details.append('C Source={0}:{1}'.format(*self._exc_info['cinfo']))
        if self._exc_info and 'inner_cause' in self._exc_info:
            details.append('Inner cause={0}'.format(self._exc_info['inner_cause']))
        rendered = f'<{", ".join(details)}>'
        if type(self) is CouchbaseException:
            return rendered
        return f'{type(self).__name__}({rendered})'

    def __str__(self) -> str:
        return self.__repr__()


# common errors
class InternalServerFailureException(CouchbaseException):
    """ Raised when the server service provides error w/in below ranges.
        Query: Error range 5xxx
        Analytics: Error range 25xxx
        KV: error code ERR_INTERNAL (0x84)
        Search: HTTP 500
    """


class TimeoutException(CouchbaseException):
    pass


class MissingConnectionException(CouchbaseException):
    pass


class AmbiguousTimeoutException(CouchbaseException):
    pass


class TemporaryFailException(CouchbaseException):
    pass


class UnAmbiguousTimeoutException(CouchbaseException):
    pass


class RequestCanceledException(CouchbaseException):
    pass


class InvalidArgumentException(CouchbaseException):
    """ Raised when a provided argmument has an invalid value
        and/or invalid type.
    """


class AuthenticationException(CouchbaseException):
    """Indicates that an error occurred authenticating the user to the cluster."""


class CasMismatchException(CouchbaseException):
    pass


CASMismatchException = CasMismatchException


class BucketNotFoundException(CouchbaseException):
    """Indicates that the bucket being referenced does not exist."""


class ValueFormatException(CouchbaseException):
    """Failed to decode or encode value"""


class HTTPException(CouchbaseException):
    pass


class ServiceUnavailableException(CouchbaseException):
    """ Raised if tt can be determined from the config unambiguously that a
        given service is not available.
        I.e. no query node in the config, or a memcached bucket is accessed
        and views or n1ql queries should be performed
    """


class FeatureUnavailableException(CouchbaseException):
    """Raised when feature that is not available with the current server version is used."""


class InternalSDKException(CouchbaseException):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should
    not be seeing this message)
    """


# kv errors


class DeltaInvalidException(CouchbaseException):
    """Indicates the delta value specified for an operation is too large."""


class DocumentExistsException(CouchbaseException):
    """Indicates that the referenced document exists already, but the operation was not expecting it to exist."""


class DocumentLockedException(CouchbaseException):
    """Indicates that the referenced document could not be used as it is currently locked,
    likely by another actor in the system."""


class DocumentNotLockedException(CouchbaseException):
    """Indicates that the referenced document is not locked, generally raised when an
    unlock operation is being performed."""


class DocumentNotFoundException(CouchbaseException):
    """Indicates that the referenced document does not exist."""


class DocumentNotJsonException(CouchbaseException):
    """Indicates a sub-document operation was attempted on a non-JSON document."""


class CannotReviveLivingDocumentException(CouchbaseException):
    """Indicates that a revive was attempted on a document that is not deleted."""


class DocumentUnretrievableException(CouchbaseException):
    """Indicates that the referenced document does not exist and therefore no replicas are found."""


class DurabilityImpossibleException(CouchbaseException):
    """Given durability requirements are impossible to achieve"""


class DurabilityInvalidLevelException(CouchbaseException):
    """Given durability level is invalid"""


class DurabilitySyncWriteAmbiguousException(CouchbaseException):
    """There is a synchronous mutation pending for given key
    The SyncWrite request has not completed in the specified time and has ambiguous
    result - it may Succeed or Fail; but the final value is not yet known"""


class DurabilitySyncWriteInProgressException(CouchbaseException):
    """Returned if an attempt is made to mutate a key which already has a
    SyncWrite pending. Client would typically retry (possibly with backoff).
    Similar to ELOCKED"""


class DurableWriteReCommitInProgressException(CouchbaseException):
    """Indicates that the server is resynchronizing replicas for earlier durable operations,
    typically after a rebalance."""


class InvalidValueException(CouchbaseException):
    """Indicates the provided value was invalid for the operation."""


class NumberTooBigException(CouchbaseException):
    """Indicates existing number is outside the valid range for arithmetic operations."""


class PathExistsException(CouchbaseException):
    """Indicates that the reference path already existed, but the operation expected that it did not."""


class PathInvalidException(CouchbaseException):
    """Indicates that the reference path was not syntactically correct."""


class PathMismatchException(CouchbaseException):
    """Indicates that the referenced path made incorrect assumptions about the structure of a document,
    for instance attempting to access a field as an object when in fact it is an array."""


class PathNotFoundException(CouchbaseException):
    """Indicates that the reference path was not found."""


class PathTooBigException(CouchbaseException):
    """Indicates that the reference path is too long, or contains too many independent components."""


class RangeScanCompletedException(CouchbaseException):
    """Indicates the range scan has completed."""


class PathTooDeepException(CouchbaseException):
    """Indicates that the reference path contains too many levels to parse."""


class ValueTooDeepException(CouchbaseException):
    """
        Indicates that the value provided, if inserted into the document,
        would cause the document to become too deep for the server to accept.
    """


class ValueTooLargeException(CouchbaseException):
    """Indicates that the value sent was too large to store, typically over 20MB."""


class XattrCannotModifyVirtualAttributeException(CouchbaseException):
    """Indicates that a sub-document operation attempted to modify a virtual attribute."""


class XattrInvalidKeyComboException(CouchbaseException):
    """Indicates that a sub-document operation attempted to access multiple extended attributes in one
    operation."""


class XattrNoAccessException(CouchbaseException):
    """Indicates that there is no permission to access a system attribute, whose name starts with an
    underscore, without SystemXattrRead or SystemXattrWrite."""


class XattrUnknownMacroException(CouchbaseException):
    """Indicates that a macro was used which the server did not understand."""


class XattrUnknownVirtualAttributeException(CouchbaseException):
    """Indicates that a sub-document operation attempted to access an unknown virtual attribute."""


class MutationTokenOutdatedException(CouchbaseException):
    """Indicates that the vbucket uuid requirements do not align with the server."""


# @TODO:  How to Deprecate?


SubdocCantInsertValueException = InvalidValueException
SubdocPathMismatchException = PathMismatchException

# Query Exceptions


class ParsingFailedException(CouchbaseException):
    """
    Raised when the query service is unable to parse a N1QL query
    """


class AlreadyQueriedException(CouchbaseException):
    """
    Raised when query (N1QL, Search, Analytics or Views) results
    have already been iterated over.
    """

    def __init__(self, message: str = 'Previously iterated over results.', **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class KeyspaceNotFoundException(CouchbaseException):
    """Keyspace not found (collection or bucket does not exist)"""


# Search Exceptions


class NoChildrenException(CouchbaseException):
    """
    Compound query is missing children"
    """

    def __init__(self, message: str = 'No child queries.', **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


# Bucket Mgmt


class BucketAlreadyExistsException(CouchbaseException):
    pass


class BucketDoesNotExistException(CouchbaseException):
    pass


class BucketNotFlushableException(CouchbaseException):
    pass

# Scope/Collection mgmt


class CollectionAlreadyExistsException(CouchbaseException):
    pass


class CollectionNotFoundException(CouchbaseException):
    pass


class UnsupportedOperation(CouchbaseException):
    pass


class ScopeAlreadyExistsException(CouchbaseException):
    pass


class ScopeNotFoundException(CouchbaseException):
    pass

# User mgmt


class GroupNotFoundException(CouchbaseException):
    """ The RBAC Group was not found"""


class UserNotFoundException(CouchbaseException):
    """ The RBAC User was not found"""


# Query index mgmt


class QueryIndexAlreadyExistsException(CouchbaseException):
    """ The query index already exists"""


class QueryIndexNotFoundException(CouchbaseException):
    """ The query index was not found"""


class WatchQueryIndexTimeoutException(CouchbaseException):
    """Unable to find all requested indexes online within specified timeout"""


# Search Index mgmt


class SearchIndexNotFoundException(CouchbaseException):
    pass

# Analytics mgmt


class DataverseAlreadyExistsException(CouchbaseException):
    """Raised when attempting to create dataverse when it already exists"""


class DataverseNotFoundException(CouchbaseException):
    """Raised when attempting to drop a dataverse which does not exist"""


class DatasetNotFoundException(CouchbaseException):
    """Raised when attempting to drop a dataset which does not exist."""


class DatasetAlreadyExistsException(CouchbaseException):
    """Raised when attempting to create a dataset which already exists"""


class AnalyticsLinkExistsException(CouchbaseException):
    """Raised when attempting to create an analytics link which already exists"""


class AnalyticsLinkNotFoundException(CouchbaseException):
    """Raised when attempting to replace or drop an analytics link that does not exists"""


# Views mgmt


class DesignDocumentNotFoundException(CouchbaseException):
    pass

# Eventing function mgmt


class EventingFunctionNotFoundException(CouchbaseException):
    """Raised when an eventing function is not found"""


class EventingFunctionCompilationFailureException(CouchbaseException):
    """Raised when compilation of an eventing function failed"""


class EventingFunctionIdenticalKeyspaceException(CouchbaseException):
    """Raised when the source and metadata keyspaces are the same"""


class EventingFunctionNotBootstrappedException(CouchbaseException):
    """Raised when an eventing function is deployed but not “fully” bootstrapped"""


class EventingFunctionNotDeployedException(CouchbaseException):
    """Raised when an eventing function is not deployed, but the action expects it to be deployed"""


class EventingFunctionNotUnDeployedException(CouchbaseException):
    """Raised when an eventing function is deployed, but the action expects it to be undeployed"""


class EventingFunctionAlreadyDeployedException(CouchbaseException):
    """Raised when an eventing function has already been deployed"""


class EventingFunctionCollectionNotFoundException(CouchbaseException):
    """Raised when collection in specified keyspace is not found"""


# python client only errors


class FeatureNotFoundException(CouchbaseException):
    """Thrown when feature is not supported by server version."""


class InvalidIndexException(CouchbaseException):
    pass


class MissingTokenException(CouchbaseException):
    pass


class QueueEmpty(CouchbaseException):
    """
    Thrown if a datastructure queue is empty
    """


class UnsuccessfulOperationException(CouchbaseException):
    """Thrown when a specific pycbc_core operation is unsuccessful."""


# Ratelimiting


class RateLimitedException(CouchbaseException):
    """
    Rate limited exceptions are an uncommitted API that is unlikely to change,
    but may still change as final consensus on its behavior has not yet been reached.

    The server decided that the caller must be rate limited due to
    exceeding a rate threshold."""


class QuotaLimitedException(CouchbaseException):
    """
    Quota limited exceptions are an uncommitted API that is unlikely to change,
    but may still change as final consensus on its behavior has not yet been reached.

    The server decided that the caller must be limited due to exceeding
    a quota threshold."""


# Transaction errors


class TransactionException(CouchbaseException):
    """
    Base class for any transaction-related exception
    """


class TransactionOperationFailed(TransactionException):
    """
    Indicates a transaction operation failed.

    The transaction will be rolled back no matter what, but this error can give some context as to why it failed.
    """


class TransactionFailed(TransactionException):
    """
    The transaction failed and was rolled back.

     No actors can see any changes made by this transaction.
     """


class TransactionExpired(TransactionException):
    """
    The transaction could not be fully completed in the configured timeout.

    It is in an undefined state, but it unambiguously did not reach the commit point.  No actors will be able to see the
    contents of this transaction.The transaction exceeded the expiry set in the TransactionConfig, and was rolled back.
    """


class TransactionCommitAmbiguous(TransactionException):
    """
    The transaction expired at the point of trying to commit it.  It is ambiguous whether the transaction has committed
    or not.  Actors may be able to see the content of this transaction.

    This error is result of inevitable and unavoidable edge cases when working with unreliable networks.  For example,
    consider an ordinary mutation being made over the network to any database.  The mutation could succeed on the
    database-side, and then just before the result is returned to the client, the network connection drops.  The client
    cannot receive the success result and will timeout - it is ambiguous to it whether the mutation succeeded or not.

    The transactions layer will work to resolve the ambiguity up until the transaction expires, but if unable to resolve
    it in that time, it is forced to raise this error.  The transaction may or may not have been successful, and
    error-handling of this is highly application-dependent.

    The asynchronous cleanup process will try to complete the transaction: roll it back if it didn't commit, roll it
    forwards if it did.
    """


# Field Level Encryption Exceptions


class CryptoException(CouchbaseException):
    def __init__(self,
                 message: str | None = 'Generic Cryptography exception',
                 **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class EncryptionFailureException(CryptoException):
    def __init__(self,
                 message: str | None = 'Generic Encryption exception',
                 **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class DecryptionFailureException(CryptoException):
    def __init__(self,
                 message: str | None = 'Generic Decryption exception',
                 **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class CryptoKeyNotFoundException(CryptoException):
    pass


class InvalidCryptoKeyException(CryptoException):
    pass


class EncrypterNotFoundException(CryptoException):
    pass


class DecrypterNotFoundException(CryptoException):
    pass


class EncrypterAlreadyExistsException(CryptoException):
    pass


class DecrypterAlreadyExistsException(CryptoException):
    pass


class InvalidCipherTextException(CryptoException):
    pass


# An exception is named by `__module__` in the last line of a traceback, in error trackers that
# group by type, and in a pickle.  Every class here states the path users import it from, so that
# name does not depend on which file holds the code.
for _cls in list(globals().values()):
    if isinstance(_cls, type) and _cls.__module__ == __name__:
        _cls.__module__ = 'couchbase.exceptions'
del _cls
