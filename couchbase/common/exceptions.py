#  Copyright 2016-2026. Couchbase, Inc.
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
    'DocumentNotFoundOnReplicaException',
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
    'ReplicaIndexCurrentlyUnavailableException',
    'ReplicaIndexOutOfBoundsException',
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
    """The category of an error."""

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
    """Details of the request that raised an exception.

    The fields here are on every context, and are ``None`` when the SDK did not record them. A
    subclass adds the fields one service reports.
    """

    def __init__(self, **kwargs: Any) -> None:
        self._base = kwargs

    @property
    def last_dispatched_to(self) -> str | None:
        """The address of the node the request was last sent to."""
        return self._base.get("last_dispatched_to", None)

    @property
    def last_dispatched_from(self) -> str | None:
        """The local address the request was last sent from."""
        return self._base.get("last_dispatched_from", None)

    @property
    def retry_attempts(self) -> int | None:
        """How many times the request was retried."""
        return self._base.get("retry_attempts", None)

    @property
    def retry_reasons(self) -> set[str] | None:
        """Why the request was retried."""
        return self._base.get("retry_reasons", None)

    @property
    def last_exception(self) -> str | None:
        """The last error the request met before it failed."""
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
        return f'{type(self).__name__}({self._base})'


class HTTPErrorContext(ErrorContext):
    """Details of a failed HTTP request to a Couchbase service."""

    _HTTP_EC_KEYS = ["client_context_id", "method", "path", "http_status",
                     "http_body"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._http_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._HTTP_EC_KEYS}

    @property
    def method(self) -> str | None:
        """The HTTP method of the request."""
        return self._http_err_ctx.get("method", None)

    @property
    def response_code(self) -> int | None:
        """The HTTP status code of the response."""
        return self._http_err_ctx.get("http_status", None)

    @property
    def path(self) -> str | None:
        """The path the request was sent to."""
        return self._http_err_ctx.get("path", None)

    @property
    def response_body(self) -> str | None:
        """The body of the response."""
        return self._http_err_ctx.get("http_body", None)

    @property
    def client_context_id(self) -> str | None:
        """The client context ID the request was sent with."""
        return self._http_err_ctx.get("client_context_id", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._http_err_ctx})'


class KeyValueErrorContext(ErrorContext):
    """Details of a failed key-value operation."""

    _KV_EC_KEYS = ['key', 'bucket_name', 'scope_name', 'collection_name',
                   'opaque', 'status_code', 'error_map_info', 'extended_error_info',
                   'retry_attempts', 'retry_reasons']

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._kv_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._KV_EC_KEYS}

    @property
    def key(self) -> str | None:
        """The key of the document the operation targeted."""
        return self._kv_err_ctx.get("key", None)

    @property
    def bucket_name(self) -> str | None:
        """The name of the bucket the document is in."""
        return self._kv_err_ctx.get("bucket_name", None)

    @property
    def scope_name(self) -> str | None:
        """The name of the scope the document is in."""
        return self._kv_err_ctx.get("scope_name", None)

    @property
    def collection_name(self) -> str | None:
        """The name of the collection the document is in."""
        return self._kv_err_ctx.get("collection_name", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._kv_err_ctx})'


class ManagementErrorContext(ErrorContext):
    """Details of a failed management request."""

    _MGMT_EC_KEYS = ["client_context_id", "content", "path", "http_status"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._mgmt_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._MGMT_EC_KEYS}

    @property
    def response_code(self) -> int | None:
        """The HTTP status code of the response."""
        return self._mgmt_err_ctx.get("http_status", None)

    @property
    def path(self) -> str | None:
        """The path the request was sent to."""
        return self._mgmt_err_ctx.get("path", None)

    @property
    def content(self) -> str | None:
        """The body of the response."""
        return self._mgmt_err_ctx.get("content", None)

    @property
    def client_context_id(self) -> str | None:
        """The client context ID the request was sent with."""
        return self._mgmt_err_ctx.get("client_context_id", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._mgmt_err_ctx})'


class TransactionsErrorContext(ErrorContext):
    """Details of a failed transaction."""

    _TXN_EC_KEYS = ["failure_type"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.txn_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._TXN_EC_KEYS}

    @property
    def failure_type(self) -> str | None:
        """What kind of failure ended the transaction."""
        return self.txn_err_ctx.get("failure_type", None)

    @property
    def last_dispatched_to(self) -> str | None:
        """Always ``None``: a transaction is not a single request."""
        return None

    @property
    def last_dispatched_from(self) -> str | None:
        """Always ``None``: a transaction is not a single request."""
        return None

    @property
    def retry_attempts(self) -> int | None:
        """Always ``None``: a transaction is not a single request."""
        return None

    @property
    def retry_reasons(self) -> set[str] | None:
        """Always ``None``: a transaction is not a single request."""
        return None

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.txn_err_ctx})'


class AnalyticsErrorContext(HTTPErrorContext):
    """Details of a failed analytics query."""

    _ANALYTICS_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._analytics_err_ctx = {k: v for k,
                                   v in kwargs.items() if k in self._ANALYTICS_EC_KEYS}

    @property
    def first_error_code(self) -> int | None:
        """The code of the first error the analytics service reported."""
        return self._analytics_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> str | None:
        """The message of the first error the analytics service reported."""
        return self._analytics_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> str | None:
        """The statement that was run."""
        return self._analytics_err_ctx.get("statement", None)

    @property
    def parameters(self) -> str | None:
        """The parameters the statement was run with."""
        return self._analytics_err_ctx.get("parameters", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._get_base()})'


class QueryErrorContext(HTTPErrorContext):
    """Details of a failed SQL++ query."""

    _QUERY_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._query_err_ctx = {k: v for k,
                               v in kwargs.items() if k in self._QUERY_EC_KEYS}

    @property
    def first_error_code(self) -> int | None:
        """The code of the first error the query service reported."""
        return self._query_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> str | None:
        """The message of the first error the query service reported."""
        return self._query_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> str | None:
        """The statement that was run."""
        return self._query_err_ctx.get("statement", None)

    @property
    def parameters(self) -> str | None:
        """The parameters the statement was run with."""
        return self._query_err_ctx.get("parameters", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._get_base()})'


class SearchErrorContext(HTTPErrorContext):
    """Details of a failed search query."""

    _SEARCH_EC_KEYS = ["index_name", "query", "parameters"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._search_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SEARCH_EC_KEYS}

    @property
    def index_name(self) -> str | None:
        """The name of the search index that was queried."""
        return self._search_err_ctx.get("index_name", None)

    @property
    def query(self) -> str | None:
        """The query that was sent."""
        return self._search_err_ctx.get("query", None)

    @property
    def parameters(self) -> str | None:
        """The parameters the query was sent with."""
        return self._search_err_ctx.get("parameters", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._get_base()})'


class ViewErrorContext(HTTPErrorContext):
    """Details of a failed view query."""

    _VIEW_EC_KEYS = ["design_document_name", "view_name", "query_string"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._view_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._VIEW_EC_KEYS}

    @property
    def design_document_name(self) -> str | None:
        """The name of the design document the view is in."""
        return self._view_err_ctx.get("design_document_name", None)

    @property
    def view_name(self) -> str | None:
        """The name of the view that was queried."""
        return self._view_err_ctx.get("view_name", None)

    @property
    def query_string(self) -> str | None:
        """The query string the request was sent with."""
        return self._view_err_ctx.get("query_string", None)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._get_base()})'


class SubdocumentErrorContext(KeyValueErrorContext):
    """Details of a failed sub-document operation."""

    _SUBDOC_EC_KEYS = ["first_error_path", "first_error_index", "deleted"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._subdoc_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SUBDOC_EC_KEYS}

    @property
    def first_error_path(self) -> str | None:
        """The path of the first spec that failed."""
        return self._subdoc_err_ctx.get("first_error_path", None)

    @property
    def first_error_index(self) -> int | None:
        """The index of the first spec that failed."""
        return self._subdoc_err_ctx.get("first_error_index", None)

    @property
    def deleted(self) -> bool:
        """Whether the document the operation targeted is deleted."""
        return self._subdoc_err_ctx.get("deleted", False)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._get_base()})'


class GenericErrorContext(ErrorContext):
    """Details of a failure that belongs to no one service.

    It has no fields beyond the ones every context carries.
    """


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
    """Where an exception came from, beyond its message.

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
    """The base class of the exceptions the SDK raises."""

    def __init__(self,
                 message: str | None = None,
                 *,
                 context: ErrorContextType | None = None,
                 error_code: int | None = None,
                 category: str | None = None,
                 exc_info: _ExceptionInfo | None = None
                 ) -> None:
        # The protocol that raises an exception resolves everything it knows at construction, so an
        # exception holds plain values and no protocol object.
        self._message = message
        self._context = context if context is not None else ErrorContext()
        self._error_code = error_code
        self._category = category
        self._exc_info = exc_info
        super().__init__(message)

    @property
    def error_code(self) -> int | None:
        """The error code behind the exception, if there is one."""
        return self._error_code

    @property
    def context(self) -> ErrorContextType:
        """The exception's error context.

        .. deprecated:: 4.0.0
            Use :attr:`error_context` instead.
        """
        from couchbase.common._supportability import Supportability
        Supportability.class_property_deprecated('CouchbaseException.context', 'CouchbaseException.error_context')
        return self.error_context

    @property
    def error_context(self) -> ErrorContextType:
        """Details of the request that raised the exception."""
        return self._context

    @property
    def message(self) -> str | None:
        """The exception's message, if it has one.

        **VOLATILE** This API is subject to change at any time.
        """
        return self._message or None

    @property
    def inner_cause(self) -> Exception | None:
        """The exception that caused this one, if there was one.

        **VOLATILE** This API is subject to change at any time.
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
    """Raised when a service reports an internal error.

    That is a 5xxx code from the query service, a 25xxx code from the analytics service,
    ``ERR_INTERNAL`` from the key-value service or HTTP 500 from the search service.
    """


class TimeoutException(CouchbaseException):
    """Raised when an operation times out."""


class MissingConnectionException(CouchbaseException):
    """Raised when an operation needs a connection the SDK does not have."""


class AmbiguousTimeoutException(CouchbaseException):
    """Raised when an operation timed out and it cannot be established whether it mutated any data.

    This usually means the request reached the server and the response did not arrive in time.
    A read-only operation never raises this; it raises :class:`UnAmbiguousTimeoutException` instead.
    """


class TemporaryFailException(CouchbaseException):
    """Raised when the server reports a temporary failure, for instance because it is busy.

    Retrying the operation later is likely to succeed.
    """


class UnAmbiguousTimeoutException(CouchbaseException):
    """Raised when an operation timed out and it is certain that it did not mutate any data.

    A read-only operation always raises this rather than :class:`AmbiguousTimeoutException`.  For a
    mutation, it usually means the SDK never sent the request, or the server reported that it failed.
    """


class RequestCanceledException(CouchbaseException):
    """Raised when a request is canceled before its outcome is known.

    This usually means the connection it was sent on closed.
    """


class InvalidArgumentException(CouchbaseException):
    """Raised when an argument has an invalid value or type."""


class AuthenticationException(CouchbaseException):
    """Raised when the cluster cannot authenticate the user."""


class CasMismatchException(CouchbaseException):
    """Raised when the CAS given to an operation does not match the document's.

    The document changed after the CAS was read. Fetch it again and retry the operation.
    """


CASMismatchException = CasMismatchException


class BucketNotFoundException(CouchbaseException):
    """Raised when the bucket a request names does not exist."""


class ValueFormatException(CouchbaseException):
    """Raised when a transcoder cannot encode or decode a value."""


class HTTPException(CouchbaseException):
    """Raised when an HTTP request to a service fails."""


class ServiceUnavailableException(CouchbaseException):
    """Raised when the cluster has no node running the service a request needs.

    For instance, a query against a cluster with no query node, or a view query against a
    memcached bucket.
    """


class FeatureUnavailableException(CouchbaseException):
    """Raised when a feature is used that the cluster's server version does not support."""


class InternalSDKException(CouchbaseException):
    """Raised when the SDK has done something wrong.

    This does not rule out a mistake in the calling code, but the exception should never be seen.
    Contact Couchbase support.
    """


# kv errors


class DeltaInvalidException(CouchbaseException):
    """Raised when the delta given to a counter operation is invalid."""


class DocumentExistsException(CouchbaseException):
    """Raised when an operation expects a document not to exist, and it does."""


class DocumentLockedException(CouchbaseException):
    """Raised when a document cannot be used because it is locked."""


class DocumentNotLockedException(CouchbaseException):
    """Raised when unlocking a document that is not locked."""


class DocumentNotFoundException(CouchbaseException):
    """Raised when the document an operation names does not exist."""


class DocumentNotFoundOnReplicaException(DocumentNotFoundException):
    """Raised when the replica ``get_replica`` read from does not hold the document.

    It is a :class:`DocumentNotFoundException`, so code that handles a missing document handles this too.
    """


class DocumentNotJsonException(CouchbaseException):
    """Raised when a sub-document operation targets a document that is not JSON."""


class CannotReviveLivingDocumentException(CouchbaseException):
    """Raised when reviving a document that is not deleted."""


class DocumentUnretrievableException(CouchbaseException):
    """Raised when a replica read finds no copy of the document."""


class DurabilityImpossibleException(CouchbaseException):
    """Raised when the durability an operation asks for cannot be achieved.

    For instance, the bucket has fewer replicas than the durability level requires.
    """


class DurabilityInvalidLevelException(CouchbaseException):
    """Raised when the server does not accept the durability level an operation asks for."""


class DurabilitySyncWriteAmbiguousException(CouchbaseException):
    """Raised when a durable write did not complete in time and its outcome is unknown.

    It may yet succeed or fail.
    """


class DurabilitySyncWriteInProgressException(CouchbaseException):
    """Raised when mutating a document that has a durable write pending.

    Retrying, possibly with a backoff, usually succeeds.
    """


class DurableWriteReCommitInProgressException(CouchbaseException):
    """Raised when the server is resynchronizing replicas for earlier durable writes.

    This typically follows a rebalance.
    """


class InvalidValueException(CouchbaseException):
    """Raised when a value is not valid for the operation it is given to."""


class NumberTooBigException(CouchbaseException):
    """Raised when a sub-document counter targets a number outside the range arithmetic supports."""


class PathExistsException(CouchbaseException):
    """Raised when a sub-document operation expects a path not to exist, and it does."""


class PathInvalidException(CouchbaseException):
    """Raised when a sub-document path is not syntactically valid."""


class PathMismatchException(CouchbaseException):
    """Raised when a sub-document path assumes the wrong structure for the document.

    For instance, it treats a field as an object when it is an array.
    """


class PathNotFoundException(CouchbaseException):
    """Raised when a sub-document path does not exist in the document."""


class PathTooBigException(CouchbaseException):
    """Raised when a sub-document path is too long or has too many components."""


class RangeScanCompletedException(CouchbaseException):
    """Raised when a range scan has already returned all of its items."""


class ReplicaIndexOutOfBoundsException(CouchbaseException):
    """Raised when ``get_replica`` asks for a replica index the bucket does not have.

    The index is at or beyond the bucket's configured number of replicas, or the bucket has none. It is
    raised without a network round trip, and retrying cannot help. Pass ``wrap=True`` to
    :meth:`~couchbase.replica_reads.GetReplicaStrategy.from_index` to read a different replica instead.
    """


class ReplicaIndexCurrentlyUnavailableException(CouchbaseException):
    """Raised when the replica ``get_replica`` asks for exists but cannot be read now.

    The cluster map does not yet place that replica on a node, typically during a rebalance, which resolves
    it. Pass ``wrap=True`` to :meth:`~couchbase.replica_reads.GetReplicaStrategy.from_index` to read another
    replica instead.
    """


class PathTooDeepException(CouchbaseException):
    """Raised when a sub-document path has too many levels to parse."""


class ValueTooDeepException(CouchbaseException):
    """Raised when inserting a value would make the document too deep for the server to accept."""


class ValueTooLargeException(CouchbaseException):
    """Raised when a value is too large to store, typically over 20MB."""


class XattrCannotModifyVirtualAttributeException(CouchbaseException):
    """Raised when a sub-document operation modifies a virtual attribute."""


class XattrInvalidKeyComboException(CouchbaseException):
    """Raised when one sub-document operation accesses more than one extended attribute."""


class XattrNoAccessException(CouchbaseException):
    """Raised when accessing a system extended attribute without permission.

    A system attribute's name starts with an underscore, and accessing one needs the
    ``SystemXattrRead`` or ``SystemXattrWrite`` privilege.
    """


class XattrUnknownMacroException(CouchbaseException):
    """Raised when a sub-document operation uses a macro the server does not know."""


class XattrUnknownVirtualAttributeException(CouchbaseException):
    """Raised when a sub-document operation accesses an unknown virtual attribute."""


class MutationTokenOutdatedException(CouchbaseException):
    """Raised when a mutation token's partition UUID does not match the server's."""


# @TODO:  How to Deprecate?


SubdocCantInsertValueException = InvalidValueException
SubdocPathMismatchException = PathMismatchException

# Query Exceptions


class ParsingFailedException(CouchbaseException):
    """Raised when a service cannot parse a query."""


class AlreadyQueriedException(CouchbaseException):
    """Raised when iterating the rows of a query result a second time."""

    def __init__(self, message: str = 'Previously iterated over results.', **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class KeyspaceNotFoundException(CouchbaseException):
    """Raised when the bucket or collection a query names does not exist."""


# Search Exceptions


class NoChildrenException(CouchbaseException):
    """Raised when a search query is given none of the IDs, terms or child queries it needs."""

    def __init__(self, message: str = 'No child queries.', **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


# Bucket Mgmt


class BucketAlreadyExistsException(CouchbaseException):
    """Raised when creating a bucket that already exists."""


class BucketDoesNotExistException(CouchbaseException):
    """Raised when a bucket management operation names a bucket that does not exist."""


class BucketNotFlushableException(CouchbaseException):
    """Raised when flushing a bucket that does not have flush enabled."""

# Scope/Collection mgmt


class CollectionAlreadyExistsException(CouchbaseException):
    """Raised when creating a collection that already exists."""


class CollectionNotFoundException(CouchbaseException):
    """Raised when the collection a request names does not exist."""


class UnsupportedOperation(CouchbaseException):
    """Raised when the server does not support an operation."""


class ScopeAlreadyExistsException(CouchbaseException):
    """Raised when creating a scope that already exists."""


class ScopeNotFoundException(CouchbaseException):
    """Raised when the scope a request names does not exist."""

# User mgmt


class GroupNotFoundException(CouchbaseException):
    """Raised when the RBAC group a request names does not exist."""


class UserNotFoundException(CouchbaseException):
    """Raised when the RBAC user a request names does not exist."""


# Query index mgmt


class QueryIndexAlreadyExistsException(CouchbaseException):
    """Raised when creating a query index that already exists."""


class QueryIndexNotFoundException(CouchbaseException):
    """Raised when the query index a request names does not exist."""


class WatchQueryIndexTimeoutException(CouchbaseException):
    """Raised when the watched query indexes are not all online before the timeout."""


# Search Index mgmt


class SearchIndexNotFoundException(CouchbaseException):
    """Raised when the search index a request names does not exist."""

# Analytics mgmt


class DataverseAlreadyExistsException(CouchbaseException):
    """Raised when creating an analytics dataverse that already exists."""


class DataverseNotFoundException(CouchbaseException):
    """Raised when the analytics dataverse a request names does not exist."""


class DatasetNotFoundException(CouchbaseException):
    """Raised when the analytics dataset a request names does not exist."""


class DatasetAlreadyExistsException(CouchbaseException):
    """Raised when creating an analytics dataset that already exists."""


class AnalyticsLinkExistsException(CouchbaseException):
    """Raised when creating an analytics link that already exists."""


class AnalyticsLinkNotFoundException(CouchbaseException):
    """Raised when the analytics link a request names does not exist."""


# Views mgmt


class DesignDocumentNotFoundException(CouchbaseException):
    """Raised when the design document a request names does not exist."""

# Eventing function mgmt


class EventingFunctionNotFoundException(CouchbaseException):
    """Raised when the eventing function a request names does not exist."""


class EventingFunctionCompilationFailureException(CouchbaseException):
    """Raised when an eventing function does not compile."""


class EventingFunctionIdenticalKeyspaceException(CouchbaseException):
    """Raised when an eventing function's source and metadata keyspaces are the same."""


class EventingFunctionNotBootstrappedException(CouchbaseException):
    """Raised when an eventing function is deployed but not fully bootstrapped."""


class EventingFunctionNotDeployedException(CouchbaseException):
    """Raised when an operation needs an eventing function deployed, and it is not."""


class EventingFunctionNotUnDeployedException(CouchbaseException):
    """Raised when an operation needs an eventing function undeployed, and it is not."""


class EventingFunctionAlreadyDeployedException(CouchbaseException):
    """Raised when deploying an eventing function that is already deployed."""


class EventingFunctionCollectionNotFoundException(CouchbaseException):
    """Raised when a collection in an eventing function's keyspace does not exist."""


# python client only errors


class FeatureNotFoundException(CouchbaseException):
    """Raised when a feature is used that the cluster's server version does not support."""


class InvalidIndexException(CouchbaseException):
    """Raised when an index into a sub-document or transaction get-multi result is out of range."""


class MissingTokenException(CouchbaseException):
    """Raised when a :class:`~couchbase.mutation_state.MutationState` is given no mutation token."""


class QueueEmpty(CouchbaseException):
    """Raised when popping from an empty :class:`~couchbase.datastructures.CouchbaseQueue`."""


class UnsuccessfulOperationException(CouchbaseException):
    """Raised when the SDK cannot start an operation.

    For instance, a range scan before the SDK has the cluster's partition map.
    """


# Ratelimiting


class RateLimitedException(CouchbaseException):
    """Raised when the server rate limits the caller for exceeding a rate threshold.

    **UNCOMMITTED** This API is unlikely to change, but may still change as final consensus on
    its behavior has not yet been reached.
    """


class QuotaLimitedException(CouchbaseException):
    """Raised when the server limits the caller for exceeding a quota threshold.

    **UNCOMMITTED** This API is unlikely to change, but may still change as final consensus on
    its behavior has not yet been reached.
    """


# Transaction errors


class TransactionException(CouchbaseException):
    """The base class of the exceptions a transaction raises."""


class TransactionOperationFailed(TransactionException):
    """Raised when an operation in a transaction fails.

    The transaction is rolled back whatever happens next; this exception says why the operation
    failed.
    """


class TransactionFailed(TransactionException):
    """Raised when a transaction failed and was rolled back.

    No actor can see any change the transaction made.
    """


class TransactionExpired(TransactionException):
    """Raised when a transaction did not complete within its timeout, and was rolled back.

    It unambiguously did not reach the commit point, so no actor can see any change it made.
    """


class TransactionCommitAmbiguous(TransactionException):
    """Raised when a transaction expired while committing, so whether it committed is unknown.

    Actors may be able to see the changes it made.

    This is an unavoidable edge case of unreliable networks. A mutation can succeed on the
    server and its response be lost when the connection drops, so the client times out without
    knowing the outcome. The transactions layer works to resolve the ambiguity until the
    transaction expires, and raises this when it cannot. How to handle it depends on the
    application.

    The asynchronous cleanup process completes the transaction: it rolls it back if it did not
    commit, and forwards if it did.
    """


# Field Level Encryption Exceptions


class CryptoException(CouchbaseException):
    """The base class of the field-level encryption exceptions."""

    def __init__(self,
                 message: str | None = 'Generic Cryptography exception',
                 **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class EncryptionFailureException(CryptoException):
    """Raised when a crypto manager fails to encrypt a value."""

    def __init__(self,
                 message: str | None = 'Generic Encryption exception',
                 **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class DecryptionFailureException(CryptoException):
    """Raised when a crypto manager fails to decrypt a value."""

    def __init__(self,
                 message: str | None = 'Generic Decryption exception',
                 **kwargs: Unpack[_ExceptionKwargs]) -> None:
        super().__init__(message, **kwargs)


class CryptoKeyNotFoundException(CryptoException):
    """Raised when a keyring has no key with the requested ID, or an encryption result has no such field."""


class InvalidCryptoKeyException(CryptoException):
    """Raised when an encrypter or decrypter is given a key it cannot use."""


class EncrypterNotFoundException(CryptoException):
    """Raised when a crypto manager has no encrypter under the requested alias."""


class DecrypterNotFoundException(CryptoException):
    """Raised when a crypto manager has no decrypter for an encryption algorithm."""


class EncrypterAlreadyExistsException(CryptoException):
    """Raised when registering an encrypter under an alias that already has one."""


class DecrypterAlreadyExistsException(CryptoException):
    """Raised when registering a decrypter for an algorithm that already has one."""


class InvalidCipherTextException(CryptoException):
    """Raised when a decrypter is given ciphertext it cannot decrypt."""


# An exception is named by `__module__` in the last line of a traceback, in error trackers that
# group by type, and in a pickle.  Every class here states the path users import it from, so that
# name does not depend on which file holds the code.
for _cls in list(globals().values()):
    if isinstance(_cls, type) and _cls.__module__ == __name__:
        _cls.__module__ = 'couchbase.exceptions'
del _cls
