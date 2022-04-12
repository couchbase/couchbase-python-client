import json
import re
import sys
from enum import Enum
from typing import (Any,
                    Dict,
                    Optional,
                    Set,
                    Union)

from couchbase.pycbc_core import exception


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
    def __init__(self, **kwargs):
        self._base = kwargs

    @property
    def last_dispatched_to(self) -> Optional[str]:
        return self._base.get("last_dispatched_to", None)

    @property
    def last_dispatched_from(self) -> Optional[str]:
        return self._base.get("last_dispatched_from", None)

    @property
    def retry_attempts(self) -> int:
        return self._base.get("retry_attempts", None)

    @property
    def retry_reasons(self) -> Set[str]:
        return self._base.get("retry_reasons", None)

    @staticmethod
    def from_dict(**kwargs):
        # type: (...) -> ErrorContext
        klass = kwargs.get("context_type", "ErrorContext")
        cl = getattr(sys.modules[__name__], klass)
        return cl(**kwargs)

    def _get_base(self):
        return self._base

    def __repr__(self):
        return f'ErrorContext({self._base})'


class TransactionsErrorContext(ErrorContext):
    _TXN_EC_KEYS = ["failure_type"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.txn_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._TXN_EC_KEYS}

    @property
    def failure_type(self) -> Optional[str]:
        return self.txn_err_ctx.get("failure_type", None)

    @property
    def last_dispatched_to(self) -> Optional[str]:
        return None

    @property
    def last_dispatched_from(self) -> Optional[str]:
        return None

    @property
    def retry_attempts(self) -> int:
        return None

    @property
    def retry_reasons(self) -> Set[str]:
        return None

    def __str__(self):
        return f'TransactionsErrorContext{{{self.txn_err_ctx}}}'


class KeyValueErrorContext(ErrorContext):
    _KV_EC_KEYS = ["key", "bucket_name", "scope_name", "collection_name",
                   "opaque", "status_code", "error_map_info", "extended_error_info"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._kv_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._KV_EC_KEYS}

    @property
    def key(self) -> Optional[str]:
        return self._kv_err_ctx.get("key", None)

    @property
    def bucket_name(self) -> Optional[str]:
        return self._kv_err_ctx.get("bucket_name", None)

    @property
    def scope_name(self) -> Optional[str]:
        return self._kv_err_ctx.get("scope_name", None)

    @property
    def collection_name(self) -> Optional[str]:
        return self._kv_err_ctx.get("collection_name", None)

    def __repr__(self):
        return "KeyValueErrorContext:{}".format(self._kv_err_ctx)


class HTTPErrorContext(ErrorContext):
    _HTTP_EC_KEYS = ["client_context_id", "method", "path", "http_status",
                     "http_body"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._http_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._HTTP_EC_KEYS}

    @property
    def method(self) -> Optional[str]:
        return self._http_err_ctx.get("method", None)

    @property
    def response_code(self) -> Optional[int]:
        return self._http_err_ctx.get("http_status", None)

    @property
    def path(self) -> Optional[str]:
        return self._http_err_ctx.get("path", None)

    @property
    def response_body(self) -> Optional[str]:
        return self._http_err_ctx.get("http_body", None)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._http_err_ctx.get("client_context_id", None)

    def __repr__(self):
        return f'HTTPErrorContext({self._http_err_ctx})'


class QueryErrorContext(HTTPErrorContext):
    _QUERY_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._query_err_ctx = {k: v for k,
                               v in kwargs.items() if k in self._QUERY_EC_KEYS}

    @property
    def first_error_code(self) -> Optional[int]:
        return self._query_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> Optional[str]:
        return self._query_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> Optional[str]:
        return self._query_err_ctx.get("statement", None)

    @property
    def parameters(self) -> Optional[str]:
        return self._query_err_ctx.get("parameters", None)

    def __repr__(self):
        return f'QueryErrorContext({self._get_base()})'


class AnalyticsErrorContext(HTTPErrorContext):
    _ANALYTICS_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._analytics_err_ctx = {k: v for k,
                                   v in kwargs.items() if k in self._ANALYTICS_EC_KEYS}

    @property
    def first_error_code(self) -> Optional[int]:
        return self._analytics_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> Optional[str]:
        return self._analytics_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> Optional[str]:
        return self._analytics_err_ctx.get("statement", None)

    @property
    def parameters(self) -> Optional[str]:
        return self._analytics_err_ctx.get("parameters", None)

    def __repr__(self):
        return f'AnalyticsErrorContext({self._get_base()})'


class SearchErrorContext(HTTPErrorContext):
    _SEARCH_EC_KEYS = ["index_name", "query", "parameters"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._search_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SEARCH_EC_KEYS}

    @property
    def index_name(self) -> Optional[str]:
        return self._search_err_ctx.get("index_name", None)

    @property
    def query(self) -> Optional[str]:
        return self._search_err_ctx.get("query", None)

    @property
    def parameters(self) -> Optional[str]:
        return self._search_err_ctx.get("parameters", None)

    def __repr__(self):
        return f'SearchErrorContext({self._get_base()})'


class ViewErrorContext(HTTPErrorContext):
    _VIEW_EC_KEYS = ["design_document_name", "view_name", "query_string"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._view_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._VIEW_EC_KEYS}

    @property
    def design_document_name(self) -> Optional[str]:
        return self._view_err_ctx.get("design_document_name", None)

    @property
    def view_name(self) -> Optional[str]:
        return self._view_err_ctx.get("view_name", None)

    @property
    def query_string(self) -> Optional[str]:
        return self._view_err_ctx.get("query_string", None)

    def __repr__(self):
        return f'ViewErrorContext({self._get_base()})'


ErrorContextType = Union[ErrorContext,
                         HTTPErrorContext,
                         KeyValueErrorContext,
                         QueryErrorContext,
                         AnalyticsErrorContext,
                         SearchErrorContext,
                         ViewErrorContext]


class PycbcException(Exception):
    def __init__(self,
                 message=None,     # type: Optional[str]
                 error_code=None,  # type: Optional[int]
                 context=None,      # type: Optional[Dict[str, Any]]
                 exc_info=None      # type: Optional[Dict[str, Any]]
                 ):
        self._message = message
        self._error_code = error_code
        self._context = context
        self._exc_info = exc_info
        super().__init__(message)

    @property
    def context(self) -> Optional[Dict[str, Any]]:
        return self._context

    @property
    def exc_info(self) -> Optional[Dict[str, Any]]:
        return self._exc_info

    @property
    def error_code(self) -> Optional[int]:
        return self._error_code

    @property
    def message(self) -> Optional[int]:
        return self._message


class CouchbaseException(Exception):
    def __init__(self,
                 base=None,  # type: Optional[exception]
                 message=None,     # type: Optional[str]
                 context=None,      # type: Optional[ErrorContextType]
                 error_code=None,  # type: Optional[int]
                 exc_info=None      # type: Optional[Dict[str, Any]]
                 ):
        self._base = base
        if isinstance(base, PycbcException):
            self._context = base.context
            self._message = base.message
            self._error_code = base.error_code
            self._exc_info = base.exc_info
        else:
            self._context = context
            self._message = message
            self._error_code = error_code
            self._exc_info = exc_info
        super().__init__(message)

    @property
    def error_code(self) -> Optional[int]:
        if self._error_code:
            return self._error_code
        if self._base:
            return self._base.err()

    @property
    def context(self) -> ErrorContextType:
        """
        ** DEPRECATED ** use error_context

        Returns:
            Union[ErrorContext, HTTPErrorContext, KeyValueErrorContext]: Exception's error context object
        """
        return self.error_context

    @property
    def error_context(self) -> ErrorContextType:
        if not self._context:
            base_ec = self._base.error_context() or dict()
            self._context = ErrorContext.from_dict(**base_ec)
        return self._context

    @classmethod
    def pycbc_create_exception(cls, base=None, message=None):
        return cls(base=base, message=message)

    def __str__(self):
        from couchbase._utils import is_null_or_empty
        details = []
        if self._base and not isinstance(self._base, PycbcException):
            details.append(
                "ec={}, category={}".format(
                    self._base.err(),
                    self._base.err_category()))
            if not is_null_or_empty(self._message):
                details.append("message={}".format(self._message))
            else:
                details.append("message={}".format(self._base.strerror()))
        else:
            if not is_null_or_empty(self._message):
                details.append(f'message={self._message}')
        if self._context:
            details.append(f'context={self._context}')
        if self._exc_info and 'cinfo' in self._exc_info:
            details.append('C Source={0}:{1}'.format(*self._exc_info['cinfo']))
        return "<{}>".format(", ".join(details))


# common errors
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
    pass


class AuthenticationException(CouchbaseException):
    """An authorization failure is returned by the server for given resource and credentials.
    Message
    "An authorization error has occurred"
    Properties
    TBD"""


class CasMismatchException(CouchbaseException):
    pass


CASMismatchException = CasMismatchException


class BucketNotFoundException(CouchbaseException):
    pass


class ValueFormatException(CouchbaseException):
    """Failed to decode or encode value"""


class HTTPException(CouchbaseException):
    """HTTP error"""


class InternalSDKException(CouchbaseException):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should
    not be seeing this message)
    """

    def __init__(self, msg=None, **kwargs):
        if msg:
            kwargs['message'] = msg
        super().__init__(**kwargs)

# kv errors


class DocumentNotFoundException(CouchbaseException):
    pass


class DocumentLockedException(CouchbaseException):
    pass


class DocumentExistsException(CouchbaseException):
    pass


class DurabilityInvalidLevelException(CouchbaseException):
    """Given durability level is invalid"""


class DurabilityImpossibleException(CouchbaseException):
    """Given durability requirements are impossible to achieve"""


class DurabilitySyncWriteInProgressException(CouchbaseException):
    """Returned if an attempt is made to mutate a key which already has a
    SyncWrite pending. Client would typically retry (possibly with backoff).
    Similar to ELOCKED"""


class DurabilitySyncWriteAmbiguousException(CouchbaseException):
    """There is a synchronous mutation pending for given key
    The SyncWrite request has not completed in the specified time and has ambiguous
    result - it may Succeed or Fail; but the final value is not yet known"""


class PathNotFoundException(CouchbaseException):
    pass


class PathExistsException(CouchbaseException):
    pass


class PathMismatchException(CouchbaseException):
    pass


class InvalidValueException(CouchbaseException):
    pass

# Subdocument Exceptions

# @TODO:  How to Deprecate??


SubdocCantInsertValueException = InvalidValueException
SubdocPathMismatchException = PathMismatchException

# Query Exceptions


class ParsingFailedException(CouchbaseException):
    pass

# Search Exceptions


class NoChildrenException(CouchbaseException):
    """
    Compound query is missing children"
    """

    def __init__(self, message='No child queries'):
        super().__init__(message=message)

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

    pass


class DataverseNotFoundException(CouchbaseException):
    """Raised when attempting to drop a dataverse which does not exist"""

    pass


class DatasetNotFoundException(CouchbaseException):
    """Raised when attempting to drop a dataset which does not exist."""

    pass


class DatasetAlreadyExistsException(CouchbaseException):
    """Raised when attempting to create a dataset which already exists"""


class AnalyticsLinkExistsException(CouchbaseException):
    """Raised when attempting to create an analytics link which already exists"""


class AnalyticsLinkNotFoundException(CouchbaseException):
    """Raised when attempting to replace or drop an analytics link that does not exists"""

# Views mgmt


class DesignDocumentNotFoundException(CouchbaseException):
    """"""

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


class FeatureUnavailableException(CouchbaseException):
    pass


class MissingTokenException(CouchbaseException):
    pass

# Ratelimiting


class RateLimitedException(CouchbaseException):
    """
    **UNCOMMITTED**
    Rate limited exceptions are an uncommitted API that is unlikely to change,
    but may still change as final consensus on its behavior has not yet been reached.

    The server decided that the caller must be rate limited due to
    exceeding a rate threshold."""


class QuotaLimitedException(CouchbaseException):
    """
    **UNCOMMITTED**
    Quota limited exceptions are an uncommitted API that is unlikely to change,
    but may still change as final consensus on its behavior has not yet been reached.

    The server decided that the caller must be limited due to exceeding
    a quota threshold."""

# CXX Error Map


CLIENT_ERROR_MAP = dict(
    {
        2: RequestCanceledException,
        3: InvalidArgumentException,
        9: CasMismatchException,
        13: AmbiguousTimeoutException,
        14: UnAmbiguousTimeoutException,
        101: DocumentNotFoundException,
        103: DocumentLockedException,
        105: DocumentExistsException,
        113: PathNotFoundException,
        114: PathMismatchException,
        119: InvalidValueException,
        123: PathExistsException,
    }
)


class ExceptionMap(Enum):
    RequestCanceledException = 2
    InvalidArgumentException = 3
    AuthenticationException = 6
    TemporaryFailException = 7
    ParsingFailedException = 8
    CasMismatchException = 9
    BucketNotFoundException = 10
    AmbiguousTimeoutException = 13
    UnAmbiguousTimeoutException = 14
    QueryIndexAlreadyExistsException = 18
    DocumentNotFoundException = 101
    DocumentLockedException = 103
    DocumentExistsException = 105
    DurabilityInvalidLevelException = 107
    DurabilityImpossibleException = 108
    DurabilitySyncWriteAmbiguousException = 109
    DurabilitySyncWriteInProgressException = 110
    PathNotFoundException = 113
    PathMismatchException = 114
    InvalidValueException = 119
    PathExistsException = 123
    DatasetNotFoundException = 303
    DataverseNotFoundException = 304
    DatasetAlreadyExistsException = 305
    DataverseAlreadyExistsException = 306
    DesignDocumentNotFoundException = 502
    InternalSDKException = 5000
    HTTPException = 5001


PYCBC_ERROR_MAP = {e.value: getattr(sys.modules[__name__], e.name) for e in ExceptionMap}

KV_ERROR_CONTEXT_MAPPING = {'kv_locked': DocumentLockedException,
                            'kv_temporary_failure': TemporaryFailException}


class ErrorMapper:
    @staticmethod
    def _process_mapping(pycbc_excptn,  # type: exception
                         compiled_map,  # type: Dict[str, CouchbaseException]
                         msg  # type: str
                         ) -> Optional[CouchbaseException]:
        matches = None
        for pattern, ex in compiled_map.items():
            try:
                matches = pattern.match(msg)
            except Exception:  # nosec
                pass
            if matches:
                print(f"found match: {ex}")
                return ex(pycbc_excptn)

        return None

    @staticmethod   # noqa: C901
    def _parse_http_response_body(pycbc_excptn,  # type: exception   # noqa: C901
                                  compiled_map,  # type: Dict[str, CouchbaseException]
                                  response_body  # type: str
                                  ) -> Optional[CouchbaseException]:
        err_text = None
        matches = None
        try:
            http_body = json.loads(response_body)
            if isinstance(http_body, str):
                exc = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, http_body)
                if exc is not None:
                    return exc
            elif isinstance(http_body, dict) and http_body.get("errors", None) is not None:
                errors = http_body.get("errors")
                if isinstance(errors, list):
                    for err in errors:
                        err_code = err.get('code', None)
                        err_text = err.get('msg', None)
                        if err_text:
                            if err_code:
                                err_text = f'{err_code} {err_text}'
                            exc = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, err_text)
                            if exc is not None:
                                return exc
                            err_text = None
                else:
                    err_text = errors.get("name", None)
            # eventing function mgmt cases
            elif isinstance(http_body, dict) and http_body.get('name', None) is not None:
                exc = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, http_body.get('name', None))
                if exc is not None:
                    return exc

            # TODO:  This doesn't seem good: re: test_bucket_flush_fail
            #   Currently the regex matches w/ the previous string check in the
            #   http_body, but parsing JSON _might_ be better?  Odd that the
            #   key is "_" though...
            if err_text is None:
                err_text = http_body.get("_", None)
        except json.decoder.JSONDecodeError:
            pass

        for pattern, ex in compiled_map.items():
            try:
                matches = pattern.match(err_text)
            except Exception:  # nosec
                pass
            if matches:
                return ex(pycbc_excptn)

        return None

    @staticmethod
    def _parse_http_context(pycbc_excptn,  # type: exception
                            err_ctx,  # type: HTTPErrorContext
                            mapping=None,  # type: Dict[str, CouchbaseException]
                            excptn_msg=None  # type: str
                            ) -> Optional[CouchbaseException]:
        from couchbase._utils import is_null_or_empty

        compiled_map = {}
        if mapping:
            compiled_map = {{str: re.compile}.get(
                type(k), lambda x: x)(k): v for k, v in mapping.items()}

        if not is_null_or_empty(excptn_msg):
            excptn = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, excptn_msg)
            if excptn is not None:
                return excptn

        if not is_null_or_empty(err_ctx.response_body):
            err_text = err_ctx.response_body
            excptn = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, err_text)
            if excptn is not None:
                return excptn

            excptn = ErrorMapper._parse_http_response_body(pycbc_excptn, compiled_map, err_text)
            if excptn is not None:
                return excptn

        return None

    @staticmethod
    def _parse_kv_context(pycbc_excptn,  # type: exception
                          err_ctx,  # type: HTTPErrorContext
                          mapping,  # type: Dict[str, CouchbaseException]
                          excptn_msg=None  # type: str
                          ) -> Optional[CouchbaseException]:
        from couchbase._utils import is_null_or_empty

        compiled_map = {{str: re.compile}.get(
            type(k), lambda x: x)(k): v for k, v in mapping.items()}

        if not is_null_or_empty(excptn_msg):
            excptn = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, excptn_msg)
            if excptn is not None:
                return excptn

        if err_ctx.retry_reasons is not None:
            for rr in err_ctx.retry_reasons:
                excptn = ErrorMapper._process_mapping(pycbc_excptn, compiled_map, rr)
                if excptn is not None:
                    return excptn

        return None

    @classmethod
    def parse_error_context(cls,
                            pycbc_excptn,  # type: exception
                            mapping=None,  # type: Dict[str, CouchbaseException]
                            excptn_msg=None  # type: str
                            ) -> CouchbaseException:
        excptn = None
        if hasattr(pycbc_excptn, 'context'):
            err_ctx = ErrorContext.from_dict(**pycbc_excptn.context)
        else:
            err_ctx = ErrorContext.from_dict(**pycbc_excptn.error_context())

        if isinstance(err_ctx, HTTPErrorContext):
            excptn = ErrorMapper._parse_http_context(pycbc_excptn, err_ctx, mapping, excptn_msg=excptn_msg)

        if isinstance(err_ctx, KeyValueErrorContext):
            if mapping is None:
                mapping = KV_ERROR_CONTEXT_MAPPING
            # err_ctx = ErrorContext.from_dict(**pycbc_excptn.error_context())
            excptn = ErrorMapper._parse_kv_context(pycbc_excptn, err_ctx, mapping)

        if excptn is not None:
            print("exception found")
            return excptn

        print("exception not found")
        if hasattr(pycbc_excptn, 'error_code'):
            excptn_cls = PYCBC_ERROR_MAP.get(pycbc_excptn.error_code, CouchbaseException)
        else:
            excptn_cls = PYCBC_ERROR_MAP.get(pycbc_excptn.err(), CouchbaseException)
        excptn = excptn_cls(pycbc_excptn, context=err_ctx)
        return excptn
