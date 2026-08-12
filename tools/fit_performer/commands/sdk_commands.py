from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (Any,
                    Callable,
                    Dict,
                    Iterator,
                    Union)

from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  AuthenticationException,
                                  BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  BucketNotFlushableException,
                                  BucketNotFoundException,
                                  CasMismatchException,
                                  CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  CouchbaseException,
                                  DeltaInvalidException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentNotJsonException,
                                  DocumentNotLockedException,
                                  DocumentUnretrievableException,
                                  DurabilityImpossibleException,
                                  DurabilityInvalidLevelException,
                                  DurabilitySyncWriteAmbiguousException,
                                  DurabilitySyncWriteInProgressException,
                                  ErrorContext,
                                  FeatureUnavailableException,
                                  GroupNotFoundException,
                                  InternalServerFailureException,
                                  InvalidArgumentException,
                                  InvalidValueException,
                                  NumberTooBigException,
                                  ParsingFailedException,
                                  PathExistsException,
                                  PathInvalidException,
                                  PathMismatchException,
                                  PathNotFoundException,
                                  PathTooBigException,
                                  PathTooDeepException,
                                  QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException,
                                  QuotaLimitedException,
                                  RateLimitedException,
                                  RequestCanceledException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException,
                                  SearchIndexNotFoundException,
                                  ServiceUnavailableException,
                                  TemporaryFailException,
                                  TimeoutException,
                                  UnAmbiguousTimeoutException,
                                  UnsupportedOperation,
                                  UserNotFoundException,
                                  ValueTooDeepException,
                                  WatchQueryIndexTimeoutException)
from couchbase.mutation_state import MutationState, MutationToken
from couchbase.transcoder import (JSONTranscoder,
                                  LegacyTranscoder,
                                  RawBinaryTranscoder,
                                  RawJSONTranscoder,
                                  RawStringTranscoder)

from ..generated.run import top_level_pb2 as run_pb
from ..generated.sdk import workload_pb2 as sdk_pb
from ..generated.shared import basic_pb2 as basic_pb
from ..generated.shared import content_pb2 as content_pb
from ..generated.shared import exceptions_pb2 as exceptions_pb

logger = logging.getLogger(__name__)

DURABILITY_LEVEL_MAP = {
    basic_pb.Durability.NONE: DurabilityLevel.NONE,
    basic_pb.Durability.MAJORITY: DurabilityLevel.MAJORITY,
    basic_pb.Durability.MAJORITY_AND_PERSIST_TO_ACTIVE: DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
    basic_pb.Durability.PERSIST_TO_MAJORITY: DurabilityLevel.PERSIST_TO_MAJORITY
}

TRANSCODER_MAP = {
    'legacy': LegacyTranscoder(),
    'json': JSONTranscoder(),
    'raw_json': RawJSONTranscoder(),
    'raw_string': RawStringTranscoder(),
    'raw_binary': RawBinaryTranscoder()
}


class SdkCommand(ABC):

    @staticmethod
    @abstractmethod
    def create_command(**kwargs) -> SdkCommand:
        """ Create, build and return SdkCommand instance.

        Forcing kwargs with the create command.  This format helps the factory more easily pass in options.

        Args:
            kwargs (Dict[str, Any]): Options that are necessary to build the command.

        Returns:
            SdkCommand: SdkCommand instance.

        """
        raise NotImplementedError('create_command method must be implemented by concrete class.')

    @abstractmethod
    def execute_command(self) -> Union[run_pb.Result, Iterator[Union[run_pb.Result, exceptions_pb.Exception]]]:
        """ Execute SdkCommand.

        Returns:
            Union[run_pb.Result, Iterator[Union[run_pb.Result, exceptions_pb.Exception]]]: gRPC result or a gRPC result
                iterator.
        """
        raise NotImplementedError('execute_command method must be implemented by concrete class.')

    @abstractmethod
    def set_options(self):
        """ Sets SdkCommand options.
        """
        raise NotImplementedError('set_options method must be implemented by concrete class.')

    @property
    def stream_type(self):
        return None

    @property
    def stream_config(self):
        return None


class SdkCommandOptions:
    @staticmethod
    def get_simple_option(options, option_name):
        if not options.HasField(option_name):
            return None

        return getattr(options, option_name)

    @staticmethod
    def resolve_parent_span(options, span_owner):
        if options is None or span_owner is None:
            return None
        if not options.HasField('parent_span_id'):
            return None
        span = span_owner.get_span(options.parent_span_id)
        if span is None:
            raise ValueError(f"Parent span '{options.parent_span_id}' not found")
        return span

    @staticmethod
    def get_timeout(options):
        try:
            if not options.HasField("timeout_msecs"):
                return None

            return timedelta(milliseconds=options.timeout_msecs)
        except ValueError:  # The timeout field is sometimes called 'timeout_millis' instead of 'timeout_msecs'
            if not options.HasField("timeout_millis"):
                return None

            return timedelta(milliseconds=options.timeout_millis)

    @staticmethod
    def get_transcoder(options):
        if not options.HasField("transcoder"):
            return None

        return TRANSCODER_MAP.get(options.transcoder.WhichOneof("transcoder"), None)

    @staticmethod
    def get_consistent_with(options):
        if not options.HasField('consistent_with'):
            return None

        state = MutationState()
        for t in options.consistent_with.tokens:
            token = MutationToken({
                'partition_id': t.partition_id,
                'partition_uuid': t.partition_uuid,
                'sequence_number': t.sequence_number,
                'bucket_name': t.bucket_name,
            })
            state.add_mutation_token(token)
        return state

    @staticmethod
    def get_preserve_expiry(options):
        return SdkCommandOptions.get_simple_option(options, "preserve_expiry")


class SdkErrorContextEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ErrorContext):
            return obj._base or dict()
        elif isinstance(obj, set):
            return list(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class SdkCommandResult:
    ERROR_MAP = {
        CouchbaseException: exceptions_pb.SDK_COUCHBASE_EXCEPTION,
        TimeoutException: exceptions_pb.SDK_TIMEOUT_EXCEPTION,
        RequestCanceledException: exceptions_pb.SDK_REQUEST_CANCELLED_EXCEPTION,
        InvalidArgumentException: exceptions_pb.SDK_INVALID_ARGUMENT_EXCEPTION,
        ServiceUnavailableException: exceptions_pb.SDK_SERVICE_NOT_AVAILABLE_EXCEPTION,
        InternalServerFailureException: exceptions_pb.SDK_INTERNAL_SERVER_FAILURE_EXCEPTION,
        AuthenticationException: exceptions_pb.SDK_AUTHENTICATION_FAILURE_EXCEPTION,
        TemporaryFailException: exceptions_pb.SDK_TEMPORARY_FAILURE_EXCEPTION,
        ParsingFailedException: exceptions_pb.SDK_PARSING_FAILURE_EXCEPTION,
        CasMismatchException: exceptions_pb.SDK_CAS_MISMATCH_EXCEPTION,
        BucketDoesNotExistException: exceptions_pb.SDK_BUCKET_NOT_FOUND_EXCEPTION,
        BucketNotFoundException: exceptions_pb.SDK_BUCKET_NOT_FOUND_EXCEPTION,
        CollectionNotFoundException: exceptions_pb.SDK_COLLECTION_NOT_FOUND_EXCEPTION,
        AmbiguousTimeoutException: exceptions_pb.SDK_AMBIGUOUS_TIMEOUT_EXCEPTION,
        UnAmbiguousTimeoutException: exceptions_pb.SDK_UNAMBIGUOUS_TIMEOUT_EXCEPTION,
        FeatureUnavailableException: exceptions_pb.SDK_FEATURE_NOT_AVAILABLE_EXCEPTION,
        ScopeNotFoundException: exceptions_pb.SDK_SCOPE_NOT_FOUND_EXCEPTION,
        QueryIndexNotFoundException: exceptions_pb.SDK_INDEX_NOT_FOUND_EXCEPTION,
        SearchIndexNotFoundException: exceptions_pb.SDK_INDEX_NOT_FOUND_EXCEPTION,
        QueryIndexAlreadyExistsException: exceptions_pb.SDK_INDEX_EXISTS_EXCEPTION,
        WatchQueryIndexTimeoutException: exceptions_pb.SDK_TIMEOUT_EXCEPTION,
        RateLimitedException: exceptions_pb.SDK_RATE_LIMITED_EXCEPTION,
        QuotaLimitedException: exceptions_pb.SDK_QUOTA_LIMITED_EXCEPTION,
        DocumentNotFoundException: exceptions_pb.SDK_DOCUMENT_NOT_FOUND_EXCEPTION,
        DocumentLockedException: exceptions_pb.SDK_DOCUMENT_LOCKED_EXCEPTION,
        DocumentExistsException: exceptions_pb.SDK_DOCUMENT_EXISTS_EXCEPTION,
        DurabilityInvalidLevelException: exceptions_pb.SDK_DURABILITY_LEVEL_NOT_AVAILABLE_EXCEPTION,
        DurabilityImpossibleException: exceptions_pb.SDK_DURABILITY_IMPOSSIBLE_EXCEPTION,
        DurabilitySyncWriteAmbiguousException: exceptions_pb.SDK_DURABILITY_AMBIGUOUS_EXCEPTION,
        DurabilitySyncWriteInProgressException: exceptions_pb.SDK_DURABLE_WRITE_IN_PROGRESS_EXCEPTION,
        PathNotFoundException: exceptions_pb.SDK_PATH_NOT_FOUND_EXCEPTION,
        PathMismatchException: exceptions_pb.SDK_PATH_MISMATCH_EXCEPTION,
        InvalidValueException: exceptions_pb.SDK_VALUE_INVALID_EXCEPTION,
        PathExistsException: exceptions_pb.SDK_PATH_EXISTS_EXCEPTION,
        CollectionAlreadyExistsException: exceptions_pb.SDK_COLLECTION_EXISTS_EXCEPTION,
        ScopeAlreadyExistsException: exceptions_pb.SDK_SCOPE_EXISTS_EXCEPTION,
        UserNotFoundException: exceptions_pb.SDK_USER_NOT_FOUND_EXCEPTION,
        GroupNotFoundException: exceptions_pb.SDK_GROUP_NOT_FOUND_EXCEPTION,
        BucketAlreadyExistsException: exceptions_pb.SDK_BUCKET_EXISTS_EXCEPTION,
        BucketNotFlushableException: exceptions_pb.SDK_BUCKET_NOT_FLUSHABLE_EXCEPTION,
        DeltaInvalidException: exceptions_pb.SDK_DELTA_INVALID_EXCEPTION,
        DocumentNotJsonException: exceptions_pb.SDK_DOCUMENT_NOT_JSON_EXCEPTION,
        DocumentUnretrievableException: exceptions_pb.SDK_DOCUMENT_UNRETRIEVABLE_EXCEPTION,
        NumberTooBigException: exceptions_pb.SDK_NUMBER_TOO_BIG_EXCEPTION,
        PathInvalidException: exceptions_pb.SDK_PATH_INVALID_EXCEPTION,
        PathTooBigException: exceptions_pb.SDK_PATH_TOO_BIG_EXCEPTION,
        PathTooDeepException: exceptions_pb.SDK_PATH_TOO_DEEP_EXCEPTION,
        ValueTooDeepException: exceptions_pb.SDK_VALUE_TOO_DEEP_EXCEPTION,
        UnsupportedOperation: exceptions_pb.SDK_UNSUPPORTED_OPERATION_EXCEPTION,
        DocumentNotLockedException: exceptions_pb.SDK_DOCUMENT_NOT_LOCKED_EXCEPTION,
    }

    @classmethod
    def as_success(cls, fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            try:
                start = perf_counter_ns()
                fn(self, *args, **kwargs)
                end = perf_counter_ns()
                sdk_result = sdk_pb.Result(success=True)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @staticmethod
    def exception_as_result(cb_exception):
        """ Returns a run.top_level.result w/ the sdk field set to a sdk.workload.result w/ an exception.
            Uses `SdkCommandResult.to_exception` in order to convert the exception to a PB exception.
        """
        sdk_result = sdk_pb.Result(exception=SdkCommandResult.to_exception(cb_exception))
        return run_pb.Result(sdk=sdk_result)

    @staticmethod
    def to_exception(cb_exception):
        """ Converts an exception to the equivalent Protobuf exception object
        """
        try:
            cb_exception_type = SdkCommandResult.ERROR_MAP[type(cb_exception)]
        except KeyError:
            cb_exception_type = None

        if cb_exception_type is not None:
            return exceptions_pb.Exception(
                couchbase=exceptions_pb.CouchbaseExceptionEx(
                    name=type(cb_exception).__name__,
                    type=cb_exception_type,
                    serialized=SdkCommandResult._serialize_couchbase_exception(cb_exception)
                )
            )
        else:
            return exceptions_pb.Exception(
                other=exceptions_pb.ExceptionOther(
                    name=type(cb_exception).__name__,
                    serialized=str(cb_exception)
                )
            )

    @staticmethod
    def to_content(cb_result, content_as: content_pb.ContentAs, index=None) -> content_pb.ContentTypes:  # noqa: C901
        res = content_pb.ContentTypes()
        as_type = content_as.WhichOneof('as')

        try:
            getattr(cb_result, 'content_as')
        except AttributeError:
            return SdkCommandResult._row_to_content(cb_result, content_as)

        if index is None:
            orig_content = cb_result.content_as._content
        else:
            orig_content = cb_result.content_as._content[index].get('value')

        if as_type == 'as_string':
            if isinstance(orig_content, list):
                if index is None:
                    res.content_as_string = json.dumps(cb_result.content_as[list], separators=(',', ':'))
                else:
                    res.content_as_string = json.dumps(cb_result.content_as[list](index), separators=(',', ':'))
            elif isinstance(orig_content, dict):
                if index is None:
                    res.content_as_string = json.dumps(cb_result.content_as[dict], separators=(',', ':'))
                else:
                    res.content_as_string = json.dumps(cb_result.content_as[dict](index), separators=(',', ':'))
            else:
                if index is None:
                    res.content_as_string = cb_result.content_as[str]
                else:
                    res.content_as_string = cb_result.content_as[str](index)
        elif as_type == 'as_byte_array':
            if isinstance(orig_content, list):
                if index is None:
                    res.content_as_bytes = json.dumps(cb_result.content_as[list], separators=(',', ':')).encode('utf-8')
                else:
                    res.content_as_bytes = json.dumps(cb_result.content_as[list](
                        index), separators=(',', ':')).encode('utf-8')
            elif isinstance(orig_content, dict):
                if index is None:
                    res.content_as_bytes = json.dumps(cb_result.content_as[dict], separators=(',', ':')).encode('utf-8')
                else:
                    res.content_as_bytes = json.dumps(cb_result.content_as[dict](
                        index), separators=(',', ':')).encode('utf-8')
            elif isinstance(orig_content, bytes):
                if index is None:
                    res.content_as_bytes = cb_result.content_as[bytes]
                else:
                    res.content_as_bytes = cb_result.content_as[bytes](index)
            else:
                if index is None:
                    res.content_as_bytes = cb_result.content_as[str].encode('utf-8')
                else:
                    res.content_as_bytes = cb_result.content_as[str](index).encode('utf-8')
        elif as_type == 'as_json_object':
            if index is None:
                res.content_as_bytes = json.dumps(cb_result.content_as[dict], separators=(',', ':')).encode('utf-8')
            else:
                res.content_as_bytes = json.dumps(cb_result.content_as[dict](
                    index), separators=(',', ':')).encode('utf-8')
        elif as_type == 'as_json_array':
            if index is None:
                res.content_as_bytes = json.dumps(cb_result.content_as[list], separators=(',', ':')).encode('utf-8')
            else:
                res.content_as_bytes = json.dumps(cb_result.content_as[list](
                    index), separators=(',', ':')).encode('utf-8')
        elif as_type == 'as_boolean':
            if index is None:
                res.content_as_bool = cb_result.content_as[bool]
            else:
                res.content_as_bool = cb_result.content_as[bool](index)
        elif as_type == 'as_integer':
            if index is None:
                res.content_as_int64 = cb_result.content_as[int]
            else:
                res.content_as_int64 = cb_result.content_as[int](index)
        elif as_type == 'as_floating_point':
            if index is None:
                res.content_as_double = cb_result.content_as[float]
            else:
                res.content_as_double = cb_result.content_as[float](index)

        return res

    @staticmethod
    def _row_to_content(row, content_as: content_pb.ContentAs):  # noqa: C901
        """
        Returns the ContentType message when the result has no content_as method - e.g. in a scan or query row
        """
        res = content_pb.ContentTypes()
        as_type = content_as.WhichOneof('as')

        if as_type == 'as_string':
            if isinstance(row, list):
                res.content_as_string = json.dumps(list(row), separators=(',', ':'))
            elif isinstance(row, dict):
                res.content_as_string = json.dumps(dict(row), separators=(',', ':'))
            else:
                res.content_as_string = str(row)
        elif as_type == 'as_byte_array':
            if isinstance(row, list):
                res.content_as_bytes = json.dumps(list(row), separators=(',', ':')).encode('utf-8')
            elif isinstance(row, dict):
                res.content_as_bytes = json.dumps(dict(row), separators=(',', ':')).encode('utf-8')
            else:
                res.content_as_bytes = str(row).encode('utf-8')
        elif as_type == 'as_json_object':
            res.content_as_bytes = json.dumps(dict(row), separators=(',', ':')).encode('utf-8')
        elif as_type == 'as_json_array':
            res.content_as_bytes = json.dumps(list(row), separators=(',', ':')).encode('utf-8')
        elif as_type == 'as_boolean':
            res.content_as_bool = bool(row)
        elif as_type == 'as_integer':
            res.content_as_int64 = int(row)
        elif as_type == 'as_floating_point':
            res.content_as_double = float(row)

        return res

    @staticmethod
    def _serialize_couchbase_exception(exc: CouchbaseException) -> str:
        return f"{exc.message} {json.dumps(exc.error_context, cls=SdkErrorContextEncoder)}"


def validate_command(validation_dict,  # type: Dict[str, Callable[[Any], bool]]
                     **kwargs,  # type: Dict[str, Any]
                     ) -> bool:
    """Validate SDK Command options.

    Helper method to parse kwargs provided when creating and SDK Command instance.

    Args:
        validation_dict (Dict[str, Callable[[Any], bool]]): Dictionary holding the mapping from the options to the
            callables to validate their values
        kwargs (Dict[str, Any]): Other options that are necessary to build the SDK Command.

    Returns:
        bool:  True if all kwargs pass validation. Raises exception otherwise.

    Raises:
        ValueError: If an invalid key is provided.
        TypeError: If value for option is incorrect type.
    """
    for k, v in kwargs.items():
        if k not in validation_dict.keys():
            raise ValueError(f'Invalid key found.  Key: {k}')
        if not validation_dict[k](v):
            raise TypeError(f'{k} has invalid type or is empty.')
    return True
