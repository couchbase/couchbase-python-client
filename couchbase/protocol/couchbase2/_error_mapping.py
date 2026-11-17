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

"""A failed gRPC call, as the exception the SDK raises for it and whether it is worth retrying.

The gateway explains a failure with a `google.rpc.Status` in the `grpc-status-details-bin` trailer:
a message, and detail blocks that refine the status code.  RFC 77 maps those onto the SDK's
exceptions.  A failure gRPC raises on the client side, such as a deadline or a connection that never
opened, carries no such trailer, so every lookup falls back to the bare code and message.  When a
detail block appears more than once, the first is the one read.

What the mapping returns is plain data, built into an exception only when the request gives up, so
that the exception's context can carry the request's whole retry history.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar

import grpc
import grpc.aio
from google.protobuf.message import DecodeError, Message
from google.rpc import error_details_pb2, status_pb2

from couchbase.common._operations import Op
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         AuthenticationException,
                                         AuthorizationFailureException,
                                         BucketAlreadyExistsException,
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
                                         DocumentTooDeepException,
                                         DurabilityImpossibleException,
                                         FeatureUnavailableException,
                                         GenericErrorContext,
                                         InternalServerFailureException,
                                         InvalidArgumentException,
                                         InvalidValueException,
                                         NumberTooBigException,
                                         PathExistsException,
                                         PathMismatchException,
                                         PathNotFoundException,
                                         QueryIndexAlreadyExistsException,
                                         QueryIndexNotFoundException,
                                         RequestCanceledException,
                                         ScopeAlreadyExistsException,
                                         ScopeNotFoundException,
                                         SearchIndexNotFoundException,
                                         ServiceUnavailableException,
                                         UnAmbiguousTimeoutException,
                                         ValueTooLargeException)
from couchbase.protocol.couchbase2._retry import RetryReason

STATUS_DETAILS_KEY = 'grpc-status-details-bin'

NOT_FOUND: dict[str, type[CouchbaseException]] = {
    'document': DocumentNotFoundException,
    'bucket': BucketNotFoundException,
    'scope': ScopeNotFoundException,
    'collection': CollectionNotFoundException,
    'queryindex': QueryIndexNotFoundException,
    'searchindex': SearchIndexNotFoundException,
    'path': PathNotFoundException,
}

# A search names an index the way a query does, and classic raises the query class when it is
# missing; the search index manager raises the search one.
NOT_FOUND_BY_OP: dict[Op, dict[str, type[CouchbaseException]]] = {
    Op.SearchQuery: {'searchindex': QueryIndexNotFoundException},
}

# There is no search-specific class for an index that already exists; classic raises the query one.
ALREADY_EXISTS: dict[str, type[CouchbaseException]] = {
    'document': DocumentExistsException,
    'bucket': BucketAlreadyExistsException,
    'scope': ScopeAlreadyExistsException,
    'collection': CollectionAlreadyExistsException,
    'queryindex': QueryIndexAlreadyExistsException,
    'searchindex': QueryIndexAlreadyExistsException,
    'path': PathExistsException,
}

# Keyed by `PreconditionFailure.violations[0].type`.  A type missing here, `POISONED_CAS` among them,
# has no SDK exception of its own and is a plain `CouchbaseException`.
PRECONDITION_FAILED: dict[str, type[CouchbaseException]] = {
    'LOCKED': DocumentLockedException,
    'NOT_LOCKED': DocumentNotLockedException,
    'DOC_TOO_DEEP': DocumentTooDeepException,
    'DOC_NOT_JSON': DocumentNotJsonException,
    'PATH_MISMATCH': PathMismatchException,
    'VALUE_OUT_OF_RANGE': InvalidValueException,
    'PATH_VALUE_OUT_OF_RANGE': NumberTooBigException,
    'VALUE_TOO_LARGE': ValueTooLargeException,
    'DOC_NOT_NUMERIC': DeltaInvalidException,
    'FLUSH_DISABLED': BucketNotFlushableException,
    'DURABILITY_IMPOSSIBLE': DurabilityImpossibleException,
}

# Codes whose exception no detail refines.  DEADLINE_EXCEEDED depends on the operation and is
# decided in `map_status`; anything missing from both is a plain `CouchbaseException`.
BY_CODE: dict[grpc.StatusCode, type[CouchbaseException]] = {
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentException,
    grpc.StatusCode.OUT_OF_RANGE: InvalidArgumentException,
    grpc.StatusCode.UNIMPLEMENTED: FeatureUnavailableException,
    grpc.StatusCode.UNAUTHENTICATED: AuthenticationException,
    grpc.StatusCode.CANCELLED: RequestCanceledException,
    grpc.StatusCode.UNAVAILABLE: ServiceUnavailableException,
    grpc.StatusCode.INTERNAL: InternalServerFailureException,
}

_M = TypeVar('_M', bound=Message)


@dataclass(frozen=True)
class MappedError:
    """A failure resolved to plain values: what to raise, and whether to retry first."""

    exception_type: type[CouchbaseException]
    message: str
    # The int value of the gRPC status code.
    status_code: int
    context: Mapping[str, Any] = field(default_factory=dict)
    retry_reason: RetryReason | None = None

    def exception(self, **context: Any) -> CouchbaseException:
        """The exception to raise, its context holding the details plus `context`, the request's own fields."""
        return self.exception_type(self.message,
                                   context=GenericErrorContext(**{**self.context, **context}))


def map_rpc_error(error: grpc.RpcError, *, readonly: bool, op: Op | None = None) -> MappedError:
    """`error` as the SDK sees it, for `op` when given.

    `readonly` decides whether a deadline leaves the outcome ambiguous.
    """
    if not isinstance(error, (grpc.Call, grpc.aio.AioRpcError)):
        return map_status(grpc.StatusCode.UNKNOWN, str(error) or None, None, readonly=readonly, op=op)
    code, message = error.code(), error.details()
    return map_status(code, message, _rich_status(code, error.trailing_metadata()), readonly=readonly, op=op)


def _rich_status(code: grpc.StatusCode, trailing: Iterable[tuple[str, Any]] | None) -> status_pb2.Status | None:
    payload = next((value for key, value in trailing or () if key == STATUS_DETAILS_KEY), None)
    if not isinstance(payload, bytes):
        return None
    try:
        rich = status_pb2.Status.FromString(payload)
    except DecodeError:
        return None
    # A trailer naming another code than the call ended with describes some other failure.
    return rich if rich.code == code.value[0] else None


def map_status(code: grpc.StatusCode,
               message: str | None,
               rich: status_pb2.Status | None,
               *,
               readonly: bool,
               op: Op | None = None) -> MappedError:
    """The bare `code` and `message` refined by `rich`, the status the gateway sent with them, if any."""
    details = list(rich.details) if rich is not None else []
    resource = _first(details, error_details_pb2.ResourceInfo)
    precondition = _first(details, error_details_pb2.PreconditionFailure)
    violation = precondition.violations[0].type if precondition and precondition.violations else None
    error_info = _first(details, error_details_pb2.ErrorInfo)
    reason = error_info.reason if error_info is not None and error_info.reason else None

    context: dict[str, Any] = {'grpc_status': code.name}
    if violation is not None:
        context['precondition_violation'] = violation
    if resource is not None:
        context['resource_name'] = resource.resource_name
        context['resource_type'] = resource.resource_type
    if reason is not None:
        context['reason'] = reason
    context.update(_diagnostics(details))

    return MappedError(_exception_type(code, resource, violation, reason, readonly, op),
                       (rich.message if rich is not None else '') or message or f'gRPC status {code.name}',
                       code.value[0],
                       context=context,
                       retry_reason=_retry_reason(code, violation))


def _exception_type(code: grpc.StatusCode,
                    resource: error_details_pb2.ResourceInfo | None,
                    violation: str | None,
                    reason: str | None,
                    readonly: bool,
                    op: Op | None) -> type[CouchbaseException]:
    resource_type = resource.resource_type if resource is not None else ''
    if code == grpc.StatusCode.NOT_FOUND:
        by_op = NOT_FOUND_BY_OP.get(op, {}) if op is not None else {}
        return by_op.get(resource_type) or NOT_FOUND.get(resource_type, CouchbaseException)
    if code == grpc.StatusCode.ALREADY_EXISTS:
        return ALREADY_EXISTS.get(resource_type, CouchbaseException)
    if code == grpc.StatusCode.FAILED_PRECONDITION:
        return PRECONDITION_FAILED.get(violation or '', CouchbaseException)
    if code == grpc.StatusCode.ABORTED:
        return CasMismatchException if reason == 'CAS_MISMATCH' else CouchbaseException
    if code == grpc.StatusCode.PERMISSION_DENIED:
        # Being refused as a user is a failure to authenticate; being refused a resource is not.
        return AuthenticationException if resource_type in ('', 'user') else AuthorizationFailureException
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        # A write that ran out of time may still have been applied; a read changed nothing either way.
        return UnAmbiguousTimeoutException if readonly else AmbiguousTimeoutException
    return BY_CODE.get(code, CouchbaseException)


def _retry_reason(code: grpc.StatusCode, violation: str | None) -> RetryReason | None:
    if code == grpc.StatusCode.UNAVAILABLE:
        return RetryReason.SERVICE_NOT_AVAILABLE
    if code == grpc.StatusCode.FAILED_PRECONDITION and violation == 'LOCKED':
        return RetryReason.KV_LOCKED
    return None


def _diagnostics(details: list[Any]) -> dict[str, Any]:
    """What the gateway says about any failure, whatever its code."""
    context: dict[str, Any] = {}
    debug = _first(details, error_details_pb2.DebugInfo)
    if debug is not None:
        context['debug_info'] = {'detail': debug.detail, 'stack_trace': list(debug.stack_entries)}
    request = _first(details, error_details_pb2.RequestInfo)
    if request is not None:
        context['request_info'] = {'request_id': request.request_id, 'serving_data': request.serving_data}
    return context


def _first(details: list[Any], message_type: type[_M]) -> _M | None:
    for detail in details:
        if detail.Is(message_type.DESCRIPTOR):
            message = message_type()
            try:
                detail.Unpack(message)
            except DecodeError:
                return None
            return message
    return None
