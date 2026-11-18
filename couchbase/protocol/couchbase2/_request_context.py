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

"""One couchbase2 request's state across its attempts, and every decision about retrying it.

The decisions are synchronous and do no I/O, so the sync and async drivers share them and differ
only in how they call and how they sleep::

    while True:
        ctx.check()
        try:
            with ctx.attempt(call, auth) as attempt:
                return call(request, timeout=ctx.dispatch(), metadata=attempt.metadata)
        except grpc.RpcError as e:
            error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
        time.sleep(ctx.on_failure(error))

`on_failure` is called after the `except` block, so the exception it raises does not chain the
gRPC error; RFC 77 asks that it not.

A request has one deadline, fixed when the context is made, and every attempt is given what is left
of it.  An exception is built only once the request gives up, so it carries the whole retry history.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, NoReturn

import grpc

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._redaction import user_data
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         CouchbaseException,
                                         DocumentLockedException,
                                         GenericErrorContext,
                                         RequestCanceledException,
                                         UnAmbiguousTimeoutException)
from couchbase.protocol.couchbase2._error_mapping import MappedError
from couchbase.protocol.couchbase2._retry import (BestEffortRetryStrategy,
                                                  RetryReason,
                                                  RetryStrategy,
                                                  controlled_backoff)
from couchbase.protocol.couchbase2._tracing import (Attempt,
                                                    GrpcTracing,
                                                    Metadata)

logger = logging.getLogger('couchbase.couchbase2')

_CODE_NAMES = {code.value[0]: code.name for code in grpc.StatusCode}


def _code_name(error: MappedError) -> str | int:
    return _CODE_NAMES.get(error.status_code, error.status_code)


def _request_id(error: MappedError) -> str:
    """The gateway's id for the failed request, for matching the retry log to the gateway's own."""
    request_id = error.context.get('request_info', {}).get('request_id')
    return f', gateway request id {request_id}' if request_id else ''


class RequestContext:
    """Everything about one request that changes from one attempt to the next."""

    def __init__(self,
                 op: Op,
                 timeout: float,
                 *,
                 readonly: bool | None = None,
                 obs_handler: ObservableRequestHandler | None = None,
                 tracing: GrpcTracing | None = None,
                 strategy: RetryStrategy | None = None,
                 closed: Callable[[], bool] = lambda: False,
                 unrecognized: Callable[[str], None] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        """`timeout` is in seconds.  `closed` says whether the cluster has been closed since.

        `unrecognized` is told each value from the gateway that the error mapping does not know.

        `readonly` is the operation's own answer unless given; a query's `read_only` option decides its own.
        `obs_handler` is the operation's, which each attempt is traced under and each retry is counted on.
        """
        self.op = op
        self.identifier = str(uuid.uuid4())
        self._readonly = op.couchbase2_readonly if readonly is None else readonly
        self.retry_attempts = 0
        self._retry_reasons: list[RetryReason] = []
        # The failure the most recent retry was for, as `<exception>, caused by <gRPC status>`.
        self.last_exception: str | None = None
        # Whether any attempt was handed to gRPC, and so may have reached the server.
        self.dispatched = False
        self._obs_handler = obs_handler
        self._tracing = tracing
        self._strategy = strategy if strategy is not None else BestEffortRetryStrategy()
        self._closed = closed
        self._unrecognized = unrecognized
        self._clock = clock
        self.deadline = clock() + timeout

    @property
    def readonly(self) -> bool:
        return self._readonly

    @property
    def idempotent(self) -> bool:
        """RFC 49's name for what a strategy reads: whether retrying cannot apply the request twice."""
        return self._readonly

    @property
    def retry_reasons(self) -> Sequence[RetryReason]:
        return tuple(self._retry_reasons)

    def remaining(self) -> float:
        return max(self.deadline - self._clock(), 0.0)

    def check(self) -> None:
        """Raise, without dispatching, when the next attempt cannot be made."""
        if self._closed():
            raise self._fail(RequestCanceledException, 'the cluster was closed')
        if self.remaining() <= 0:
            exception_type: type[CouchbaseException]
            if RetryReason.KV_LOCKED in self._retry_reasons:
                # Python names the lock that held the request rather than the timeout it ended in.
                exception_type = DocumentLockedException
            elif self.readonly or not self.dispatched:
                # Only a write that some attempt sent may have been applied.
                exception_type = UnAmbiguousTimeoutException
            else:
                exception_type = AmbiguousTimeoutException
            raise self._fail(exception_type, f'{self.op.value} timed out')

    def dispatch(self) -> float:
        """The timeout to give the attempt about to be sent."""
        self.dispatched = True
        return self.remaining()

    def attempt(self, call: object, metadata: Metadata, *, stream: bool = False) -> Attempt:
        """The attempt about to call `call`, which traces it and gives the metadata it sends."""
        if self._tracing is None:
            return Attempt(metadata, stream=stream)
        return self._tracing.attempt(call, self._obs_handler, metadata, stream=stream)

    def on_failure(self, error: MappedError) -> float:
        """The seconds to wait before retrying after `error`, or raise when the request gives up."""
        self._note_unrecognized(error)
        reason = error.retry_reason
        if reason is None:
            raise self._exception(error)
        if reason.always_retry:
            delay: timedelta | None = controlled_backoff(self.retry_attempts)
        else:
            delay = self._strategy.retry_after(self, reason).delay
        if delay is None:
            raise self._exception(error)

        # Recorded before the closed check, so a request cancelled here still says why it retried.
        self.retry_attempts += 1
        if self._obs_handler is not None:
            self._obs_handler.record_retry()
        if reason not in self._retry_reasons:
            self._retry_reasons.append(reason)
        self.last_exception = (f'{error.exception_type.__name__}({error.message}), '
                               f'caused by gRPC status {_code_name(error)}')
        logger.debug('Retrying %s request %s for %s, attempt %d, after gRPC status %s (%s)%s',
                     self.op.value, self.identifier, reason.value, self.retry_attempts,
                     _code_name(error), user_data(error.message), _request_id(error))
        if self._closed():
            raise self._fail(RequestCanceledException, 'the cluster was closed')
        # A delay past the deadline is cut to it, so the next `check` answers with a timeout that
        # carries this history rather than the request ending on the failure itself.
        return min(delay.total_seconds(), self.remaining())

    def on_stream_failure(self, error: MappedError) -> NoReturn:
        """Raise for `error`, met after the stream returned its first message.

        Retrying would replay what the caller has already read, so a failure that would otherwise
        retry cancels the request instead.  Any other is raised as it maps.
        """
        self._note_unrecognized(error)
        if error.retry_reason is None:
            raise self._exception(error)
        raise self._fail(RequestCanceledException,
                         (f'{self.op.value} failed with gRPC status {_code_name(error)} after its stream began, '
                          'and a stream is not retried once it has returned a message'))

    def _note_unrecognized(self, error: MappedError) -> None:
        if error.unrecognized is not None and self._unrecognized is not None:
            self._unrecognized(error.unrecognized)

    def _history(self) -> dict[str, Any]:
        history: dict[str, Any] = {'retry_attempts': self.retry_attempts,
                                   'retry_reasons': {reason.value for reason in self._retry_reasons}}
        if self.last_exception is not None:
            history['last_exception'] = self.last_exception
        return history

    def _exception(self, error: MappedError) -> CouchbaseException:
        return error.exception(**self._history())

    def _fail(self, exception_type: type[CouchbaseException], message: str) -> CouchbaseException:
        return exception_type(message, context=GenericErrorContext(**self._history()))
