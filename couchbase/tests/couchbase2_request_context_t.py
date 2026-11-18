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

import logging
from datetime import timedelta

import grpc
import pytest
from google.protobuf import any_pb2
from google.rpc import error_details_pb2, status_pb2

from couchbase.common import _redaction
from couchbase.common._operations import Op
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CouchbaseException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  GenericErrorContext,
                                  RequestCanceledException,
                                  ServiceUnavailableException,
                                  UnAmbiguousTimeoutException)
from couchbase.protocol.couchbase2._error_mapping import MappedError, map_status
from couchbase.protocol.couchbase2._request_context import RequestContext
from couchbase.protocol.couchbase2._retry import (BestEffortRetryStrategy,
                                                  RetryAction,
                                                  RetryReason,
                                                  RetryRequest,
                                                  controlled_backoff)

Code = grpc.StatusCode


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _Closed:
    def __init__(self) -> None:
        self.closed = False

    def __call__(self) -> bool:
        return self.closed


class _Strategy:
    """Answers with `delay`, and records what it was asked."""

    def __init__(self, delay: timedelta | None) -> None:
        self.delay = delay
        self.asked: list[tuple[str, bool, int, tuple[RetryReason, ...], RetryReason]] = []

    def retry_after(self, request: RetryRequest, reason: RetryReason) -> RetryAction:
        self.asked.append((request.identifier, request.idempotent, request.retry_attempts,
                           tuple(request.retry_reasons), reason))
        return RetryAction(self.delay)


def _unavailable(message: str = 'gateway away') -> MappedError:
    return map_status(Code.UNAVAILABLE, message, None, readonly=False)


def _locked() -> MappedError:
    detail = any_pb2.Any()
    detail.Pack(error_details_pb2.PreconditionFailure(
        violations=[error_details_pb2.PreconditionFailure.Violation(type='LOCKED')]))
    rich = status_pb2.Status(code=Code.FAILED_PRECONDITION.value[0], message='locked', details=[detail])
    return map_status(Code.FAILED_PRECONDITION, 'locked', rich, readonly=False)


def _not_found() -> MappedError:
    detail = any_pb2.Any()
    detail.Pack(error_details_pb2.ResourceInfo(resource_type='document'))
    rich = status_pb2.Status(code=Code.NOT_FOUND.value[0], message='no such document', details=[detail])
    return map_status(Code.NOT_FOUND, 'no such document', rich, readonly=False)


def _context(op: Op = Op.Upsert,
             timeout: float = 2.5,
             *,
             clock: _Clock | None = None,
             closed: _Closed | None = None,
             strategy: _Strategy | None = None,
             readonly: bool | None = None) -> RequestContext:
    return RequestContext(op, timeout, readonly=readonly, strategy=strategy,
                          closed=closed or _Closed(), clock=clock or _Clock())


def _error_context(exc: CouchbaseException) -> GenericErrorContext:
    assert isinstance(exc.error_context, GenericErrorContext)
    return exc.error_context


class DeadlineUnitTests:

    def test_set_once_from_the_timeout(self) -> None:
        clock = _Clock()
        ctx = _context(timeout=2.5, clock=clock)
        clock.now += 1.0
        assert ctx.deadline == 102.5
        assert ctx.remaining() == pytest.approx(1.5)

    def test_each_attempt_is_given_what_is_left(self) -> None:
        clock = _Clock()
        ctx = _context(timeout=2.5, clock=clock)
        assert ctx.dispatch() == pytest.approx(2.5)
        clock.now += 2.0
        assert ctx.dispatch() == pytest.approx(0.5)

    def test_remaining_never_goes_negative(self) -> None:
        clock = _Clock()
        ctx = _context(timeout=1.0, clock=clock)
        clock.now += 5.0
        assert ctx.remaining() == 0.0

    def test_readonly_is_the_couchbase2_answer(self) -> None:
        assert _context(Op.Exists).readonly is True
        assert _context(Op.Upsert).readonly is False


class NoBudgetUnitTests:

    def test_check_passes_with_time_left(self) -> None:
        _context().check()

    @pytest.mark.parametrize('op, dispatched, expected', [
        (Op.Get, True, UnAmbiguousTimeoutException),
        (Op.Upsert, False, UnAmbiguousTimeoutException),
        (Op.Upsert, True, AmbiguousTimeoutException),
    ], ids=['readonly', 'never dispatched', 'dispatched write'])
    def test_answered_without_dispatch(self, op: Op, dispatched: bool, expected: type[CouchbaseException]) -> None:
        clock = _Clock()
        ctx = _context(op, clock=clock)
        if dispatched:
            ctx.dispatch()
        clock.now = ctx.deadline
        with pytest.raises(CouchbaseException) as raised:
            ctx.check()
        assert type(raised.value) is expected
        assert ctx.dispatched is dispatched

    def test_the_timeout_carries_the_history(self) -> None:
        clock = _Clock()
        ctx = _context(clock=clock)
        ctx.dispatch()
        ctx.on_failure(_unavailable())
        clock.now = ctx.deadline
        with pytest.raises(AmbiguousTimeoutException) as raised:
            ctx.check()
        context = _error_context(raised.value)
        assert context.retry_attempts == 1
        assert context.retry_reasons == {'service_not_available'}
        assert context.last_exception is not None

    @pytest.mark.parametrize('op', [Op.Get, Op.Upsert])
    def test_a_lock_held_to_the_deadline_is_named(self, op: Op) -> None:
        clock = _Clock()
        ctx = _context(op, clock=clock)
        ctx.dispatch()
        ctx.on_failure(_locked())
        ctx.on_failure(_unavailable())
        clock.now = ctx.deadline
        with pytest.raises(CouchbaseException) as raised:
            ctx.check()
        assert type(raised.value) is DocumentLockedException
        assert _error_context(raised.value).retry_reasons == {'key_value_locked', 'service_not_available'}

    def test_a_closed_cluster_cancels_before_dispatch(self) -> None:
        closed = _Closed()
        ctx = _context(closed=closed)
        closed.closed = True
        with pytest.raises(RequestCanceledException):
            ctx.check()
        assert ctx.dispatched is False


class OnFailureUnitTests:

    def test_not_retryable_raises_the_mapped_exception(self) -> None:
        ctx = _context()
        with pytest.raises(DocumentNotFoundException) as raised:
            ctx.on_failure(_not_found())
        context = _error_context(raised.value)
        assert context.retry_attempts == 0
        assert context.retry_reasons == set()
        assert context.last_exception is None
        assert raised.value.__cause__ is None

    @pytest.mark.parametrize('error, reason', [(_unavailable(), RetryReason.SERVICE_NOT_AVAILABLE),
                                               (_locked(), RetryReason.KV_LOCKED)],
                             ids=['unavailable', 'locked'])
    def test_retryable_records_and_waits(self, error: MappedError, reason: RetryReason) -> None:
        ctx = _context()
        assert ctx.on_failure(error) == pytest.approx(0.001)
        assert ctx.retry_attempts == 1
        assert ctx.retry_reasons == (reason,)

    def test_a_write_retries(self) -> None:
        ctx = _context(Op.Insert)
        ctx.on_failure(_locked())
        assert ctx.retry_attempts == 1

    def test_each_reason_is_recorded_once_in_first_seen_order(self) -> None:
        ctx = _context(timeout=60.0)
        for error in (_locked(), _unavailable(), _locked()):
            ctx.on_failure(error)
        assert ctx.retry_attempts == 3
        assert ctx.retry_reasons == (RetryReason.KV_LOCKED, RetryReason.SERVICE_NOT_AVAILABLE)

    def test_the_backoff_follows_the_attempts(self) -> None:
        ctx = _context(timeout=60.0)
        delays = [ctx.on_failure(_unavailable()) for _ in range(7)]
        assert delays == pytest.approx([0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 1.0])

    def test_the_backoff_is_cut_to_the_deadline(self) -> None:
        clock = _Clock()
        ctx = _context(timeout=60.0, clock=clock)
        for _ in range(5):
            ctx.on_failure(_unavailable())
        clock.now = ctx.deadline - 0.25
        assert ctx.on_failure(_unavailable()) == pytest.approx(0.25)

    def test_no_time_left_still_retries_so_the_timeout_carries_the_history(self) -> None:
        clock = _Clock()
        ctx = _context(clock=clock)
        clock.now = ctx.deadline + 1.0
        assert ctx.on_failure(_unavailable()) == 0.0
        assert ctx.retry_attempts == 1

    def test_logs_each_retry(self, caplog: pytest.LogCaptureFixture) -> None:
        ctx = _context()
        with caplog.at_level(logging.DEBUG, logger='couchbase.couchbase2'):
            ctx.on_failure(_unavailable())
        record, = caplog.records
        assert record.levelno == logging.DEBUG
        assert record.getMessage().endswith('for service_not_available, attempt 1, '
                                            'after gRPC status UNAVAILABLE (gateway away)')

    def test_the_retry_log_names_the_gateways_request(self, caplog: pytest.LogCaptureFixture) -> None:
        detail = any_pb2.Any()
        detail.Pack(error_details_pb2.RequestInfo(request_id='req-7'))
        rich = status_pb2.Status(code=Code.UNAVAILABLE.value[0], message='gateway away', details=[detail])
        with caplog.at_level(logging.DEBUG, logger='couchbase.couchbase2'):
            _context().on_failure(map_status(Code.UNAVAILABLE, None, rich, readonly=False))
        assert caplog.records[0].getMessage().endswith('(gateway away), gateway request id req-7')

    def test_the_retry_log_tags_the_message_as_user_data(self,
                                                         caplog: pytest.LogCaptureFixture,
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_redaction, '_enabled', True)
        with caplog.at_level(logging.DEBUG, logger='couchbase.couchbase2'):
            _context().on_failure(_unavailable())
        assert '(<ud>gateway away</ud>)' in caplog.records[0].getMessage()


class UnrecognizedUnitTests:

    def _unmapped(self) -> MappedError:
        return map_status(Code.DATA_LOSS, 'lost', None, readonly=False)

    def test_a_failure_reports_what_it_did_not_recognize(self) -> None:
        told: list[str] = []
        ctx = RequestContext(Op.Upsert, 2.5, unrecognized=told.append, clock=_Clock())
        with pytest.raises(CouchbaseException):
            ctx.on_failure(self._unmapped())
        assert told == ['gRPC status DATA_LOSS']

    def test_a_stream_failure_reports_it_too(self) -> None:
        told: list[str] = []
        ctx = RequestContext(Op.Query, 2.5, unrecognized=told.append, clock=_Clock())
        with pytest.raises(CouchbaseException):
            ctx.on_stream_failure(self._unmapped())
        assert told == ['gRPC status DATA_LOSS']

    def test_a_recognized_failure_reports_nothing(self) -> None:
        told: list[str] = []
        ctx = RequestContext(Op.Upsert, 2.5, unrecognized=told.append, clock=_Clock())
        ctx.on_failure(_unavailable())
        assert told == []


class ClosedUnitTests:

    def test_the_reason_is_recorded_before_the_close(self) -> None:
        closed = _Closed()
        ctx = _context(closed=closed)
        closed.closed = True
        with pytest.raises(RequestCanceledException) as raised:
            ctx.on_failure(_unavailable())
        context = _error_context(raised.value)
        assert context.retry_attempts == 1
        assert context.retry_reasons == {'service_not_available'}

    def test_a_failure_not_retried_is_not_cancelled(self) -> None:
        closed = _Closed()
        ctx = _context(closed=closed)
        closed.closed = True
        with pytest.raises(DocumentNotFoundException):
            ctx.on_failure(_not_found())


class LastExceptionUnitTests:

    def test_none_before_a_retry(self) -> None:
        assert _context().last_exception is None

    def test_the_previous_retried_failure(self) -> None:
        ctx = _context()
        ctx.on_failure(_unavailable('gateway away'))
        with pytest.raises(DocumentNotFoundException) as raised:
            ctx.on_failure(_not_found())
        assert _error_context(raised.value).last_exception == (
            'ServiceUnavailableException(gateway away), caused by gRPC status UNAVAILABLE')

    def test_the_most_recent_retry_wins(self) -> None:
        ctx = _context()
        ctx.on_failure(_unavailable())
        ctx.on_failure(_locked())
        assert ctx.last_exception == 'DocumentLockedException(locked), caused by gRPC status FAILED_PRECONDITION'


class StreamFailureUnitTests:

    @pytest.mark.parametrize('error', [_unavailable(), _locked()], ids=['unavailable', 'locked'])
    def test_a_failure_that_would_retry_cancels(self, error: MappedError) -> None:
        ctx = _context(Op.Query)
        with pytest.raises(RequestCanceledException, match='query failed with gRPC status') as raised:
            ctx.on_stream_failure(error)
        assert ctx.retry_attempts == 0
        assert _error_context(raised.value).retry_reasons == set()

    def test_any_other_is_raised_as_it_maps(self) -> None:
        with pytest.raises(DocumentNotFoundException):
            _context(Op.Query).on_stream_failure(_not_found())

    def test_carries_the_retries_before_the_stream_began(self) -> None:
        ctx = _context(Op.Query)
        ctx.on_failure(_unavailable('gateway away'))
        with pytest.raises(RequestCanceledException) as raised:
            ctx.on_stream_failure(_unavailable())
        context = _error_context(raised.value)
        assert context.retry_attempts == 1
        assert context.retry_reasons == {'service_not_available'}
        assert context.last_exception == 'ServiceUnavailableException(gateway away), caused by gRPC status UNAVAILABLE'


class StrategyUnitTests:

    def test_the_default_is_best_effort(self) -> None:
        assert _context().on_failure(_unavailable()) == pytest.approx(
            controlled_backoff(0).total_seconds())

    def test_is_not_asked_about_a_reason_that_always_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(RetryReason, 'always_retry', property(lambda self: True))
        strategy = _Strategy(None)
        ctx = _context(strategy=strategy)
        assert ctx.on_failure(_unavailable()) == pytest.approx(controlled_backoff(0).total_seconds())
        assert ctx.on_failure(_locked()) == pytest.approx(controlled_backoff(1).total_seconds())
        assert strategy.asked == []

    def test_is_asked_before_the_attempt_is_recorded(self) -> None:
        strategy = _Strategy(timedelta(milliseconds=7))
        ctx = _context(Op.Get, strategy=strategy)
        assert ctx.on_failure(_unavailable()) == pytest.approx(0.007)
        assert ctx.on_failure(_locked()) == pytest.approx(0.007)
        assert strategy.asked == [
            (ctx.identifier, True, 0, (), RetryReason.SERVICE_NOT_AVAILABLE),
            (ctx.identifier, True, 1, (RetryReason.SERVICE_NOT_AVAILABLE,), RetryReason.KV_LOCKED),
        ]

    def test_do_not_retry_raises_the_failure(self) -> None:
        ctx = _context(strategy=_Strategy(None))
        with pytest.raises(ServiceUnavailableException) as raised:
            ctx.on_failure(_unavailable())
        assert _error_context(raised.value).retry_attempts == 0
        assert ctx.retry_reasons == ()

    def test_cannot_wait_past_the_deadline(self) -> None:
        ctx = _context(timeout=2.5, strategy=_Strategy(timedelta(hours=1)))
        assert ctx.on_failure(_unavailable()) == pytest.approx(2.5)

    def test_is_not_asked_about_a_failure_that_does_not_retry(self) -> None:
        strategy = _Strategy(timedelta(seconds=1))
        with pytest.raises(DocumentNotFoundException):
            _context(strategy=strategy).on_failure(_not_found())
        assert strategy.asked == []

    def test_identifier_is_per_request(self) -> None:
        assert _context().identifier != _context().identifier

    @pytest.mark.parametrize('op, readonly, idempotent', [
        (Op.Exists, None, True),
        (Op.Upsert, None, False),
        (Op.Query, True, True),
        (Op.Query, False, False),
    ])
    def test_a_read_only_request_is_idempotent(self, op: Op, readonly: bool | None, idempotent: bool) -> None:
        assert _context(op, readonly=readonly).idempotent is idempotent


class BackoffUnitTests:

    @pytest.mark.parametrize('attempts, ms', [(0, 1), (1, 10), (2, 50), (3, 100), (4, 500), (5, 1000), (50, 1000)])
    def test_controlled_backoff(self, attempts: int, ms: int) -> None:
        assert controlled_backoff(attempts) == timedelta(milliseconds=ms)

    @pytest.mark.parametrize('op', [Op.Get, Op.Upsert])
    @pytest.mark.parametrize('reason', list(RetryReason))
    def test_best_effort_retries_every_reason(self, reason: RetryReason, op: Op) -> None:
        ctx = _context(op)
        assert BestEffortRetryStrategy().retry_after(ctx, reason) == RetryAction(controlled_backoff(0))

    @pytest.mark.parametrize('reason', list(RetryReason))
    def test_no_reason_always_retries(self, reason: RetryReason) -> None:
        assert reason.always_retry is False

    @pytest.mark.parametrize('reason', list(RetryReason))
    def test_every_reason_allows_non_idempotent_retry(self, reason: RetryReason) -> None:
        assert reason.allows_non_idempotent_retry is True

    def test_do_not_retry(self) -> None:
        assert RetryAction.do_not_retry().delay is None
