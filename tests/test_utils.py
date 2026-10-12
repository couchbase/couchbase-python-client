#  Copyright 2016-2024. Couchbase, Inc.
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

"""
Consolidated retry / sleep utilities for tests/.

RetryHelper provides three families of class methods:
  - Sync  (try_n_times, try_n_times_till_exception, wait_until, sleep)
  - Async (try_n_times_async, try_n_times_till_exception_async, wait_until_async, sleep_async)
  - Twisted (try_n_times_twisted, try_n_times_till_exception_twisted, wait_until_twisted)

The environment classes (TestEnvironment, AsyncTestEnvironment,
TwistedTestEnvironment) expose their own try_n_times() / sleep() as thin delegates, so a
suite reaches everything it needs through `cb_env` and never imports this module.

ConsistencyChecker uses time.sleep directly in a deadline-polling loop; it is
intentionally NOT wired through RetryHelper.

Every helper here raises on total failure rather than returning a sentinel.  A helper that
is silent takes `setup()` down with it: pytest's free "ERROR at setup" versus "FAILED"
distinction becomes unreachable, so a provisioning failure arrives as N assertion failures
spread across the suite instead of one error naming the cause.

`wait_until` is the polling primitive.  Roughly 35 call sites name try_n_times but mean
"poll until this condition holds", and a swallowed wait is silent by construction.  A
predicate that raises counts as "not yet", so the not-found exception those sites rely on
keeps working, and a timeout reports the condition by name.

Deliberately not here: `retry(op, on=...)` with a transient/deterministic exception
classification.  It cannot be added on its own.  Classifying exceptions means failing fast on
anything not known to be transient, and the polling call sites reach their condition
*through* exceptions such a classification would call deterministic: `check_index` polls
until `QueryIndexNotFoundException` stops arriving.  A strict try_n_times therefore breaks
every one of those sites unless they move to `wait_until` in the same change.  What is here
is the half that is correct on its own: failures stop being silent.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from typing import Any, NoReturn

from tests.skips import env_degraded


async def _maybe_await(outcome: Any) -> Any:
    """Await *outcome* when it is awaitable, otherwise pass it straight back.

    Not every acouchbase call is a coroutine.  The accessors are deliberately synchronous:
    `cluster.bucket()`, `bucket.scope()` and `bucket.collection()` each return an object.
    An async helper that awaits unconditionally turns handing it one of those into
    `TypeError: object AsyncBucket can't be used in 'await' expression`, which the enclosing
    `except Exception` then retries as if the server were at fault.  The caller waits out
    every attempt and is finally told the operation exhausted its retries, which points at
    the cluster instead of at the call.
    """
    return await outcome if inspect.isawaitable(outcome) else outcome


def _wait_timeout(condition: str, timeout: float, last_error: Exception | None,
                  note: str | None) -> NoReturn:
    """Report a wait_until deadline, shared by the three transport variants.

    `last_error` is omitted rather than rendered when there was none, so the field means "the
    predicate raised this on its final poll" and not "there may or may not have been an error".
    """
    fields: dict[str, Any] = {'condition': condition, 'timeout_s': timeout}
    if last_error is not None:
        fields['last_error'] = f'{type(last_error).__name__}: {last_error}'
    env_degraded(stage='wait-until',
                 note=note or 'condition never became true',
                 **fields)


def _describe(func: Callable[..., Any]) -> str:
    """Name a retried callable in a way that is useful in a failure log.

    `repr()` of a bound method or a nested function is mostly a hex address, which tells a
    reader nothing about which operation is failing.  `__qualname__` carries the class and
    the enclosing function, so a closure defined inside a test reports as
    `BucketManagementTestSuite.test_change_expiry.<locals>.get_bucket_expiry_equal`.
    """
    return getattr(func, '__qualname__', None) or repr(func)


class RetryHelper:
    """Consolidated retry/sleep helpers for all three transport variants."""

    # ------------------------------------------------------------------
    # Sync
    # ------------------------------------------------------------------

    @classmethod
    def try_n_times(
        cls,
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Call *func* up to *num_times*, sleeping *seconds_between* seconds on each failure.

        Returns the first successful return value.  Re-raises the last exception when every
        attempt fails, rather than returning None.  The original exception is re-raised
        as-is so that callers which catch a specific SDK type still work; the attempt history
        is on stdout immediately above it.

        Does not sleep after the final attempt: that would add seconds_between to the
        latency of every total failure for no benefit.
        """
        last_exc: Exception | None = None
        for attempt in range(1, num_times + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt < num_times:
                    print(f'{_describe(func)} attempt {attempt}/{num_times} failed: '
                          f'{type(exc).__name__}: {exc}; sleeping {seconds_between}s...')
                    time.sleep(seconds_between)
                else:
                    print(f'{_describe(func)} attempt {attempt}/{num_times} failed: '
                          f'{type(exc).__name__}: {exc}; giving up.')
        if last_exc is None:
            # Only reachable when num_times < 1, i.e. the caller asked for zero attempts and
            # would otherwise have received a silent None as though the operation succeeded.
            env_degraded(stage='retry',
                         op=_describe(func),
                         attempts=num_times,
                         note='retry helper was asked for fewer than one attempt')
        raise last_exc

    @classmethod
    def try_n_times_till_exception(
        cls,
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        expected_exceptions: tuple[type[Exception], ...] = (Exception,),
        raise_exception: bool = False,
        **kwargs: Any,
    ) -> None:
        """Call *func* up to *num_times*, expecting it to eventually raise one of
        *expected_exceptions*.  Sleeps *seconds_between* seconds between calls.

        Once an expected exception is raised, the method returns (or re-raises if
        *raise_exception* is True).  Any unexpected exception propagates immediately.

        Raises when the expected exception never arrives.  This is almost always a teardown
        assertion ("the bucket is gone", "the collection is gone"), and returning normally
        would let a resource that never went away report as a clean teardown, leaving the
        next test to fail on the leftover.  Teardown owns its postconditions.
        """
        for _ in range(num_times):
            try:
                func(*args, **kwargs)
                time.sleep(seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                return
            except Exception:
                raise
        env_degraded(stage='wait-till-exception',
                     op=_describe(func),
                     expected='|'.join(e.__name__ for e in expected_exceptions),
                     attempts=num_times,
                     note='expected exception never arrived; the condition never became true')

    @classmethod
    def wait_until(cls,
                   predicate: Callable[[], Any],
                   *,
                   condition: str,
                   timeout: float = 30,
                   interval: float = 1,
                   note: str | None = None) -> None:
        """Poll *predicate* until it returns truthy, or raise naming *condition*.

        This is the primitive for an observable postcondition, as distinct from retrying an
        operation.  Retries are for operations whose postcondition you cannot observe; if you
        can observe it, perform the operation once and poll for the effect.

        A predicate that raises counts as "not yet", so a caller can poll straight through
        the not-found exception that means the resource has not appeared.  The last such
        exception is reported if the deadline passes, since otherwise a predicate that raises
        every single time is indistinguishable from one that simply stayed false.
        """
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while True:
            try:
                if predicate():
                    return
                last_error = None
            except Exception as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                _wait_timeout(condition, timeout, last_error, note)
            time.sleep(interval)

    @classmethod
    def sleep(cls, num_seconds: float) -> None:
        time.sleep(num_seconds)

    # ------------------------------------------------------------------
    # Async
    # ------------------------------------------------------------------

    @classmethod
    async def try_n_times_async(
        cls,
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Async variant of try_n_times: awaits *func* and sleeps with asyncio.

        Re-raises the last exception on total failure; see the sync variant.
        """
        last_exc: Exception | None = None
        for attempt in range(1, num_times + 1):
            try:
                return await _maybe_await(func(*args, **kwargs))
            except Exception as exc:
                last_exc = exc
                if attempt < num_times:
                    print(f'{_describe(func)} attempt {attempt}/{num_times} failed: '
                          f'{type(exc).__name__}: {exc}; sleeping {seconds_between}s...')
                    await asyncio.sleep(seconds_between)
                else:
                    print(f'{_describe(func)} attempt {attempt}/{num_times} failed: '
                          f'{type(exc).__name__}: {exc}; giving up.')
        if last_exc is None:
            env_degraded(stage='retry',
                         op=_describe(func),
                         attempts=num_times,
                         note='retry helper was asked for fewer than one attempt')
        raise last_exc

    @classmethod
    async def try_n_times_till_exception_async(
        cls,
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        expected_exceptions: tuple[type[Exception], ...] = (Exception,),
        raise_exception: bool = False,
        **kwargs: Any,
    ) -> None:
        """Async variant of try_n_times_till_exception.

        Raises when the expected exception never arrives; see the sync variant.
        """
        for _ in range(num_times):
            try:
                await _maybe_await(func(*args, **kwargs))
                await asyncio.sleep(seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                return
            except Exception:
                raise
        env_degraded(stage='wait-till-exception',
                     op=_describe(func),
                     expected='|'.join(e.__name__ for e in expected_exceptions),
                     attempts=num_times,
                     note='expected exception never arrived; the condition never became true')

    @classmethod
    async def wait_until_async(cls,
                               predicate: Callable[[], Any],
                               *,
                               condition: str,
                               timeout: float = 30,
                               interval: float = 1,
                               note: str | None = None) -> None:
        """Async variant of wait_until.  *predicate* may be sync or return an awaitable."""
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while True:
            try:
                outcome = await _maybe_await(predicate())
                if outcome:
                    return
                last_error = None
            except Exception as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                _wait_timeout(condition, timeout, last_error, note)
            await asyncio.sleep(interval)

    @classmethod
    async def sleep_async(cls, num_seconds: float) -> None:
        await asyncio.sleep(num_seconds)

    # ------------------------------------------------------------------
    # Twisted
    # ------------------------------------------------------------------

    @classmethod
    def try_n_times_twisted(
        cls,
        run_in_reactor_thread: Callable[..., Any],
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        is_deferred: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Twisted variant of try_n_times.

        *run_in_reactor_thread* is the reactor-thread bridge from
        ``tests/environments/twisted_reactor.py``.

        When *is_deferred* is True the call is dispatched through
        ``run_in_reactor_thread``; when False the function is called directly
        (used for setup/teardown helpers that are themselves synchronous).

        Re-raises the last exception on total failure; see the sync variant.
        """
        last_exc: Exception | None = None
        for attempt in range(1, num_times + 1):
            try:
                if is_deferred:
                    return run_in_reactor_thread(func, *args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt < num_times:
                    print(f'{_describe(func)} attempt {attempt}/{num_times} failed: '
                          f'{type(exc).__name__}: {exc}; sleeping {seconds_between}s...')
                    run_in_reactor_thread(cls._deferred_sleep, seconds_between)
                else:
                    print(f'{_describe(func)} attempt {attempt}/{num_times} failed: '
                          f'{type(exc).__name__}: {exc}; giving up.')
        if last_exc is None:
            env_degraded(stage='retry',
                         op=_describe(func),
                         attempts=num_times,
                         note='retry helper was asked for fewer than one attempt')
        raise last_exc

    @classmethod
    def try_n_times_till_exception_twisted(
        cls,
        run_in_reactor_thread: Callable[..., Any],
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        is_deferred: bool = True,
        expected_exceptions: tuple[type[Exception], ...] = (Exception,),
        raise_exception: bool = False,
        **kwargs: Any,
    ) -> None:
        """Twisted variant of try_n_times_till_exception.

        Raises when the expected exception never arrives; see the sync variant.
        """
        for _ in range(num_times):
            try:
                if is_deferred:
                    run_in_reactor_thread(func, *args, **kwargs)
                else:
                    func(*args, **kwargs)
                run_in_reactor_thread(cls._deferred_sleep, seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                return
            except Exception:
                raise
        env_degraded(stage='wait-till-exception',
                     op=_describe(func),
                     expected='|'.join(e.__name__ for e in expected_exceptions),
                     attempts=num_times,
                     note='expected exception never arrived; the condition never became true')

    @classmethod
    def wait_until_twisted(cls,
                           run_in_reactor_thread: Callable[..., Any],
                           predicate: Callable[[], Any],
                           *,
                           condition: str,
                           timeout: float = 30,
                           interval: float = 1,
                           is_deferred: bool = True,
                           note: str | None = None) -> None:
        """Twisted variant of wait_until.

        *predicate* is dispatched through *run_in_reactor_thread* when *is_deferred*, matching
        the convention the other twisted helpers use.
        """
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while True:
            try:
                outcome = run_in_reactor_thread(predicate) if is_deferred else predicate()
                if outcome:
                    return
                last_error = None
            except Exception as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                _wait_timeout(condition, timeout, last_error, note)
            run_in_reactor_thread(cls._deferred_sleep, interval)

    @staticmethod
    def _deferred_sleep(sleep_seconds: float) -> Any:
        """Return a Twisted Deferred that resolves after *sleep_seconds*.

        Must be called via run_in_reactor_thread so that the calling thread
        blocks until the Deferred fires.
        """
        from twisted.internet import defer, reactor  # noqa: PLC0415
        d: defer.Deferred = defer.Deferred()
        # The reactor is installed at runtime and typed as a module, so callLater is
        # invisible to the checker.  Structural, permanent, no ticket.
        # A cast to IReactorTime does not work either: zope declares interface
        # methods without self, so mypy reads `delay` as the receiver and every argument
        # shifts by one.  Not our code and not our stub.
        reactor.callLater(sleep_seconds, d.callback, '')  # type: ignore[attr-defined]
        return d
