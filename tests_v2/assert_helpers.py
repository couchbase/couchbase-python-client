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

"""Context-aware assertion helpers for tests_v2/ suites.

Bare `assert` in a suite module is rewritten by pytest and already prints both sides, so a
message earns its place only where the values are not what the failure is about: what a
timed-out join or a second live instance means is not in the comparison.  The other case is an
assert pytest does not rewrite.  Nothing calls `pytest.register_assert_rewrite`, so an assert
anywhere in `tests_v2/` outside a suite module reports only `AssertionError`, which is why
every helper below carries its own message.

`cb_assert_eventually` additionally covers *when*.  It exists for one recurring bug: an
assertion evaluated at a fixed instant against state that becomes true asynchronously,
usually after a bare `sleep()` chosen to be "long enough".  That shape fails intermittently
and, worse, passes for the wrong reason on a fast machine.  The convention it establishes:

    A bare sleep() followed by an assertion is a defect.

Note the division of labour with `RetryHelper.wait_until`.  Both poll; they differ in what a
timeout *means*.  `wait_until` waits for the environment to become what the suite was promised
and reports env-degraded, because nothing about the product has been tested yet.
`cb_assert_eventually` waits for the product to do what the test claims it does, so a timeout
is an ordinary test failure.  Using the environment helper for a product assertion would
misfile a real bug as a broken cluster.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Container
from typing import TYPE_CHECKING, Any

import pytest

from couchbase.exceptions import DocumentNotFoundException

if TYPE_CHECKING:
    # Annotation only, and it has to stay that way: test_environment imports this module, so a
    # runtime import here closes the cycle.
    from tests_v2.environments.test_environment import SyncTestEnvironment


def cb_assert_eq(actual: Any, expected: Any, *, what: str, ctx: str = '') -> None:
    assert actual == expected, f'{what}: expected={expected!r} actual={actual!r}. {ctx}'.rstrip()


def cb_assert_isinstance(val: Any, typ: type | tuple[type, ...], *, what: str, ctx: str = '') -> None:
    expected = typ.__name__ if isinstance(typ, type) else ' | '.join(t.__name__ for t in typ)
    assert isinstance(val, typ), (
        f'{what}: expected type {expected!r} got {type(val).__name__!r} (value={val!r}). {ctx}'.rstrip()
    )


def cb_assert_gte(actual: Any, minimum: Any, *, what: str, ctx: str = '') -> None:
    assert actual >= minimum, f'{what}: expected>={minimum!r} actual={actual!r}. {ctx}'.rstrip()


def cb_assert_has_key(d: dict, key: str, *, ctx: str = '') -> None:
    assert key in d, f'Expected key {key!r} to be present. {ctx}'.rstrip()


def cb_assert_result(result: Any, *, expected_type: type | None = None, ctx: str = '') -> None:
    """Assert a Couchbase result object is valid with meaningful context."""
    assert result is not None, f'Result is None. {ctx}'.rstrip()
    if expected_type is not None:
        cb_assert_isinstance(result, expected_type, what='result type', ctx=ctx)


def cb_assert_collection_contains(collection: Container, key: Any, *, ctx: str = '') -> None:
    """Assert key is present in collection (dict, set, list, etc.) with failure context."""
    assert key in collection, f'Expected {key!r} to be present in {collection!r}. {ctx}'.rstrip()


def cb_assert_eventually(check: Callable[[], Any],
                         *,
                         what: str,
                         timeout: float = 30,
                         interval: float = 1,
                         tolerate: tuple[type[BaseException], ...] = (AssertionError,),
                         ctx: str = '') -> None:
    """Poll *check* until it passes, failing with a message that names the deadline.

    *check* passes when it returns without raising and does not return an explicitly falsy
    value.  That covers both shapes these call sites take: a predicate returning a bool, and a
    block of plain asserts that returns None.

    *tolerate* lists the exceptions that mean "not yet" rather than "wrong".  It defaults to
    AssertionError alone, so an unexpected SDK exception still surfaces immediately instead of
    being polled over for the full timeout.  Widen it deliberately, per call site, when the
    condition is reached *through* an exception, since a document that is not yet gone raises
    DocumentNotFoundException only once it is.

    Deliberately reports the elapsed budget on failure.  "assert failed" after a hidden
    sleep(5) tells a reader nothing about whether the wait was too short or the product is
    broken; "still false after 30s (polled every 1s)" distinguishes them.
    """
    deadline = time.monotonic() + timeout
    attempts = 0
    last_error: BaseException | None = None
    while True:
        attempts += 1
        try:
            outcome = check()
            if outcome is None or outcome:
                return
            last_error = None
        except tolerate as exc:
            last_error = exc
        if time.monotonic() >= deadline:
            break
        time.sleep(interval)

    detail = f' Last failure: {type(last_error).__name__}: {last_error}' if last_error else ''
    pytest.fail(
        f'{what}: still not true after {timeout}s '
        f'(polled every {interval}s, {attempts} attempts).{detail} {ctx}'.rstrip()
    )


def assert_all_expired(cb_env: SyncTestEnvironment,
                       keys: list[str],
                       *,
                       okay_key: str | None = None,
                       timeout: float = 15,
                       interval: float = 3,
                       ctx: str = '') -> None:
    """Poll until every key in *keys* is gone, except *okay_key*, which must remain.

    Sync-only, and a free function rather than a TestEnvironment method on purpose.  The body
    recognises expiry by catching DocumentNotFoundException out of a blocking get, and under
    acouchbase that get returns an unawaited coroutine which never raises, so every key would
    land in `present` and the check would silently invert.  As an inherited method that
    inversion has no diagnostic; as a function annotated SyncTestEnvironment, handing it an
    async environment is a type error the gate catches.

    *okay_key* has to be asserted *present*, not merely tolerated.  A check that accepts it
    whether its get succeeds or raises DocumentNotFoundException is unfalsifiable for the tests
    that need it: their whole point is that a per-key expiry of 0 overrides the global expiry
    and keeps that one document alive, so its survival is the single outcome such a check
    cannot distinguish.
    """
    expected_present = [okay_key] if okay_key is not None else []

    def _expired() -> None:
        missing, present = [], []
        for k in keys:
            try:
                cb_env.collection.get(k)
                present.append(k)
            except DocumentNotFoundException:
                missing.append(k)
        # AssertionError is cb_assert_eventually's default `tolerate`, so a not-yet-expired
        # document polls again while an unexpected SDK exception still surfaces at once.
        assert sorted(present) == sorted(expected_present), (
            f'expected only {expected_present} to remain, found {sorted(present)} present '
            f'and {sorted(missing)} expired'
        )

    cb_assert_eventually(_expired,
                         what=f'{len(keys) - len(expected_present)} document(s) expired',
                         timeout=timeout,
                         interval=interval,
                         ctx=ctx)
