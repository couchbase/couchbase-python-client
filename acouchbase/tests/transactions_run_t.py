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

# Hand-written and neither generated nor merged.  The sync file of the same name drives the sync
# `Transactions.run` through a fake impl whose primitives return; this one drives acouchbase's,
# whose loop awaits each primitive and the caller's logic, so its fake returns awaitables.
from __future__ import annotations

import asyncio
import threading
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from acouchbase.transactions.transactions import AttemptContext, Transactions
from couchbase.exceptions import (TransactionCommitAmbiguous,
                                  TransactionExpired,
                                  TransactionFailed,
                                  TransactionOperationFailed)
from couchbase.transactions import TransactionResult
from tests.test_features import TestProtocol

if TYPE_CHECKING:
    from couchbase.common._interfaces._transactions import AsyncTransactionsInterface


class _FakeAttempt:
    # `run` is control flow over the attempt primitives, so a fake is not a shortcut around a
    # cluster: a server cannot be made to ask for another attempt, or to fail one commit and not
    # the next, on demand.

    def __init__(self, commits: list[Any]) -> None:
        self._commits = commits
        self.calls: list[str] = []

    async def begin_attempt(self) -> None:
        self.calls.append('begin_attempt')

    async def rollback(self) -> None:
        self.calls.append('rollback')

    async def commit(self) -> dict[str, Any] | None:
        self.calls.append('commit')
        outcome = self._commits.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast('dict[str, Any] | None', outcome)


class _FakeTransactions:

    def __init__(self, *commits: Any) -> None:
        self.attempt = _FakeAttempt(list(commits))
        self.begun: list[tuple[Any, dict[str, Any]]] = []

    def begin_transaction(self, transaction_options: Any = None, **kwargs: Any) -> _FakeAttempt:
        self.begun.append((transaction_options, kwargs))
        return self.attempt

    def close(self) -> None:
        pass


async def _run(impl: _FakeTransactions, logic: Any, *args: Any, **kwargs: Any) -> TransactionResult:
    return await Transactions(cast('AsyncTransactionsInterface', impl)).run(logic, *args, **kwargs)


def _recording(contexts: list[AttemptContext]) -> Any:
    async def logic(ctx: AttemptContext) -> None:
        contexts.append(ctx)
    return logic


async def _no_op(ctx: AttemptContext) -> None:
    pass


class TransactionsRunUnitTests:

    @pytest.mark.asyncio
    async def test_a_committed_attempt_returns_its_result(self) -> None:
        impl = _FakeTransactions({'transaction_id': 'txn-1', 'unstaging_complete': True})
        contexts: list[AttemptContext] = []
        result = await _run(impl, _recording(contexts))
        assert isinstance(result, TransactionResult)
        assert result.transaction_id == 'txn-1'
        assert result.unstaging_complete is True
        assert impl.attempt.calls == ['begin_attempt', 'commit']
        assert len(contexts) == 1
        assert isinstance(contexts[0], AttemptContext)

    @pytest.mark.asyncio
    async def test_an_empty_commit_runs_another_attempt(self) -> None:
        impl = _FakeTransactions(None, {'transaction_id': 'txn-1'})
        contexts: list[AttemptContext] = []
        await _run(impl, _recording(contexts))
        assert impl.attempt.calls == ['begin_attempt', 'commit', 'begin_attempt', 'commit']
        assert len(contexts) == 2
        assert contexts[0] is contexts[1]

    @pytest.mark.asyncio
    @pytest.mark.parametrize('error', [TransactionCommitAmbiguous('ambiguous'),
                                       TransactionExpired('expired'),
                                       TransactionFailed('failed')])
    async def test_a_failed_commit_is_raised_without_another_attempt(self, error: Exception) -> None:
        impl = _FakeTransactions(error, {'transaction_id': 'txn-1'})
        contexts: list[AttemptContext] = []
        with pytest.raises(type(error)) as exc_info:
            await _run(impl, _recording(contexts))
        assert exc_info.value is error
        assert impl.attempt.calls == ['begin_attempt', 'commit']
        assert len(contexts) == 1

    @pytest.mark.asyncio
    async def test_a_failed_operation_is_handed_to_the_commit(self) -> None:
        impl = _FakeTransactions(None, {'transaction_id': 'txn-1'})
        failures = [TransactionOperationFailed('cas mismatch')]

        async def logic(ctx: AttemptContext) -> None:
            if failures:
                raise failures.pop()

        result = await _run(impl, logic)
        assert result.transaction_id == 'txn-1'
        assert impl.attempt.calls == ['begin_attempt', 'commit', 'begin_attempt', 'commit']

    @pytest.mark.asyncio
    async def test_a_failed_operation_the_commit_fails_is_the_cause(self) -> None:
        impl = _FakeTransactions(TransactionFailed('failed'))
        cause = TransactionOperationFailed('not retryable')

        async def logic(ctx: AttemptContext) -> None:
            raise cause

        with pytest.raises(TransactionFailed) as exc_info:
            await _run(impl, logic)
        assert exc_info.value.inner_cause is cause
        assert exc_info.value.message == 'failed'
        assert impl.attempt.calls == ['begin_attempt', 'commit']

    @pytest.mark.asyncio
    async def test_a_failed_operation_the_commit_expires_is_raised_as_it_is(self) -> None:
        expired = TransactionExpired('expired')
        impl = _FakeTransactions(expired)

        async def logic(ctx: AttemptContext) -> None:
            raise TransactionOperationFailed('cas mismatch')

        with pytest.raises(TransactionExpired) as exc_info:
            await _run(impl, logic)
        assert exc_info.value is expired
        assert impl.attempt.calls == ['begin_attempt', 'commit']

    @pytest.mark.asyncio
    async def test_logic_that_raises_rolls_back_and_fails_the_transaction(self) -> None:
        impl = _FakeTransactions()
        cause = ValueError('from the logic')

        async def logic(ctx: AttemptContext) -> None:
            raise cause

        with pytest.raises(TransactionFailed) as exc_info:
            await _run(impl, logic)
        assert exc_info.value.inner_cause is cause
        assert exc_info.value.__suppress_context__ is True
        assert impl.attempt.calls == ['begin_attempt', 'rollback']

    @pytest.mark.asyncio
    async def test_an_expired_transaction_rolls_back_and_is_raised_as_it_is(self) -> None:
        impl = _FakeTransactions()
        expired = TransactionExpired('expired')

        async def logic(ctx: AttemptContext) -> None:
            raise expired

        with pytest.raises(TransactionExpired) as exc_info:
            await _run(impl, logic)
        assert exc_info.value is expired
        assert impl.attempt.calls == ['begin_attempt', 'rollback']

    # Regression guard (PYCBC-1978): a cancellation is not an Exception, and without the rollback the
    # staged mutations keep their documents blocked to every other transaction.
    @pytest.mark.asyncio
    async def test_a_cancelled_attempt_rolls_back_and_is_raised_as_it_is(self) -> None:
        impl = _FakeTransactions()
        started = asyncio.Event()

        async def logic(ctx: AttemptContext) -> None:
            started.set()
            await asyncio.Event().wait()

        task = asyncio.ensure_future(_run(impl, logic))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert impl.attempt.calls == ['begin_attempt', 'rollback']

    @pytest.mark.asyncio
    async def test_the_options_and_keywords_reach_the_impl_once(self) -> None:
        impl = _FakeTransactions(None, {'transaction_id': 'txn-1'})
        options = object()
        await _run(impl, _no_op, options, extra='value')
        assert impl.begun == [(options, {'extra': 'value'})]


# The impl under test is classic's.
@pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)
class AsyncAttemptContextImplUnitTests:

    @pytest.mark.asyncio
    @pytest.mark.parametrize('completion, value', [('callback', None), ('errback', RuntimeError('late'))])
    async def test_a_cancelled_operation_ignores_its_late_completion(self, completion: str, value: Any) -> None:
        # The binding completes an operation on its own thread whatever became of the task awaiting
        # it, so a completion can arrive after that task was cancelled.
        from acouchbase.protocol.classic._transactions_impl import AsyncAttemptContextImpl

        loop = asyncio.get_running_loop()
        loop_errors: list[dict[str, Any]] = []
        loop.set_exception_handler(lambda _, context: loop_errors.append(context))
        impl = cast(Any, object.__new__(AsyncAttemptContextImpl))
        impl._loop = loop
        req: dict[str, Any] = {}

        def op(**callbacks: Any) -> None:
            req.update(callbacks)

        ftr = impl._execute(op, {}, lambda res: res)
        ftr.cancel()
        late = threading.Thread(target=req[completion], args=(value,))
        late.start()
        late.join()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        loop.set_exception_handler(None)
        assert loop_errors == []
