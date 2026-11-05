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

import importlib
import logging
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from couchbase.exceptions import (TransactionCommitAmbiguous,
                                  TransactionExpired,
                                  TransactionFailed,
                                  TransactionOperationFailed)
from couchbase.transactions import TransactionResult
from couchbase.transactions.transaction_get_result import TransactionGetResult
from couchbase.transactions.transactions import AttemptContext, Transactions
from couchbase.transcoder import (JSONTranscoder,
                                  RawBinaryTranscoder,
                                  Transcoder)
from tests.test_features import TestProtocol

if TYPE_CHECKING:
    from couchbase.common._interfaces._transactions import TransactionsInterface


class _UndecodableTranscoder(Transcoder):
    # Raises on decode, so a test that reads a result through it shows the body was never decoded.

    def encode_value(self, value: Any) -> tuple[bytes, int]:
        raise NotImplementedError

    def decode_value(self, value: bytes, flags: int | None) -> Any:
        raise AssertionError('decoded')


class _FakeAttempt:
    # `run` is control flow over the attempt primitives, so a fake is not a shortcut around a
    # cluster: a server cannot be made to ask for another attempt, or to fail one commit and not
    # the next, on demand.

    def __init__(self, commits: list[Any]) -> None:
        self._commits = commits
        self.calls: list[str] = []

    def begin_attempt(self) -> None:
        self.calls.append('begin_attempt')

    def rollback(self) -> None:
        self.calls.append('rollback')

    def commit(self) -> dict[str, Any] | None:
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


def _run(impl: _FakeTransactions, logic: Any, *args: Any, **kwargs: Any) -> TransactionResult:
    return Transactions(cast('TransactionsInterface', impl)).run(logic, *args, **kwargs)


class TransactionsRunUnitTests:
    # No cb_env and no TEST_MANIFEST: the loop is the facade's on every protocol, driven here
    # through a fake impl.  The acouchbase counterpart is hand-written, its loop awaiting each
    # primitive and the caller's logic.

    def test_a_committed_attempt_returns_its_result(self) -> None:
        impl = _FakeTransactions({'transaction_id': 'txn-1', 'unstaging_complete': True})
        contexts: list[AttemptContext] = []
        result = _run(impl, contexts.append)
        assert isinstance(result, TransactionResult)
        assert result.transaction_id == 'txn-1'
        assert result.unstaging_complete is True
        assert impl.attempt.calls == ['begin_attempt', 'commit']
        assert len(contexts) == 1
        assert isinstance(contexts[0], AttemptContext)

    def test_an_empty_commit_runs_another_attempt(self) -> None:
        impl = _FakeTransactions(None, {'transaction_id': 'txn-1'})
        contexts: list[AttemptContext] = []
        _run(impl, contexts.append)
        assert impl.attempt.calls == ['begin_attempt', 'commit', 'begin_attempt', 'commit']
        assert len(contexts) == 2
        assert contexts[0] is contexts[1]

    # A commit that raises is the core's final answer: it has already rolled back or retried
    # whatever it could, and an ambiguous commit may have landed, so running the logic again
    # could apply it twice.
    @pytest.mark.parametrize('error', [TransactionCommitAmbiguous('ambiguous'),
                                       TransactionExpired('expired'),
                                       TransactionFailed('failed')])
    def test_a_failed_commit_is_raised_without_another_attempt(self, error: Exception) -> None:
        impl = _FakeTransactions(error, {'transaction_id': 'txn-1'})
        contexts: list[AttemptContext] = []
        with pytest.raises(type(error)) as exc_info:
            _run(impl, contexts.append)
        assert exc_info.value is error
        assert impl.attempt.calls == ['begin_attempt', 'commit']
        assert len(contexts) == 1

    # A failed operation is recorded against the attempt, so the loop commits rather than rolling
    # back, and the impl decides between another attempt and a final error.
    def test_a_failed_operation_is_handed_to_the_commit(self) -> None:
        impl = _FakeTransactions(None, {'transaction_id': 'txn-1'})
        failures = [TransactionOperationFailed('cas mismatch')]

        def logic(ctx: AttemptContext) -> None:
            if failures:
                raise failures.pop()

        result = _run(impl, logic)
        assert result.transaction_id == 'txn-1'
        assert impl.attempt.calls == ['begin_attempt', 'commit', 'begin_attempt', 'commit']

    def test_a_failed_operation_the_commit_fails_is_the_cause(self) -> None:
        impl = _FakeTransactions(TransactionFailed('failed'))
        cause = TransactionOperationFailed('not retryable')

        def logic(ctx: AttemptContext) -> None:
            raise cause

        with pytest.raises(TransactionFailed) as exc_info:
            _run(impl, logic)
        assert exc_info.value.inner_cause is cause
        assert exc_info.value.message == 'failed'
        assert impl.attempt.calls == ['begin_attempt', 'commit']

    def test_a_failed_operation_the_commit_expires_is_raised_as_it_is(self) -> None:
        expired = TransactionExpired('expired')
        impl = _FakeTransactions(expired)

        def logic(ctx: AttemptContext) -> None:
            raise TransactionOperationFailed('cas mismatch')

        with pytest.raises(TransactionExpired) as exc_info:
            _run(impl, logic)
        assert exc_info.value is expired
        assert impl.attempt.calls == ['begin_attempt', 'commit']

    def test_logic_that_raises_rolls_back_and_fails_the_transaction(self) -> None:
        impl = _FakeTransactions()
        cause = ValueError('from the logic')

        def logic(ctx: AttemptContext) -> None:
            raise cause

        with pytest.raises(TransactionFailed) as exc_info:
            _run(impl, logic)
        assert exc_info.value.inner_cause is cause
        assert exc_info.value.__suppress_context__ is True
        assert impl.attempt.calls == ['begin_attempt', 'rollback']

    def test_an_expired_transaction_rolls_back_and_is_raised_as_it_is(self) -> None:
        impl = _FakeTransactions()
        expired = TransactionExpired('expired')

        def logic(ctx: AttemptContext) -> None:
            raise expired

        with pytest.raises(TransactionExpired) as exc_info:
            _run(impl, logic)
        assert exc_info.value is expired
        assert impl.attempt.calls == ['begin_attempt', 'rollback']

    # Regression guard (PYCBC-1978): an interrupt is not an Exception, and without the rollback the
    # staged mutations keep their documents blocked to every other transaction.
    def test_an_interrupted_attempt_rolls_back_and_is_raised_as_it_is(self) -> None:
        impl = _FakeTransactions()

        def logic(ctx: AttemptContext) -> None:
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            _run(impl, logic)
        assert impl.attempt.calls == ['begin_attempt', 'rollback']

    def test_the_options_and_keywords_reach_the_impl_once(self) -> None:
        impl = _FakeTransactions(None, {'transaction_id': 'txn-1'})
        options = object()
        _run(impl, lambda ctx: None, options, extra='value')
        assert impl.begun == [(options, {'extra': 'value'})]


class TransactionGetResultUnitTests:
    # The result decodes the body the binding hands it, so a dict in the binding's shape stands in
    # for a server here as it does for the attempt above.

    @pytest.mark.parametrize('body, flags, transcoder, expected',
                             [(b'', 3 << 24, RawBinaryTranscoder(), b''),
                              (b'0', 2 << 24, JSONTranscoder(), 0),
                              (b'{}', 2 << 24, JSONTranscoder(), {})],
                             ids=['empty binary', 'zero', 'empty object'])
    def test_a_falsy_body_reads_back_as_itself(self, body: bytes, flags: int, transcoder: Any, expected: Any) -> None:
        result = TransactionGetResult(cast(Any, {'id': 'key', 'cas': 1, 'value': (body, flags)}), transcoder)

        assert result.content_as[type(expected)] == expected

    def test_repr_prints_the_encoded_value_without_decoding_it(self) -> None:
        result = TransactionGetResult(cast(Any, {'id': 'key', 'cas': 1, 'value': (b'{"a":1}', 2 << 24)}),
                                      _UndecodableTranscoder())

        assert repr(result) == "TransactionGetResult(id='key', cas=1, value=b'{\"a\":1}', flags=33554432)"


# The log under test is classic's.
@pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)
class TransactionsDebugLogUnitTests:

    def test_no_document_body_or_query_data_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        from couchbase.protocol.classic._transactions_impl import _execute

        req = {'value': (b'{"secret":1}', 0),
               'statement': 'SELECT 1',
               'options': {'positional_parameters': ['secret'],
                           'named_parameters': {'p': 'secret'},
                           'raw': {'r': 'secret'},
                           'metrics': True}}

        def transaction_query_op(**_: Any) -> bytes:
            return b'{"results":[{"secret":1}]}'

        with caplog.at_level(logging.DEBUG, logger='couchbase.transactions'):
            _execute(transaction_query_op, req)
        assert 'secret' not in caplog.text
        assert '<12 bytes>' in caplog.text
        assert '<26 bytes>' in caplog.text
        assert "'metrics': True" in caplog.text

    @pytest.mark.parametrize('module', ['transaction_get_result'])
    def test_a_result_module_logs_under_couchbase_transactions(self, module: str) -> None:
        imported = importlib.import_module(f'couchbase.common.transactions.{module}')
        assert imported._log.name == f'couchbase.transactions.{module}'
