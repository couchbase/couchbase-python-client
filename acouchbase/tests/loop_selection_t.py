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

# Hand-written and cluster-free: which event loop acouchbase resolves, and what it may do to it.
#
# LoopValidator checks a loop for the methods it expects and, when the check fails, closes that
# loop and installs a SelectorEventLoop as the thread default.  Closing is safe only for a loop the
# SDK could have created, so a loop that is already running is returned untouched whatever the
# check says.  A loop that is merely the thread default is still closed and replaced, which is what
# txcouchbase relies on at import time; txcouchbase/tests/loop_selection_t.py covers that side.
#
# Regression guard (PYCBC-1954): ProactorEventLoop is the Windows default and fails the check, so
# resolving from inside asyncio.run() closed the caller's own loop.  These tests run on every
# platform: the rejected loop is a real ProactorEventLoop on Windows and a stand-in with the same
# verdict elsewhere.
#
# Resolution mutates the calling thread's default loop, so every case runs in a thread of its own.
# A fresh thread starts with no loop set, which is also the state module scope and the txcouchbase
# import resolve from.
from __future__ import annotations

import asyncio
import selectors
import sys
import threading
from typing import (Callable,
                    Dict,
                    Tuple,
                    TypeVar)

from acouchbase import LoopValidator, get_event_loop

T = TypeVar('T')


class _ProactorShapedEventLoop(asyncio.SelectorEventLoop):
    # ProactorEventLoop inherits these four from AbstractEventLoop rather than implementing them,
    # and inheriting them unchanged is what LoopValidator looks for.  Reinstating the abstract
    # versions reproduces that verdict on a loop that still runs.  The loop's own machinery uses
    # the private _add_reader, so it is unaffected.
    add_reader = asyncio.AbstractEventLoop.add_reader
    remove_reader = asyncio.AbstractEventLoop.remove_reader
    add_writer = asyncio.AbstractEventLoop.add_writer
    remove_writer = asyncio.AbstractEventLoop.remove_writer


def _accepted_loop() -> asyncio.AbstractEventLoop:
    return asyncio.SelectorEventLoop(selectors.DefaultSelector())


def _rejected_loop() -> asyncio.AbstractEventLoop:
    if sys.platform == 'win32':
        return asyncio.ProactorEventLoop()
    return _ProactorShapedEventLoop(selectors.DefaultSelector())


def _in_fresh_thread(fn: Callable[[], T]) -> T:
    outcome: Dict[str, T] = {}
    errors: Dict[str, BaseException] = {}

    def run() -> None:
        try:
            outcome['value'] = fn()
        except BaseException as ex:  # noqa: B036
            errors['error'] = ex

    worker = threading.Thread(target=run)
    worker.start()
    worker.join()
    if 'error' in errors:
        raise errors['error']
    return outcome['value']


def _resolve_while_running(loop: asyncio.AbstractEventLoop) -> Tuple[bool, bool, bool]:
    # Whether the resolved loop is the running one, whether resolution closed it, and whether
    # it is still the thread default afterwards.
    asyncio.set_event_loop(loop)

    async def resolve() -> asyncio.AbstractEventLoop:
        return get_event_loop()

    try:
        resolved = loop.run_until_complete(resolve())
        return resolved is loop, loop.is_closed(), asyncio.get_event_loop() is loop
    finally:
        loop.close()
        asyncio.set_event_loop(None)


class AsyncLoopSelectionUnitTests:
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and this file is
    # one tree's own.

    def test_rejected_loop_verdict(self) -> None:
        # The premise every other test rests on, asserted rather than assumed.
        def check() -> Tuple[bool, bool]:
            accepted, rejected = _accepted_loop(), _rejected_loop()
            try:
                return (LoopValidator._is_valid_loop(accepted),
                        LoopValidator._is_valid_loop(rejected))
            finally:
                accepted.close()
                rejected.close()

        assert _in_fresh_thread(check) == (True, False)

    def test_running_loop_is_returned_when_accepted(self) -> None:
        same_loop, closed, _ = _in_fresh_thread(lambda: _resolve_while_running(_accepted_loop()))
        assert same_loop is True
        assert closed is False

    def test_running_loop_is_returned_when_rejected(self) -> None:
        same_loop, closed, _ = _in_fresh_thread(lambda: _resolve_while_running(_rejected_loop()))
        assert same_loop is True
        assert closed is False

    def test_running_loop_is_not_replaced_as_thread_default(self) -> None:
        _, _, still_default = _in_fresh_thread(lambda: _resolve_while_running(_rejected_loop()))
        assert still_default is True

    def test_accepted_loop_is_returned_when_not_running(self) -> None:
        def check() -> Tuple[bool, bool]:
            loop = _accepted_loop()
            asyncio.set_event_loop(loop)
            try:
                return get_event_loop() is loop, loop.is_closed()
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        assert _in_fresh_thread(check) == (True, False)

    def test_rejected_loop_is_closed_and_replaced_when_not_running(self) -> None:
        def check() -> Tuple[bool, bool, bool, bool]:
            loop = _rejected_loop()
            asyncio.set_event_loop(loop)
            try:
                resolved = get_event_loop()
                return (resolved is not loop,
                        loop.is_closed(),
                        isinstance(resolved, asyncio.SelectorEventLoop),
                        asyncio.get_event_loop() is resolved)
            finally:
                if not loop.is_closed():
                    loop.close()
                asyncio.set_event_loop(None)

        replaced, original_closed, is_selector, installed = _in_fresh_thread(check)
        assert replaced is True
        assert original_closed is True
        assert is_selector is True
        assert installed is True

    def test_supplied_loop_is_returned_when_accepted(self) -> None:
        def check() -> Tuple[bool, bool]:
            loop = _accepted_loop()
            try:
                return get_event_loop(loop) is loop, loop.is_closed()
            finally:
                loop.close()

        assert _in_fresh_thread(check) == (True, False)
