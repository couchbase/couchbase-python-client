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

"""
Which event loop acouchbase resolves, and what it is allowed to do to it.

LoopValidator checks a loop against a set of methods it expects and, when the check fails, closes
that loop and installs a SelectorEventLoop as the thread default.  Closing is only ever safe for a
loop the SDK itself created, so a loop that is already running is returned untouched whatever the
check says.  A loop that is merely the thread default is still closed and replaced, which is what
txcouchbase relies on at import time.

Windows is where the distinction shows.  ProactorEventLoop is the platform default and fails the
check, so resolving from inside asyncio.run() used to close the caller's own loop.  These tests run
everywhere: the rejected loop is a real ProactorEventLoop on Windows and a stand-in with the same
verdict elsewhere.

Loop selection mutates the calling thread's default loop, so every case runs in a thread of its
own.  A fresh thread starts with no loop set, which is also the state the module-scope and
txcouchbase-import paths resolve from.

No live cluster is needed.  Nothing here constructs a cluster.
"""

import asyncio
import selectors
import sys
import threading

import pytest

from acouchbase import LoopValidator, get_event_loop


class _ProactorShapedEventLoop(asyncio.SelectorEventLoop):
    # ProactorEventLoop inherits these four from AbstractEventLoop rather than implementing them,
    # and inheriting them unchanged is what LoopValidator looks for.  Reinstating the abstract
    # versions reproduces that verdict on a loop that still runs, so the tests are not Windows-only.
    # The loop's own machinery uses the private _add_reader, so it is unaffected.
    add_reader = asyncio.AbstractEventLoop.add_reader
    remove_reader = asyncio.AbstractEventLoop.remove_reader
    add_writer = asyncio.AbstractEventLoop.add_writer
    remove_writer = asyncio.AbstractEventLoop.remove_writer


def _accepted_loop():
    return asyncio.SelectorEventLoop(selectors.DefaultSelector())


def _rejected_loop():
    if sys.platform == 'win32':
        return asyncio.ProactorEventLoop()
    return _ProactorShapedEventLoop(selectors.DefaultSelector())


def _in_fresh_thread(fn):
    outcome = {}

    def run():
        try:
            outcome['value'] = fn()
        except BaseException as ex:  # noqa: B036
            outcome['error'] = ex

    worker = threading.Thread(target=run)
    worker.start()
    worker.join()
    if 'error' in outcome:
        raise outcome['error']
    return outcome['value']


class AsyncLoopSelectionTestSuite:
    TEST_MANIFEST = [
        'test_accepted_loop_is_returned_when_not_running',
        'test_rejected_loop_is_closed_and_replaced_when_not_running',
        'test_rejected_loop_verdict',
        'test_running_loop_is_not_replaced_as_thread_default',
        'test_running_loop_is_returned_when_accepted',
        'test_running_loop_is_returned_when_rejected',
        'test_supplied_loop_is_returned_when_accepted',
    ]

    def test_rejected_loop_verdict(self):
        # The premise every other test rests on, asserted rather than assumed.
        def check():
            accepted, rejected = _accepted_loop(), _rejected_loop()
            try:
                return LoopValidator._is_valid_loop(accepted), LoopValidator._is_valid_loop(rejected)
            finally:
                accepted.close()
                rejected.close()

        accepted_verdict, rejected_verdict = _in_fresh_thread(check)
        assert accepted_verdict is True
        assert rejected_verdict is False

    def test_running_loop_is_returned_when_accepted(self):
        def check():
            loop = _accepted_loop()
            asyncio.set_event_loop(loop)

            async def resolve():
                return get_event_loop()

            try:
                resolved = loop.run_until_complete(resolve())
                return resolved is loop, loop.is_closed()
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        same_loop, closed_during_run = _in_fresh_thread(check)
        assert same_loop is True
        assert closed_during_run is False

    def test_running_loop_is_returned_when_rejected(self):
        def check():
            loop = _rejected_loop()
            asyncio.set_event_loop(loop)

            async def resolve():
                return get_event_loop()

            try:
                resolved = loop.run_until_complete(resolve())
                return resolved is loop, loop.is_closed()
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        same_loop, closed_during_run = _in_fresh_thread(check)
        assert same_loop is True
        assert closed_during_run is False

    def test_running_loop_is_not_replaced_as_thread_default(self):
        def check():
            loop = _rejected_loop()
            asyncio.set_event_loop(loop)

            async def resolve():
                get_event_loop()

            try:
                loop.run_until_complete(resolve())
                return asyncio.get_event_loop() is loop
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        assert _in_fresh_thread(check) is True

    def test_accepted_loop_is_returned_when_not_running(self):
        def check():
            loop = _accepted_loop()
            asyncio.set_event_loop(loop)
            try:
                resolved = get_event_loop()
                return resolved is loop, loop.is_closed()
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        same_loop, closed = _in_fresh_thread(check)
        assert same_loop is True
        assert closed is False

    def test_rejected_loop_is_closed_and_replaced_when_not_running(self):
        def check():
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

    def test_supplied_loop_is_returned_when_accepted(self):
        def check():
            loop = _accepted_loop()
            try:
                return get_event_loop(loop) is loop, loop.is_closed()
            finally:
                loop.close()

        same_loop, closed = _in_fresh_thread(check)
        assert same_loop is True
        assert closed is False


class ClassicAsyncLoopSelectionTests(AsyncLoopSelectionTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAsyncLoopSelectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAsyncLoopSelectionTests) if valid_test_method(meth)]
        manifest_invalid = set(AsyncLoopSelectionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
