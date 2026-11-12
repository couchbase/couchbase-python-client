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
# acouchbase runs on any loop, so resolution never checks, closes or replaces one.  It returns, in
# order, a supplied loop, the running loop, the thread's current loop, and otherwise a new loop it
# sets as the current one.  A cluster accepts a supplied loop or takes the running one, and either
# must be running.  txcouchbase's own requirement is covered in txcouchbase/tests/loop_selection_t.py.
#
# Regression guard (PYCBC-1954): resolving from inside asyncio.run() closed the caller's own loop
# on Windows.  Every case runs on every platform: the loop Twisted cannot drive is a real
# ProactorEventLoop on Windows and a stand-in elsewhere, and acouchbase must accept both.
#
# Resolution can set the calling thread's current loop, so every case runs in a thread of its own.
# A fresh thread starts with no loop set, which is also the state module scope starts from.
from __future__ import annotations

import asyncio
import inspect
import selectors
import sys
import threading
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

import pytest

from acouchbase import (_current_event_loop,
                        _get_event_loop,
                        couchbase_loop_factory,
                        get_event_loop)
from acouchbase.cluster import AsyncCluster
from couchbase.auth import PasswordAuthenticator
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.options import ClusterOptions
from tests.environments.test_environment import TestProtocol

if TYPE_CHECKING:
    from couchbase.common._interfaces._cluster import AsyncClusterInterface

T = TypeVar('T')

CONNSTR = 'couchbase://localhost'


class _ProactorShapedEventLoop(asyncio.SelectorEventLoop):
    # ProactorEventLoop inherits these four from AbstractEventLoop rather than implementing them.
    # Reinstating the abstract versions reproduces that on a loop that still runs.  The loop's own
    # machinery uses the private _add_reader, so it is unaffected.
    add_reader = asyncio.AbstractEventLoop.add_reader
    remove_reader = asyncio.AbstractEventLoop.remove_reader
    add_writer = asyncio.AbstractEventLoop.add_writer
    remove_writer = asyncio.AbstractEventLoop.remove_writer


def _selector_loop() -> asyncio.AbstractEventLoop:
    return asyncio.SelectorEventLoop(selectors.DefaultSelector())


def _proactor_loop() -> asyncio.AbstractEventLoop:
    if sys.platform == 'win32':
        return asyncio.ProactorEventLoop()
    return _ProactorShapedEventLoop(selectors.DefaultSelector())


LOOP_FACTORIES = [pytest.param(_selector_loop, id='selector'), pytest.param(_proactor_loop, id='proactor')]


def _in_fresh_thread(fn: Callable[[], T]) -> T:
    outcome: dict[str, T] = {}
    errors: dict[str, BaseException] = {}

    def run() -> None:
        try:
            # Any warning fails the case, asyncio's own included: 3.12 and 3.13 warn when
            # asyncio.get_event_loop() creates a loop, which resolution must never let it do.
            with warnings.catch_warnings():
                warnings.simplefilter('error')
                outcome['value'] = fn()
        except BaseException as ex:  # noqa: B036
            errors['error'] = ex
        finally:
            asyncio.set_event_loop(None)

    worker = threading.Thread(target=run)
    worker.start()
    worker.join()
    if 'error' in errors:
        raise errors['error']
    return outcome['value']


class AsyncLoopSelectionUnitTests:
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and this file is
    # one tree's own.

    @pytest.mark.parametrize('make_loop', LOOP_FACTORIES)
    def test_supplied_loop_is_returned(self, make_loop: Callable[[], asyncio.AbstractEventLoop]) -> None:
        def check() -> tuple[bool, bool]:
            loop = make_loop()
            try:
                return _get_event_loop(loop) is loop, loop.is_closed()
            finally:
                loop.close()

        assert _in_fresh_thread(check) == (True, False)

    @pytest.mark.parametrize('make_loop', LOOP_FACTORIES)
    def test_running_loop_is_returned(self, make_loop: Callable[[], asyncio.AbstractEventLoop]) -> None:
        def check() -> tuple[bool, bool, bool]:
            loop = make_loop()
            asyncio.set_event_loop(loop)

            async def resolve() -> asyncio.AbstractEventLoop:
                return _get_event_loop()

            try:
                resolved = loop.run_until_complete(resolve())
                return resolved is loop, loop.is_closed(), asyncio.get_event_loop() is loop
            finally:
                loop.close()

        assert _in_fresh_thread(check) == (True, False, True)

    @pytest.mark.parametrize('make_loop', LOOP_FACTORIES)
    def test_current_loop_is_returned(self, make_loop: Callable[[], asyncio.AbstractEventLoop]) -> None:
        def check() -> tuple[bool, bool]:
            loop = make_loop()
            asyncio.set_event_loop(loop)
            try:
                return _get_event_loop() is loop, loop.is_closed()
            finally:
                loop.close()

        assert _in_fresh_thread(check) == (True, False)

    def test_new_loop_is_created_and_set_when_there_is_none(self) -> None:
        def check() -> tuple[bool, bool]:
            resolved = _get_event_loop()
            try:
                return resolved.is_closed(), asyncio.get_event_loop() is resolved
            finally:
                resolved.close()

        assert _in_fresh_thread(check) == (False, True)

    def test_get_event_loop_is_deprecated(self) -> None:
        def check() -> bool:
            loop = _selector_loop()
            try:
                with pytest.warns(CouchbaseDeprecationWarning, match='get_event_loop'):
                    return get_event_loop(loop) is loop
            finally:
                loop.close()

        assert _in_fresh_thread(check) is True

    def test_couchbase_loop_factory_is_deprecated_and_sets_nothing(self) -> None:
        def check() -> tuple[bool, bool]:
            with pytest.warns(CouchbaseDeprecationWarning, match='couchbase_loop_factory'):
                loop = couchbase_loop_factory()
            try:
                return loop.is_closed(), _current_event_loop() is None
            finally:
                loop.close()

        assert _in_fresh_thread(check) == (False, True)

    def test_cluster_loop_is_keyword_only(self) -> None:
        for fn in (AsyncCluster.__init__, AsyncCluster.connect):
            assert inspect.signature(fn).parameters['loop'].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)
class AsyncClusterLoopUnitTests:
    # Classic only: nothing connects, which needs classic's skip_connect.

    @staticmethod
    def _unconnected_cluster(loop: asyncio.AbstractEventLoop | None = None) -> AsyncClusterInterface:
        from acouchbase.protocol.classic._cluster_impl import AsyncClusterImpl
        return AsyncClusterImpl(CONNSTR,
                                ClusterOptions(PasswordAuthenticator('Administrator', 'password')),
                                loop=loop,
                                skip_connect='TEST_SKIP_CONNECT')

    @pytest.mark.parametrize('make_loop', LOOP_FACTORIES)
    def test_cluster_uses_the_supplied_running_loop(self, make_loop: Callable[[], asyncio.AbstractEventLoop]) -> None:
        def check() -> bool:
            loop = make_loop()

            async def resolve() -> asyncio.AbstractEventLoop:
                return self._unconnected_cluster(loop).loop

            try:
                return loop.run_until_complete(resolve()) is loop
            finally:
                loop.close()

        assert _in_fresh_thread(check) is True

    @pytest.mark.parametrize('make_loop', LOOP_FACTORIES)
    def test_cluster_uses_the_running_loop(self, make_loop: Callable[[], asyncio.AbstractEventLoop]) -> None:
        def check() -> bool:
            loop = make_loop()

            async def resolve() -> asyncio.AbstractEventLoop:
                return self._unconnected_cluster().loop

            try:
                return loop.run_until_complete(resolve()) is loop
            finally:
                loop.close()

        assert _in_fresh_thread(check) is True

    def test_cluster_refuses_a_supplied_loop_that_is_not_running(self) -> None:
        def check() -> None:
            loop = _selector_loop()
            try:
                with pytest.raises(RuntimeError, match='not running'):
                    self._unconnected_cluster(loop).loop
            finally:
                loop.close()

        _in_fresh_thread(check)

    def test_cluster_refuses_when_no_loop_is_running(self) -> None:
        def check() -> None:
            loop = _selector_loop()
            asyncio.set_event_loop(loop)
            try:
                with pytest.raises(RuntimeError, match='not running'):
                    self._unconnected_cluster().loop
                assert loop.is_closed() is False
            finally:
                loop.close()

        _in_fresh_thread(check)
