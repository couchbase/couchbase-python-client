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

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any, cast

import grpc
import grpc.aio
import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.common import _redaction
from couchbase.options import ClusterOptions
from couchbase.protocol.couchbase2._channel import CLIENT_IDLE_TIMEOUT_MS
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._connectivity import (ConnectivityLog,
                                                         watch,
                                                         watch_async)
from tests.couchbase2_gateway import (Certificates,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

State = grpc.ChannelConnectivity
LOGGER = 'couchbase.couchbase2'
IDLE_TIMEOUT_S = CLIENT_IDLE_TIMEOUT_MS / 1000


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _Log:
    """A `ConnectivityLog` on a clock the test moves, whose last request the test sets."""

    def __init__(self) -> None:
        self.clock = _Clock()
        self.last_request: float | None = None
        self.log = ConnectivityLog('gw.local:18098', lambda: self.last_request, self.clock)

    def through(self, *states: State) -> None:
        for state in states:
            self.log.observe(state)


def _lines(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    return [(r.levelno, r.getMessage()) for r in caplog.records if r.name == LOGGER]


@pytest.fixture
def log() -> _Log:
    return _Log()


@pytest.fixture(autouse=True)
def _debug(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)


class ConnectivityLogUnitTests:

    def test_the_first_connection_is_info(self, log: _Log, caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.CONNECTING, State.READY)
        assert _lines(caplog) == [(logging.DEBUG, 'couchbase2: connecting to the gateway at gw.local:18098'),
                                  (logging.INFO, 'couchbase2: connected to the gateway at gw.local:18098')]

    def test_an_outage_warns_once_and_reports_how_long_it_lasted(self,
                                                                 log: _Log,
                                                                 caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.CONNECTING, State.TRANSIENT_FAILURE)
        log.clock.now += 2.5
        log.through(State.CONNECTING, State.TRANSIENT_FAILURE, State.CONNECTING, State.READY)
        levels = [level for level, _ in _lines(caplog)]
        assert levels.count(logging.WARNING) == 1
        assert _lines(caplog)[-1] == (logging.INFO, 'couchbase2: connected to the gateway at gw.local:18098 '
                                                    'again, 2.5s after the connection was lost')

    def test_going_idle_soon_after_a_request_is_a_lost_connection(self,
                                                                  log: _Log,
                                                                  caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.CONNECTING, State.READY)
        log.last_request = log.clock.now
        log.clock.now += 5
        log.through(State.IDLE)
        assert _lines(caplog)[-1] == (logging.INFO, 'couchbase2: lost the connection to the gateway at '
                                                    'gw.local:18098; reconnecting on the next request')

    def test_a_lost_connection_that_cannot_reconnect_warns_once(self,
                                                                log: _Log,
                                                                caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.CONNECTING, State.READY)
        log.last_request = log.clock.now
        log.through(State.IDLE)
        log.clock.now += 2.5
        log.through(State.CONNECTING, State.TRANSIENT_FAILURE, State.CONNECTING, State.TRANSIENT_FAILURE)
        assert [level for level, _ in _lines(caplog)].count(logging.WARNING) == 1
        log.through(State.CONNECTING, State.READY)
        assert _lines(caplog)[-1] == (logging.INFO, 'couchbase2: connected to the gateway at gw.local:18098 '
                                                    'again, 2.5s after the connection was lost')

    def test_going_idle_after_the_idle_timeout_is_not(self, log: _Log, caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.CONNECTING, State.READY)
        log.last_request = log.clock.now
        log.clock.now += IDLE_TIMEOUT_S
        log.through(State.IDLE, State.CONNECTING, State.READY)
        assert [level for level, _ in _lines(caplog)[2:]] == [logging.DEBUG] * 3
        assert _lines(caplog)[-1][1] == 'couchbase2: reconnected to the gateway at gw.local:18098'

    def test_idle_before_any_request_is_not_a_lost_connection(self,
                                                              log: _Log,
                                                              caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.CONNECTING, State.READY, State.IDLE)
        assert _lines(caplog)[-1][0] == logging.DEBUG

    def test_a_repeated_state_and_shutdown_log_nothing(self, log: _Log, caplog: pytest.LogCaptureFixture) -> None:
        log.through(State.IDLE, State.SHUTDOWN)
        assert _lines(caplog) == []

    def test_the_gateway_is_tagged_as_system_data(self,
                                                  log: _Log,
                                                  caplog: pytest.LogCaptureFixture,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_redaction, '_enabled', True)
        log.through(State.READY)
        assert '<sd>gw.local:18098</sd>' in _lines(caplog)[0][1]


class _SyncChannel:
    def __init__(self) -> None:
        self.subscribed: list[tuple[Callable[[State], None], bool]] = []
        self.unsubscribed: list[Callable[[State], None]] = []

    def subscribe(self, callback: Callable[[State], None], try_to_connect: bool = False) -> None:
        self.subscribed.append((callback, try_to_connect))

    def unsubscribe(self, callback: Callable[[State], None]) -> None:
        self.unsubscribed.append(callback)


class _AioChannel:
    """Moves through `states`, then times out each wait; it is its own private `_channel`, which the watch calls."""

    def __init__(self, *states: State) -> None:
        self.states = [State.IDLE, *states]
        self.connect_asked = False
        self.deadlines: list[float] = []
        self._channel = self

    def get_state(self, try_to_connect: bool = False) -> State:
        self.connect_asked |= try_to_connect
        return self.states[0]

    async def watch_connectivity_state(self, last_observed_state: int, deadline: float) -> bool:
        self.deadlines.append(deadline - time.time())
        if len(self.states) == 1:
            await asyncio.sleep(0)
            return False
        self.states.pop(0)
        return True


class WatchUnitTests:

    def test_the_sync_watch_subscribes_without_connecting_and_unsubscribes(self, log: _Log) -> None:
        channel = _SyncChannel()
        unwatch = watch(cast('grpc.Channel', channel), log.log)
        assert channel.subscribed == [(log.log.observe, False)]
        unwatch()
        assert channel.unsubscribed == [log.log.observe]

    def test_the_async_watch_logs_until_cancelled(self, log: _Log, caplog: pytest.LogCaptureFixture) -> None:
        channel = _AioChannel(State.CONNECTING, State.READY)

        async def run() -> bool:
            task = watch_async(cast('grpc.aio.Channel', channel), log.log)
            for _ in range(10):
                await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return task.cancelled()

        assert run_on_own_loop(run())
        assert _lines(caplog)[-1] == (logging.INFO, 'couchbase2: connected to the gateway at gw.local:18098')
        assert not channel.connect_asked

    def test_the_async_watch_ends_at_shutdown(self, log: _Log) -> None:
        async def run() -> bool:
            task = watch_async(cast('grpc.aio.Channel', _AioChannel(State.SHUTDOWN)), log.log)
            await asyncio.wait_for(task, 5)
            return task.done()

        assert run_on_own_loop(run())

    def test_every_async_wait_has_a_deadline(self, log: _Log) -> None:
        channel = _AioChannel(State.CONNECTING)

        async def run() -> None:
            task = watch_async(cast('grpc.aio.Channel', channel), log.log)
            for _ in range(10):
                await asyncio.sleep(0)
            task.cancel()

        run_on_own_loop(run())
        assert len(channel.deadlines) > 2
        assert all(0 < left < 0.25 for left in channel.deadlines)

    def test_the_async_watch_ends_when_the_wait_fails(self, log: _Log) -> None:
        class _Closed(_AioChannel):
            async def watch_connectivity_state(self, last_observed_state: int, deadline: float) -> bool:
                raise RuntimeError('closed')

        async def run() -> bool:
            task = watch_async(cast('grpc.aio.Channel', _Closed()), log.log)
            await asyncio.wait_for(task, 5)
            return task.done() and not task.cancelled()

        assert run_on_own_loop(run())


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


def _options(certificates: Certificates) -> ClusterOptions:
    return ClusterOptions(PasswordAuthenticator('Administrator', 'password'), trust_store_path=str(certificates.ca))


def _logged(caplog: pytest.LogCaptureFixture, text: str, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(text in message for _, message in _lines(caplog)):
            return True
        time.sleep(0.05)
    return False


class ClusterConnectivityUnitTests:

    def test_the_sync_cluster_logs_connecting_and_losing_the_gateway(self,
                                                                     certificates: Certificates,
                                                                     caplog: pytest.LogCaptureFixture) -> None:
        with serving(new_gateway(), certificates) as port:
            impl = ClusterImpl(f'couchbase2://localhost:{port}', _options(certificates))
            try:
                impl.bucket('b').scope('s').collection('c').upsert('k', {})
                assert _logged(caplog, f'connected to the gateway at localhost:{port}')
            except BaseException:
                impl.close_connection()
                raise
        try:
            assert _logged(caplog, f'lost the connection to the gateway at localhost:{port}')
        finally:
            impl.close_connection()

    def test_the_async_cluster_logs_connecting_and_stops_watching_on_close(
            self, certificates: Certificates, caplog: pytest.LogCaptureFixture) -> None:
        async def run(port: int) -> Any:
            impl = AsyncClusterImpl(f'couchbase2://localhost:{port}', _options(certificates))
            await impl.bucket('b').scope('s').collection('c').upsert('k', {})
            for _ in range(200):
                if any('connected to the gateway' in message for _, message in _lines(caplog)):
                    break
                await asyncio.sleep(0.05)
            stubs = await impl._stubs()
            await impl.close_connection()
            return stubs.watch.done() and not stubs.watch.cancelled()

        with serving(new_gateway(), certificates) as port:
            assert run_on_own_loop(run(port))
        assert (logging.INFO, f'couchbase2: connected to the gateway at localhost:{port}') in _lines(caplog)
