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

"""The channel's connection to the gateway, logged as it changes.

A healthy connection logs once, at info, when it first connects.  Losing a connection logs at info,
since the next request reconnects; failing to connect logs a warning once per outage, and connecting
again logs at info with how long it was down.  Everything else is debug.

The channel has one connection, so its state is that connection's.  When the connection closes the
channel goes idle and reconnects on the next call.  It also goes idle after
`CLIENT_IDLE_TIMEOUT_MS` with no call, which is not an outage, so a channel that goes idle sooner
than that after the last request has lost its connection.

Neither watch asks the channel to connect, and neither can say why a connection failed: the cause
arrives on the next failed request, which the retry log shows.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

import grpc
import grpc.aio

from couchbase.common._redaction import system_data
from couchbase.protocol.couchbase2._channel import CLIENT_IDLE_TIMEOUT_MS

logger = logging.getLogger('couchbase.couchbase2')

State = grpc.ChannelConnectivity


class ConnectivityLog:
    """Turns the states a channel passes through into log lines."""

    def __init__(self,
                 target: str,
                 last_request: Callable[[], float | None],
                 clock: Callable[[], float] = time.monotonic) -> None:
        """`last_request` is when the most recent request began on `clock`, or None before the first."""
        self._gateway = system_data(target)
        self._last_request = last_request
        self._clock = clock
        self._previous = State.IDLE
        self._connected = False
        self._down_since: float | None = None
        self._warned = False

    def observe(self, state: State) -> None:
        if state == self._previous or state == State.SHUTDOWN:
            return
        if state == State.READY:
            self._ready()
        elif state == State.TRANSIENT_FAILURE:
            self._failed()
        elif state == State.IDLE:
            self._idle()
        else:
            logger.debug('couchbase2: connecting to the gateway at %s', self._gateway)
        self._previous = state

    def _ready(self) -> None:
        if self._down_since is not None:
            logger.info('couchbase2: connected to the gateway at %s again, %.1fs after the connection was lost',
                        self._gateway, self._clock() - self._down_since)
        elif not self._connected:
            logger.info('couchbase2: connected to the gateway at %s', self._gateway)
        else:
            logger.debug('couchbase2: reconnected to the gateway at %s', self._gateway)
        self._connected = True
        self._down_since = None
        self._warned = False

    def _failed(self) -> None:
        if self._down_since is None:
            self._down_since = self._clock()
        if self._warned:
            logger.debug('couchbase2: still unable to connect to the gateway at %s', self._gateway)
            return
        self._warned = True
        logger.warning('couchbase2: unable to connect to the gateway at %s; retrying with backoff.  '
                       'A request that fails meanwhile reports the cause.', self._gateway)

    def _idle(self) -> None:
        last = self._last_request()
        if (self._previous == State.READY
                and last is not None
                and self._clock() - last < CLIENT_IDLE_TIMEOUT_MS / 1000):
            self._down_since = self._clock()
            logger.info('couchbase2: lost the connection to the gateway at %s; reconnecting on the next request',
                        self._gateway)
        else:
            logger.debug('couchbase2: the connection to the gateway at %s is idle', self._gateway)


def watch(channel: grpc.Channel, log: ConnectivityLog) -> Callable[[], None]:
    """Log `channel`'s states until the returned callable is called.

    gRPC calls back on a thread of its own and logs anything the callback raises.
    """
    channel.subscribe(log.observe, try_to_connect=False)
    return lambda: channel.unsubscribe(log.observe)


# How long each async watch waits before it is renewed, as gRPC's own sync `subscribe` does.
_WATCH_DEADLINE_S = 0.2


def watch_async(channel: grpc.aio.Channel, log: ConnectivityLog) -> asyncio.Task[None]:
    """A task on the running loop logging `channel`'s states; the caller awaits it after closing the channel.

    Each wait has a deadline because one left pending can complete after its loop has closed, which on
    Windows kills gRPC's poller thread.  `wait_for_state_change` takes none, so this makes the call it wraps,
    on the channel's private `_channel`, with one.
    """
    core: Any = getattr(channel, '_channel')

    async def run() -> None:
        try:
            state = channel.get_state(try_to_connect=False)
            while state != State.SHUTDOWN:
                await core.watch_connectivity_state(state.value[0], time.time() + _WATCH_DEADLINE_S)
                state = channel.get_state(try_to_connect=False)
                try:
                    log.observe(state)
                except Exception:  # nosec
                    # A broken handler loses the line, not the watch.
                    pass
        except Exception:  # nosec
            # A closed channel refuses the next wait.
            return

    return asyncio.get_running_loop().create_task(run())
