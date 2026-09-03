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

"""Owns the Twisted reactor for the whole session: installed at import time by
`txcouchbase/__init__.py`, started here on a thread of its own, and stopped by
`stop_reactor()`.  `txcouchbase/tests_v2/conftest.py` drives both ends from its pytest
hooks.

**The state lives in the shared tree because this tree has one module name in every
layout, and an api package's does not.**  `tests_v2/` sits at the repo root for a
developer, is copied verbatim to the root of the artifact-isolation tree a CI run builds,
and is in no wheel, so `tests_v2.environments.twisted_reactor` is one module object
wherever the suite runs.  A CI tree copies `txcouchbase/` under a short name so that
`import couchbase` resolves to the installed wheel, and a wheel that ships
`txcouchbase.tests_v2` then answers that same conftest a second time under its original
name.  Holding the reactor there means pytest starts it on the copy it loaded while every
test dispatches into the other copy's `_REACTOR`, which is `None` and stays `None`.

Twisted is imported inside the functions, never at module scope.  This module is reachable
from `tests_v2.environments.twisted_environment`, which is a registered pytest plugin, so a
module-scope import would pull the reactor into every couchbase/ and acouchbase/ run.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import (Any,
                    ClassVar,
                    Optional)


class TwistedObjects:
    # The reactor is a module, not an instance: `twisted.internet.reactor` is what `install()`
    # writes into `sys.modules`, and it answers reactor attributes by delegation.  There is no
    # class to name here, which is why this is `Any` rather than an interface.
    _REACTOR: ClassVar[Any] = None
    _TWISTED_THREAD: ClassVar[Optional[threading.Thread]] = None


def run_in_reactor_thread(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call `fn` on the reactor thread and block the caller until it resolves.

    This is the bridge that lets synchronous pytest test functions drive txcouchbase's
    Deferred-returning API.
    """
    # Checked here because `blockingCallFromThread` raises `AttributeError: 'NoneType' object
    # has no attribute 'callFromThread'` three frames inside `twisted.internet.threads`,
    # naming neither the reactor, the flag, nor the harness.
    if TwistedObjects._REACTOR is None:
        raise RuntimeError(
            'The Twisted reactor was never started, so there is nothing to dispatch to.  '
            'txcouchbase/tests_v2/conftest.py starts it from pytest_configure, and only when '
            'the run carries --txcouchbase (-m txcouchbase also works).'
        )

    from twisted.internet import threads
    return threads.blockingCallFromThread(TwistedObjects._REACTOR, fn, *args, **kwargs)


def _run_reactor(reactor: Any) -> None:
    # installSignalHandlers=False because this runs on a worker thread, and Twisted's default
    # start-up installs a SIGINT handler, which CPython permits only on the main thread.  Left
    # to default it raises ValueError there, and Twisted logs it as an unhandled error inside a
    # system event trigger: fourteen lines of traceback ending in "signal only works in main
    # thread of the main interpreter", printed before the first test and visible under -s.
    #
    # pytest owns SIGINT here regardless, so the reactor never wanted it.  Ctrl+C still stops
    # the run, and `stop_reactor()` still stops the reactor through blockingCallFromThread.
    reactor.run(installSignalHandlers=False)


def init_reactor() -> None:
    from twisted.internet import asyncioreactor
    from twisted.internet.error import ReactorAlreadyInstalledError

    from acouchbase import get_event_loop
    try:
        asyncioreactor.install(get_event_loop())
    except ReactorAlreadyInstalledError:
        # Expected on every run, not a warning.  `txcouchbase/__init__.py` installs the
        # asyncioreactor over `get_event_loop()` at import time, and this tree imports
        # txcouchbase long before pytest_configure gets here, so the install this function
        # asks for has already happened with the same arguments.  Swallowing it silently
        # loses nothing: the checks below report the case that is worth reporting, which is a
        # reactor that is not the one wanted.
        pass
    finally:
        import twisted.internet.reactor  # noqa: F401 (side-effect: sets reactor)

    import twisted.internet.reactor as _reactor
    TwistedObjects._REACTOR = _reactor

    if not hasattr(TwistedObjects._REACTOR, '_asyncioEventloop'):
        raise RuntimeError(
            f'Reactor installed is not the asyncioreactor, it is '
            f'{type(TwistedObjects._REACTOR).__name__}.'
        )
    # Not the same check.  Having *an* asyncio reactor is not the same as having one driving
    # the loop acouchbase hands out, and a mismatch is silent: every Deferred resolves on a
    # loop nothing here ever runs, so the suite hangs rather than failing.
    if TwistedObjects._REACTOR._asyncioEventloop is not get_event_loop():
        raise RuntimeError('The installed asyncioreactor is running a different event loop '
                           'than acouchbase.get_event_loop() returns.')

    TwistedObjects._TWISTED_THREAD = threading.Thread(
        target=lambda: _run_reactor(TwistedObjects._REACTOR),
        daemon=True,
    )
    TwistedObjects._REACTOR.suggestThreadPoolSize(10)
    TwistedObjects._TWISTED_THREAD.start()


def stop_reactor() -> None:
    if TwistedObjects._TWISTED_THREAD is not None:
        from twisted.internet import threads
        threads.blockingCallFromThread(TwistedObjects._REACTOR, TwistedObjects._REACTOR.stop)
        TwistedObjects._TWISTED_THREAD.join()
