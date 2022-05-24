#  Copyright 2016-2022. Couchbase, Inc.
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

import threading

from twisted.internet import threads
from twisted.internet.error import ReactorAlreadyInstalledError


class TwistedObjects:
    _REACTOR = None
    _GREENLET = None
    _TWISTED_THREAD = None


def pytest_configure(config):
    run_tx = config.getoption('--txcouchbase')
    if config and config.option and config.option.markexpr:
        run_tx = config.getoption('--txcouchbase')
        if 'txcouchbase' in config.option.markexpr:
            run_tx = True

    if run_tx is True:
        init_reactor()


def run_in_reactor_thread(fn, *args, **kwargs):
    result = threads.blockingCallFromThread(TwistedObjects._REACTOR, fn, *args, **kwargs)
    return result


def run_reactor(reactor):
    reactor.run()


def init_reactor():
    from twisted.internet import asyncioreactor

    from acouchbase import get_event_loop
    try:
        asyncioreactor.install(get_event_loop())
    except ReactorAlreadyInstalledError as ex:
        print(f'Twisted setup: {ex}')
    finally:
        import twisted.internet.reactor

    TwistedObjects._REACTOR = twisted.internet.reactor
    if not hasattr(TwistedObjects._REACTOR, '_asyncioEventloop'):
        raise RuntimeError(
            "Reactor installed is not the asyncioreactor.")

    TwistedObjects._TWISTED_THREAD = threading.Thread(target=lambda: run_reactor(TwistedObjects._REACTOR))
    TwistedObjects._REACTOR.suggestThreadPoolSize(10)
    TwistedObjects._TWISTED_THREAD.start()

# hook to catch prior to running tests
# def pytest_runtest_call(item):
#   pass


def pytest_unconfigure():
    if TwistedObjects._TWISTED_THREAD:
        threads.blockingCallFromThread(TwistedObjects._REACTOR, TwistedObjects._REACTOR.stop)
        TwistedObjects._TWISTED_THREAD.join()
