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

# Hand-written and cluster-free: the event loop txcouchbase installs its reactor with.
#
# Twisted's asyncioreactor drives add_reader and add_writer directly, which ProactorEventLoop, the
# Windows default, does not implement.  So importing txcouchbase resolves a loop the reactor can
# drive, in order: the loop of an asyncio reactor that is already installed, the running loop, the
# thread's current loop, and otherwise a new SelectorEventLoop it sets as the current one.  A
# current loop the reactor cannot drive is replaced with a warning and never closed; a running one,
# or a reactor of another kind, is an error.  acouchbase/tests/loop_selection_t.py covers acouchbase.
#
# Installing a reactor is process-wide and cannot be undone, and this session's conftest has
# already installed one, so each starting state imports txcouchbase in a subprocess of its own.
from __future__ import annotations

import json
import os
import subprocess  # nosec
import sys
from typing import Any

import pytest

import couchbase

# Derived from couchbase rather than txcouchbase so the child resolves the same packages as this
# process, whether that is a checkout or an installed wheel.
_PACKAGE_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(couchbase.__file__)))

_REACTOR_PROBE = """
import asyncio
import importlib
import json
import selectors
import sys
import warnings


class _ProactorShapedEventLoop(asyncio.SelectorEventLoop):
    # Inherits the four reader and writer methods unimplemented, the way ProactorEventLoop does.
    add_reader = asyncio.AbstractEventLoop.add_reader
    remove_reader = asyncio.AbstractEventLoop.remove_reader
    add_writer = asyncio.AbstractEventLoop.add_writer
    remove_writer = asyncio.AbstractEventLoop.remove_writer


def proactor_loop():
    if sys.platform == 'win32':
        return asyncio.ProactorEventLoop()
    return _ProactorShapedEventLoop(selectors.DefaultSelector())


scenario = sys.argv[1]
start = None
if scenario in ('selector', 'running_selector', 'asyncio_reactor'):
    start = asyncio.SelectorEventLoop(selectors.DefaultSelector())
elif scenario in ('proactor', 'running_proactor'):
    start = proactor_loop()
if start is not None:
    asyncio.set_event_loop(start)
if scenario == 'asyncio_reactor':
    from twisted.internet import asyncioreactor
    asyncioreactor.install(start)
elif scenario == 'other_reactor':
    from twisted.internet import selectreactor
    selectreactor.install()


async def import_while_running():
    importlib.import_module('txcouchbase')


error = None
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    try:
        if scenario.startswith('running_'):
            start.run_until_complete(import_while_running())
        else:
            importlib.import_module('txcouchbase')
    except Exception as ex:
        error = type(ex).__name__

reactor_loop = None
if error is None:
    from twisted.internet import reactor
    reactor_loop = reactor._asyncioEventloop
try:
    current = asyncio.get_event_loop()
except RuntimeError:
    current = None
json.dump({
    'error': error,
    'reactor_loop_is_start': reactor_loop is not None and reactor_loop is start,
    'reactor_loop_is_current': reactor_loop is not None and reactor_loop is current,
    'reactor_loop_type': type(reactor_loop).__name__,
    'reactor_loop_is_selector': isinstance(reactor_loop, asyncio.SelectorEventLoop),
    'start_closed': start is not None and start.is_closed(),
    'warnings': [(w.category.__name__, str(w.message)) for w in caught],
}, sys.stdout)
"""


def _probe(scenario: str) -> dict[str, Any]:
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(filter(None, [_PACKAGE_PARENT_DIR, os.environ.get('PYTHONPATH')]))
    proc = subprocess.run([sys.executable, '-c', _REACTOR_PROBE, scenario],  # nosec
                          env=env,
                          capture_output=True,
                          text=True,
                          timeout=120)
    assert proc.returncode == 0, f'The {scenario} probe exited with {proc.returncode}.\n{proc.stderr}'
    probe: dict[str, Any] = json.loads(proc.stdout)
    return probe


def _loop_warnings(probe: dict[str, Any]) -> list[str]:
    # Twisted may warn about its own deprecations on import; only warnings about loops count here.
    return [message for _, message in probe['warnings'] if 'loop' in message.lower()]


class TxLoopSelectionUnitTests:
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and this file is
    # one tree's own.

    def test_new_selector_loop_when_there_is_none(self) -> None:
        probe = _probe('none')
        assert probe['error'] is None
        assert probe['reactor_loop_is_selector'] is True
        assert probe['reactor_loop_is_current'] is True
        assert _loop_warnings(probe) == []

    def test_current_selector_loop_is_used(self) -> None:
        probe = _probe('selector')
        assert probe['error'] is None
        assert probe['reactor_loop_is_start'] is True
        assert _loop_warnings(probe) == []

    def test_current_proactor_loop_is_replaced_not_closed(self) -> None:
        probe = _probe('proactor')
        assert probe['error'] is None
        assert probe['reactor_loop_is_start'] is False
        assert probe['reactor_loop_is_selector'] is True
        assert probe['reactor_loop_is_current'] is True
        assert probe['start_closed'] is False
        assert [category for category, _ in probe['warnings']].count('RuntimeWarning') == 1

    def test_running_selector_loop_is_used(self) -> None:
        probe = _probe('running_selector')
        assert probe['error'] is None
        assert probe['reactor_loop_is_start'] is True

    def test_running_proactor_loop_is_refused(self) -> None:
        probe = _probe('running_proactor')
        assert probe['error'] == 'InvalidArgumentException'
        assert probe['start_closed'] is False

    def test_installed_asyncio_reactor_is_used(self) -> None:
        probe = _probe('asyncio_reactor')
        assert probe['error'] is None
        assert probe['reactor_loop_is_start'] is True

    @pytest.mark.skipif(sys.platform == 'win32', reason='selectreactor is the only other reactor every platform has')
    def test_other_reactor_is_refused(self) -> None:
        assert _probe('other_reactor')['error'] == 'InvalidArgumentException'
