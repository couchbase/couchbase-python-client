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
# Importing txcouchbase runs asyncioreactor.install(get_event_loop()), so the loop acouchbase
# resolves becomes the reactor's loop.  Two things depend on that:
#
#   * _validate_loop requires reactor._asyncioEventloop to be the loop get_event_loop() returns,
#     and that identity holds only because resolution installs the loop it creates as the thread
#     default.  Anything that changes loop resolution has to keep that invariant or replace it.
#   * Twisted's AsyncioSelectorReactor drives add_reader and add_writer directly and refuses a
#     ProactorEventLoop, so on Windows the reactor works only because resolution substitutes a
#     selector loop for the platform default.
#
# Installing a reactor is process-wide and cannot be undone, and this session's conftest has
# already installed one, so the probe imports txcouchbase in a subprocess starting from a thread
# default the validator rejects.  acouchbase/tests/loop_selection_t.py covers resolution itself.
from __future__ import annotations

import json
import os
import subprocess  # nosec
import sys
from typing import Any, Dict

import pytest

import couchbase

# Derived from couchbase rather than txcouchbase so the child resolves the same packages as this
# process, whether that is a checkout or an installed wheel.
_PACKAGE_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(couchbase.__file__)))

_REACTOR_PROBE = """
import asyncio
import json
import selectors
import sys


class _ProactorShapedEventLoop(asyncio.SelectorEventLoop):
    # Inherits the four methods LoopValidator checks, the way ProactorEventLoop does.
    add_reader = asyncio.AbstractEventLoop.add_reader
    remove_reader = asyncio.AbstractEventLoop.remove_reader
    add_writer = asyncio.AbstractEventLoop.add_writer
    remove_writer = asyncio.AbstractEventLoop.remove_writer


# The Windows starting state.  Without it the POSIX default is already acceptable and the import
# exercises nothing.
if sys.platform == 'win32':
    rejected = asyncio.ProactorEventLoop()
else:
    rejected = _ProactorShapedEventLoop(selectors.DefaultSelector())
asyncio.set_event_loop(rejected)

import txcouchbase  # noqa: F401,E402  installing the reactor is the point of this import
from acouchbase import get_event_loop  # noqa: E402
from twisted.internet import reactor  # noqa: E402

reactor_loop = getattr(reactor, '_asyncioEventloop', None)
loop = get_event_loop()
json.dump({
    'reactor_loop_is_resolved_loop': reactor_loop is loop,
    'reactor_loop_replaced_the_default': reactor_loop is not None and reactor_loop is not rejected,
    'is_selector_loop': isinstance(reactor_loop, asyncio.SelectorEventLoop),
    'rejected_loop_closed': rejected.is_closed(),
    'reactor_loop_closed': reactor_loop is not None and reactor_loop.is_closed(),
    'reactor_loop_type': type(reactor_loop).__name__,
}, sys.stdout)
"""


class TxLoopSelectionUnitTests:
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and this file is
    # one tree's own.

    @pytest.fixture(name='reactor_probe', scope='class')
    def import_txcouchbase_in_a_subprocess(self) -> Dict[str, Any]:
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [_PACKAGE_PARENT_DIR,
                                                          os.environ.get('PYTHONPATH')]))
        proc = subprocess.run([sys.executable, '-c', _REACTOR_PROBE],  # nosec
                              env=env,
                              capture_output=True,
                              text=True,
                              timeout=120)
        assert proc.returncode == 0, (f'Importing txcouchbase exited with {proc.returncode}.\n'
                                      f'{proc.stderr}')
        probe: Dict[str, Any] = json.loads(proc.stdout)
        return probe

    def test_reactor_is_installed_with_the_resolved_loop(self, reactor_probe: Dict[str, Any]) -> None:
        assert reactor_probe['reactor_loop_is_resolved_loop'] is True, (
            f"the reactor holds a {reactor_probe['reactor_loop_type']} that is not the loop "
            f'get_event_loop() returns; _validate_loop will reject every cluster.')

    def test_reactor_loop_is_a_selector_loop(self, reactor_probe: Dict[str, Any]) -> None:
        assert reactor_probe['is_selector_loop'] is True, (
            f"reactor installed with a {reactor_probe['reactor_loop_type']}, which Twisted's "
            f'asyncioreactor does not support.')

    def test_rejected_thread_default_is_closed_and_replaced(self,
                                                            reactor_probe: Dict[str, Any]) -> None:
        assert reactor_probe['reactor_loop_replaced_the_default'] is True
        assert reactor_probe['rejected_loop_closed'] is True

    def test_resolved_loop_survives_the_import(self, reactor_probe: Dict[str, Any]) -> None:
        assert reactor_probe['reactor_loop_closed'] is False
