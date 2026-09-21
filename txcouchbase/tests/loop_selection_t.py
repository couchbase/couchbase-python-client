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
The event loop txcouchbase installs its reactor with.

Importing txcouchbase runs asyncioreactor.install(get_event_loop()), so the loop acouchbase
resolves becomes the reactor's loop.  Two things depend on that and neither is asserted anywhere
else:

  * _validate_loop requires reactor._asyncioEventloop to be the loop get_event_loop() returns,
    and that identity holds only because resolution installs the loop it creates as the thread
    default.  Anything that changes loop resolution has to keep that invariant or replace it.
  * Twisted's AsyncioSelectorReactor drives add_reader and add_writer directly and refuses a
    ProactorEventLoop, so on Windows the reactor works only because resolution substitutes a
    selector loop for the platform default.  That substitution is the one acouchbase behaviour
    txcouchbase cannot lose.

Installing a Twisted reactor is process-wide and cannot be undone, so the probe runs in a
subprocess.  That also keeps these tests independent of whether the suite was invoked with
--txcouchbase.

No live cluster is needed.  The probe imports txcouchbase and inspects the reactor.
"""

import json
import os
import subprocess  # nosec
import sys

import pytest

pytest.importorskip('twisted', reason='txcouchbase requires Twisted, which is an optional dependency.')

import couchbase  # noqa: E402  imported for its location only; importing txcouchbase installs a reactor

# Derived from couchbase rather than txcouchbase on purpose: resolving txcouchbase's path here
# would import it, and that installs a reactor in the test process.
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


# Start from a thread default the validator rejects.  That is the Windows starting state; without
# it the platform default is already acceptable on POSIX and the import exercises nothing.
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
    'reactor_type': type(reactor).__name__,
}, sys.stdout)
"""


class TxLoopSelectionTestSuite:
    TEST_MANIFEST = [
        'test_reactor_is_installed_with_the_resolved_loop',
        'test_reactor_loop_is_a_selector_loop',
        'test_rejected_thread_default_is_closed_and_replaced',
        'test_resolved_loop_survives_the_import',
    ]

    @pytest.fixture(name='reactor_probe', scope='class')
    def import_txcouchbase_in_a_subprocess(self):
        env = dict(os.environ)
        # keep the child on the same packages as the parent, whatever the working directory
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [_PACKAGE_PARENT_DIR, os.environ.get('PYTHONPATH')]))
        proc = subprocess.run([sys.executable, '-c', _REACTOR_PROBE],  # nosec
                              env=env,
                              capture_output=True,
                              text=True,
                              timeout=120)
        assert proc.returncode == 0, (f'Importing txcouchbase exited with {proc.returncode}.\n'
                                      f'{proc.stderr}')
        return json.loads(proc.stdout)

    def test_reactor_is_installed_with_the_resolved_loop(self, reactor_probe):
        assert reactor_probe['reactor_loop_is_resolved_loop'] is True, (
            f"the reactor holds a {reactor_probe['reactor_loop_type']} that is not the loop "
            f'get_event_loop() returns; _validate_loop will reject every cluster.')

    def test_reactor_loop_is_a_selector_loop(self, reactor_probe):
        assert reactor_probe['is_selector_loop'] is True, (
            f"reactor installed with a {reactor_probe['reactor_loop_type']}, which Twisted's "
            f'asyncioreactor does not support.')

    def test_rejected_thread_default_is_closed_and_replaced(self, reactor_probe):
        assert reactor_probe['reactor_loop_replaced_the_default'] is True
        assert reactor_probe['rejected_loop_closed'] is True

    def test_resolved_loop_survives_the_import(self, reactor_probe):
        assert reactor_probe['reactor_loop_closed'] is False


class TxLoopSelectionTests(TxLoopSelectionTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(TxLoopSelectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(TxLoopSelectionTests) if valid_test_method(meth)]
        manifest_invalid = set(TxLoopSelectionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
