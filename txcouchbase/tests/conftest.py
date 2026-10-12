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

"""Starts and stops the Twisted reactor for the whole session.

The reactor itself, and the bridge that dispatches to it, are in
`tests/environments/twisted_reactor.py`.  That module's docstring says why: the reactor
is one object per process, and only the shared tree has one module name in every layout a
run can be assembled under.  A conftest under an api package does not, so it holds the
hooks and none of the state.
"""

import pathlib

import pytest

from tests.environments.twisted_reactor import (TwistedObjects,
                                                init_reactor,
                                                stop_reactor)

# This directory.  `pytest_collection_modifyitems` is a global hook, called on every
# registered plugin with the whole item list, so the check below has to pick out its own
# tree rather than assume the run is confined to it.
_TREE_DIR = pathlib.Path(__file__).parent


def pytest_configure(config: pytest.Config) -> None:
    run_tx = config.getoption('--txcouchbase', default=False)
    if config.option and config.option.markexpr:
        if 'txcouchbase' in config.option.markexpr:
            run_tx = True

    if run_tx is True:
        init_reactor()


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Abort loudly when this tree was collected without --txcouchbase.

    Without the flag `pytest_configure` never calls `init_reactor()`, so the reactor is
    installed but never started and the first SDK call of every test raises
    `RuntimeError: Reactor is not running.` from `txcouchbase/logic/cluster_impl.py`, about
    twenty frames down, through `acouchbase/logic/client_adapter.py`.  Nothing in that
    traceback names the flag, or even the test harness, so it reads as an SDK defect in the
    async client rather than as a mistyped command line.  Every test in the tree fails that
    way, identically, which is the shape of a real regression and not of operator error.

    This is deliberately a `UsageError` and not a skip.  `tests/skips.py` categories are
    claims about the world, each meaning "this test could not run *and that is fine*", and
    they are counted as such from the JUnit XML.  A forgotten flag is not fine and is not a
    property of the cluster: recording it as an expected skip would put a run that exercised
    nothing into the same bucket as one that legitimately skipped, which is exactly the
    conflation that module exists to prevent.

    `trylast` matters.  Conftest hooks run before the builtin mark filter, so without it
    `items` would still hold everything a `-m "not pycbc_txcouchbase"` is about to deselect,
    and deselecting this tree by marker would abort the run it was meant to permit.
    """
    if TwistedObjects._REACTOR is not None or config.option.collectonly:
        return

    if not any(_TREE_DIR in item.path.parents for item in items):
        return

    raise pytest.UsageError(
        'txcouchbase/tests was collected without --txcouchbase, so the Twisted reactor '
        'was never started.  Every test would fail inside the SDK with "RuntimeError: '
        'Reactor is not running.", naming neither the reactor nor this flag as the cause.  '
        'Re-run the same command with --txcouchbase appended (-m txcouchbase also works), '
        'or deselect this tree with -m "not pycbc_txcouchbase".'
    )


def pytest_unconfigure() -> None:
    stop_reactor()
