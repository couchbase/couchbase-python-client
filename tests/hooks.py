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

"""Session-wide hooks for every api tree's suites.

A plugin module and not a conftest.  The root conftest registers it by name through
`pytest_plugins`, and a `conftest.py` loaded that way is also loaded by path whenever
a run collects this directory, which pytest rejects as one module registered twice.

Enforces the @pytest.mark.unsupported_protocol(*protocols) mark, registered once
here for every suite rather than per-suite, reports which config file the run
resolved and which server it will reach, and keeps a parallel run on the one
dist mode that honours the suites' `xdist_group` marks.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING, Any

import pytest

from tests.skips import server_label, skip_protocol_mismatch

if TYPE_CHECKING:
    from tests.couchbase_config import ConfigSource


@pytest.hookimpl(wrapper=True)
def pytest_runtest_setup(item: pytest.Item) -> Generator[None, None, None]:
    # Runs after the default setup hook (wrapper), so fixtures (including
    # couchbase_test_config) are already resolved into item.funcargs.
    yield
    marker = item.get_closest_marker('unsupported_protocol')
    if marker is None:
        return
    # funcargs lives on Function, not the base Item type this hook is declared with.
    funcargs = getattr(item, 'funcargs', {})
    config = funcargs.get('couchbase_test_config')
    if config is None:
        return
    if config.protocol in marker.args:
        skip_protocol_mismatch(unsupported=','.join(getattr(p, 'name', str(p)) for p in marker.args),
                               active=config.protocol,
                               server=server_label(config.is_mock_server))


# Every `xdist_group` a suite may name, and the cluster state its members share.  Only
# `--dist loadgroup` honours the mark; under any other mode two members of a group can run at
# once, and the collision is invisible on the mock, which has no bucket, query index or
# analytics management.
XDIST_GROUPS = {
    # ns_server refuses a bucket create with a 503 while any bucket drop is in progress anywhere
    # on the cluster, so two suites creating and dropping buckets on two workers fail each other
    # however distinct their bucket names are.  The refusal also lands on creates that are the
    # operation under test, where a retry would record two spans or meter readings for one call.
    'bucket_lifecycle',
    # The DEFAULT analytics path puts `test-dataset` in the Default dataverse and connects and
    # disconnects its Local link, and the analytics management suite creates and drops a
    # dataset of that name there too.  A link belongs to its dataverse rather than to one
    # dataset, so a per-worker dataset name would still leave one suite's teardown
    # disconnecting the link another is waiting to ingest through.
    'default_dataverse',
    # Indexes on `default`.`_default`.`_default` belong to whichever suite made them, and these
    # suites clear or drop them at setup and teardown.  Run side by side, one suite deletes the
    # primary index another is in the middle of using.
    'default_keyspace_indexes',
}


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_make_scheduler(config: pytest.Config, log: Any) -> Any:
    """Schedule `--dist loadgroup` by group where a test has one, and by class where it does not.

    Stock loadgroup sends every ungrouped test out on its own, so one class runs on several
    workers at once and each builds the class environment.  Fixed cluster-wide names then
    collide: two params of one class create and drop the same user or dataverse side by side,
    and each class switch on a worker drops and recreates its named scope under the same
    name, faster than the search service catches up.  Grouping the rest by class, as
    loadscope does, gives every class the one setup it gets sequentially.

    xdist appends `@<group>` to a grouped test's node id on the worker, before the controller
    sees it, so the suffix is all this needs to read.
    """
    if config.getvalue('dist') != 'loadgroup':
        return None
    from xdist.scheduler import LoadGroupScheduling, LoadScopeScheduling

    # xdist has no public hook for the scheduling scope, so this overrides `_split_scope`,
    # which is private.  dev_requirements.txt pins xdist's minor version for that reason.
    class GroupOrClassScheduling(LoadGroupScheduling):
        def _split_scope(self, nodeid: str) -> str:
            # The check against `]` is xdist's own: a parametrize id may itself contain `@`.
            if nodeid.rfind('@') > nodeid.rfind(']'):
                return nodeid.split('@')[-1]
            return LoadScopeScheduling._split_scope(self, nodeid)

    return GroupOrClassScheduling(config, log)


def _check_dist_mode(config: pytest.Config) -> None:
    """Abort a parallel run that would ignore the `xdist_group` marks.

    `-n` on its own selects `--dist load`, which schedules each test independently of its mark,
    so the groups above are silently dissolved and the races they prevent come back, but only
    against a real cluster.  Only loadgroup reaches the scheduler above.
    """
    dist = config.getoption('dist', default='no')
    if dist in ('no', 'loadgroup'):
        return
    raise pytest.UsageError(
        f'--dist {dist} ignores the xdist_group marks that keep suites sharing cluster state on '
        f'one worker ({", ".join(sorted(XDIST_GROUPS))}).  Run in parallel with '
        f'`-n <N> --dist loadgroup`.'
    )


def _check_xdist_groups(items: list[pytest.Item]) -> None:
    """Abort when a suite names a group that is not in XDIST_GROUPS.

    A misspelt group is still a valid mark: it makes a group of its own and schedules its
    members away from the suites it was meant to be serialized with.
    """
    unknown = sorted({str(mark.args[0] if mark.args else mark.kwargs.get('name', 'default'))
                      for item in items
                      for mark in item.iter_markers('xdist_group')} - XDIST_GROUPS)
    if unknown:
        raise pytest.UsageError(
            f'xdist_group name(s) {", ".join(unknown)} are not in XDIST_GROUPS in tests/hooks.py.'
        )


def pytest_configure(config: pytest.Config) -> None:
    """Fetch the gocaves binary before xdist forks, so the workers share one download.

    The controller runs this hook before it spawns anything; a worker has `workerinput`
    and skips it, having inherited a binary that is already there.  `ensure_mock_binary()`
    returns immediately when the run is not on the mock, or when the file exists, which
    is every local run after the first.

    Not a correctness guard.  `CavesMockServer` downloads through a temp name and an
    atomic rename, so concurrent fetches are safe on their own; this is what stops N
    workers each pulling the same 16MB on a cold machine.

    The dist-mode check is here for the same reason the fetch is: it has to run once, on
    the controller, before any worker starts.
    """
    if hasattr(config, 'workerinput'):
        return
    _check_dist_mode(config)
    from tests.couchbase_config import ensure_mock_binary
    ensure_mock_binary()


def _config_source() -> ConfigSource:
    """Import describe_config_source() at call time, not at module import.

    `tests.couchbase_config` is itself a registered plugin, listed in the root conftest's
    `pytest_plugins` after this module.  Importing it at the top of this file gets it into
    sys.modules before pytest registers it, and pytest then reports that it can no longer
    rewrite the module's assertions.  Nothing there uses `assert`, so the warning is inert,
    but a standing warning is where the next real one goes to hide.
    """
    from tests.couchbase_config import describe_config_source
    return describe_config_source()


def pytest_report_header(config: pytest.Config) -> str:
    """State which config file this run read, which server it will reach, and what decided it.

    `$PYCBC_SERVER_ENABLED` and `$PYCBC_GOCAVES_ENABLED` shadow the `enabled` keys in
    `test_config.ini`, so the file alone does not say where a run goes.  A variable left
    exported from an earlier shell sends the run to the other server, where it passes, and a
    passing run is not investigated.  The bracketed part of the line names the input that
    won, on line one, before anything connects.

    pytest hides report headers under `-q`, which is how every regression in this repo is
    run, so `_echo_config_source_when_quiet()` below covers that case.  This hook stays
    because it is the idiomatic place for the line whenever pytest will show it.
    """
    return f'test config: {_config_source().summary()}'


def _echo_config_source_when_quiet(config: pytest.Config) -> None:
    """Write the config line under `-q`, where pytest_report_header is suppressed.

    Only under `-q`, so a normal-verbosity run does not print it twice.  Both paths read the
    same _config_source(), so this line cannot disagree with the guard that follows it.
    """
    if getattr(config.option, 'verbose', 0) >= 0:
        return
    reporter = config.pluginmanager.get_plugin('terminalreporter')
    if reporter is None:
        return
    reporter.write_line(f'test config: {_config_source().summary()}')


def _check_server_intent(config: pytest.Config) -> None:
    """Abort when --real-server / --mock disagrees with the config the run resolved.

    The header above tells you where the run went; this makes the run refuse to go somewhere
    you did not ask for.  Both are needed, because the header only helps someone who reads it,
    and the case being guarded is one where nothing looks wrong: a full regression pointed at
    the wrong config passes, in a quarter of the wall-clock, having skipped the tests it
    existed to check.

    A UsageError and not a skip: a run that reached the wrong server did not legitimately
    fail to run, and `tests/skips.py` categories all mean "this could not run and that is
    fine".

    Neither flag is the default.  Passing one is how a run that matters, a release
    regression or a defect verification, says so out loud.
    """
    want_real = config.getoption('--real-server', default=False)
    want_mock = config.getoption('--mock', default=False)
    if want_real and want_mock:
        raise pytest.UsageError('--real-server and --mock are contradictory; pass at most one.')

    source = _config_source()

    # Unconditional, because no flag can make this run: CouchbaseConfig.load_config() rejects
    # the combination.  Left to itself it surfaces as an env-degraded exception raised inside
    # the session fixture, which every test then errors behind.  Here it is one line, before
    # anything runs, naming the file to edit.
    if source.server == 'conflict':
        raise pytest.UsageError(
            f'{source.summary()}.  Exactly one of them must be enabled.  Nothing ran.'
        )

    if not (want_real or want_mock):
        return
    wanted = 'real' if want_real else 'mock'
    if source.server == wanted:
        return

    flag = '--real-server' if want_real else '--mock'
    raise pytest.UsageError(
        f'{flag} was passed, but the resolved config selects server={source.server}.  '
        f'{source.summary()}.  Nothing ran.'
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Check the collected run before any test starts.

    `trylast` so that `items` is what the mark filter left, and a run whose every test was
    deselected echoes and checks nothing.
    """
    if not items:
        return

    _echo_config_source_when_quiet(config)
    _check_server_intent(config)
    _check_xdist_groups(items)
