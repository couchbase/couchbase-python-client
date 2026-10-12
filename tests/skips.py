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

"""How a test run reports a test it did not run, and why.

A skip is a claim about the world: this test could not run *and that is fine*.  A run whose
skips are unstructured prose cannot check that claim, so a suite that loses coverage to a
broken environment is indistinguishable from one that legitimately skipped the same tests.
That is the failure mode this module exists to close: every skip carries a machine-readable
category, and the one category that is *not* fine to skip on does not have a skip path at all.

Two exits, one grammar:

    SKIP[feature-unsupported] feature=QueryIndexManagement required=7.0 observed=6.6 server=real
    ENV[env-degraded] stage=connect server=mock note="cluster never became reachable"

`SKIP[...]` reaches CI through `<skipped message="...">` in the JUnit XML, which is the only
reliable channel: `record_property` is function-scoped and most of these fire from
class-scoped fixtures.  `ENV[...]` reaches CI as the exception message on an errored setup.

Why the environment exception lives here rather than in `tests/environments/`: it shares
the category vocabulary with the skips, and the whole point of the taxonomy is that the skip
path and the raise path cannot drift apart.  `tests.environments` re-exports it, so that
import path resolves too.

Parsing these back out, and asserting an expected skip count per server version, is a
separate job and deliberately not implemented here.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, NoReturn

import pytest

# A `known-issue` skip must name the ticket that will retire it.  Without one the skip cannot
# be audited: nobody can tell whether the underlying defect was fixed three releases ago.
_TICKET_RE = re.compile(r'^[A-Z][A-Z0-9]+-\d+$')

# The one admissible non-ticket, for a defect real enough to disable a test over but not yet
# filed.  It exists so that the debt is greppable (`grep -r 'ticket=UNTRACKED'`) rather than
# dissolving into prose, and so that an author who has no key to hand does not invent a
# plausible one: `_TICKET_RE` cannot tell a real key from a fabricated one, but a grep finds
# every `UNTRACKED`.  Currently unused, which is the intended steady state.  File the ticket
# first; reach for this only when the defect blocks the work in hand and the filing cannot
# wait.
UNTRACKED_TICKET = 'UNTRACKED'


class SkipCategory(Enum):
    """Why a test did not run.

    Exactly one of these applies to any given skip.  Each has a `skip_*` function below,
    except `ENV_DEGRADED`, which deliberately has none.
    """

    # The server is real and healthy, but too old for the feature (or too new, for a
    # feature that was removed).  Expected, and countable from the server version alone.
    FEATURE_UNSUPPORTED = 'feature-unsupported'

    # The feature exists on a real server but the mock does not implement it.  Expected,
    # and countable from the run's mock type.
    MOCK_LIMITATION = 'mock-limitation'

    # The service the feature runs on is not deployed on this cluster.  A legitimate
    # deployment to run against: an analytics suite against a kv-only cluster is not a
    # broken environment, it is a cluster that was never asked to serve analytics.
    # Countable from the cluster's own service list, and distinct from the two categories
    # either side of it: `feature-unsupported` means the service is there and too old,
    # `topology` means the service is there and the wrong shape.
    SERVICE_ABSENT = 'service-absent'

    # The cluster is healthy but the wrong *shape* for this test: a single node when the
    # test needs several, no replicas configured when the test reads one.  A legitimate
    # deployment to run against, so this is expected rather than degraded.
    TOPOLOGY = 'topology'

    # A known product or test defect, deliberately not exercised.  Requires a ticket, so
    # the skip can be retired when the ticket closes.
    KNOWN_ISSUE = 'known-issue'

    # The environment failed to be what the suite was promised.  **Never a skip.**  There
    # is no `skip_env_degraded()` for this category on purpose; see `env_degraded()`.
    ENV_DEGRADED = 'env-degraded'

    # The test does not run under the wire protocol this run is exercising.  Expected, and
    # countable from the run's protocol rather than from the server version.
    PROTOCOL_MISMATCH = 'protocol-mismatch'


def render_value(value: Any) -> str:
    """Render one field value into the `key=value` grammar.

    Enums render as `.name`, so a `ServerFeatures` member reads `QueryIndexManagement`
    rather than `ServerFeatures.QueryIndexManagement` or its lowercase value.  Kept
    generic rather than importing the enums, so this module stays a leaf that
    `test_features` and `environments/` can both depend on.
    """
    if value is None:
        return 'unknown'
    if isinstance(value, Enum):
        return str(value.name)
    if isinstance(value, bool):
        return 'true' if value else 'false'
    text = str(value)
    if text == '':
        return '""'
    if any(ch.isspace() or ch == '"' for ch in text):
        escaped = text.replace('\\', '\\\\').replace('"', '\\"')
        return f'"{escaped}"'
    return text


def server_label(is_mock: bool) -> str:
    """The value for the conventional `server=` field."""
    return 'mock' if is_mock else 'real'


def _format(kind: str, category: SkipCategory, note: str | None, fields: dict[str, Any]) -> str:
    parts = [f'{kind}[{category.value}]']
    parts.extend(f'{key}={render_value(value)}' for key, value in fields.items())
    if note:
        parts.append(f'note={render_value(note)}')
    return ' '.join(parts)


class CouchbaseTestEnvironmentException(Exception):
    """Raised when something with the test environment is incorrect.

    Carries the taxonomy category as a *field* rather than through a class hierarchy, so
    that classifying a new failure mode never means adding a subclass.  It defaults to
    `ENV_DEGRADED` because that is what every existing raise site means, and because the
    other categories describe tests that were correctly not run rather than a broken
    environment.
    """

    def __init__(self,
                 message: str | None = None,
                 *,
                 category: SkipCategory = SkipCategory.ENV_DEGRADED) -> None:
        super().__init__(message)
        self.category = category

    def __repr__(self) -> str:
        return f'{type(self).__name__}[{self.category.value}]({super().__str__()})'

    def __str__(self) -> str:
        return self.__repr__()


def skip_reason(category: SkipCategory, *, note: str | None = None, **fields: Any) -> str:
    """Build a parseable skip reason without raising.

    Useful where the reason is computed in one place and acted on in another, as
    `EnvironmentFeatures.supports_feature` does.
    """
    if category is SkipCategory.ENV_DEGRADED:
        raise ValueError(
            'env-degraded is not a skip; a degraded environment must raise. '
            'Use env_degraded() or env_reason().'
        )
    if category is SkipCategory.KNOWN_ISSUE:
        ticket = str(fields.get('ticket', ''))
        if not (_TICKET_RE.match(ticket) or ticket == UNTRACKED_TICKET):
            raise ValueError(
                f'known-issue skips need ticket=<KEY> (e.g. ticket=PYCBC-1857); got '
                f'{ticket!r}.  A skip nobody can trace back to a defect never gets retired.'
            )
    return _format('SKIP', category, note, fields)


def env_reason(*, note: str | None = None, **fields: Any) -> str:
    """Build a parseable environment-failure message without raising."""
    return _format('ENV', SkipCategory.ENV_DEGRADED, note, fields)


def _skip(category: SkipCategory, *, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip the current test (or class, from a class-scoped fixture) with a parseable reason.

    Private on purpose.  Callers reach for one of the `skip_*` functions below, which is
    what keeps `ENV_DEGRADED` off the skip path: `skip_reason` refuses that category, but
    only once the line runs, and a class-scoped fixture can go a long time without running.
    With no `skip_env_degraded` in existence there is nothing to catch, because there is
    nothing to write.  Adding a category means adding a function here, which is the moment
    to decide what fields it must carry.
    """
    pytest.skip(skip_reason(category, note=note, **fields))


def skip_feature_unsupported(*, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip because the server is healthy but does not have the feature.

    Conventionally carries `feature=`, and whichever of `required=`/`observed=` the gate
    compared, so a run's skips can be counted against its server version.
    """
    _skip(SkipCategory.FEATURE_UNSUPPORTED, note=note, **fields)


def skip_mock_limitation(*, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip because the mock does not implement what a real server would.

    Conventionally carries `mock=`, which renders `unknown` for an unconfigured mock: still
    a mock, and still a skip.
    """
    _skip(SkipCategory.MOCK_LIMITATION, note=note, **fields)


def skip_service_absent(*, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip because the cluster does not run the service this test needs.

    Conventionally carries `service=` and the `feature=` that asked for it, plus `deployed=`
    listing what the cluster does run, so a reader can tell a kv-only cluster from a probe
    that came back empty.

    Never reachable on the mock.  The mock's story about its own services is not true
    (GoCAVES advertises query, search and analytics ports and implements none of them), so
    the service axis is not consulted there at all and `mock-limitation` answers first.
    """
    _skip(SkipCategory.SERVICE_ABSENT, note=note, **fields)


def skip_topology(*, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip because the cluster is the wrong shape for this test, not because it is broken.

    Conventionally carries what was wanted against what is deployed, e.g. `nodes=` with
    `required_nodes=`.  If the cluster has the shape and still cannot deliver it, that is
    `env_degraded()` instead.
    """
    _skip(SkipCategory.TOPOLOGY, note=note, **fields)


def skip_known_issue(*, ticket: str, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip a test that a known defect would otherwise fail.

    `ticket` is required by the signature rather than checked at skip time, so a skip that
    nobody can trace back to a defect cannot be written in the first place.  Pass
    `UNTRACKED_TICKET` only when the defect blocks the work in hand and the filing cannot
    wait.
    """
    _skip(SkipCategory.KNOWN_ISSUE, note=note, ticket=ticket, **fields)


def skip_protocol_mismatch(*, note: str | None = None, **fields: Any) -> NoReturn:
    """Skip because the test does not run under the protocol this run is exercising.

    Conventionally carries `unsupported=` against `active=`, so the count is checkable from
    the run's protocol rather than from the server version.
    """
    _skip(SkipCategory.PROTOCOL_MISMATCH, note=note, **fields)


def env_degraded(*, note: str | None = None, **fields: Any) -> NoReturn:
    """Fail loudly because the environment is not what the suite was promised.

    This is the counterpart to `skip()`, and the reason `ENV_DEGRADED` is in the same enum
    as the skip categories: the two exits share one vocabulary so that a CI run can account
    for every test it did not execute.
    """
    raise CouchbaseTestEnvironmentException(env_reason(note=note, **fields))
