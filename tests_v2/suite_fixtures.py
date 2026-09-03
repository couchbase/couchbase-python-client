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

"""Fixtures shared by every suite in every api tree.

Only one thing may live here, and the rule is not stylistic:

    **A fixture in this module must be pure.  No SDK call, no I/O, no await.**

The generator resolves which methods need bridging from an *in-file* class map, seeded from
each class's in-file base classes.  A base class imported from another module contributes an
empty set, because the generator never parsed it.  So an async-needing fixture placed here
would be inherited by an acouchbase mirror with no `await` inserted and no diagnostic: the call
would return a coroutine, the attribute read on it would raise `AttributeError`, and the suite
would report a broken environment.  The generator cannot warn about this, which is exactly why
the constraint is written down rather than left to be noticed.

That is why the topology *facts* are gathered by `TestEnvironment._collect_topology`, which is
overridden per api style, while the fixtures below only ever read the resulting plain ints.  It
is the same division `assert_helpers` follows, and the reason `assert_all_expired` is a function
annotated `SyncTestEnvironment` rather than a method: put the style-dependent half where the
style is already known, and the style-independent half becomes shareable.

A fixture that genuinely needs an SDK call belongs in the suite file, duplicated, where the
generator can rewrite it.
"""

from __future__ import annotations

import pytest

from tests_v2.environments.test_environment import SuiteRequirements, TestEnvironment
from tests_v2.mock_server import MockServerType
from tests_v2.skips import (env_degraded,
                            server_label,
                            skip_mock_limitation,
                            skip_topology)
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class TopologySuite(SuiteRequirements):
    """A suite that needs node-count, replica, server-group and durability-support fixtures.

    Two of these are feature gates rather than topology facts, and they are here for a reason
    that is easy to mistake for laziness.  `REQUIRED_FEATURES` skips a whole suite, which is the
    right tool when every test in it needs the feature.  Server groups and synchronous
    durability are not like that: `binary_durability_t` gates 8 of its 20 tests on synchronous
    durability and the other 12 run on any server, so declaring it suite-wide would retire 12
    tests' worth of client-durability coverage against a pre-6.5 cluster.  A per-test fixture is
    the only thing that expresses that, and it needs a home shared by the three durability
    suites that all want the same one.

    Subclasses SuiteRequirements rather than sitting beside it as a mixin, so a suite still has
    exactly one base class:

        class DurabilityMultiTestSuite(TopologySuite):   # not (SuiteRequirements, SomeMixin)
            ...

    Inheriting this is also what makes `build_environment` collect the facts, since it tests
    `issubclass(suite_cls, TopologySuite)`.  A suite therefore gets the fixtures and the data
    together or not at all, which an eighth SuiteRequirements flag could have drifted.  Suites
    with no interest in topology keep inheriting SuiteRequirements directly and pay nothing.

    **If a second group of shared fixtures ever appears**, do not add a second sibling class and
    reach for multiple inheritance.  Fold both groups into one suite base and gate the I/O with a
    declared flag per group: forgetting the flag raises from `TestEnvironment.topology` with a
    message naming the fix, so a declared flag fails loudly rather than silently, and one base
    class per suite is worth more than the derivation.

    Every fixture body here is pure; see the module docstring for why that is mandatory.
    """

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env: TestEnvironment) -> int:
        return cb_env.topology.num_nodes

    @pytest.fixture(scope='class')
    def num_replicas(self, cb_env: TestEnvironment) -> int:
        replicas = cb_env.topology.test_bucket_replicas
        if replicas is None:
            # Reading this needs the bucket manager, and the suite did not declare one.  A
            # broken environment rather than a skip: the suite asked for a number the
            # framework could not supply, which is a wiring mistake in the suite, not a
            # property of the cluster it is running against.
            env_degraded(stage='topology',
                         server=server_label(cb_env.is_mock_server),
                         note='num_replicas needs TestEnvironmentBucketManager in MANAGERS')
        return replicas

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes: int) -> None:
        if num_nodes == 1:
            skip_topology(nodes=num_nodes,
                          required_nodes='2+',
                          note='test needs more than a single node')

    @pytest.fixture(scope='class')
    def check_single_node(self, num_nodes: int) -> None:
        # Not the inverse of check_multi_node in intent: the tests behind this assert that a
        # durability requirement *cannot* be met, which is only true on a single node.
        if num_nodes != 1:
            skip_topology(nodes=num_nodes,
                          required_nodes=1,
                          note='test asserts durability is impossible, which needs a single node')

    @pytest.fixture(scope='class')
    def check_replicas_configured(self, num_replicas: int) -> None:
        """Skip unless the test bucket asks for at least one replica.

        Not interchangeable with check_replicas_online, and near enough in name that
        collapsing the two onto one fixture reads as deduplication.  It is not: this one skips
        a 0-replica bucket that the other happily runs, and the other skips GoCAVES and
        offline replicas that this one ignores.

        It is load-bearing for the four `test_server_durable_*_single_node` tests, which is the
        non-obvious part.  Those assert that PERSIST_TO_MAJORITY raises
        DurabilityImpossibleException on one node, and that is only true when the bucket wants
        more copies than one node can host.  Against a single-node 0-replica bucket a majority
        is one node, so the write *succeeds* and `pytest.raises` fails the test rather than
        skipping it.  A predicate on the replica count is what keeps that from being reported as
        an SDK defect.
        """
        if num_replicas == 0:
            skip_topology(replicas=num_replicas,
                          required_replicas='1+',
                          note='bucket declares no replicas, so there is no durability to test')

    @pytest.fixture(scope='class')
    def check_replicas_online(self, cb_env: TestEnvironment, num_replicas: int, num_nodes: int) -> None:
        if cb_env.is_mock_server and cb_env.mock_server_type == MockServerType.GoCAVES:
            skip_mock_limitation(mock=cb_env.mock_server_type,
                                 server=server_label(cb_env.is_mock_server),
                                 note='GoCAVES is inconsistent with replicas')
        online = cb_env.topology.test_bucket_kv_endpoints
        needed = num_replicas + 1
        if online < needed:
            # Two very different situations, which the taxonomy forces apart.  A cluster with
            # fewer nodes than the bucket's replica count cannot satisfy it however healthy
            # it is, which is a legitimate shape to run against.  A cluster with enough nodes
            # whose replicas are nonetheless not online is broken, and skipping there retires
            # the replica coverage on a green run, which is what the taxonomy exists to stop.
            if num_nodes < needed:
                skip_topology(nodes=num_nodes,
                              replicas=num_replicas,
                              kv_endpoints=online,
                              required_nodes=needed,
                              note='cluster has too few nodes to host the requested replicas')
            env_degraded(stage='check-replicas',
                         nodes=num_nodes,
                         replicas=num_replicas,
                         kv_endpoints=online,
                         required_endpoints=needed,
                         server=server_label(cb_env.is_mock_server),
                         note='enough nodes for the requested replicas, but they are not online')

    @pytest.fixture(scope='class')
    def check_server_groups_supported(self, cb_env: TestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.ServerGroups, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_sync_durability_supported(self, cb_env: TestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.SynchronousDurability, cb_env.server_context)
