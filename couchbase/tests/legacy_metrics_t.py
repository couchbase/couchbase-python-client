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

from __future__ import annotations

from collections.abc import Generator

import pytest

from couchbase.logic.observability import OpName
from tests.environments.metrics_environment import MetricsEnvironment
from tests.environments.test_environment import (ConnectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestProtocol,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.observability_test_impls import LegacyRecordingMeter


class LegacyMetricsTestSuite(SuiteRequirements):
    # A canary rather than the parity the deprecated tracer gets, and the asymmetry is the
    # SDK's: `handler.py` carries `is_legacy` on the wrapped tracer and branches on it
    # repeatedly, while `CouchbaseMeter` is an empty subclass of `Meter` and the only meter the
    # handler distinguishes is `NoOpMeter`.  A deprecated meter therefore reaches the SDK
    # through the same code the `metrics_*` suites already assert in detail, so each test here
    # asks the one question those cannot: was the deprecated meter handed its latency at all.
    #
    # Five operations rather than one, because the SDK labels the recorder per operation: a
    # canary driving only `upsert` could not tell a meter that stopped being called from a
    # label that stopped being set.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A meter is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    NUM_DOCS = 10

    TEST_MANIFEST = [
        'test_get',
        'test_insert',
        'test_remove',
        'test_replace',
        'test_upsert',
    ]

    @pytest.fixture(name='legacy_meter')
    def legacy_meter_fixture(self, cb_env: SyncTestEnvironment) -> LegacyRecordingMeter:
        # Cleared here rather than in a teardown so that each test's assertion is about its
        # own operation: the meter is class-scoped and has already recorded every upsert the
        # data seeding performed by the time the first test runs.
        meter = MetricsEnvironment.legacy_meter(cb_env)
        meter.clear()
        return meter

    def test_get(self, cb_env: SyncTestEnvironment, legacy_meter: LegacyRecordingMeter) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get(key)
        assert legacy_meter.measured_op_names() == [OpName.Get.value]

    def test_insert(self, cb_env: SyncTestEnvironment, legacy_meter: LegacyRecordingMeter) -> None:
        key, value = cb_env.get_new_doc()
        cb_env.collection.insert(key, value)
        assert legacy_meter.measured_op_names() == [OpName.Insert.value]

    def test_remove(self, cb_env: SyncTestEnvironment, legacy_meter: LegacyRecordingMeter) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.remove(key)
        assert legacy_meter.measured_op_names() == [OpName.Remove.value]

    def test_replace(self, cb_env: SyncTestEnvironment, legacy_meter: LegacyRecordingMeter) -> None:
        key, value = cb_env.get_existing_doc()
        cb_env.collection.replace(key, value)
        assert legacy_meter.measured_op_names() == [OpName.Replace.value]

    def test_upsert(self, cb_env: SyncTestEnvironment, legacy_meter: LegacyRecordingMeter) -> None:
        key, value = cb_env.get_existing_doc()
        cb_env.collection.upsert(key, value)
        assert legacy_meter.measured_op_names() == [OpName.Upsert.value]


class LegacyMetricsTests(LegacyMetricsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(LegacyMetricsTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                LegacyMetricsTestSuite,
                                meter=MetricsEnvironment.make_legacy_meter())
        yield env
        teardown_environment(env, LegacyMetricsTestSuite)
