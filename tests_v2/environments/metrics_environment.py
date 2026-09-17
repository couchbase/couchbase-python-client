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

"""Meter setup and validator access for the metrics suites.

Static methods over a `cb_env`, the `QueryEnvironment` idiom, rather than a
`TestEnvironment` subclass: `tests_v2` has no per-feature environment subclasses, and the
cluster side is already in `test_environment.py`.  `_build_cluster_options` takes `meter=`,
`ConnectionStrategy` `NEW_CONNECTION` builds the fresh cluster a per-suite meter needs, and
`build_environment` forwards both.  A suite passes its meter in and reaches the validator back
through here.

**One module for all three apis, unlike the rest of `environments/`.**  Its siblings split per
api style because their bodies await; nothing here does.  The meter is configured through the
shared `couchbase.options.ClusterOptions` that all three apis take, and it is read back off
`cluster._impl.observability_instruments`, which `couchbase`, `acouchbase` and `txcouchbase`
each hold under that same name.  So there is no async or twisted variant of this file to
write, and the generated suites import it unchanged.
"""

from __future__ import annotations

from enum import IntEnum

from couchbase.logic.observability import CollectionDetails, MeterProtocol
from tests_v2.environments.test_environment import TestEnvironment
from tests_v2.environments.tracing_environment import expects_cluster_labels
from tests_v2.metrics_validators import HttpMeterValidator, KeyValueMeterValidator
from tests_v2.observability_test_impls import (InertMeter,
                                               LegacyRecordingMeter,
                                               RecordingMeter,
                                               TestMeterType)


class MeterType(IntEnum):
    """Which meter a suite's cluster is built with.

    `Basic` records and is asserted on; `NoOp` proves the SDK's no-op short-circuit is taken.
    Unrelated to `TestProtocol`, and unrelated to the tracer's `TracingType`, which does have a
    `Legacy` member where this does not.  The SDK branches on the tracer it was given, so a
    deprecated tracer is a mode of the suite; it has no matching branch for the meter, so a
    deprecated meter is a different object handed to the same `meter=` rather than a mode.  See
    `make_legacy_meter`.
    """
    Basic = 1
    NoOp = 2


class MetricsEnvironment:
    """Meter construction and validator access for a metrics suite."""

    @staticmethod
    def make_meter(meter_type: MeterType) -> TestMeterType:
        return RecordingMeter() if meter_type is MeterType.Basic else InertMeter()

    @staticmethod
    def make_legacy_meter() -> LegacyRecordingMeter:
        """A meter on the deprecated `couchbase.metrics` bases, for the canary suite.

        Not a `MeterType` member, and that is the point: the deprecated meter is selected by
        nothing but the class an application happens to derive from, so it is a different
        meter object rather than a different mode of the same suite.
        """
        return LegacyRecordingMeter()

    @staticmethod
    def _configured_meter(cb_env: TestEnvironment) -> MeterProtocol:
        """The meter the suite's cluster was actually built with.

        Read back off the cluster rather than remembered by the fixture that made it.  The
        fixture would have to hand it to every consumer, and a suite that asserted on a meter
        the cluster is not using would pass while testing nothing.  Reaching through `_impl`
        buys that guarantee, and it is the same reach `cluster._impl._cluster_info` already
        makes elsewhere in this tree.
        """
        return cb_env.cluster._impl.observability_instruments.meter

    @staticmethod
    def meter(cb_env: TestEnvironment) -> TestMeterType:
        meter = MetricsEnvironment._configured_meter(cb_env)
        assert isinstance(meter, (RecordingMeter, InertMeter)), (
            f'cluster was not built with a test meter, found {type(meter).__name__!r}; '
            'the suite needs CONNECTION_STRATEGY = NEW_CONNECTION and meter= passed to '
            'build_environment'
        )
        return meter

    @staticmethod
    def legacy_meter(cb_env: TestEnvironment) -> LegacyRecordingMeter:
        meter = MetricsEnvironment._configured_meter(cb_env)
        assert isinstance(meter, LegacyRecordingMeter), (
            f'cluster was not built with the deprecated test meter, found '
            f'{type(meter).__name__!r}; the suite needs CONNECTION_STRATEGY = NEW_CONNECTION '
            'and meter= passed to build_environment'
        )
        return meter

    @staticmethod
    def collection_details(cb_env: TestEnvironment) -> CollectionDetails:
        """Where the suite's operations land, in the shape the SDK labels them with.

        Constructed rather than written as a dict literal.  `CollectionDetails` is the
        SDK's own `TypedDict` and its members are `bucket_name`, `scope_name` and
        `collection_name`, none of which are the words a reader would reach for unprompted.
        Calling it puts those names at the construction site, where a near-miss like `'bucket'`
        or `'scope'` is a keyword error rather than a key nothing ever reads.
        """
        return CollectionDetails(bucket_name=cb_env.bucket.name,
                                 scope_name=cb_env.scope.name,
                                 collection_name=cb_env.collection.name)

    @staticmethod
    def kv_meter_validator(cb_env: TestEnvironment) -> KeyValueMeterValidator:
        return KeyValueMeterValidator(MetricsEnvironment.meter(cb_env),
                                      MetricsEnvironment.collection_details(cb_env),
                                      expects_cluster_labels(cb_env))

    @staticmethod
    def http_meter_validator(cb_env: TestEnvironment) -> HttpMeterValidator:
        """No collection details, unlike the key-value validator.

        An http operation carries only as much of the bucket/scope/collection triple as
        applies to it, and which part that is changes per operation rather than per suite, so
        the test states it at the assertion instead of the environment supplying it once.
        """
        return HttpMeterValidator(MetricsEnvironment.meter(cb_env), expects_cluster_labels(cb_env))
