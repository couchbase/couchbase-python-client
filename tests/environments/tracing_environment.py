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

"""Tracer setup and validator access for the tracing suites.

The tracing half of what `metrics_environment.py` does for the metrics suites, and for the same
reasons: static methods over a `cb_env` rather than a `TestEnvironment` subclass, one module for
all three apis because nothing here awaits, and the tracer read back off the cluster rather than
remembered by the fixture that built it.

**Four tracing types where metrics has two.**  `MeterType` has no `Legacy` member because the
deprecated meter is not a mode of the suite, just a different object to pass in.  A tracer is
different: `ClusterOptions(tracer=...)` accepts a deprecated `CouchbaseTracer` as well as a
current `RequestTracer`, and the SDK notices the difference and drives a different code path,
recording strictly less.  So the deprecated tracer is a fourth parameter of the tracing suites,
and every one of them runs four times.
"""

from __future__ import annotations

from enum import IntEnum

from couchbase.logic.observability import CollectionDetails
from tests.environments.test_environment import TestEnvironment
from tests.observability_test_impls import (InertTracer,
                                            LegacyRecordingTracer,
                                            RecordingTracer,
                                            TestTracerType,
                                            ThresholdRecordingTracer)
from tests.tracing_validators import HttpSpanValidator, KeyValueSpanValidator


def expects_cluster_labels(cb_env: TestEnvironment) -> bool:
    """Whether the SDK can label this cluster's spans and metrics with its name and UUID.

    `supports_cluster_labels` reads the name from `/pools/default`, which every supported
    server carries.  The SDK reads it from the bucket configuration instead, and that only
    carries `clusterName` and `clusterUUID` from 7.6.4 (MB-63870), so an older cluster with a
    name still produces no labels.
    """
    if not cb_env.supports_cluster_labels:
        return False
    short, patch = cb_env.server_version_short, cb_env.server_version_patch
    if short is None or patch is None:
        return False
    return short > 7.6 or (short == 7.6 and patch >= 4)


class TracingType(IntEnum):
    """Which tracer a suite's cluster is built with.

    `Basic` records the span tree and is asserted on.  `ThresholdLogging` is the SDK's own
    threshold-logging tracer, so what gets asserted is its roll-ups rather than the tree.
    `Legacy` is the deprecated `couchbase.tracing` base, which carries neither a span status nor
    the cluster labels.  `NoOp` proves the SDK's no-op short-circuit is taken and no span is
    created at all.
    """
    Basic = 1
    Legacy = 2
    NoOp = 3
    ThresholdLogging = 4


class TracingEnvironment:
    """Tracer construction and validator access for a tracing suite."""

    @staticmethod
    def make_tracer(tracing_type: TracingType) -> TestTracerType:
        if tracing_type is TracingType.Basic:
            return RecordingTracer()
        if tracing_type is TracingType.Legacy:
            return LegacyRecordingTracer()
        if tracing_type is TracingType.NoOp:
            return InertTracer()
        return ThresholdRecordingTracer()

    @staticmethod
    def tracer(cb_env: TestEnvironment) -> TestTracerType:
        """The tracer the suite's cluster was actually built with.

        Read back off the cluster for the same reason the meter is: a suite that asserted on a
        tracer the cluster is not using would pass while testing nothing.  `WrappedTracer` is
        the SDK's own record of which of the two tracer bases it found, so unwrapping it here is
        reading the SDK's answer rather than repeating the question.
        """
        tracer = cb_env.cluster._impl.observability_instruments.tracer.tracer
        assert isinstance(tracer, (RecordingTracer, LegacyRecordingTracer,
                                   InertTracer, ThresholdRecordingTracer)), (
            f'cluster was not built with a test tracer, found {type(tracer).__name__!r}; the '
            'suite needs CONNECTION_STRATEGY = NEW_CONNECTION and tracer= passed to '
            'build_environment'
        )
        return tracer

    @staticmethod
    def collection_details(cb_env: TestEnvironment) -> CollectionDetails:
        return CollectionDetails(bucket_name=cb_env.bucket.name,
                                 scope_name=cb_env.scope.name,
                                 collection_name=cb_env.collection.name)

    @staticmethod
    def kv_span_validator(cb_env: TestEnvironment) -> KeyValueSpanValidator:
        return KeyValueSpanValidator(TracingEnvironment.tracer(cb_env),
                                     TracingEnvironment.collection_details(cb_env),
                                     expects_cluster_labels(cb_env))

    @staticmethod
    def http_span_validator(cb_env: TestEnvironment) -> HttpSpanValidator:
        return HttpSpanValidator(TracingEnvironment.tracer(cb_env), expects_cluster_labels(cb_env))
