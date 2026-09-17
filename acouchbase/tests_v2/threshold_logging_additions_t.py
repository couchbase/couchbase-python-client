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

import asyncio
from collections.abc import Callable, Generator
from time import time_ns
from typing import (Any,
                    ClassVar,
                    cast)

import pytest

from couchbase.logic.observability.observability_types import (DispatchAttributeName,
                                                               OpAttributeName,
                                                               ServiceType)
from couchbase.logic.observability.threshold_logging import (ThresholdLoggingReporter,
                                                             ThresholdLoggingSpan,
                                                             ThresholdLoggingTracer)
from couchbase.logic.options import ClusterTracingOptionsBase
from tests_v2.manifest import validate_test_manifest
from tests_v2.observability_test_impls import RecordingReporter

KV_THRESHOLD_MS = 0.5


class AsyncThresholdLoggingTests:
    # Not a generator sidecar, despite the name.  The sync counterpart is a cluster-free class
    # that nothing is generated from, so there is no generated class to merge into and this
    # file is collected directly, which is why it is a concrete `*Tests` class.
    #
    # The bar for living here: the test has to assert something that only exists under asyncio.
    # Both drive `run_in_executor` and `call_soon_threadsafe`, which is how `AsyncClientAdapter`
    # hands a span between the event loop and the C++ I/O thread.  The bar is about what a test
    # touches, not how it is spelled, so an `async def` with no `await` in it belongs in the
    # sync suite instead, where all three apis get it.

    TEST_MANIFEST: ClassVar[list[str]] = [
        'test_async_callback_lifecycle',
        'test_concurrent_async_operations',
    ]

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(self.__class__, self.__class__)

    @pytest.fixture(name='make_recording_tracer')
    def make_recording_tracer_fixture(
        self,
    ) -> Generator[Callable[[int], tuple[ThresholdLoggingTracer, RecordingReporter]], None, None]:
        tracers: list[ThresholdLoggingTracer] = []

        def _make(sample_size: int) -> tuple[ThresholdLoggingTracer, RecordingReporter]:
            config = {'key_value_threshold': KV_THRESHOLD_MS, 'threshold_sample_size': sample_size}
            tracer = ThresholdLoggingTracer(cast(ClusterTracingOptionsBase, config))
            tracer._reporter.stop()
            reporter = RecordingReporter()
            tracer._reporter = cast(ThresholdLoggingReporter, reporter)
            tracers.append(tracer)
            return tracer, reporter

        yield _make

        for tracer in tracers:
            tracer.close()

    @pytest.mark.asyncio
    async def test_async_callback_lifecycle(
        self,
        make_recording_tracer: Callable[[int], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        # The root span is created on the event loop, the dispatch child is created and ended
        # on an executor thread, the loop is woken with `call_soon_threadsafe`, and the loop
        # ends the root.  What is only true here is that a real event loop is what the span is
        # handed back to.
        tracer, reporter = make_recording_tracer(10)

        loop = asyncio.get_running_loop()
        iterations = 20

        for _ in range(iterations):
            # `request_span` is declared to return the `RequestSpan` base, and the rollup
            # counters asserted below belong to the concrete span it always builds.
            root = cast(ThresholdLoggingSpan,
                        tracer.request_span('upsert', start_time=time_ns() - 2_000_000))
            root.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

            callback_done = asyncio.Event()

            def io_callback(root: Any = root) -> None:
                dispatch = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                               parent_span=root,
                                               start_time=time_ns() - 500_000)
                dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 500)
                dispatch.set_attribute(DispatchAttributeName.PeerAddress.value, '127.0.0.1')
                dispatch.end(end_time=time_ns())
                loop.call_soon_threadsafe(callback_done.set)

            fut = loop.run_in_executor(None, io_callback)
            await callback_done.wait()
            await fut

            root.end(end_time=time_ns())

            assert root._total_server_duration_ns == 500
            assert root._span_snapshot is not None

        assert len(reporter.records) == iterations

    @pytest.mark.asyncio
    async def test_concurrent_async_operations(
        self,
        make_recording_tracer: Callable[[int], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        # `asyncio.gather` lets all fifty coroutines sit suspended at their `await` while fifty
        # executor threads write dispatch children through fifty different parents.  The record
        # count is what proves none of them ended up attached to the wrong root.
        tracer, reporter = make_recording_tracer(100)

        loop = asyncio.get_running_loop()
        operation_count = 50

        async def run_op(op_id: int) -> None:
            root = tracer.request_span(f'upsert_{op_id}', start_time=time_ns() - 2_000_000)
            root.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

            callback_done = asyncio.Event()

            def io_callback(root: Any = root) -> None:
                dispatch = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                               parent_span=root,
                                               start_time=time_ns() - 400_000)
                dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 200)
                dispatch.end(end_time=time_ns())
                loop.call_soon_threadsafe(callback_done.set)

            fut = loop.run_in_executor(None, io_callback)
            await callback_done.wait()
            await fut

            root.end(end_time=time_ns())

        await asyncio.gather(*[run_op(i) for i in range(operation_count)])

        assert len(reporter.records) == operation_count

        recorded_names = sorted(record['operation_name'] for _, record, _ in reporter.records)
        assert recorded_names == sorted(f'upsert_{i}' for i in range(operation_count))

        for _, record, duration in reporter.records:
            assert duration > 0
            assert record['total_duration_us'] > 0
            assert record['total_dispatch_duration_us'] > 0
