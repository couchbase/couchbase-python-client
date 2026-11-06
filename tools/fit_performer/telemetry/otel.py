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

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider as OtelMeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.sampling import (ALWAYS_OFF,
                                              ALWAYS_ON,
                                              TraceIdRatioBased)

from couchbase.observability.otel_metrics import get_otel_meter
from couchbase.observability.otel_tracing import get_otel_tracer

if TYPE_CHECKING:
    from opentelemetry.context import Token
    from opentelemetry.context.context import Context
    from opentelemetry.sdk.trace.sampling import Sampler

    from couchbase.observability.metrics import Meter
    from couchbase.observability.tracing import RequestTracer
    from couchbase.options import ClusterOptions

    from observability import config_pb2 as observability_config_pb
    from observability import top_pb2 as observability_pb

    from .span_owner import SpanOwner

logger = logging.getLogger(__name__)


def _parse_resources(resources_map: Mapping[str, observability_pb.Attribute]) -> dict[str, str | int | bool]:
    resources: dict[str, str | int | bool] = {}
    for key, attr in resources_map.items():
        which = attr.WhichOneof('value')
        if which == 'value_string':
            resources[key] = attr.value_string
        elif which == 'value_long':
            resources[key] = attr.value_long
        elif which == 'value_boolean':
            resources[key] = attr.value_boolean
    return resources


def create_tracer_provider(tracing_config: observability_config_pb.TracingConfig
                           ) -> tuple[RequestTracer, TracerProvider]:
    # TODO:  We should probably put this "behind" an ENV variable
    logging.getLogger('opentelemetry').setLevel(logging.DEBUG)

    resources = _parse_resources(tracing_config.resources)

    epsilon = 0.00001
    sampler: Sampler
    if tracing_config.sampling_percentage < epsilon:
        sampler = ALWAYS_OFF
    elif tracing_config.sampling_percentage > 1.0 - epsilon:
        sampler = ALWAYS_ON
    else:
        sampler = TraceIdRatioBased(tracing_config.sampling_percentage)

    provider = TracerProvider(
        resource=Resource.create(resources),
        sampler=sampler
    )

    exporter = OTLPSpanExporter(
        endpoint=tracing_config.endpoint_hostname,
        insecure=True
    )

    processor: BatchSpanProcessor | SimpleSpanProcessor
    if tracing_config.batching:
        kwargs: dict[str, Any] = {}
        if tracing_config.export_every_millis > 0:
            kwargs['schedule_delay_millis'] = tracing_config.export_every_millis
        processor = BatchSpanProcessor(exporter, **kwargs)
    else:
        processor = SimpleSpanProcessor(exporter)

    provider.add_span_processor(processor)

    return get_otel_tracer(provider), provider


def create_meter_provider(metrics_config: observability_config_pb.MetricsConfig
                          ) -> tuple[Meter, OtelMeterProvider]:
    # TODO:  We should probably put this "behind" an ENV variable
    logging.getLogger('opentelemetry').setLevel(logging.DEBUG)

    resources = _parse_resources(metrics_config.resources)

    exporter = OTLPMetricExporter(
        endpoint=metrics_config.endpoint_hostname,
        insecure=True
    )

    reader_kwargs: dict[str, Any] = {}
    if metrics_config.export_every_millis > 0:
        reader_kwargs['export_interval_millis'] = metrics_config.export_every_millis
    reader = PeriodicExportingMetricReader(exporter, **reader_kwargs)

    meter_provider = OtelMeterProvider(
        resource=Resource.create(resources),
        metric_readers=[reader]
    )

    return get_otel_meter(meter_provider), meter_provider


def worker_otel_setup(tracer: RequestTracer | None,
                      meter: Meter | None,
                      span_contexts: dict[str, str],
                      options: ClusterOptions,
                      ) -> tuple[list[Token[Context]], SpanOwner | None]:
    """Set up OTel context for a worker process.

    Injects tracer/meter into cluster options, attaches ambient W3C context tokens,
    and builds a SpanOwner populated with remote-parent proxies.

    Returns (otel_tokens, worker_span_owner); pass tokens to worker_otel_teardown.
    """
    if tracer is not None:
        options['tracer'] = tracer
    if meter is not None:
        options['meter'] = meter

    otel_tokens: list[Token[Context]] = []
    worker_span_owner: SpanOwner | None = None
    if span_contexts:
        from opentelemetry import context as otel_context
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        from .span_owner import SpanOwner

        prop = TraceContextTextMapPropagator()
        for traceparent in span_contexts.values():
            otel_tokens.append(otel_context.attach(prop.extract({'traceparent': traceparent})))
        worker_span_owner = SpanOwner.from_worker_contexts(span_contexts)

    return otel_tokens, worker_span_owner


def worker_otel_teardown(otel_tokens: list[Token[Context]],
                         tracer_provider: TracerProvider | None,
                         meter_provider: OtelMeterProvider | None,
                         ) -> None:
    """Detach ambient OTel context tokens and flush/shut down providers after worker exits."""
    if otel_tokens:
        from opentelemetry import context as otel_context
        for token in reversed(otel_tokens):
            otel_context.detach(token)
    if tracer_provider is not None:
        tracer_provider.force_flush()
    if meter_provider is not None:
        meter_provider.shutdown()
