#!/usr/bin/env python3
"""
OpenTelemetry Metrics with Prometheus and Tracing with Jaeger Example

Demonstrates observability with metrics (histograms) and tracing

What this example shows:
- Metrics: Operation latency histograms exposed via Prometheus
- Tracing: Spans for operations, exported to Jaeger

Installation:
    pip install couchbase[otel]
    pip install opentelemetry-exporter-otlp-proto-grpc~=1.22

Quick Start:
    # Start backend services (Prometheus, Jaeger & OTLP Collector)
    docker-compose up -d

    # Run example
    # NOTE: if running from the repo and using the examples path, update the PYTHONPATH (temporarily for the example):
    # export PYTHONPATH="${PYTHONPATH}:$(pwd)"
    python examples/observability/otel_otlp_metrics_and_tracing_exporter.py

    # View in Prometheus: http://localhost:9090
    # View traces in Jaeger: http://localhost:16686

Expected Output:
- Console showing operations being performed
- Histograms visible in Prometheus UI
- Spans visible in Jaeger UI
"""

import time
from datetime import timedelta

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.observability.otel_metrics import get_otel_meter
from couchbase.observability.otel_tracing import get_otel_tracer
from couchbase.options import (ClusterOptions,
                               GetOptions,
                               QueryOptions,
                               RemoveOptions,
                               UpsertOptions)

# Configuration
SERVICE_NAME = "couchbase-otel-example"
OTLP_ENDPOINT = "localhost:4317"
CONNECTION_STRING = 'couchbase://localhost'
BUCKET_NAME = 'default'
USERNAME = 'Administrator'
PASSWORD = 'password'  # nosec


def setup_otel_metrics(resource):
    """Setup OpenTelemetry for metrics."""

    exporter = OTLPMetricExporter(
        endpoint=OTLP_ENDPOINT,
        insecure=True
    )
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[reader]
    )
    metrics.set_meter_provider(meter_provider)

    return meter_provider, get_otel_meter(meter_provider)


def setup_otel_tracing(resource):

    # 1. Create the Provider
    tracer_provider = TracerProvider(resource=resource, sampler=ALWAYS_ON)

    # 2. Create the OTLP gRPC Exporter
    exporter = OTLPSpanExporter(
        endpoint=OTLP_ENDPOINT,
        insecure=True
    )

    # 3. Attach the Exporter to a Batch Processor
    span_processor = BatchSpanProcessor(exporter)
    tracer_provider.add_span_processor(span_processor)

    # 4. Set the global provider
    trace.set_tracer_provider(tracer_provider)

    # Return the Couchbase wrapper and the provider (so we can shut it down)
    return tracer_provider, get_otel_tracer(tracer_provider)


def perform_operations(collection, cluster):
    print("\n" + "-"*80)
    print("Performing Operations (Generating Traces & Metrics)")
    print("-"*80)
    print()

    # Test documents
    docs = {
        'testdoc:1': {'name': 'Alice', 'age': 30, 'type': 'user', 'user_role': 'admin'},
        'testdoc:2': {'name': 'Bob', 'age': 25, 'type': 'user', 'user_role': 'developer'},
        'testdoc:3': {'name': 'Charlie', 'age': 35, 'type': 'user', 'user_role': 'manager'},
        'testdoc:4': {'name': 'Diana', 'age': 28, 'type': 'user', 'user_role': 'designer'},
        'testdoc:5': {'name': 'Eve', 'age': 32, 'type': 'user', 'user_role': 'analyst'},
    }

    # Upsert operations
    print("1. Upserting documents...")
    for key, doc in docs.items():
        result = collection.upsert(key, doc)
        print(f"   ✓ Upserted '{key}'")

    # Get operations
    print("\n2. Retrieving documents...")
    for round_num in range(1, 4):
        print(f"   Round {round_num}:")
        for key in docs.keys():
            result = collection.get(key)
            content = result.content_as[dict]
        print(f"     ✓ Retrieved all {len(docs)} documents")

    # Replace operations
    print("\n3. Replacing documents...")
    for key in list(docs.keys())[:3]:  # Just first 3
        get_result = collection.get(key)
        doc = get_result.content_as[dict]
        doc['updated'] = True
        collection.replace(key, doc, cas=get_result.cas)
        print(f"   ✓ Replaced '{key}'")

    # Touch operations
    print("\n4. Touching documents (updating expiry)...")
    for key in list(docs.keys())[3:]:  # Last 2
        collection.touch(key, timedelta(hours=1))
        print(f"   ✓ Touched '{key}'")

    # Query operation
    print("\n5. Executing N1QL query...")
    query = f"SELECT name, user_role FROM `{BUCKET_NAME}` WHERE type = 'user' LIMIT 3"  # nosec
    try:
        result = cluster.query(query)
        rows = list(result.rows())
        print(f"   ✓ Query returned {len(rows)} rows")
        for row in rows:
            print(f"      - {row['name']}: {row['user_role']}")
    except Exception as e:
        print(f"   ⚠ Query failed: {e}")

    # Cleanup
    print("\n6. Cleaning up...")
    for key in docs.keys():
        try:
            collection.remove(key)
            print(f"   ✓ Removed '{key}'")
        except Exception:  # nosec
            pass

    print("\n" + "-"*80)
    print("All operations completed!")
    print("-"*80)


def perform_operations_with_parent(collection, cluster):
    app_tracer = trace.get_tracer("my-python-app")
    docs = {f'doc:{i}': {'name': f'User_{i}', 'type': 'user'} for i in range(3)}

    print("\nExecuting workload (generating traces and metrics)...")

    with app_tracer.start_as_current_span("process_batch") as parent_span:
        parent_span.set_attribute("batch.size", len(docs))

        for key, doc in docs.items():
            collection.upsert(key, doc, UpsertOptions(parent_span=parent_span))
            collection.get(key, GetOptions(parent_span=parent_span))

        try:
            query = f"SELECT * FROM `{BUCKET_NAME}` LIMIT 2"  # nosec
            cluster.query(query, QueryOptions(parent_span=parent_span)).execute()
        except Exception:
            pass  # nosec

        for key in docs.keys():
            try:
                collection.remove(key, RemoveOptions(parent_span=parent_span))
            except Exception:
                pass  # nosec
    print("✓ Workload w/ parent complete")


def main():
    print("="*80)
    print("OpenTelemetry OTLP Combined (Metrics + Tracing) Example")
    print("="*80 + "\n")

    t_provider = None
    m_provider = None

    resource = Resource.create(attributes={
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
    })

    try:
        print("Setting up OpenTelemetry...")
        m_provider, cb_meter = setup_otel_metrics(resource)
        t_provider, cb_tracer = setup_otel_tracing(resource)

        print(f"Connecting to Couchbase at {CONNECTION_STRING}...")
        opts = ClusterOptions(
            PasswordAuthenticator(USERNAME, PASSWORD),
            tracer=cb_tracer,
            meter=cb_meter
        )

        cluster = Cluster.connect(CONNECTION_STRING, opts)
        collection = cluster.bucket(BUCKET_NAME).default_collection()

        perform_operations(collection, cluster)
        perform_operations_with_parent(collection, cluster)
        cluster.close()

        # Wait a moment for the periodic metric reader to catch the final data
        time.sleep(2)

    except Exception as e:
        print(f"\nERROR: {e}")
    finally:
        print("\nFlushing telemetry data...")
        if t_provider:
            t_provider.shutdown()
        if m_provider:
            m_provider.shutdown()
        print("✓ Telemetry flushed gracefully.\n")

        print("📊 VIEW YOUR DATA:")
        print("  • Traces:  http://localhost:16686 (Jaeger)")
        print("  • Metrics: http://localhost:9090  (Prometheus)")


if __name__ == "__main__":
    main()
