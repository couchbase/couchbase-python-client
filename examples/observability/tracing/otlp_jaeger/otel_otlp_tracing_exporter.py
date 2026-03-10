#!/usr/bin/env python3
"""
OpenTelemetry Tracing with Jaeger Example

Demonstrates observability with tracing

Installation:
    pip install couchbase[otel]
    pip install opentelemetry-exporter-otlp-proto-grpc~=1.22

Quick Start:
    # Start backend services (Jaeger & OTLP Collector)
    docker-compose up -d

    # Run example
    # NOTE: if running from the repo and using the examples path, update the PYTHONPATH (temporarily for the example):
    # export PYTHONPATH="${PYTHONPATH}:$(pwd)"
    python examples/observability/otel_otlp_tracing_exporter.py

    # View in Jaeger: http://localhost:16686

Expected Output:
- Console showing operations being performed
- Spans visible in Jaeger UI
"""

from datetime import timedelta

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.observability.otel_tracing import get_otel_tracer
from couchbase.options import ClusterOptions

# Configuration
SERVICE_NAME = 'couchbase-otel-tracing-example'
TRACING_ENDPOINT = 'localhost:4317'
CONNECTION_STRING = 'couchbase://localhost'
BUCKET_NAME = 'default'
USERNAME = 'Administrator'
PASSWORD = 'password'  # nosec


def setup_otel_tracing():
    resource = Resource.create(attributes={
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
    })

    # 1. Create the Provider
    tracer_provider = TracerProvider(resource=resource, sampler=ALWAYS_ON)

    # 2. Create the OTLP gRPC Exporter
    exporter = OTLPSpanExporter(
        endpoint=TRACING_ENDPOINT,
        insecure=True
    )

    # 3. Attach the Exporter to a Batch Processor
    span_processor = BatchSpanProcessor(exporter)
    tracer_provider.add_span_processor(span_processor)

    # 4. Set the global provider
    trace.set_tracer_provider(tracer_provider)

    # Return the Couchbase wrapper and the provider (so we can shut it down)
    return get_otel_tracer(tracer_provider), tracer_provider


def print_banner():
    print("\n" + "="*80)
    print("OpenTelemetry OTLP Tracing Export Example (Jaeger)")
    print("="*80 + "\n")


def perform_operations(collection, cluster):
    print("\n" + "-"*80)
    print("Performing Operations (Generating Traces)")
    print("-"*80)
    print()

    # Test documents
    docs = {
        'tracing:1': {'name': 'Alice', 'age': 30, 'type': 'user', 'user_role': 'admin'},
        'tracing:2': {'name': 'Bob', 'age': 25, 'type': 'user', 'user_role': 'developer'},
        'tracing:3': {'name': 'Charlie', 'age': 35, 'type': 'user', 'user_role': 'manager'},
        'tracing:4': {'name': 'Diana', 'age': 28, 'type': 'user', 'user_role': 'designer'},
        'tracing:5': {'name': 'Eve', 'age': 32, 'type': 'user', 'user_role': 'analyst'},
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


def main():
    print_banner()
    tracer_provider = None

    try:
        print("Setting up OpenTelemetry Tracing...")
        couchbase_tracer, tracer_provider = setup_otel_tracing()
        print("✓ OpenTelemetry Tracing configured\n")

        print(f"Connecting to Couchbase at {CONNECTION_STRING}...")
        opts = ClusterOptions(
            PasswordAuthenticator(USERNAME, PASSWORD),
            tracer=couchbase_tracer,  # Inject the tracer here!
        )

        cluster = Cluster.connect(CONNECTION_STRING, opts)
        bucket = cluster.bucket(BUCKET_NAME)
        collection = bucket.default_collection()
        print("✓ Connected to Couchbase\n")

        perform_operations(collection, cluster)

        print("\nClosing cluster connection...")
        cluster.close()

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # CRITICAL: Forces all pending spans in the BatchSpanProcessor
        # to be exported over gRPC before Python exits!
        if tracer_provider:
            tracer_provider.shutdown()
            print("✓ OpenTelemetry tracer shut down and spans flushed.")


if __name__ == "__main__":
    main()
