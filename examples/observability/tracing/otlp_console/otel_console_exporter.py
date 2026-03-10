#!/usr/bin/env python3
"""
OpenTelemetry Tracing with Console Exporter Example

This example demonstrates how to use OpenTelemetry tracing with the Couchbase Python SDK
using a console exporter for development and debugging.

What this example does:
- Sets up OpenTelemetry with a console span exporter
- Creates a Couchbase cluster connection with OTel tracing enabled
- Performs various KV operations (upsert, get, etc.)
- Exports all spans to console for inspection

Requirements:
- pip install couchbase[otel]
- Couchbase Server running on localhost:8091

Usage:
    # NOTE: if running from the repo and using the examples path, update the PYTHONPATH (temporarily for the example):
    # export PYTHONPATH="${PYTHONPATH}:$(pwd)"
    python otel_console_exporter.py

Expected output:
- Console logs showing all spans captured with their attributes
- Detailed timing and relationship information for each operation
"""

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.observability.otel_tracing import get_otel_tracer
from couchbase.options import ClusterOptions

# Configuration
SERVICE_NAME = 'couchbase-otel-tracing-example'
CONNECTION_STRING = 'couchbase://192.168.107.128'
BUCKET_NAME = 'default'
USERNAME = 'Administrator'
PASSWORD = 'password'  # nosec


def setup_otel_console_exporter():
    """Set up OpenTelemetry with console exporter for span visualization."""
    # Create a resource with service name for consistency
    resource = Resource.create(attributes={
        "service.name": SERVICE_NAME,
    })

    # Create a tracer provider with the resource
    provider = TracerProvider(resource=resource)

    # Add console exporter to see spans in console
    console_exporter = ConsoleSpanExporter()
    processor = SimpleSpanProcessor(console_exporter)
    provider.add_span_processor(processor)

    # Set as the global tracer provider
    trace.set_tracer_provider(provider)

    return provider


def main():
    # Set up OpenTelemetry
    tracer_provider = setup_otel_console_exporter()

    # Wrap the OTel tracer with our SDK wrapper
    couchbase_tracer = get_otel_tracer(tracer_provider)

    print("\n" + "="*70)
    print("OpenTelemetry Tracing with Console Exporter Example")
    print("="*70)
    print("\nThis example demonstrates Couchbase SDK tracing exported to console.")
    print("Watch the console output for span details!\n")

    try:
        print(f"Connecting to {CONNECTION_STRING}...")

        # Create cluster options with custom tracer
        opts = ClusterOptions(
            PasswordAuthenticator(USERNAME, PASSWORD),
            tracer=couchbase_tracer  # Enable OTel tracing
        )

        # Connect to the cluster
        cluster = Cluster.connect(CONNECTION_STRING, opts)
        bucket = cluster.bucket(BUCKET_NAME)
        collection = bucket.default_collection()

        print("Connected successfully!\n")
        print("="*70)
        print("Performing operations with OTel tracing...")
        print("="*70 + "\n")

        # Define test document
        test_document = {
            'id': 1,
            'name': 'OpenTelemetry Demo',
            'description': 'Testing Couchbase with OpenTelemetry tracing',
            'timestamp': 1234567890
        }

        test_key = 'otel_demo_key'

        # Upsert operation
        print(f"1. Upserting document '{test_key}'...")
        upsert_result = collection.upsert(test_key, test_document)
        print(f"   Upsert completed (CAS: {upsert_result.cas})")
        print()

        # Get operation
        print(f"2. Retrieving document '{test_key}'...")
        get_result = collection.get(test_key)
        retrieved_doc = get_result.content_as[dict]
        print(f"   Retrieved: {retrieved_doc['name']}")
        print()

        # Replace operation
        print(f"3. Replacing document '{test_key}'...")
        test_document['description'] = 'Updated: Testing Couchbase with OpenTelemetry'
        replace_result = collection.replace(test_key, test_document, cas=get_result.cas)
        print(f"   Replace completed (CAS: {replace_result.cas})")
        print()

        # Get after replace
        print(f"4. Retrieving updated document '{test_key}'...")
        get_result = collection.get(test_key)
        print(f"   Updated description: {get_result.content_as[dict]['description']}")
        print()

        # Remove operation
        print(f"5. Removing document '{test_key}'...")
        remove_result = collection.remove(test_key)
        print(f"   Remove completed (CAS: {remove_result.cas})")
        print()

        # Try to get removed document (will fail)
        print(f"6. Attempting to retrieve removed document '{test_key}'...")
        try:
            collection.get(test_key)
            print("   Unexpectedly succeeded!")
        except Exception as e:
            print(f"   Successfully got expected error: {type(e).__name__}")
        print()

        print("="*70)
        print("Operations completed!")
        print("="*70)
        print("\nCheck the console output above for detailed span information.")
        print("Each span shows:")
        print("  - Operation name and type")
        print("  - Duration and timestamps")
        print("  - Attributes (bucket, scope, collection, etc.)")
        print("  - Parent-child relationships")
        print()

        # Clean up
        print("Closing cluster connection...")
        cluster.close()
        print("Done!\n")

    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nMake sure:")
        print("  1. Couchbase Server is running on localhost:8091")
        print("  2. The 'default' bucket exists")
        print("  3. Username and password are correct")
        print("  4. Required packages are installed:")
        print("     pip install couchbase[otel]")
        import traceback
        traceback.print_exc()
    finally:
        if tracer_provider:
            tracer_provider.shutdown()
            print("✓ OpenTelemetry tracer shut down.")


if __name__ == "__main__":
    main()
