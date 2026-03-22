#!/usr/bin/env python3
"""
Environment-Based Console Logging Example

This example demonstrates using PYCBC_LOG_LEVEL to enable SDK logging to the console.

Usage:
    export PYCBC_LOG_LEVEL=info
    python env_based_console.py

What this example demonstrates:
- Setting PYCBC_LOG_LEVEL automatically configures all SDK logging
- Both C++ logs (operations, network) and Python logs (threshold, metrics) appear
- All logs use a consistent format
- Logs appear on console immediately

Expected output:
- Connection and operation logs from C++ SDK
- Threshold logging reports (if operations exceed thresholds)
- Metrics logging reports (after metric interval)

Requirements:
- Couchbase Server running on localhost:8091 (or update CONNECTION_STRING)
- 'default' bucket exists
- Valid credentials

Try different log levels:
    export PYCBC_LOG_LEVEL=debug    # More detailed logs
    export PYCBC_LOG_LEVEL=trace    # Most detailed (verbose)
    export PYCBC_LOG_LEVEL=warning  # Only warnings and errors
"""

import time
from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

# Connection settings
CONNECTION_STRING = 'couchbase://localhost'
BUCKET_NAME = 'default'
USERNAME = 'Administrator'
PASSWORD = 'password'  # nosec

print("="*70)
print("Environment-Based Console Logging Example")
print("="*70)
print("\nThis example demonstrates automatic SDK logging configuration")
print("using the PYCBC_LOG_LEVEL environment variable.")
print("\nMake sure you set PYCBC_LOG_LEVEL before running this script:")
print("  export PYCBC_LOG_LEVEL=info")
print("\n" + "="*70 + "\n")

try:
    print(f"Connecting to {CONNECTION_STRING}...")

    # Create cluster options with low threshold to trigger threshold logging
    # These thresholds are intentionally low for demonstration purposes *ONLY*
    opts = ClusterOptions(
        PasswordAuthenticator(USERNAME, PASSWORD),
        tracing_threshold_kv=timedelta(milliseconds=10),  # 10ms threshold
        tracing_threshold_query_flush_interval=timedelta(milliseconds=5000),  # Report every 5 seconds
    )

    # Connect to cluster
    cluster = Cluster(CONNECTION_STRING, opts)
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.default_collection()

    print("Connected successfully!\n")

    # Perform some operations
    print("Performing operations...")
    print("-" * 70)

    # Upsert operations
    for i in range(5):
        key = f'logging_example_{i}'
        doc = {
            'id': i,
            'name': f'Document {i}',
            'timestamp': time.time()
        }
        result = collection.upsert(key, doc)
        print(f"✅ Upserted {key} (CAS: {result.cas})")

    # Get operations
    for i in range(5):
        key = f'logging_example_{i}'
        result = collection.get(key)
        print(f"✅ Retrieved {key}")

    print("\n" + "="*70)
    print("Operations completed!")
    print("="*70)
    print("\nWatch the logs above for:")
    print("  • C++ SDK logs: Connection, operation details")
    print("  • Threshold logs: JSON reports of slow operations")
    print("  • Metrics logs: Will appear after metric interval (default: 10 minutes)")
    print("\nTo see metrics logs sooner, set a shorter interval in ClusterOptions")
    print("or wait for the next metrics report cycle.")
    print()

    # Clean up
    cluster.close()
    print("Cluster connection closed.\n")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nMake sure:")
    print("  1. PYCBC_LOG_LEVEL environment variable is set (e.g., export PYCBC_LOG_LEVEL=info)")
    print("  2. Couchbase Server is running on localhost:8091")
    print("  3. The 'default' bucket exists")
    print("  4. Username and password are correct")
    import traceback
    traceback.print_exc()
