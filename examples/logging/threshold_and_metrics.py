#!/usr/bin/env python3
"""
Threshold and Metrics Logging Example

This example demonstrates threshold logging and metrics logging in action.

Usage:
    export PYCBC_LOG_LEVEL=info
    python threshold_and_metrics.py

What this example demonstrates:
- Threshold logging: Reports operations that exceed configured thresholds
- Metrics logging: Reports operation latency percentiles
- How to interpret JSON log output
- How to configure threshold and metric intervals

Expected output:
- Threshold logging JSON reports (every 5 seconds)
- Metrics logging JSON reports (every 30 seconds for this example)

Threshold Log Example:
{
  "kv": {
    "total_count": 3,
    "top_requests": [
      {
        "operation_name": "upsert",
        "total_duration_us": 1500,
        "last_dispatch_duration_us": 1200
      }
    ]
  }
}

Metrics Log Example:
{
  "meta": {"emit_interval_s": 30},
  "operations": {
    "kv": {
      "upsert": {
        "total_count": 100,
        "percentiles_us": {
          "50.0": 150,
          "90.0": 350,
          "99.0": 800,
          "99.9": 1200,
          "100.0": 1500
        }
      }
    }
  }
}

Requirements:
- Couchbase Server running on localhost:8091
- 'default' bucket exists
- Valid credentials
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
print("Threshold and Metrics Logging Example")
print("="*70)
print("\nThis example demonstrates threshold and metrics logging by")
print("performing operations and waiting for periodic reports.")
print("\nMake sure PYCBC_LOG_LEVEL=info is set!")
print("="*70 + "\n")

try:
    print("Connecting to cluster...")

    # Configure cluster with:
    # - Low KV threshold (10ms) to easier trigger threshold logs (but still might not trigger on a fast local setup)
    # - Short flush interval (5s) to see threshold logs quickly
    # - Short metrics interval (30s) to see metrics logs quickly
    opts = ClusterOptions(
        PasswordAuthenticator(USERNAME, PASSWORD),
        # Threshold logging configuration
        tracing_threshold_kv=timedelta(milliseconds=10),                        # 10ms threshold for KV ops
        tracing_threshold_query=timedelta(milliseconds=100),                    # 100ms threshold for queries
        tracing_threshold_queue_size=10,                                        # Keep top 10 slow operations
        tracing_threshold_queue_flush_interval=timedelta(milliseconds=5000),    # Report every 5 seconds
        # Metrics logging configuration
        metrics_emit_interval=timedelta(milliseconds=30000),                    # Report every 30 seconds
    )

    cluster = Cluster(CONNECTION_STRING, opts)
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.default_collection()

    print("Connected!\n")

    print("Configuration:")
    print("  • KV threshold: 10ms (operations slower than this will be logged)")
    print("  • Threshold report interval: 5 seconds")
    print("  • Metrics report interval: 30 seconds")
    print()

    # Phase 1: Perform operations
    print("="*70)
    print("Phase 1: Performing operations...")
    print("="*70)

    for i in range(20):
        key = f'test_op_{i}'
        doc = {'id': i, 'type': 'fast', 'data': 'normal operation'}
        collection.upsert(key, doc)
        if (i + 1) % 5 == 0:
            print(f"  {i+1} upsert operations completed...")

    for i in range(20):
        key = f'test_op_{i}'
        collection.get(key)
        if (i + 1) % 5 == 0:
            print(f"  {i+1} get operations completed...")

    print(f"✅ 20 upsert & get operations completed\n")

    # Phase 2: Wait for threshold logging report
    print("="*70)
    print("Phase 2: Waiting for threshold logging report...")
    print("="*70)
    print("Threshold logs report every 5 seconds.")
    print("Watch for JSON output from 'couchbase.threshold' logger...\n")

    time.sleep(6)  # Wait for threshold report

    print("✅ Threshold report should have appeared above (JSON format)")
    print()

    # Phase 3: More operations for metrics
    print("="*70)
    print("Phase 3: Performing more operations for metrics logging...")
    print("="*70)

    for i in range(30):
        key = f'metrics_op_{i}'
        doc = {'id': i, 'type': 'metrics', 'data': 'for latency percentiles'}
        collection.upsert(key, doc)
        if (i + 1) % 10 == 0:
            print(f"  {i+1} operations completed...")

    print(f"✅ 30 operations completed\n")

    # Phase 4: Wait for metrics logging report
    print("="*70)
    print("Phase 4: Waiting for metrics logging report...")
    print("="*70)
    print("Metrics logs report every 30 seconds.")
    print("Watch for JSON output from 'couchbase.metrics' logger...")
    print("(This will take ~30 seconds)\n")

    # Wait for metrics report (30 second interval)
    for remaining in range(30, 0, -5):
        print(f"  Waiting... ({remaining} seconds remaining)")
        time.sleep(5)

    time.sleep(2)  # Extra buffer

    print("\n✅ Metrics report should have appeared above (JSON format)")
    print()

    # Summary
    print("="*70)
    print("Example Complete!")
    print("="*70)
    print("\nIn the logs above, you should see:")
    print()
    print("1. THRESHOLD LOGS (from couchbase.threshold logger):")
    print("   • JSON format")
    print("   • Service types: 'kv', 'query', etc.")
    print("   • total_count: Number of operations that exceeded threshold")
    print("   • top_requests: Top 10 slowest operations with details")
    print()
    print("2. METRICS LOGS (from couchbase.metrics logger):")
    print("   • JSON format")
    print("   • meta: emit_interval_s (report interval)")
    print("   • operations: Service → Operation → Percentiles")
    print("   • Percentiles: P50, P90, P99, P99.9, P100 (in microseconds)")
    print()
    print("For more details on interpreting these logs, see LOGGING.md")
    print()

    # Clean up
    cluster.close()
    print("Cluster connection closed.\n")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nMake sure:")
    print("  1. PYCBC_LOG_LEVEL=info is set")
    print("  2. Couchbase Server is running")
    print("  3. The 'default' bucket exists")
    print("  4. Credentials are correct")
    import traceback
    traceback.print_exc()
