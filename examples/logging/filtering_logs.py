#!/usr/bin/env python3
"""
Filtering and Customizing SDK Logs Example

This example demonstrates advanced logging techniques including filtering,
custom handlers, and separate log destinations.

Usage:
    python filtering_logs.py

What this example demonstrates:
- Filtering logs by logger name
- Filtering logs by level
- Separate handlers for different log types
- Custom log formats
- Suppressing specific loggers

Requirements:
- Couchbase Server running on localhost:8091
- 'default' bucket exists
- Valid credentials
- PYCBC_LOG_LEVEL must NOT be set (we use programmatic logging)
"""

import logging
import sys
from datetime import timedelta

from couchbase import configure_logging
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

# Connection settings
CONNECTION_STRING = 'couchbase://localhost'
BUCKET_NAME = 'default'
USERNAME = 'Administrator'
PASSWORD = 'password'  # nosec

print("="*70)
print("Filtering and Customizing SDK Logs Example")
print("="*70)
print("\nThis example demonstrates advanced logging configuration:")
print("  • Custom log filters")
print("  • Separate handlers for different log types")
print("  • Level-based filtering")
print("  • Custom formatters")
print()
print("="*70 + "\n")


# Custom filter class
class ThresholdOnlyFilter(logging.Filter):
    """Filter that only allows threshold logs."""

    def filter(self, record):
        return record.name.startswith('couchbase.threshold')


class MetricsOnlyFilter(logging.Filter):
    """Filter that only allows metrics logs."""

    def filter(self, record):
        return record.name.startswith('couchbase.metrics')


# Example 1: Show only threshold logs on console
print("Configuring loggers...")
print("-" * 70)

# Configure C++ SDK logs to go to a file (we'll suppress them for this example)
file_handler = logging.FileHandler('couchbase-cpp.log')  # nosec
cpp_logger = logging.getLogger('myapp.couchbase')
cpp_logger.addHandler(file_handler)
cpp_logger.setLevel(logging.INFO)

try:
    configure_logging('myapp.couchbase', level=logging.INFO)
except RuntimeError as e:
    print(f"❌ ERROR: {e}")
    print("\nMake sure PYCBC_LOG_LEVEL is NOT set.")
    exit(1)

# Configure Python SDK logs with separate handlers for threshold and metrics
print("✅ C++ logs → couchbase-cpp.log (suppressed from console)")

# Threshold logs: Show on console with custom format
threshold_console_handler = logging.StreamHandler(sys.stdout)
threshold_console_handler.setFormatter(logging.Formatter(
    '🎯 THRESHOLD: %(message)s'
))
threshold_console_handler.addFilter(ThresholdOnlyFilter())

threshold_logger = logging.getLogger('couchbase.threshold')
threshold_logger.addHandler(threshold_console_handler)
threshold_logger.setLevel(logging.INFO)
threshold_logger.propagate = False  # Don't propagate to parent

print("✅ Threshold logs → Console with custom format")

# Metrics logs: Show on console with different format
metrics_console_handler = logging.StreamHandler(sys.stdout)
metrics_console_handler.setFormatter(logging.Formatter(
    '📊 METRICS: %(message)s'
))
metrics_console_handler.addFilter(MetricsOnlyFilter())

metrics_logger = logging.getLogger('couchbase.metrics')
metrics_logger.addHandler(metrics_console_handler)
metrics_logger.setLevel(logging.INFO)
metrics_logger.propagate = False  # Don't propagate to parent

print("✅ Metrics logs → Console with custom format")

# Optionally: Suppress transaction logs completely
transaction_logger = logging.getLogger('couchbase.transactions')
transaction_logger.setLevel(logging.WARNING)  # Only warnings and errors

print("✅ Transaction logs → WARNING and above only")
print()

print("="*70)
print("Logging configured! Performing operations...")
print("="*70)
print("\nWatch for threshold and metrics logs with custom formatting:")
print("  🎯 THRESHOLD: {...}  ← Threshold log")
print("  📊 METRICS: {...}    ← Metrics log")
print()

try:
    # Connect with low threshold to trigger threshold logging
    opts = ClusterOptions(
        PasswordAuthenticator(USERNAME, PASSWORD),
        tracing_threshold_kv=timedelta(milliseconds=10),  # 10ms threshold
        tracing_threshold_queue_flush_interval=timedelta(milliseconds=5000),  # Report every 5 seconds
        metrics_emit_interval=timedelta(milliseconds=15000),  # Report every 15 seconds (faster for demo)
    )

    cluster = Cluster(CONNECTION_STRING, opts)
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.default_collection()

    print("Connected to cluster\n")

    # Perform operations
    print("Performing 30 operations...")
    for i in range(30):
        key = f'filtering_example_{i}'
        doc = {'id': i, 'data': f'Document {i}'}
        collection.upsert(key, doc)

        # Add occasional delays to trigger thresholds
        if i % 10 == 0:
            import time
            time.sleep(0.015)  # 15ms delay

    print("✅ Operations completed\n")

    # Wait for logs
    print("Waiting for threshold report (5 seconds)...")
    import time
    time.sleep(6)
    print("✅ Threshold report might appear (depends on op durations) above with 🎯 prefix\n")

    print("Waiting for metrics report (15 seconds)...")
    time.sleep(16)
    print("✅ Metrics report should appear above with 📊 prefix\n")

    print("="*70)
    print("Example Complete!")
    print("="*70)
    print("\nKey takeaways:")
    print("  • Filters allow showing only specific loggers")
    print("  • Custom formatters make logs easier to read")
    print("  • propagate=False prevents duplicate logs")
    print("  • Different handlers can route logs to different destinations")
    print()
    print(f"C++ SDK logs were written to: couchbase-cpp.log")
    print("You can view them with: cat couchbase-cpp.log")
    print()

    # Clean up
    cluster.close()
    print("Cluster connection closed.\n")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nMake sure:")
    print("  1. PYCBC_LOG_LEVEL is NOT set")
    print("  2. Couchbase Server is running")
    print("  3. The 'default' bucket exists")
    print("  4. Credentials are correct")
    import traceback
    traceback.print_exc()
