#!/usr/bin/env python3
"""
Environment-Based File Logging Example

This example demonstrates using PYCBC_LOG_FILE to direct SDK logs to a file.

Usage:
    export PYCBC_LOG_LEVEL=debug
    export PYCBC_LOG_FILE=couchbase-sdk.log
    python env_based_file.py

    # View the Python logs
    tail -f couchbase-sdk.log

    # View the C++ logs
    tail -f couchbase-sdk.log.000000.txt

What this example demonstrates:
- Directing all SDK logs to a file
- Both C++ and Python SDK logs are in different files
- Consistent log format across all components
- Optional console output with PYCBC_ENABLE_CONSOLE

Try with dual output (file + console):
    export PYCBC_LOG_LEVEL=debug
    export PYCBC_LOG_FILE=couchbase-sdk.log
    export PYCBC_ENABLE_CONSOLE=1
    python env_based_file.py

Requirements:
- Couchbase Server running on localhost:8091
- 'default' bucket exists
- Valid credentials
"""

import os
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

# Check for logging configuration
log_file = os.getenv('PYCBC_LOG_FILE', None)
log_level = os.getenv('PYCBC_LOG_LEVEL', None)
enable_console = os.getenv('PYCBC_ENABLE_CONSOLE', None)

print("="*70)
print("Environment-Based File Logging Example")
print("="*70)
print("\nCurrent Configuration:")
print(f"  PYCBC_LOG_LEVEL: {log_level or 'NOT SET'}")
print(f"  PYCBC_LOG_FILE: {log_file or 'NOT SET'}")
print(f"  PYCBC_ENABLE_CONSOLE: {enable_console or 'NOT SET (defaults to 0)'}")
print()

if not log_level:
    print("⚠️  WARNING: PYCBC_LOG_LEVEL not set!")
    print("   Set it to enable logging: export PYCBC_LOG_LEVEL=debug")
    print()

if not log_file:
    print("⚠️  WARNING: PYCBC_LOG_FILE not set!")
    print("   Logs will go to console instead of file")
    print("   Set it to enable file logging: export PYCBC_LOG_FILE=/tmp/couchbase-sdk.log")
    print()
else:
    print(f"✅ Logs will be written to: {log_file}")
    if enable_console:
        print("✅ Console output also enabled (PYCBC_ENABLE_CONSOLE=1)")
    else:
        print("   Console output disabled (set PYCBC_ENABLE_CONSOLE=1 to enable)")
    print()

print("="*70 + "\n")

try:
    print(f"Connecting to {CONNECTION_STRING}...")

    # Create cluster with low thresholds to trigger threshold logging
    # These thresholds are intentionally low for demonstration purposes *ONLY*
    opts = ClusterOptions(
        PasswordAuthenticator(USERNAME, PASSWORD),
        tracing_threshold_kv=timedelta(milliseconds=10),  # 10ms threshold
        tracing_threshold_query_flush_interval=timedelta(milliseconds=5000),  # Report every 5 seconds
    )

    cluster = Cluster(CONNECTION_STRING, opts)
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.default_collection()

    print("Connected successfully!\n")

    # Perform operations
    print("Performing operations (check log file for details)...")
    print("-" * 70)

    for i in range(10):
        key = f'file_logging_example_{i}'
        doc = {'id': i, 'name': f'Doc {i}', 'timestamp': time.time()}
        collection.upsert(key, doc)
        print(f"✅ Operation {i+1}/10 completed")

    print("\n" + "="*70)
    print("Operations completed!")
    print("="*70)

    if log_file:
        print(f"\n📄 Logs written to: {log_file}")
        print(f"\nTo view logs, run:")
        print(f"  tail -f {log_file}")
        print(f"\nOr:")
        print(f"  cat {log_file}")
        print()

        # Try to show the last few log lines
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    print("Last few log entries:")
                    print("-" * 70)
                    for line in lines[-5:]:
                        print(line.rstrip())
                    print("-" * 70)
        except Exception as e:
            print(f"Could not read log file: {e}")

    print()

    # Clean up
    cluster.close()
    print("Cluster connection closed.\n")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nMake sure:")
    print("  1. Environment variables are set:")
    print("       export PYCBC_LOG_LEVEL=debug")
    print("       export PYCBC_LOG_FILE=couchbase-sdk.log")
    print("  2. Couchbase Server is running on localhost:8091")
    print("  3. The 'default' bucket exists")
    print("  4. Username and password are correct")
    print("  5. You have write permission to the log file directory")
    import traceback
    traceback.print_exc()
