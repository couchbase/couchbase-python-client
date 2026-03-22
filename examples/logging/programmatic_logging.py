#!/usr/bin/env python3
"""
Programmatic Logging Integration Example

This example demonstrates using configure_logging() to integrate SDK logging
with your application's logging system.

Usage:
    python programmatic_logging.py

What this example demonstrates:
- Using configure_logging() instead of environment variables
- Integrating SDK logs with application logging infrastructure
- Separate configuration for C++ logs vs Python SDK logs
- Custom log formatting
- File handlers with rotation

Important Notes:
- Do NOT set PYCBC_LOG_LEVEL when using configure_logging()
- The two approaches (environment vs programmatic) are mutually exclusive
- configure_logging() only configures C++ logs
- You must separately configure the 'couchbase' logger for threshold/metrics logs

Requirements:
- Couchbase Server running on localhost:8091
- 'default' bucket exists
- Valid credentials
- PYCBC_LOG_LEVEL must NOT be set
"""

import logging
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
print("Programmatic Logging Integration Example")
print("="*70)
print("\nThis example shows how to integrate SDK logging with your")
print("application's logging system using configure_logging().\n")
print("="*70 + "\n")

# Step 1: Set up application-level logging
print("Step 1: Configuring application logging...")

# Configure root logger for application
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app_logger = logging.getLogger('myapp')
app_logger.info("Application logging configured")

# Step 2: Configure C++ SDK logs
print("Step 2: Configuring C++ SDK logs...")

try:
    configure_logging('myapp.couchbase', level=logging.INFO)
    app_logger.info("C++ SDK logging configured")
except RuntimeError as e:
    print(f"❌ ERROR: {e}")
    print("\nMake sure PYCBC_LOG_LEVEL is NOT set:")
    print("  unset PYCBC_LOG_LEVEL")
    print("  python programmatic_logging.py")
    exit(1)

# Step 3: Configure Python SDK logs (threshold, metrics, transactions)
print("Step 3: Configuring Python SDK logs (threshold, metrics, etc.)...")

couchbase_logger = logging.getLogger('couchbase')
couchbase_handler = logging.StreamHandler()
couchbase_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
couchbase_logger.addHandler(couchbase_handler)
couchbase_logger.setLevel(logging.INFO)
app_logger.info("Python SDK logging configured")

print("\n" + "="*70)
print("Logging configuration complete!")
print("="*70 + "\n")

# Step 4: Use the SDK
print("Step 4: Performing SDK operations (watch logs)...\n")

try:
    app_logger.info(f"Connecting to {CONNECTION_STRING}...")

    # Create cluster with low thresholds
    # These thresholds are intentionally low for demonstration purposes *ONLY*
    opts = ClusterOptions(
        PasswordAuthenticator(USERNAME, PASSWORD),
        tracing_threshold_kv=timedelta(milliseconds=10),  # 10ms threshold
        tracing_threshold_query_flush_interval=timedelta(milliseconds=5000),  # Report every 5 seconds
    )

    cluster = Cluster(CONNECTION_STRING, opts)
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.default_collection()

    app_logger.info("Connected successfully!")

    # Perform operations
    app_logger.info("Performing operations...")

    for i in range(5):
        key = f'programmatic_logging_{i}'
        doc = {'id': i, 'name': f'Doc {i}'}
        collection.upsert(key, doc)
        app_logger.info(f"Upserted {key}")

    for i in range(5):
        key = f'programmatic_logging_{i}'
        result = collection.get(key)
        app_logger.info(f"Retrieved {key}")

    print("\n" + "="*70)
    print("Operations completed!")
    print("="*70)
    print("\nIn the logs above, you should see:")
    print("  • Application logs: 'myapp' logger")
    print("  • C++ SDK logs: 'myapp.couchbase' logger")
    print("  • Threshold logs: 'couchbase.threshold' logger (if operations exceeded thresholds)")
    print()

    # Clean up
    cluster.close()
    app_logger.info("Cluster connection closed")

except Exception as e:
    app_logger.error(f"Error: {e}")
    print("\nMake sure:")
    print("  1. PYCBC_LOG_LEVEL is NOT set (unset PYCBC_LOG_LEVEL)")
    print("  2. Couchbase Server is running on localhost:8091")
    print("  3. The 'default' bucket exists")
    print("  4. Username and password are correct")
    import traceback
    traceback.print_exc()
