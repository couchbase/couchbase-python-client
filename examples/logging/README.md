# Logging Examples

This directory contains example scripts demonstrating various logging configurations for the Couchbase Python SDK.

## Examples

### 1. env_based_console.py

Basic console logging using environment variables.

```bash
export PYCBC_LOG_LEVEL=info
python env_based_console.py
```

Demonstrates:
- Simplest logging setup
- Automatic configuration of C++ and Python SDK logs
- Immediate console output

---

### 2. env_based_file.py

File-based logging using environment variables.

```bash
export PYCBC_LOG_LEVEL=debug
export PYCBC_LOG_FILE=/tmp/couchbase-sdk.log
python env_based_file.py

# View logs
tail -f /tmp/couchbase-sdk.log
```

Demonstrates:
- Directing logs to a file
- Optional dual output (file + console) with PYCBC_ENABLE_CONSOLE
- Reviewing log files

---

### 3. programmatic_logging.py

Programmatic logging configuration for integration with application logging.

```bash
python programmatic_logging.py
```

Demonstrates:
- Using `configure_logging()` instead of environment variables
- Integrating with application logging infrastructure
- Custom log formats
- Separate configuration for C++ vs Python SDK logs

---

### 4. threshold_and_metrics.py

Observing threshold and metrics logging in action.

```bash
export PYCBC_LOG_LEVEL=info
python threshold_and_metrics.py
```

Demonstrates:
- Threshold logging reports (slow operations)
- Metrics logging reports (latency percentiles)
- How to interpret JSON log output
- Configuring report intervals

---

### 5. filtering_logs.py

Advanced filtering and customization.

```bash
python filtering_logs.py
```

Demonstrates:
- Custom log filters
- Separate handlers for different log types
- Custom log formats
- Level-based filtering

---

## Requirements

All examples require:
- Couchbase Server running (default: localhost:8091)
- 'default' bucket exists
- Valid credentials (default: Administrator/password)

Update the connection settings in each script if needed.

## For More Information

See **[../../LOGGING.md](../../LOGGING.md)** for complete logging documentation.
