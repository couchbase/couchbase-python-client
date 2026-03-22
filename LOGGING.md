# Logging in the Couchbase Python SDK

## Overview

The Couchbase Python SDK provides comprehensive logging capabilities for debugging and monitoring. The SDK supports two logging approaches:

1. **Environment-based logging** - Simple configuration via environment variables (recommended for development)
2. **Programmatic logging** - Integration with Python's logging system (recommended for production)

### SDK Logging Components

The SDK has two main logging components:

- **C++ Logging**: Most SDK operations (KV, queries, management, etc.) - implemented in C++
- **Python Logging**: Threshold logging, metrics logging, transactions, and other Python-side components

When you configure logging using either approach, you control both components.

---

## Quick Start

### Environment-Based Logging

Set environment variables before running your application:

```bash
export PYCBC_LOG_LEVEL=info
python my_app.py
```

This automatically configures:
- ✅ C++ SDK logging
- ✅ Python SDK logging (threshold, metrics, transactions)
- ✅ Consistent format across all logs
- ✅ Same destination (console or file)

### Programmatic Logging

For more control, configure logging in your code:

```python
import logging
from couchbase import configure_logging

# Configure C++ SDK logs
configure_logging('my_app', level=logging.DEBUG)

# Configure Python SDK logs (threshold, metrics, transactions)
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(your_handler)
couchbase_logger.setLevel(logging.INFO)
```

---

## Reference

### Environment Variables

#### PYCBC_LOG_LEVEL

Sets the log level for both C++ and Python SDK logging.

**Values:** `trace`, `debug`, `info`, `warning`, `error`, `critical`, `off`

**Example:**
```bash
export PYCBC_LOG_LEVEL=info
```

**What it configures**:
- C++ SDK operations (connection, KV, queries, etc.)
- Python SDK loggers: `couchbase.threshold`, `couchbase.metrics`, `couchbase.transactions.*`

---

#### PYCBC_LOG_FILE

Optional. Directs logs to a file instead of console.

**Example:**
```bash
export PYCBC_LOG_LEVEL=debug
export PYCBC_LOG_FILE=/var/log/couchbase-sdk.log
```

**Result**: All SDK logs (C++ and Python) are written to the specified file.

---

#### PYCBC_ENABLE_CONSOLE

Optional. When using `PYCBC_LOG_FILE`, also output to console.

**Values:** `0` (disabled, default) or `1` (enabled)

**Example:**
```bash
export PYCBC_LOG_LEVEL=debug
export PYCBC_LOG_FILE=/var/log/couchbase-sdk.log
export PYCBC_ENABLE_CONSOLE=1
```

**Result**: Logs written to both file AND console.

---

### Log Levels

| Level | Value | Description |
|-------|-------|-------------|
| TRACE | 5 | Most detailed, includes all operations |
| DEBUG | 10 | Detailed debugging information |
| INFO | 20 | General informational messages |
| WARNING | 30 | Warning messages |
| ERROR | 40 | Error messages |
| CRITICAL | 50 | Critical errors |

---

### Log Format

SDK logs use a consistent format across C++ and Python components:

```
[TIMESTAMP.ms] RELATIVE_MS [LEVEL] [PID, THREAD_NAME (TID)] LOGGER_NAME - MESSAGE
```

**Example:**
```
[2026-03-21 14:23:45.123] 1500ms [INFO] [12345, MainThread (140735268359296)] couchbase.threshold - {"kv":{"total_count":5,"top_requests":[...]}}
```

**Fields**:
- **TIMESTAMP.ms**: Absolute timestamp with milliseconds
- **RELATIVE_MS**: Milliseconds since logger initialization
- **LEVEL**: Log level (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **PID**: Process ID
- **THREAD_NAME**: Thread name
- **TID**: Thread ID
- **LOGGER_NAME**: Logger name (e.g., `couchbase.threshold`)
- **MESSAGE**: Log message content

---

### Python SDK Loggers

#### couchbase.threshold

Threshold logging reports operations that exceed configured thresholds.

**Log Level:** INFO

**Output Format:** JSON

**Example:**
```json
{
  "kv": {
    "total_count": 5,
    "top_requests": [
      {
        "operation_name": "get",
        "total_duration_us": 1500,
        "last_dispatch_duration_us": 1200,
        "last_server_duration_us": 1000
      }
    ]
  }
}
```

**Configuration**:
- Default thresholds: KV=500ms, Query/Search/Analytics=1000ms
- Default sample size: 10 operations per service
- Default report interval: 10 seconds

---

#### couchbase.metrics

Metrics logging reports operation latency percentiles.

**Log Level:** INFO

**Output Format:** JSON

**Example:**
```json
{
  "meta": {
    "emit_interval_s": 600
  },
  "operations": {
    "kv": {
      "get": {
        "total_count": 1000,
        "percentiles": {
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
```

**Configuration**:
- Default report interval: 600 seconds (10 minutes)
- Percentiles: P50, P90, P99, P99.9, P100
- Values in microseconds

---

#### couchbase.transactions.*

Transaction-related logging from Python components.

**Loggers:**
- `couchbase.transactions.transactions`
- `couchbase.transactions.logic.transactions_logic`
- `couchbase.transactions.logic.attempt_context_logic`

---

## Advanced Topics

### Programmatic Configuration

#### Complete Example

```python
import logging
from couchbase import configure_logging
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

# Set up logging BEFORE creating cluster
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Configure C++ SDK logs
configure_logging('myapp', level=logging.DEBUG)

# Configure Python SDK logs (threshold, metrics, transactions)
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.setLevel(logging.INFO)

# Now create cluster and perform operations
cluster = Cluster('couchbase://localhost',
                  ClusterOptions(PasswordAuthenticator('user', 'pass')))

# Operations will now be logged
collection = cluster.bucket('default').default_collection()
collection.upsert('key', {'value': 'data'})
```

---

#### File Logging with Rotation

```python
import logging
from logging.handlers import RotatingFileHandler
from couchbase import configure_logging

# Create rotating file handler
handler = RotatingFileHandler(
    '/var/log/myapp-couchbase.log',
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))

# Configure C++ logs
cpp_logger = logging.getLogger('myapp.couchbase')
cpp_logger.addHandler(handler)
configure_logging('myapp.couchbase', level=logging.INFO)

# Configure Python SDK logs
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(handler)
couchbase_logger.setLevel(logging.INFO)
```

---

### Filtering Logs

#### Filter by Logger Name

```python
import logging

class LoggerFilter(logging.Filter):
    def filter(self, record):
        # Only show threshold logs
        return record.name.startswith('couchbase.threshold')

couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addFilter(LoggerFilter())
```

#### Filter by Level

```python
import logging

# Show only WARNING and above for threshold logs
threshold_logger = logging.getLogger('couchbase.threshold')
threshold_logger.setLevel(logging.WARNING)

# Show all INFO and above for metrics logs
metrics_logger = logging.getLogger('couchbase.metrics')
metrics_logger.setLevel(logging.INFO)
```

---

### Custom Log Handlers

#### Send Logs to Syslog

```python
import logging
from logging.handlers import SysLogHandler

# Configure syslog handler
syslog_handler = SysLogHandler(address='/dev/log')
syslog_handler.setLevel(logging.INFO)

# Add to couchbase logger
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(syslog_handler)
```

#### Separate Files for Different Log Types

```python
import logging

# Threshold logs to one file
threshold_handler = logging.FileHandler('/var/log/couchbase-threshold.log')
threshold_logger = logging.getLogger('couchbase.threshold')
threshold_logger.addHandler(threshold_handler)
threshold_logger.propagate = False

# Metrics logs to another file
metrics_handler = logging.FileHandler('/var/log/couchbase-metrics.log')
metrics_logger = logging.getLogger('couchbase.metrics')
metrics_logger.addHandler(metrics_handler)
metrics_logger.propagate = False
```

---

### Protocol Logger

**⚠️ WARNING:** The protocol logger logs raw network traffic including sensitive data.

```python
from couchbase import enable_protocol_logger_to_save_network_traffic_to_file

enable_protocol_logger_to_save_network_traffic_to_file('/tmp/protocol.log')
```

**Use cases:**
- Deep debugging of network issues
- Analyzing protocol-level problems
- Working with Couchbase support

**Security:** Never use in production. Logs contain authentication tokens and document data.

---

## Troubleshooting

### "Cannot create logger" Error

**Error:**
```
RuntimeError: Cannot create logger. Another logger has already been initialized.
```

**Cause:** Trying to use both `PYCBC_LOG_LEVEL` and `configure_logging()`.

**Solution:** Choose one approach:
- **Option A**: Remove `PYCBC_LOG_LEVEL` environment variable
- **Option B**: Don't call `configure_logging()` in your code

These approaches are mutually exclusive.

---

### Missing Threshold or Metrics Logs

**Problem:** C++ logs appear but threshold/metrics logs don't.

**Cause:** When using `configure_logging()`, the `couchbase` logger isn't configured.

**Solution:**
```python
import logging
from couchbase import configure_logging

# Configure C++ logs
configure_logging('my_app', level=logging.INFO)

# ALSO configure Python SDK logs
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(your_handler)
couchbase_logger.setLevel(logging.INFO)
```

**With environment variables**, this happens automatically:
```bash
export PYCBC_LOG_LEVEL=info  # Configures both C++ and Python automatically
```

---

### Duplicate Logs

**Problem:** Same log appears multiple times.

**Cause:** Multiple handlers or propagation issues.

**Solution:**
```python
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.handlers.clear()  # Clear existing handlers
couchbase_logger.addHandler(your_handler)
couchbase_logger.propagate = False  # Prevent propagation
```

---

### Logs Not Appearing with PYCBC_LOG_LEVEL

**Problem:** Set `PYCBC_LOG_LEVEL=info` but don't see logs.

**Common causes:**

1. **Environment variable not set before import**
   ```python
   # WRONG: Set after import
   import couchbase
   import os
   os.environ['PYCBC_LOG_LEVEL'] = 'info'  # Too late!

   # CORRECT: Set before running Python
   # In shell: export PYCBC_LOG_LEVEL=info
   # Then: python my_app.py
   ```

2. **Typo in environment variable name**
   - Correct: `PYCBC_LOG_LEVEL`
   - Wrong: `PYCBC_LOGLEVEL`, `PYCBC_LOG`, `COUCHBASE_LOG_LEVEL`

3. **Invalid log level value**
   - Valid: `trace`, `debug`, `info`, `warning`, `error`, `critical`, `off`
   - Invalid: `verbose`, `warn` (use `warning`), `fatal`

---

## Best Practices

### Development

- ✅ Use `PYCBC_LOG_LEVEL=debug` for detailed debugging
- ✅ Use console output for immediate feedback
- ✅ Monitor threshold logs to identify slow operations
- ✅ Check metrics logs for latency distribution

### Production

- ✅ Use `configure_logging()` to integrate with application logging
- ✅ Set appropriate log levels (INFO or WARNING)
- ✅ Use file handlers with rotation
- ✅ Monitor threshold logging for performance issues
- ❌ Never enable protocol logger (security risk)
- ❌ Avoid TRACE/DEBUG levels (performance impact)

### Performance

- ✅ Logging has minimal impact at INFO level and above
- ⚠️ DEBUG and TRACE levels can impact performance
- ✅ File logging is faster than console logging
- ✅ Threshold logging runs on a separate thread (minimal impact)
- ✅ Metrics logging runs on a separate thread (minimal impact)

---

## Examples

For working example scripts demonstrating various logging configurations, see the `examples/logging/` directory:

- **`env_based_console.py`** - Basic console logging with `PYCBC_LOG_LEVEL`
- **`env_based_file.py`** - File-based logging with `PYCBC_LOG_FILE`
- **`programmatic_logging.py`** - Programmatic configuration with `configure_logging()`
- **`threshold_and_metrics.py`** - Observing threshold and metrics logs
- **`filtering_logs.py`** - Advanced filtering and custom handlers

Each example includes:
- Clear usage instructions
- Expected output
- Explanation of what's being demonstrated

Run any example:
```bash
cd examples/logging
python env_based_console.py
```

---

## FAQ

**Q: How do I enable logging?**

A: Use environment variables (easiest):
```bash
export PYCBC_LOG_LEVEL=info
python my_app.py
```

Or use `configure_logging()` (more control):
```python
from couchbase import configure_logging
configure_logging('my_app', level=logging.INFO)
```

---

**Q: Why don't I see threshold logs with configure_logging()?**

A: `configure_logging()` only configures C++ logs. You need to separately configure the `couchbase` logger:

```python
import logging
from couchbase import configure_logging

# Configure C++ logs
configure_logging('my_app', level=logging.INFO)

# ALSO configure Python SDK logs
couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(your_handler)
couchbase_logger.setLevel(logging.INFO)
```

With `PYCBC_LOG_LEVEL`, both are configured automatically.

---

**Q: Can I use both PYCBC_LOG_LEVEL and configure_logging()?**

A: No, these approaches are mutually exclusive. If you try to use both, you'll get:
```
RuntimeError: Cannot create logger. Another logger has already been initialized.
```

Choose one based on your needs:
- Development/debugging → Use `PYCBC_LOG_LEVEL`
- Production/integration → Use `configure_logging()`

---

**Q: How do I change the log format?**

A:
- **With `PYCBC_LOG_LEVEL`**: Format is fixed (matches C++ client format)
- **With `configure_logging()`**: Set your formatter on the handler:

```python
import logging

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))

couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(handler)
```

---

**Q: What's the performance impact of logging?**

A:
- **INFO and above**: Minimal impact
- **DEBUG**: Moderate impact
- **TRACE**: Significant impact
- **Threshold/Metrics logging**: Minimal impact (runs on separate threads)

Recommendation: Use INFO or WARNING in production.  Sometimes TRACE level logging is needed for root cause analysis.

---

**Q: How do I suppress threshold logs?**

A: Set the threshold logger level to WARNING or higher:

```python
import logging

logging.getLogger('couchbase.threshold').setLevel(logging.WARNING)
```

Or disable completely:
```python
logging.getLogger('couchbase.threshold').disabled = True
```

---

**Q: How do I suppress metrics logs?**

A: Same as threshold logs:

```python
import logging

logging.getLogger('couchbase.metrics').setLevel(logging.WARNING)
```

---

**Q: Where are transaction logs?**

A: Transaction logs use `couchbase.transactions.*` loggers. Configure the `couchbase` parent logger to capture them:

```python
import logging

couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(your_handler)
couchbase_logger.setLevel(logging.INFO)
```

This captures threshold, metrics, and transaction logs.

---

**Q: Can I send different SDK logs to different files?**

A: Yes! Configure individual loggers:

```python
import logging

# Threshold logs to one file
threshold_handler = logging.FileHandler('/var/log/threshold.log')
threshold_logger = logging.getLogger('couchbase.threshold')
threshold_logger.addHandler(threshold_handler)
threshold_logger.propagate = False

# Metrics logs to another file
metrics_handler = logging.FileHandler('/var/log/metrics.log')
metrics_logger = logging.getLogger('couchbase.metrics')
metrics_logger.addHandler(metrics_handler)
metrics_logger.propagate = False
```

---

**Q: How do I parse threshold/metrics JSON logs?**

A: The logs are JSON strings. Parse them:

```python
import json
import logging

class JSONLogHandler(logging.Handler):
    def emit(self, record):
        if record.name == 'couchbase.threshold':
            try:
                data = json.loads(record.getMessage())
                # Process threshold data
                for service, report in data.items():
                    print(f"Service: {service}")
                    print(f"Total count: {report['total_count']}")
                    for req in report['top_requests']:
                        print(f"  Op: {req['operation_name']}, Duration: {req['total_duration_us']}us")
            except json.JSONDecodeError:
                pass

couchbase_logger = logging.getLogger('couchbase')
couchbase_logger.addHandler(JSONLogHandler())
```

---

**Q: How do I adjust threshold logging configuration?**

A: Use `ClusterOptions`:

```python
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator

opts = ClusterOptions(
    PasswordAuthenticator('user', 'pass'),
    tracing_threshold_kv=1000,           # KV threshold: 1000ms
    tracing_threshold_query=2000,        # Query threshold: 2000ms
    tracing_threshold_queue_size=20,     # Sample size: 20 operations
    tracing_threshold_queue_flush_interval=30000  # Report every 30 seconds
)

cluster = Cluster('couchbase://localhost', opts)
```

---

**Q: How do I adjust metrics logging configuration?**

A: Use `ClusterOptions`:

```python
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator

opts = ClusterOptions(
    PasswordAuthenticator('user', 'pass'),
    metrics_emit_interval=300000  # Report every 300 seconds (5 minutes)
)

cluster = Cluster('couchbase://localhost', opts)
```

---

## See Also

- **[examples/logging/](examples/logging/)** - Working example scripts
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html)
- [Couchbase Python SDK Documentation](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [Couchbase Server Logging](https://docs.couchbase.com/server/current/manage/manage-logging/manage-logging.html)
