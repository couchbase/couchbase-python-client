#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Logging configuration for the Couchbase Python SDK.

This module provides two approaches for configuring SDK logging:

1. **Environment-based logging (Recommended for development/debugging)**
   Set environment variables before importing the SDK:
   - PYCBC_LOG_LEVEL: Log level (trace, debug, info, warning, error, critical)
   - PYCBC_LOG_FILE: Optional file path for log output
   - PYCBC_ENABLE_CONSOLE: Enable console output when file logging is set (0 or 1)

   Example:
       export PYCBC_LOG_LEVEL=debug
       python my_app.py

   This configures both C++ and Python logging in the SDK automatically.

2. **Programmatic logging (Recommended for production/integration)**
   Use configure_logging() to integrate with your application's logging:

   Example:
       import logging
       from couchbase import configure_logging

       # Configure C++ SDK logs
       configure_logging('my_app', level=logging.DEBUG)

       # Configure Python SDK logs (threshold, metrics, etc.)
       couchbase_logger = logging.getLogger('couchbase')
       couchbase_logger.addHandler(your_handler)
       couchbase_logger.setLevel(logging.INFO)

Note: These two approaches are mutually exclusive - you cannot use both simultaneously.

For detailed logging documentation, see LOGGING.md in the repository root.
"""

import atexit
import logging
from functools import partial, partialmethod
from typing import Any, Optional

from couchbase.logic.pycbc_core import pycbc_logger, shutdown_logger
from couchbase.logic.pycbc_core.core_metadata import get_metadata

# Initialize global logger instance
_PYCBC_LOGGER = pycbc_logger()

# Add TRACE level to Python's logging module
logging.TRACE = 5
logging.addLevelName(logging.TRACE, 'TRACE')
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


def _configure_python_logging_for_console(log_level_str: str,
                                          log_file: Optional[str] = None,
                                          enable_console: Optional[bool] = True) -> None:
    """
    **INTERNAL** Configure Python's logging system to match C++ logger configuration.

    This ensures Python-based SDK loggers (threshold logging, metrics, transactions, etc.)
    output to the same destination as C++ logs when using PYCBC_LOG_LEVEL.

    Args:
        log_level_str: Log level string (e.g., 'debug', 'info', 'trace')
        log_file: Optional file path for file logging
        enable_console: Whether to enable console output (when log_file is set)
    """
    # Map log level string to Python logging level
    level_map = {
        'trace': logging.TRACE,    # 5
        'debug': logging.DEBUG,     # 10
        'info': logging.INFO,       # 20
        'warning': logging.WARNING,  # 30
        'warn': logging.WARNING,    # 30
        'error': logging.ERROR,     # 40
        'critical': logging.CRITICAL,  # 50
        'off': logging.CRITICAL + 10  # Effectively disable
    }

    python_log_level = level_map.get(log_level_str.lower(), logging.INFO)

    # Get or create the 'couchbase' parent logger
    couchbase_logger = logging.getLogger('couchbase')
    couchbase_logger.setLevel(python_log_level)

    # Remove any existing handlers to avoid duplicates on re-configuration
    couchbase_logger.handlers.clear()

    # Use format matching C++ client logs
    log_format_arr = [
        '[%(asctime)s.%(msecs)03d]',
        '%(relativeCreated)dms',
        '[%(levelname)s]',
        '[%(process)d, %(threadName)s (%(thread)d)] %(name)s',
        '- %(message)s',
    ]
    log_format = ' '.join(log_format_arr)
    log_date_format = '%Y-%m-%d %H:%M:%S'

    formatter = logging.Formatter(log_format, datefmt=log_date_format)

    # Configure handlers based on C++ logger configuration
    if log_file:
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(python_log_level)
        file_handler.setFormatter(formatter)
        couchbase_logger.addHandler(file_handler)

        # Console handler if enabled
        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(python_log_level)
            console_handler.setFormatter(formatter)
            couchbase_logger.addHandler(console_handler)
    else:
        # Console-only handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(python_log_level)
        console_handler.setFormatter(formatter)
        couchbase_logger.addHandler(console_handler)

    # Prevent propagation to root logger to avoid duplicate logs
    couchbase_logger.propagate = False
    couchbase_logger.debug(get_metadata(as_str=True))


def configure_console_logger():
    """
    **INTERNAL** Configure logging based on PYCBC_LOG_LEVEL environment variable.

    This function is called automatically when the couchbase module is imported.
    It configures both C++ and Python logging based on environment variables:

    - PYCBC_LOG_LEVEL: Log level (trace, debug, info, warning, error, critical)
    - PYCBC_LOG_FILE: Optional file path for log output
    - PYCBC_ENABLE_CONSOLE: Enable console output when file logging is set (default: off)

    When PYCBC_LOG_LEVEL is set, this configures:
    1. C++ SDK logging (bulk of SDK operations)
    2. Python SDK logging (threshold logging, metrics, transactions, etc.)
    """
    import os

    log_level = os.getenv('PYCBC_LOG_LEVEL', None)
    if log_level:
        log_file = os.getenv('PYCBC_LOG_FILE', None)
        if log_file:
            enable_console_logging = 0 if os.getenv('PYCBC_ENABLE_CONSOLE', None) is None else 1
            _PYCBC_LOGGER.create_logger(level=log_level.lower(),
                                        filename=log_file,
                                        enable_console=enable_console_logging)
        else:
            _PYCBC_LOGGER.create_logger(level=log_level.lower())

        # Configure Python logging for SDK loggers (threshold, metrics, transactions, etc.)
        _configure_python_logging_for_console(
            log_level,
            log_file,
            enable_console_logging if log_file else True
        )


def configure_logging(name, level=logging.INFO, parent_logger=None):
    """
    Configure the Python SDK to route C++ logs through Python's logging system.

    This is the programmatic alternative to using PYCBC_LOG_LEVEL environment variable.
    It allows integration with your application's existing logging configuration.

    Note: This configures C++ SDK logs only. Python-side SDK components (threshold
    logging, metrics, transactions) use separate loggers under the 'couchbase' hierarchy.

    To also capture threshold logging reports and other Python SDK logs, configure
    the 'couchbase' logger:

    Example:
        import logging
        from couchbase import configure_logging

        # Configure C++ logs to route through Python
        configure_logging('my_app', level=logging.DEBUG)

        # Also capture Python SDK logs (threshold, metrics, transactions, etc.)
        couchbase_logger = logging.getLogger('couchbase')
        couchbase_logger.addHandler(your_handler)
        couchbase_logger.setLevel(logging.INFO)

    Args:
        name: Name for the logger
        level: Python logging level (default: logging.INFO)
        parent_logger: Optional parent logger

    Raises:
        RuntimeError: If PYCBC_LOG_LEVEL environment variable is already set.
                     Cannot use both configuration methods simultaneously.
    """
    if parent_logger:
        name = f'{parent_logger.name}.{name}'
    logger = logging.getLogger(name)
    if _PYCBC_LOGGER.is_console_logger() or _PYCBC_LOGGER.is_file_logger():
        raise RuntimeError(('Cannot create logger.  Another logger has already been '
                            'initialized. Make sure the PYCBC_LOG_LEVEL and PYCBC_LOG_FILE env '
                            'variable are not set if using configure_logging.'))
    _PYCBC_LOGGER.configure_logging_sink(logger, level)
    logger.debug(get_metadata(as_str=True))


def enable_protocol_logger_to_save_network_traffic_to_file(filename: str) -> None:
    """
    **VOLATILE** This API is subject to change at any time.

    Exposes the underlying couchbase++ library protocol logger. This method is for logging/debugging
    purposes and must be used with caution as network details will be logged to the provided file.

    Args:
        filename (str): The name of the file the protocol logger will write to.

    Raises:
        InvalidArgumentException: If a filename is not provided.
    """
    _PYCBC_LOGGER.enable_protocol_logger(filename)


def _pycbc_teardown(**kwargs: Any) -> None:
    """**INTERNAL** Cleanup function called at interpreter shutdown."""
    global _PYCBC_LOGGER
    # if using a console logger we let the natural course of shutdown happen, if using Python logging
    # we need a cleaner mechanism to shutdown the C++ logger prior to the Python interpreter starting to finalize
    if (_PYCBC_LOGGER
        and isinstance(_PYCBC_LOGGER, pycbc_logger)
            and not (_PYCBC_LOGGER.is_console_logger() or _PYCBC_LOGGER.is_file_logger())):
        shutdown_logger()
        _PYCBC_LOGGER = None


# Register teardown function
atexit.register(_pycbc_teardown)

# Auto-configure logging on module import
configure_console_logger()


# Export public API
__all__ = [
    'configure_logging',
    'enable_protocol_logger_to_save_network_traffic_to_file',
]
