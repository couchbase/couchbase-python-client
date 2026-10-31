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
   - PYCBC_LOG_LEVEL: Log level (trace, debug, info, warning, error, critical, off)
   - PYCBC_LOG_FILE: Optional file path for log output
   - PYCBC_ENABLE_CONSOLE: Also log to the console when file logging is set (0 or 1)

   Example:
       export PYCBC_LOG_LEVEL=debug
       python my_app.py

   This configures both the native core's logging and the SDK's Python loggers.  If the
   application has already set up logging, the SDK's Python records go to the application's
   handlers rather than to a console handler of the SDK's own.

2. **Programmatic logging (Recommended for production/integration)**
   Use configure_logging() to integrate with your application's logging:

   Example:
       import logging
       from couchbase import configure_logging

       # Route the native core's logs through Python logging
       configure_logging('my_app', level=logging.DEBUG)

       # Configure Python SDK logs (threshold, metrics, etc.)
       couchbase_logger = logging.getLogger('couchbase')
       couchbase_logger.addHandler(your_handler)
       couchbase_logger.setLevel(logging.INFO)

Note: These two approaches are mutually exclusive: you cannot use both simultaneously.

For detailed logging documentation, see LOGGING.md in the repository root.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from couchbase.common.exceptions import InvalidArgumentException

LOGGER_NAME = 'couchbase'
LOG_LEVEL_ENV_VAR = 'PYCBC_LOG_LEVEL'
LOG_FILE_ENV_VAR = 'PYCBC_LOG_FILE'
ENABLE_CONSOLE_ENV_VAR = 'PYCBC_ENABLE_CONSOLE'

TRACE = 5

# The native core logs below DEBUG.  An application that has already named the level keeps its name.
if logging.getLevelName(TRACE) == f'Level {TRACE}':
    logging.addLevelName(TRACE, 'TRACE')

# Each accepted spelling maps to the name handed to the native core and the level set on the
# Python loggers, so the two sides always agree.
_LOG_LEVELS: dict[str, tuple[str, int]] = {
    'trace': ('trace', TRACE),
    'debug': ('debug', logging.DEBUG),
    'info': ('info', logging.INFO),
    'warn': ('warning', logging.WARNING),
    'warning': ('warning', logging.WARNING),
    'error': ('error', logging.ERROR),
    'critical': ('critical', logging.CRITICAL),
    'off': ('off', logging.CRITICAL + 10),
}

_ENABLED_VALUES = frozenset(('1', 'true', 'yes', 'on'))

_LOG_FORMAT = ' '.join([
    '[%(asctime)s.%(msecs)03d]',
    '%(relativeCreated)dms',
    '[%(levelname)s]',
    '[%(process)d, %(threadName)s (%(thread)d)] %(name)s',
    '- %(message)s',
])
_LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


@dataclass(frozen=True)
class EnvLogging:
    """**INTERNAL** The logging the environment asked for, read once at import."""
    level: str
    python_level: int
    filename: str | None
    enable_console: bool


class CoreLogBackend(Protocol):
    """**INTERNAL** The logging a protocol's native core does on its own.  A protocol with such a core
    registers one through `register_core_log_backend`; the public functions here hand it their requests."""

    def apply_env(self, env: EnvLogging) -> None: ...

    def route_to(self, logger: logging.Logger, level: int) -> None: ...

    def enable_protocol_logger(self, filename: str) -> None: ...


def read_env_logging(environ: Mapping[str, str]) -> tuple[EnvLogging | None, str | None]:
    """**INTERNAL** Returns the requested logging, and the unrecognized level if there was one."""
    raw_level = environ.get(LOG_LEVEL_ENV_VAR)
    if not raw_level:
        return None, None
    known = _LOG_LEVELS.get(raw_level.strip().lower())
    level, python_level = known if known is not None else _LOG_LEVELS['info']
    filename = environ.get(LOG_FILE_ENV_VAR) or None
    enable_console = (filename is None
                      or environ.get(ENABLE_CONSOLE_ENV_VAR, '').strip().lower() in _ENABLED_VALUES)
    env = EnvLogging(level=level, python_level=python_level, filename=filename, enable_console=enable_console)
    return env, None if known is not None else raw_level


def configure_python_logging(env: EnvLogging, unrecognized_level: str | None = None,
                             logger_name: str = LOGGER_NAME) -> None:
    """**INTERNAL** Applies the environment's logging to the SDK's Python loggers.

    The application owns its handlers.  The SDK only sets its own logger's level and lets records
    propagate to whatever the application installed; it adds a console handler only when nothing else
    would handle them.  A requested log file always gets a handler, since nothing else writes there.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(env.python_level)
    handled_elsewhere = logger.hasHandlers()
    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

    if env.filename is not None:
        file_handler = logging.FileHandler(env.filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if env.enable_console and not handled_elsewhere:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        # An application that configures logging later would otherwise print every record twice.
        logger.propagate = False

    if unrecognized_level is not None:
        logger.warning(f'Unrecognized {LOG_LEVEL_ENV_VAR} value {unrecognized_level!r}, defaulting to INFO. '
                       f'Allowed values: {", ".join(_LOG_LEVELS)}.')


class CoreLogRouter:
    """**INTERNAL** Holds the native core's logging requests until a protocol with a core registers."""

    def __init__(self, env: EnvLogging | None) -> None:
        self._env = env
        self._backend: CoreLogBackend | None = None
        self._pending_route: tuple[logging.Logger, int] | None = None
        self._pending_protocol_logger: str | None = None
        self._protocols_without_core: list[str] = []

    def register(self, backend: CoreLogBackend) -> None:
        if self._backend is not None:
            return
        # A request is dropped only once the backend has applied it, and the backend is recorded
        # last, so a registration that raises leaves the next one everything still owed.
        if self._env is not None:
            backend.apply_env(self._env)
        if self._pending_route is not None:
            backend.route_to(*self._pending_route)
            self._pending_route = None
        if self._pending_protocol_logger is not None:
            backend.enable_protocol_logger(self._pending_protocol_logger)
            self._pending_protocol_logger = None
        self._backend = backend

    def register_protocol_without_core(self, protocol: str) -> None:
        self._protocols_without_core.append(protocol)
        if self._pending_protocol_logger is not None:
            self._warn_no_protocol_logger(protocol)

    def route_to(self, logger: logging.Logger, level: int) -> None:
        if self._env is not None:
            raise RuntimeError(('Cannot create logger.  Another logger has already been '
                                f'initialized. Make sure the {LOG_LEVEL_ENV_VAR} and {LOG_FILE_ENV_VAR} env '
                                'variable are not set if using configure_logging.'))
        if self._backend is not None:
            self._backend.route_to(logger, level)
            return
        if self._pending_route is not None:
            raise RuntimeError('Cannot create logger.  configure_logging has already been called.')
        self._pending_route = (logger, level)

    def enable_protocol_logger(self, filename: str) -> None:
        # The native core takes any string without a NUL; checking here keeps the error at the call
        # when the request is held.
        if not isinstance(filename, str) or not filename or '\0' in filename:
            raise InvalidArgumentException(f'The protocol logger needs a filename, got {filename!r}.')
        if self._backend is not None:
            self._backend.enable_protocol_logger(filename)
            return
        self._pending_protocol_logger = filename
        for protocol in self._protocols_without_core:
            self._warn_no_protocol_logger(protocol)

    @staticmethod
    def _warn_no_protocol_logger(protocol: str) -> None:
        logging.getLogger(LOGGER_NAME).warning(
            f'The {protocol} protocol has no protocol logger, so it does not capture network traffic.')


def _configure_from_env() -> CoreLogRouter:
    env, unrecognized_level = read_env_logging(os.environ)
    if env is not None:
        configure_python_logging(env, unrecognized_level)
    return CoreLogRouter(env)


_ROUTER = _configure_from_env()


def register_core_log_backend(backend: CoreLogBackend) -> None:
    """**INTERNAL** Called by a protocol with a native core when that core loads.  The first one wins."""
    _ROUTER.register(backend)


def register_protocol_without_core_logging(protocol: str) -> None:
    """**INTERNAL** Called by a protocol with no native core logging when it loads, so a protocol logger
    request it cannot honor is reported rather than silently held."""
    _ROUTER.register_protocol_without_core(protocol)


def configure_logging(name: str, level: int = logging.INFO, parent_logger: logging.Logger | None = None) -> None:
    """
    Configure the Python SDK to route the native core's logs through Python's logging system.

    This is the programmatic alternative to using PYCBC_LOG_LEVEL environment variable.
    It allows integration with your application's existing logging configuration.

    Note: This configures the native core's logs only. Python-side SDK components (threshold
    logging, metrics, transactions) use separate loggers under the 'couchbase' hierarchy.

    To also capture threshold logging reports and other Python SDK logs, configure
    the 'couchbase' logger:

    Example:
        import logging
        from couchbase import configure_logging

        # Route the native core's logs through Python
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
    _ROUTER.route_to(logging.getLogger(name), level)


def enable_protocol_logger_to_save_network_traffic_to_file(filename: str) -> None:
    """
    **VOLATILE** This API is subject to change at any time.

    Exposes the underlying couchbase++ library protocol logger. This method is for logging/debugging
    purposes and must be used with caution as network details will be logged to the provided file.

    A call made before the native core has loaded is held, and takes effect when it loads.  A loaded
    protocol that has no protocol logger logs a warning.

    Args:
        filename (str): The name of the file the protocol logger will write to.

    Raises:
        InvalidArgumentException: If the filename is not a non-empty string without a NUL character.
    """
    _ROUTER.enable_protocol_logger(filename)


__all__ = [
    'configure_logging',
    'enable_protocol_logger_to_save_network_traffic_to_file',
]
