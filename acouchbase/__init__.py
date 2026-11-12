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

__all__ = [
    'configure_logging',
    'couchbase_loop_factory',
    'enable_protocol_logger_to_save_network_traffic_to_file',
    'get_event_loop',
    'get_metadata',
    'get_transactions_protocol',
]

import asyncio
import sys
from typing import TYPE_CHECKING, Any

from couchbase.common._logging import configure_logging, enable_protocol_logger_to_save_network_traffic_to_file
from couchbase.common._supportability import Supportability


def _current_event_loop() -> asyncio.AbstractEventLoop | None:
    """The thread's current loop, or None.  Unlike ``asyncio.get_event_loop()``, never creates one."""
    if sys.version_info < (3, 14):
        # Before 3.14 the default policy creates and installs a loop when the main thread has none,
        # and 3.12 and 3.13 warn as they do it.  Ask the policy's thread state first, so the SDK
        # never owns a loop it did not mean to create.  A policy without that state is asked
        # directly.
        local = getattr(asyncio.get_event_loop_policy(), '_local', None)
        if local is not None and getattr(local, '_loop', None) is None and not getattr(local, '_set_called', True):
            return None
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        return None


def _get_event_loop(loop: asyncio.AbstractEventLoop | None = None) -> asyncio.AbstractEventLoop:
    """**INTERNAL** :func:`get_event_loop` without the deprecation warning."""
    if loop is not None:
        return loop
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        # Nothing is running in this thread, so fall through to the current loop.
        pass  # nosec
    current = _current_event_loop()
    if current is not None:
        return current
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    return new_loop


def couchbase_loop_factory() -> asyncio.AbstractEventLoop:
    """Build a new event loop.

    .. deprecated:: 4.7.0
        acouchbase runs on any event loop.  Use :func:`asyncio.new_event_loop`, or let
        :func:`asyncio.run` create the loop.

    Returns:
        A new event loop of the platform's default type.  It is not set as the thread's current loop.
    """
    Supportability.method_deprecated('couchbase_loop_factory', 'asyncio.new_event_loop')
    return asyncio.new_event_loop()


def get_event_loop(evloop: asyncio.AbstractEventLoop | None = None) -> asyncio.AbstractEventLoop:
    """Get an event loop for acouchbase.

    .. deprecated:: 4.7.0
        acouchbase runs on any event loop.  Run the application with :func:`asyncio.run`, and use
        :func:`asyncio.get_running_loop` inside it where a loop is needed.

    Resolves, in order: ``evloop`` when given, the running loop, the thread's current loop, and
    otherwise a new loop of the platform's default type, which is set as the thread's current loop.
    No loop is checked, closed or replaced.

    Args:
        evloop (asyncio.AbstractEventLoop, optional): A loop to return as is.

    Returns:
        The resolved event loop.
    """
    Supportability.method_deprecated('get_event_loop', 'asyncio.run or asyncio.get_running_loop')
    return _get_event_loop(evloop)


if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_metadata  # noqa: F401
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_transactions_protocol  # noqa: F401


# Read from the C++ core, so they load the binding only when asked for.
def __getattr__(name: str) -> Any:
    if name in ('get_metadata', 'get_transactions_protocol'):
        from couchbase.protocol.classic._pycbc_core import core_metadata
        return getattr(core_metadata, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
