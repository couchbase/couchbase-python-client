#  Copyright 2016-2022. Couchbase, Inc.
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
import selectors
from typing import (TYPE_CHECKING,
                    Any,
                    TypeGuard)

from couchbase.common._logging import configure_logging, enable_protocol_logger_to_save_network_traffic_to_file


class LoopValidator:
    REQUIRED_METHODS = {'add_reader', 'remove_reader',
                        'add_writer', 'remove_writer'}

    @staticmethod
    def _get_working_loop() -> asyncio.AbstractEventLoop:
        try:
            # A running loop belongs to the caller.  Never validate, close or replace it; closing
            # is only ever safe for a loop this class created.
            return asyncio.get_running_loop()
        except RuntimeError:
            # Nothing is running in this thread, so fall through and resolve one.
            pass  # nosec

        try:
            # Python <= 3.13: Returns existing or auto-creates a new loop.
            # Python 3.14+: Returns existing or raises RuntimeError.
            evloop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop exists in the current thread.
            evloop = None

        if LoopValidator._is_valid_loop(evloop):
            return evloop

        # Only attempt to close the loop if it actually exists
        if evloop is not None and not evloop.is_closed():
            evloop.close()

        selector = selectors.SelectSelector()
        new_loop = asyncio.SelectorEventLoop(selector)
        asyncio.set_event_loop(new_loop)
        return new_loop

    @staticmethod
    def _is_valid_loop(evloop: asyncio.AbstractEventLoop | None) -> TypeGuard[asyncio.AbstractEventLoop]:
        if not evloop:
            return False
        for meth in LoopValidator.REQUIRED_METHODS:
            abs_meth, actual_meth = (
                getattr(asyncio.AbstractEventLoop, meth), getattr(evloop.__class__, meth))
            if abs_meth == actual_meth:
                return False
        return True

    @staticmethod
    def get_event_loop(evloop: asyncio.AbstractEventLoop | None) -> asyncio.AbstractEventLoop:
        if LoopValidator._is_valid_loop(evloop):
            return evloop
        return LoopValidator._get_working_loop()

    @staticmethod
    def close_loop() -> None:
        try:
            evloop = asyncio.get_event_loop()
            if not evloop.is_closed():
                evloop.close()
        except RuntimeError:
            # If there is no loop to get, there is no loop to close.
            pass  # nosec


def couchbase_loop_factory() -> asyncio.AbstractEventLoop:
    """Dumb factory that simply builds a SelectorEventLoop.

    Returns:
        A SelectorEventLoop instance.
    """
    selector = selectors.SelectSelector()
    return asyncio.SelectorEventLoop(selector)


def get_event_loop(evloop: asyncio.AbstractEventLoop | None = None) -> asyncio.AbstractEventLoop:
    """Get an event loop compatible with acouchbase.

    Some Event loops, such as ProactorEventLoop (the default asyncio event loop for Python 3.8 on Windows)
    are not compatible with acouchbase as they don't implement all members in the abstract base class.

    Args:
        evloop (asyncio.AbstractEventLoop, optional): An optional event loop to validate. If not provided, the default
            event loop will be used.

    Returns:
        The preferred event loop, if compatible, otherwise, a compatible alternative event loop.
    """
    return LoopValidator.get_event_loop(evloop)


if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_metadata  # noqa: F401
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_transactions_protocol  # noqa: F401


# Read from the C++ core, so they load the binding only when asked for.
def __getattr__(name: str) -> Any:
    if name in ('get_metadata', 'get_transactions_protocol'):
        from couchbase.protocol.classic._pycbc_core import core_metadata
        return getattr(core_metadata, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
