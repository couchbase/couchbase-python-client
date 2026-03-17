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

import asyncio
import selectors
from typing import Optional


class LoopValidator:
    REQUIRED_METHODS = {'add_reader', 'remove_reader',
                        'add_writer', 'remove_writer'}

    @staticmethod
    def _get_working_loop() -> asyncio.AbstractEventLoop:
        try:
            # Python <= 3.13: Returns existing or auto-creates a new loop.
            # Python 3.14+: Returns existing or raises RuntimeError.
            evloop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop exists in the current thread.
            evloop = None

        gen_new_loop = not LoopValidator._is_valid_loop(evloop)
        if gen_new_loop:
            # Only attempt to close the loop if it actually exists
            if evloop is not None and not evloop.is_closed():
                evloop.close()

            selector = selectors.SelectSelector()
            new_loop = asyncio.SelectorEventLoop(selector)
            asyncio.set_event_loop(new_loop)
            return new_loop

        return evloop

    @staticmethod
    def _is_valid_loop(evloop: asyncio.AbstractEventLoop) -> bool:
        if not evloop:
            return False
        for meth in LoopValidator.REQUIRED_METHODS:
            abs_meth, actual_meth = (
                getattr(asyncio.AbstractEventLoop, meth), getattr(evloop.__class__, meth))
            if abs_meth == actual_meth:
                return False
        return True

    @staticmethod
    def get_event_loop(evloop: asyncio.AbstractEventLoop) -> asyncio.AbstractEventLoop:
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
    """
    Dumb factory that simply builds a SelectorEventLoop.

    Returns:
        A SelectorEventLoop instance.
    """
    selector = selectors.SelectSelector()
    return asyncio.SelectorEventLoop(selector)


def get_event_loop(evloop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.AbstractEventLoop:
    """
    Get an event loop compatible with acouchbase.

    Some Event loops, such as ProactorEventLoop (the default asyncio event loop for Python 3.8 on Windows)
    are not compatible with acouchbase as they don't implement all members in the abstract base class.

    Args:
        evloop (asyncio.AbstractEventLoop, optional): An optional event loop to validate. If not provided, the default event loop will be used.

    Returns:
        The preferred event loop, if compatible, otherwise, a compatible alternative event loop.
    """  # noqa: E501
    return LoopValidator.get_event_loop(evloop)
