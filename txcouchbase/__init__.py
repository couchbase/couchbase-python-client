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
    'enable_protocol_logger_to_save_network_traffic_to_file',
    'get_metadata',
    'get_transactions_protocol',
]

import asyncio
import sys
import warnings
from typing import TYPE_CHECKING, Any

from twisted.internet import asyncioreactor

from acouchbase import _current_event_loop
from couchbase.common._logging import configure_logging, enable_protocol_logger_to_save_network_traffic_to_file
from couchbase.common.exceptions import InvalidArgumentException

_READER_METHODS = ('add_reader', 'remove_reader', 'add_writer', 'remove_writer')


def _drives_readers(loop: asyncio.AbstractEventLoop) -> bool:
    # Twisted's asyncio reactor calls these directly.  ProactorEventLoop inherits the abstract
    # versions, which raise NotImplementedError.
    return all(getattr(type(loop), name) is not getattr(asyncio.AbstractEventLoop, name) for name in _READER_METHODS)


def _install_reactor() -> None:
    """Install Twisted's asyncio reactor on a loop it can drive, unless one is already installed.

    Resolves, in order: the loop of an asyncio reactor that is already installed, the running loop,
    the thread's current loop, and otherwise a new SelectorEventLoop, which is set as the thread's
    current loop.  A current loop the reactor cannot drive is replaced, with a warning, and never closed.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If another kind of reactor is
            installed, or the running loop cannot be driven by the reactor.
    """
    if 'twisted.internet.reactor' in sys.modules:
        from twisted.internet import reactor
        if not hasattr(reactor, '_asyncioEventloop'):
            raise InvalidArgumentException(
                f'txcouchbase needs Twisted\'s asyncioreactor, but {type(reactor).__name__} is installed.  '
                'Import txcouchbase before anything that installs a reactor.')
        return

    try:
        loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None:
        if not _drives_readers(loop):
            raise InvalidArgumentException(
                f'txcouchbase was imported while a {type(loop).__name__} is running, which Twisted\'s '
                'asyncioreactor cannot drive.  Run a loop that implements add_reader and add_writer, '
                'such as asyncio.SelectorEventLoop.')
    else:
        loop = _current_event_loop()
        if loop is None or not _drives_readers(loop):
            if loop is not None:
                warnings.warn(
                    f'txcouchbase replaced the current {type(loop).__name__}, which Twisted\'s asyncioreactor '
                    'cannot drive, with a SelectorEventLoop.  The replaced loop was not closed.  Set a '
                    'SelectorEventLoop as the current loop before importing txcouchbase to avoid this.',
                    RuntimeWarning,
                    stacklevel=3)
            loop = asyncio.SelectorEventLoop()
            asyncio.set_event_loop(loop)

    asyncioreactor.install(loop)


_install_reactor()

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_metadata  # noqa: F401
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_transactions_protocol  # noqa: F401


# Read from the C++ core, so they load the binding only when asked for.
def __getattr__(name: str) -> Any:
    if name in ('get_metadata', 'get_transactions_protocol'):
        from couchbase.protocol.classic._pycbc_core import core_metadata
        return getattr(core_metadata, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
