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

from typing import TYPE_CHECKING, Any

from twisted.internet import asyncioreactor

from acouchbase import get_event_loop
from couchbase.common._logging import configure_logging, enable_protocol_logger_to_save_network_traffic_to_file

asyncioreactor.install(get_event_loop())


if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_metadata  # noqa: F401
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_transactions_protocol  # noqa: F401


# Read from the C++ core, so they load the binding only when asked for.
def __getattr__(name: str) -> Any:
    if name in ('get_metadata', 'get_transactions_protocol'):
        from couchbase.protocol.classic._pycbc_core import core_metadata
        return getattr(core_metadata, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
