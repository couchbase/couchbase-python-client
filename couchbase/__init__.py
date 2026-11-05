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

import platform
from typing import TYPE_CHECKING, Any

try:
    from couchbase._version import __version__
except ImportError:
    __version__ = '0.0.0-could-not-find-version'

PYCBC_VERSION = f'python/{__version__}'
USER_AGENT_EXTRA = ''

try:
    USER_AGENT_EXTRA = f'python/{platform.python_version()}'
except Exception:  # nosec
    pass


# Applies PYCBC_LOG_LEVEL on import
from couchbase.common._logging import (  # nopep8 # isort:skip # noqa: F401, E402
    configure_logging,
    enable_protocol_logger_to_save_network_traffic_to_file,
)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_metadata  # noqa: F401
    from couchbase.protocol.classic._pycbc_core.core_metadata import get_transactions_protocol  # noqa: F401


# Read from the C++ core, so they load the binding only when asked for.
def __getattr__(name: str) -> Any:
    if name in ('get_metadata', 'get_transactions_protocol'):
        from couchbase.protocol.classic._pycbc_core import core_metadata
        return getattr(core_metadata, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
