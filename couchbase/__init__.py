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

import platform

try:
    # Importing the ssl package allows us to utilize some Python voodoo to find OpenSSL.
    # This is particularly helpful on M1 macs (PYCBC-1386).
    import ssl  # noqa: F401 # nopep8 # isort:skip # noqa: E402
    import couchbase.logic.pycbc_core  # noqa: F401 # nopep8 # isort:skip # noqa: E402
except ImportError:
    import os  # nopep8 # isort:skip # noqa: E402
    import sys  # nopep8 # isort:skip # noqa: E402
    # should only need to do this on Windows w/ Python >= 3.8 due to the changes made for how DLLs are resolved
    if sys.platform.startswith('win32') and (3, 8) <= sys.version_info:
        open_ssl_dir = os.getenv('PYCBC_OPENSSL_DIR')
        # if not set by environment, try to use libcrypto and libssl that comes w/ Windows Python install
        if not open_ssl_dir:
            for p in sys.path:
                if os.path.split(p)[-1] == 'DLLs':
                    open_ssl_dir = p
                    break

        if open_ssl_dir:
            os.add_dll_directory(open_ssl_dir)
        else:
            print(('PYCBC: Caught import error. '
                   'Most likely due to not finding OpenSSL libraries. '
                   'Set PYCBC_OPENSSL_DIR to location where OpenSSL libraries can be found.'))

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


# Import logging configuration - auto-configures on import
import couchbase.logic.logging_config  # nopep8 # isort:skip # noqa: F401, E402

# Import public API functions
from couchbase.logic.logging_config import (  # nopep8 # isort:skip # noqa: F401, E402
    configure_logging,
    enable_protocol_logger_to_save_network_traffic_to_file,
)

from couchbase.logic.pycbc_core.core_metadata import (  # nopep8 # isort:skip # noqa: F401, E402
    get_metadata,
    get_transactions_protocol,
)
