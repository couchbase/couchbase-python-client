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

import platform
from functools import partial, partialmethod
from typing import (List,
                    Optional,
                    Tuple)

try:
    # Importing the ssl package allows us to utilize some Python voodoo to find OpenSSL.
    # This is particularly helpful on M1 macs (PYCBC-1386).
    import ssl  # noqa: F401 # nopep8 # isort:skip # noqa: E402
    import couchbase.pycbc_core  # noqa: F401 # nopep8 # isort:skip # noqa: E402
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

PYCBC_VERSION = f'pycbc/{__version__}'

try:
    python_version_info = platform.sys.version.split(' ')
    if len(python_version_info) > 0:
        PYCBC_VERSION = f'{PYCBC_VERSION} (python/{python_version_info[0]})'
except Exception:  # nosec
    pass


""" Add support for logging, adding a TRACE level to logging """
import json  # nopep8 # isort:skip # noqa: E402
import logging  # nopep8 # isort:skip # noqa: E402

from couchbase.pycbc_core import CXXCBC_METADATA, pycbc_logger  # nopep8 # isort:skip # noqa: E402

_PYCBC_LOGGER = pycbc_logger()
_CXXCBC_METADATA_JSON = json.loads(CXXCBC_METADATA)
logging.TRACE = 5
logging.addLevelName(logging.TRACE, 'TRACE')
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


"""

pycbc teardown methods

"""
import atexit  # nopep8 # isort:skip # noqa: E402


def _pycbc_teardown(**kwargs):
    """**INTERNAL**"""
    global _PYCBC_LOGGER
    if _PYCBC_LOGGER:
        # TODO:  see about synchronizing the logger's shutdown here
        _PYCBC_LOGGER = None


atexit.register(_pycbc_teardown)

"""

Metadata + version methods

"""
_METADATA_KEYS = ['openssl_default_cert_dir',
                  'openssl_default_cert_file',
                  'openssl_headers',
                  'openssl_runtime',
                  'txns_forward_compat_extensions',
                  'txns_forward_compat_protocol_version',
                  'version']


def get_metadata(as_str=False, detailed=False):
    metadata = _CXXCBC_METADATA_JSON if detailed is True else {
        k: v for k, v in _CXXCBC_METADATA_JSON.items() if k in _METADATA_KEYS}
    return json.dumps(metadata) if as_str is True else metadata


def get_transactions_protocol() -> Optional[Tuple[Optional[float], Optional[List[str]]]]:
    """Get the transactions protocol version and supported extensions.

    Returns:
        Optional[Tuple[Optional[float], Optional[List[str]]]]: The transactions protocol version and
        support extensions, if found in the cxx client's metadata.
    """
    if not _CXXCBC_METADATA_JSON:
        return None

    version = _CXXCBC_METADATA_JSON.get('txns_forward_compat_protocol_version', None)
    if version:
        version = float(version)
    extensions = _CXXCBC_METADATA_JSON.get('txns_forward_compat_extensions', None)
    if extensions:
        extensions = extensions.split(',')
    return version, extensions


"""

Logging methods

"""


def configure_console_logger():
    import os
    log_level = os.getenv('PYCBC_LOG_LEVEL', None)
    if log_level:
        _PYCBC_LOGGER.create_console_logger(log_level.lower())
        logging.getLogger().debug(get_metadata(as_str=True))


def configure_logging(name, level=logging.INFO, parent_logger=None):
    if parent_logger:
        name = f'{parent_logger.name}.{name}'
    logger = logging.getLogger(name)
    _PYCBC_LOGGER.configure_logging_sink(logger, level)
    logger.debug(get_metadata(as_str=True))


configure_console_logger()
