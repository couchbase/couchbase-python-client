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

import importlib
import os
import sys

# Importing ssl first lets Python locate OpenSSL for the extension, which matters on Apple silicon (PYCBC-1386).
import ssl  # noqa: F401 # isort:skip

_OPENSSL_DIR_ENV_VAR = 'PYCBC_OPENSSL_DIR'


def _add_openssl_dll_directory() -> bool:
    """Windows resolves an extension's DLLs only from registered directories.  Registers the one named by the
    environment, or else the interpreter's own DLLs directory, which ships libssl and libcrypto."""
    if sys.platform != 'win32':
        return False
    openssl_dir = os.getenv(_OPENSSL_DIR_ENV_VAR) or next(
        (p for p in sys.path if os.path.split(p)[-1] == 'DLLs'), None)
    if not openssl_dir:
        return False
    os.add_dll_directory(openssl_dir)
    return True


def _binding_import_error(exc: ImportError) -> ImportError:
    if isinstance(exc, ModuleNotFoundError) and exc.name == f'{__name__}._core':
        return ImportError(f'The Couchbase SDK native extension ({exc.name}) is not installed. '
                           'Reinstall the couchbase package, or build it from source.',
                           name=exc.name, path=exc.path)
    message = f'The Couchbase SDK native extension could not be loaded: {exc}'
    if sys.platform == 'win32':
        message += f' If OpenSSL was not found, set {_OPENSSL_DIR_ENV_VAR} to the directory holding its libraries.'
    return ImportError(message, name=exc.name, path=exc.path)


# Not `from . import _core`, which reports a missing extension as a plain ImportError naming this package.
try:
    importlib.import_module(f'{__name__}._core')
except ImportError as exc:
    if isinstance(exc, ModuleNotFoundError) or not _add_openssl_dll_directory():
        raise _binding_import_error(exc) from exc
    try:
        importlib.import_module(f'{__name__}._core')
    except ImportError as retry_exc:
        raise _binding_import_error(retry_exc) from retry_exc

from ._core import (CXXCBC_METADATA,  # noqa: E402 # isort:skip
                    FMT_BYTES,
                    FMT_COMMON_MASK,
                    FMT_JSON,
                    FMT_LEGACY_MASK,
                    FMT_PICKLE,
                    FMT_UTF8,
                    create_new_attempt_context,
                    create_transaction_context,
                    create_transactions,
                    destroy_transactions,
                    pycbc_connection,
                    pycbc_exception,
                    pycbc_hdr_histogram,
                    pycbc_kv_request,
                    pycbc_logger,
                    pycbc_result,
                    pycbc_scan_iterator,
                    pycbc_streamed_result,
                    shutdown_logger,
                    transaction_commit,
                    transaction_config_as_dict,
                    transaction_get_multi_op,
                    transaction_get_multi_result,
                    transaction_get_result,
                    transaction_op,
                    transaction_operations,
                    transaction_options_as_dict,
                    transaction_query_op,
                    transaction_query_options_as_dict,
                    transaction_rollback)

# We want all of these to be accessible via couchbase.protocol.classic._pycbc_core
__all__ = [
    'FMT_BYTES',
    'FMT_JSON',
    'FMT_UTF8',
    'FMT_PICKLE',
    'FMT_COMMON_MASK',
    'FMT_LEGACY_MASK',
    'CXXCBC_METADATA',
    'create_new_attempt_context',
    'create_transactions',
    'create_transaction_context',
    'destroy_transactions',
    'pycbc_connection',
    'pycbc_exception',
    'pycbc_hdr_histogram',
    'pycbc_kv_request',
    'pycbc_logger',
    'pycbc_result',
    'pycbc_scan_iterator',
    'pycbc_streamed_result',
    'shutdown_logger',
    'transaction_commit',
    'transaction_config_as_dict',
    'transaction_get_result',
    'transaction_get_multi_op',
    'transaction_get_multi_result',
    'transaction_op',
    'transaction_operations',
    'transaction_options_as_dict',
    'transaction_query_op',
    'transaction_query_options_as_dict',
    'transaction_rollback',
]

# Last, since it reads the names above: hands the core's logger to the shared logging configuration.
import couchbase.protocol.classic._logging_config  # noqa: E402,F401 # isort:skip
