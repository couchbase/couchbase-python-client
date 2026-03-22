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


import json
from typing import (List,
                    Optional,
                    Tuple)

from couchbase.logic.pycbc_core import CXXCBC_METADATA

_CXXCBC_METADATA_JSON = json.loads(CXXCBC_METADATA)

_METADATA_KEYS = [
    'openssl_default_cert_dir',
    'openssl_default_cert_file',
    'openssl_headers',
    'openssl_runtime',
    'txns_forward_compat_extensions',
    'txns_forward_compat_protocol_version',
    'version'
]


def get_metadata(as_str=False, detailed=False):
    """
    Get metadata about the C++ client library.

    Args:
        as_str: If True, return metadata as JSON string. Otherwise return dict.
        detailed: If True, return all metadata. Otherwise return only common keys.

    Returns:
        dict or str: Metadata information
    """
    metadata = _CXXCBC_METADATA_JSON if detailed is True else {
        k: v for k, v in _CXXCBC_METADATA_JSON.items() if k in _METADATA_KEYS}
    return json.dumps(metadata) if as_str is True else metadata


def get_transactions_protocol() -> Optional[Tuple[Optional[float], Optional[List[str]]]]:
    """
    Get the transactions protocol version and supported extensions.

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


__all__ = [
    'get_metadata',
    'get_transactions_protocol',
]
