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

"""The gateway a `couchbase2://` connection string names.

The query string has already been read into `ClusterSettings` by the common parser, so what is
left here is the host and port.  A couchbase2 cluster talks to exactly one gateway, which balances
across the nodes itself, so a second host is refused rather than dropped.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from couchbase.common.exceptions import InvalidArgumentException

SCHEME = 'couchbase2'
DEFAULT_PORT = 18098


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int

    @property
    def target(self) -> str:
        """The address as gRPC takes it, with an IPv6 literal in brackets."""
        host = f'[{self.host}]' if ':' in self.host else self.host
        return f'{host}:{self.port}'


def parse_endpoint(connstr: str) -> Endpoint:
    """The one gateway `connstr` names, which is `ClusterSettings.connstr`: no query string."""
    parsed = urlparse(connstr)
    if parsed.scheme != SCHEME:
        raise InvalidArgumentException(message=f'Expected a {SCHEME}:// connection string, got {connstr!r}.')
    if ',' in parsed.netloc or ';' in parsed.netloc:
        raise InvalidArgumentException(
            message=(f'A {SCHEME}:// connection string takes one host, the gateway, which spreads the load '
                     f'over the cluster itself; got {parsed.netloc!r}.'))
    if '@' in parsed.netloc:
        raise InvalidArgumentException(
            message=f'Credentials go in the authenticator, not the connection string; got {parsed.netloc!r}.')
    if parsed.path not in ('', '/'):
        raise InvalidArgumentException(
            message=f'A {SCHEME}:// connection string names no bucket; got the path {parsed.path!r}.')
    if not parsed.hostname:
        raise InvalidArgumentException(message=f'The connection string {connstr!r} names no host.')
    try:
        port = parsed.port
    except ValueError as ex:
        raise InvalidArgumentException(message=f'The connection string {connstr!r} has an invalid port.') from ex
    return Endpoint(parsed.hostname, DEFAULT_PORT if port is None else port)
