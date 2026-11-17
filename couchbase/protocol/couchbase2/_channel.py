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

"""The gRPC channel to the gateway: its credentials, its options, and the metadata every call carries.

A sync cluster owns a `grpc.Channel` and an async cluster a `grpc.aio.Channel`.  An aio channel
binds to the event loop current when a call is made, so the sync API never borrows one.

The channel is TLS, always, and always verifies the gateway.  By default it trusts what classic
trusts: OpenSSL's default verify paths, the Capella root and the Mozilla bundle.  A configured trust
store replaces all three.  A password or JWT travels as per-call
`authorization` metadata rather than as call credentials, and a certificate authenticator's pair is
presented as the channel's own identity.
"""

from __future__ import annotations

import base64
import os
import re
import ssl
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

import certifi
import grpc
import grpc.aio

from couchbase import PYCBC_VERSION, USER_AGENT_EXTRA
from couchbase.common._cluster_settings import ClusterSettings
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import TLSVerifyMode
from couchbase.protocol.couchbase2._connstr import Endpoint, parse_endpoint

KEEPALIVE_TIME_MS = 30_000
KEEPALIVE_TIMEOUT_MS = 5_000
# gRPC's default of 4 MiB is smaller than the largest document Couchbase serves.
MAX_RECEIVE_MESSAGE_LENGTH = 25 * 1024 * 1024

CAPELLA_ROOT_CA = """-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIRANLVkgOvtaXiQJi0V6qeNtswDQYJKoZIhvcNAQELBQAw
JDESMBAGA1UECgwJQ291Y2hiYXNlMQ4wDAYDVQQLDAVDbG91ZDAeFw0xOTEyMDYy
MjEyNTlaFw0yOTEyMDYyMzEyNTlaMCQxEjAQBgNVBAoMCUNvdWNoYmFzZTEOMAwG
A1UECwwFQ2xvdWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCfvOIi
enG4Dp+hJu9asdxEMRmH70hDyMXv5ZjBhbo39a42QwR59y/rC/sahLLQuNwqif85
Fod1DkqgO6Ng3vecSAwyYVkj5NKdycQu5tzsZkghlpSDAyI0xlIPSQjoORA/pCOU
WOpymA9dOjC1bo6rDyw0yWP2nFAI/KA4Z806XeqLREuB7292UnSsgFs4/5lqeil6
rL3ooAw/i0uxr/TQSaxi1l8t4iMt4/gU+W52+8Yol0JbXBTFX6itg62ppb/Eugmn
mQRMgL67ccZs7cJ9/A0wlXencX2ohZQOR3mtknfol3FH4+glQFn27Q4xBCzVkY9j
KQ20T1LgmGSngBInAgMBAAGjQjBAMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYE
FJQOBPvrkU2In1Sjoxt97Xy8+cKNMA4GA1UdDwEB/wQEAwIBhjANBgkqhkiG9w0B
AQsFAAOCAQEARgM6XwcXPLSpFdSf0w8PtpNGehmdWijPM3wHb7WZiS47iNen3oq8
m2mm6V3Z57wbboPpfI+VEzbhiDcFfVnK1CXMC0tkF3fnOG1BDDvwt4jU95vBiNjY
xdzlTP/Z+qr0cnVbGBSZ+fbXstSiRaaAVcqQyv3BRvBadKBkCyPwo+7svQnScQ5P
Js7HEHKVms5tZTgKIw1fbmgR2XHleah1AcANB+MAPBCcTgqurqr5G7W2aPSBLLGA
fRIiVzm7VFLc7kWbp7ENH39HVG6TZzKnfl9zJYeiklo5vQQhGSMhzBsO70z4RRzi
DPFAN/4qZAgD5q3AFNIq2WWADFQGSwVJhg==
-----END CERTIFICATE-----
"""

_PEM_CERTIFICATE = re.compile(r'-----BEGIN CERTIFICATE-----[A-Za-z0-9+/=\s]+?-----END CERTIFICATE-----')
# OpenSSL reads only the files of a CA directory named for a subject hash.
_HASHED_NAME = re.compile(r'[0-9a-f]{8}\.[0-9]+')


@dataclass(frozen=True)
class ChannelConfig:
    """What the channel is built from, read once from the cluster's settings."""

    endpoint: Endpoint
    trust_store_path: str | None
    include_mozilla_roots: bool
    client_cert_path: str | None
    client_key_path: str | None
    authorization: str | None
    user_agent: str

    @classmethod
    def from_settings(cls, settings: ClusterSettings) -> ChannelConfig:
        options, auth = settings.cluster_options, settings.auth
        if options.get('enable_tls') is False:
            raise InvalidArgumentException(
                message='couchbase2:// is always TLS, so enable_tls=False cannot be honoured.')
        cert_path, key_path = auth.get('cert_path'), auth.get('key_path')
        if bool(cert_path) != bool(key_path):
            raise InvalidArgumentException(
                message=('Client certificate authentication needs both a certificate and a private key; '
                         'only one was configured.'))
        if TLSVerifyMode.to_str(options.get('tls_verify', 'peer')) == 'none':
            raise InvalidArgumentException(
                message='couchbase2:// always verifies the gateway, so tls_verify=none cannot be honoured.')
        return cls(endpoint=parse_endpoint(settings.connstr),
                   # The authenticator's trust store wins over the cluster option's, as on classic.
                   trust_store_path=auth.get('trust_store_path') or options.get('trust_store_path'),
                   include_mozilla_roots=not options.get('disable_mozilla_ca_certificates', False),
                   client_cert_path=cert_path,
                   client_key_path=key_path,
                   authorization=_authorization(auth),
                   user_agent=';'.join(part for part in (f'couchbase-{PYCBC_VERSION}',
                                                         USER_AGENT_EXTRA,
                                                         options.get('user_agent_extra'))
                                       if part))


def _authorization(auth: dict[str, Any]) -> str | None:
    if auth.get('jwt_token'):
        return f"Bearer {auth['jwt_token']}"
    if 'username' in auth:
        token = base64.b64encode(f"{auth['username']}:{auth['password']}".encode()).decode('ascii')
        return f'Basic {token}'
    return None


def with_credentials(config: ChannelConfig, auth: dict[str, Any]) -> ChannelConfig:
    """`config` sending the credentials in `auth`, which an open channel takes on its next call."""
    return replace(config, authorization=_authorization(auth))


def call_metadata(config: ChannelConfig) -> tuple[tuple[str, str], ...]:
    """The metadata every call carries."""
    return (('authorization', config.authorization),) if config.authorization else ()


def _read_required(path: str, what: str) -> bytes:
    # A configured file that cannot be read is an error, never a fall back to the default roots,
    # which would trust more issuers than the configuration asked for.
    try:
        with open(path, 'rb') as f:
            contents = f.read()
    except OSError as ex:
        raise InvalidArgumentException(message=f'Cannot read the configured {what} {path!r}: {ex}') from ex
    if not contents.strip():
        raise InvalidArgumentException(message=f'The configured {what} {path!r} is empty.')
    return contents


def default_verify_roots() -> str:
    """The certificates in OpenSSL's default verify paths, which classic trusts too.

    gRPC takes its roots as one PEM blob that replaces its own, so the default CA file and the
    hashed files of the default CA directory are read here.  A path that is missing or unreadable
    adds nothing, as it does for OpenSSL.
    """
    paths = ssl.get_default_verify_paths()
    files = [paths.cafile] if paths.cafile else []
    if paths.capath and os.path.isdir(paths.capath):
        files += sorted(os.path.join(paths.capath, name) for name in os.listdir(paths.capath)
                        if _HASHED_NAME.fullmatch(name))
    pems: dict[str, None] = {}
    for path in files:
        try:
            with open(path, encoding='ascii', errors='replace') as f:
                text = f.read()
        except OSError:
            continue
        pems.update(dict.fromkeys(match.group(0) + '\n' for match in _PEM_CERTIFICATE.finditer(text)))
    return ''.join(pems)


def root_certificates(config: ChannelConfig) -> bytes:
    """The roots the channel trusts, as the one PEM blob gRPC takes as the whole store."""
    if config.trust_store_path:
        return _read_required(config.trust_store_path, 'trust store')
    pem = default_verify_roots() + CAPELLA_ROOT_CA
    if config.include_mozilla_roots:
        pem += certifi.contents()
    return pem.encode('ascii')


def _channel_options(config: ChannelConfig) -> list[tuple[str, Any]]:
    return [
        ('grpc.keepalive_time_ms', KEEPALIVE_TIME_MS),
        ('grpc.keepalive_timeout_ms', KEEPALIVE_TIMEOUT_MS),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.max_receive_message_length', MAX_RECEIVE_MESSAGE_LENGTH),
        ('grpc.primary_user_agent', config.user_agent),
    ]


def channel_arguments(config: ChannelConfig) -> tuple[grpc.ChannelCredentials, Sequence[tuple[str, Any]]]:
    """The credentials and options either channel type is built with.

    Reads the trust roots from disk, so an async caller runs this off the event loop.
    """
    identity: dict[str, bytes] = {}
    if config.client_cert_path and config.client_key_path:
        identity = {'private_key': _read_required(config.client_key_path, 'client key'),
                    'certificate_chain': _read_required(config.client_cert_path, 'client certificate')}
    credentials = grpc.ssl_channel_credentials(root_certificates=root_certificates(config), **identity)
    return credentials, _channel_options(config)


def open_channel(config: ChannelConfig) -> grpc.Channel:
    """A sync channel.  It connects lazily, on the first call, and reconnects for as long as it is open."""
    credentials, options = channel_arguments(config)
    return grpc.secure_channel(config.endpoint.target, credentials, options)


def open_aio_channel(config: ChannelConfig,
                     credentials: grpc.ChannelCredentials,
                     options: Sequence[tuple[str, Any]]) -> grpc.aio.Channel:
    """An async channel, from `channel_arguments` run off the loop; lazy as `open_channel` is."""
    return grpc.aio.secure_channel(config.endpoint.target, credentials, options)
