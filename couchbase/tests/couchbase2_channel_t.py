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

from __future__ import annotations

import asyncio
import pathlib
import shutil
import subprocess
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, NoReturn

import certifi
import grpc
import grpc.aio
import pytest

from couchbase.auth import (CertificateAuthenticator,
                            JwtAuthenticator,
                            PasswordAuthenticator)
from couchbase.common._cluster_settings import ClusterSettings
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import ClusterOptions
from couchbase.protocol.couchbase2._channel import (CAPELLA_ROOT_CA,
                                                    CLIENT_IDLE_TIMEOUT_MS,
                                                    ChannelConfig,
                                                    call_metadata,
                                                    channel_arguments,
                                                    default_verify_roots,
                                                    describe,
                                                    open_aio_channel,
                                                    open_channel,
                                                    root_certificates)
from couchbase.protocol.couchbase2._connstr import (DEFAULT_PORT,
                                                    Endpoint,
                                                    parse_endpoint)
from tests.event_loop import run_on_own_loop

ECHO = '/test.Echo/Echo'


def _no_histogram(**kwargs: Any) -> NoReturn:
    raise AssertionError('parsing a cluster\'s options recorded a value')


def _config(connstr: str, *options: ClusterOptions, **kwargs: Any) -> ChannelConfig:
    settings = ClusterSettings.build_cluster_settings(connstr, *options, _histogram_factory=_no_histogram, **kwargs)
    return ChannelConfig.from_settings(settings)


def _password() -> ClusterOptions:
    return ClusterOptions(PasswordAuthenticator('Administrator', 'password'))


class EndpointUnitTests:

    @pytest.mark.parametrize('connstr, host, port', [
        ('couchbase2://gateway.local', 'gateway.local', DEFAULT_PORT),
        ('couchbase2://gateway.local/', 'gateway.local', DEFAULT_PORT),
        ('couchbase2://gateway.local:443', 'gateway.local', 443),
        ('couchbase2://10.0.0.1:18098', '10.0.0.1', 18098),
        ('couchbase2://[::1]', '::1', DEFAULT_PORT),
    ])
    def test_one_host(self, connstr: str, host: str, port: int) -> None:
        assert parse_endpoint(connstr) == Endpoint(host, port)

    @pytest.mark.parametrize('endpoint, target', [
        (Endpoint('gateway.local', 443), 'gateway.local:443'),
        (Endpoint('::1', 18098), '[::1]:18098'),
    ])
    def test_target(self, endpoint: Endpoint, target: str) -> None:
        assert endpoint.target == target

    @pytest.mark.parametrize('connstr, match', [
        ('couchbase2://a,b', 'one host'),
        ('couchbase2://a;b', 'one host'),
        ('couchbase2://a:18098,b:18098', 'one host'),
        ('couchbase2://gateway.local/default', 'names no bucket'),
        ('couchbase2://user@gateway.local', 'authenticator'),
        ('couchbase2://', 'names no host'),
        ('couchbase2://gateway.local:port', 'invalid port'),
        ('couchbase://gateway.local', 'couchbase2://'),
    ])
    def test_refused(self, connstr: str, match: str) -> None:
        with pytest.raises(InvalidArgumentException, match=match):
            parse_endpoint(connstr)


class ChannelConfigUnitTests:

    def test_defaults(self) -> None:
        config = _config('couchbase2://gateway.local', _password())
        assert config.endpoint == Endpoint('gateway.local', DEFAULT_PORT)
        assert config.trust_store_path is None
        assert config.include_mozilla_roots is True
        assert config.client_cert_path is None
        assert config.user_agent.startswith('couchbase-python/')

    def test_password_is_basic_metadata(self) -> None:
        # base64('Administrator:password')
        assert call_metadata(_config('couchbase2://h', _password())) == (
            ('authorization', 'Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=='),)

    def test_jwt_is_bearer_metadata(self) -> None:
        config = _config('couchbase2://h', ClusterOptions(JwtAuthenticator('a.b.c')))
        assert call_metadata(config) == (('authorization', 'Bearer a.b.c'),)

    def test_certificate_is_the_channel_identity(self) -> None:
        config = _config('couchbase2://h', ClusterOptions(CertificateAuthenticator('/c.pem', '/k.pem')))
        assert (config.client_cert_path, config.client_key_path) == ('/c.pem', '/k.pem')
        assert call_metadata(config) == ()

    def test_half_an_identity_is_refused(self) -> None:
        # The connection string's `cert_path` reaches the authenticator as a client certificate.
        with pytest.raises(InvalidArgumentException, match='both a certificate and a private key'):
            _config('couchbase2://h?cert_path=/c.pem', _password())

    def test_tls_cannot_be_disabled(self) -> None:
        with pytest.raises(InvalidArgumentException, match='always TLS'):
            _config('couchbase2://h?enable_tls=false', _password())

    @pytest.mark.parametrize('query', ['tls_verify=none', 'tls_verify=no_verify', 'ssl=no_verify'])
    def test_verification_cannot_be_turned_off(self, query: str) -> None:
        with pytest.raises(InvalidArgumentException, match='tls_verify=none'):
            _config(f'couchbase2://h?{query}', _password())

    def test_the_authenticators_trust_store_wins(self) -> None:
        options = ClusterOptions(PasswordAuthenticator('u', 'p', cert_path='/auth.pem'), trust_store_path='/opt.pem')
        assert _config('couchbase2://h', options).trust_store_path == '/auth.pem'
        assert _config('couchbase2://h', _password(), trust_store_path='/opt.pem').trust_store_path == '/opt.pem'

    def test_user_agent_extra_is_appended(self) -> None:
        assert _config('couchbase2://h', _password(), user_agent_extra='app/1').user_agent.endswith(';app/1')

    def test_mozilla_roots_can_be_left_out(self) -> None:
        config = _config('couchbase2://h?disable_mozilla_ca_certificates=true', _password())
        assert config.include_mozilla_roots is False


class DescribeUnitTests:

    @pytest.mark.parametrize('query, kwargs, roots', [
        ('', {}, 'default, Capella and Mozilla roots'),
        ('', {'disable_mozilla_ca_certificates': True}, 'default and Capella roots'),
        ('', {'trust_store_path': '/ca.pem'}, 'trust store'),
    ], ids=['default', 'capella', 'trust-store'])
    def test_how_the_gateway_is_checked(self, query: str, kwargs: dict[str, Any], roots: str) -> None:
        config = _config(f'couchbase2://gateway.local{query}', _password(), **kwargs)
        assert describe(config) == f'tls verification: "{roots}", client certificate: false'

    def test_a_client_certificate_is_named_but_not_shown(self) -> None:
        config = _config('couchbase2://gateway.local',
                         ClusterOptions(CertificateAuthenticator(cert_path='/c.pem', key_path='/k.pem')))
        assert describe(config) == ('tls verification: "default, Capella and Mozilla roots", '
                                    'client certificate: true')

    def test_the_idle_timeout_is_set(self) -> None:
        _, options = channel_arguments(_config('couchbase2://gateway.local', _password()))
        assert ('grpc.client_idle_timeout_ms', CLIENT_IDLE_TIMEOUT_MS) in options


class RootCertificatesUnitTests:

    def test_default_is_the_default_paths_capella_and_mozilla(self) -> None:
        roots = root_certificates(_config('couchbase2://h', _password()))
        assert roots == (default_verify_roots() + CAPELLA_ROOT_CA + certifi.contents()).encode()

    def test_capella_stays_without_mozilla(self) -> None:
        roots = root_certificates(_config('couchbase2://h?disable_mozilla_ca_certificates=true', _password()))
        assert roots == (default_verify_roots() + CAPELLA_ROOT_CA).encode()

    def test_a_trust_store_replaces_both(self, tmp_path: pathlib.Path) -> None:
        store = tmp_path / 'ca.pem'
        store.write_bytes(b'-----BEGIN CERTIFICATE-----\n')
        assert root_certificates(_config('couchbase2://h', _password(),
                                 trust_store_path=str(store))) == store.read_bytes()

    @pytest.mark.parametrize('contents, match', [(None, 'Cannot read'), (b'\n', 'is empty')])
    def test_an_unusable_trust_store_is_refused(self,
                                                tmp_path: pathlib.Path,
                                                contents: bytes | None,
                                                match: str) -> None:
        store = tmp_path / 'ca.pem'
        if contents is not None:
            store.write_bytes(contents)
        with pytest.raises(InvalidArgumentException, match=match):
            root_certificates(_config('couchbase2://h', _password(), trust_store_path=str(store)))


def _pem(body: str) -> str:
    return f'-----BEGIN CERTIFICATE-----\n{body}\n-----END CERTIFICATE-----\n'


class DefaultVerifyRootsUnitTests:
    # OpenSSL reads `SSL_CERT_FILE` and `SSL_CERT_DIR` in place of its built-in default paths.

    def test_the_file_and_the_hashed_files_of_the_directory(self,
                                                            tmp_path: pathlib.Path,
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / 'ca.pem').write_text(_pem('QQ==') + 'not a certificate\n' + _pem('Qg=='))
        certs = tmp_path / 'certs'
        certs.mkdir()
        (certs / '0a1b2c3d.0').write_text(_pem('Qw=='))
        # A copy of what the file holds, and a file OpenSSL would not read for its name.
        (certs / '0a1b2c3d.1').write_text(_pem('QQ=='))
        (certs / 'bundle.pem').write_text(_pem('RA=='))
        monkeypatch.setenv('SSL_CERT_FILE', str(tmp_path / 'ca.pem'))
        monkeypatch.setenv('SSL_CERT_DIR', str(certs))
        assert default_verify_roots() == _pem('QQ==') + _pem('Qg==') + _pem('Qw==')

    def test_missing_paths_add_nothing(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('SSL_CERT_FILE', str(tmp_path / 'absent.pem'))
        monkeypatch.setenv('SSL_CERT_DIR', str(tmp_path / 'absent'))
        assert default_verify_roots() == ''


@dataclass(frozen=True)
class _Pki:
    ca: pathlib.Path
    localhost: pathlib.Path
    localhost_key: pathlib.Path
    gateway: pathlib.Path
    gateway_key: pathlib.Path
    client: pathlib.Path
    client_key: pathlib.Path


# `req` always reads a config file.  A platform's own may be missing, as on Windows, or may add
# extensions of its own that clash with the ones asked for, so every `req` names this one.
_REQ_CONFIG = """\
[req]
distinguished_name = dn
[dn]
[ca]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[gateway]
basicConstraints = critical, CA:FALSE
extendedKeyUsage = serverAuth
"""


def _openssl(*args: str, cwd: pathlib.Path) -> None:
    done = subprocess.run(['openssl', *args], cwd=cwd, capture_output=True, text=True)  # nosec B603 B607
    if done.returncode:
        pytest.fail(f'openssl {args[0]} exited {done.returncode}: {done.stderr.strip()}')


@pytest.fixture(scope='module')
def pki(tmp_path_factory: pytest.TempPathFactory) -> _Pki:
    if shutil.which('openssl') is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    d = tmp_path_factory.mktemp('pki')
    (d / 'req.cnf').write_text(_REQ_CONFIG)
    days = '2'
    _openssl('req', '-config', 'req.cnf', '-x509', '-extensions', 'ca', '-newkey', 'rsa:2048', '-nodes',
             '-keyout', 'ca.key', '-out', 'ca.crt', '-days', days, '-subj', '/CN=test-ca', cwd=d)
    for name, ext in (('localhost', 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n'),
                      ('client', 'extendedKeyUsage=clientAuth\n')):
        (d / f'{name}.ext').write_text(ext)
        _openssl('req', '-config', 'req.cnf', '-newkey', 'rsa:2048', '-nodes', '-keyout', f'{name}.key',
                 '-out', f'{name}.csr', '-subj', f'/CN={name}', cwd=d)
        _openssl('x509', '-req', '-in', f'{name}.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key', '-CAcreateserial',
                 '-out', f'{name}.crt', '-days', days, '-extfile', f'{name}.ext', cwd=d)
    # What the gateway makes for itself when it is given no certificate: self-signed, no SAN.
    _openssl('req', '-config', 'req.cnf', '-x509', '-extensions', 'gateway', '-newkey', 'rsa:2048', '-nodes',
             '-keyout', 'gateway.key', '-out', 'gateway.crt', '-days', days,
             '-subj', '/O=Couchbase/CN=stellar-gateway', cwd=d)
    return _Pki(d / 'ca.crt', d / 'localhost.crt', d / 'localhost.key', d / 'gateway.crt', d / 'gateway.key',
                d / 'client.crt', d / 'client.key')


def _echo(request: bytes, context: grpc.ServicerContext) -> bytes:
    metadata = {key: value for key, value in context.invocation_metadata() if isinstance(value, str)}
    return f"{metadata.get('authorization')}|{metadata.get('user-agent')}".encode()


@contextmanager
def _server(key: pathlib.Path,
            chain: pathlib.Path,
            client_ca: pathlib.Path | None = None) -> Iterator[int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        'test.Echo', {'Echo': grpc.unary_unary_rpc_method_handler(_echo)}),))
    credentials = grpc.ssl_server_credentials([(key.read_bytes(), chain.read_bytes())],
                                              root_certificates=client_ca.read_bytes() if client_ca else None,
                                              require_client_auth=client_ca is not None)
    port = server.add_secure_port('localhost:0', credentials)
    server.start()
    try:
        yield port
    finally:
        server.stop(None)


def _call(config: ChannelConfig) -> str:
    with open_channel(config) as channel:
        reply: bytes = channel.unary_unary(ECHO, bytes, bytes)(b'', timeout=10, metadata=call_metadata(config))
    return reply.decode()


class ChannelUnitTests:
    # A real TLS server in this process, with certificates made for the module.

    def test_verified_against_a_trust_store(self, pki: _Pki) -> None:
        with _server(pki.localhost_key, pki.localhost) as port:
            config = _config(f'couchbase2://localhost:{port}', _password(), trust_store_path=str(pki.ca))
            authorization, user_agent = _call(config).split('|')
        assert authorization == 'Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=='
        assert user_agent.startswith(config.user_agent)

    def test_an_untrusted_certificate_fails_the_call(self, pki: _Pki) -> None:
        with _server(pki.gateway_key, pki.gateway) as port:
            with pytest.raises(grpc.RpcError) as ex:
                _call(_config(f'couchbase2://localhost:{port}?disable_mozilla_ca_certificates=true', _password()))
        assert ex.value.code() is grpc.StatusCode.UNAVAILABLE

    def test_a_ca_in_the_default_paths_is_trusted(self, pki: _Pki, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('SSL_CERT_FILE', str(pki.ca))
        with _server(pki.localhost_key, pki.localhost) as port:
            assert _call(_config(f'couchbase2://localhost:{port}', _password())).startswith('Basic ')

    def test_a_certificate_authenticator_is_presented(self, pki: _Pki) -> None:
        with _server(pki.localhost_key, pki.localhost, client_ca=pki.ca) as port:
            authenticator = CertificateAuthenticator(str(pki.client), str(pki.client_key))
            config = _config(f'couchbase2://localhost:{port}', ClusterOptions(authenticator),
                             trust_store_path=str(pki.ca))
            assert _call(config).startswith('None|')

    def test_the_aio_channel(self, pki: _Pki) -> None:
        async def call(config: ChannelConfig) -> bytes:
            credentials, options = await asyncio.get_running_loop().run_in_executor(None, channel_arguments, config)
            async with open_aio_channel(config, credentials, options) as channel:
                echo = channel.unary_unary(ECHO, bytes, bytes)
                reply: bytes = await echo(b'', timeout=10, metadata=call_metadata(config))
            return reply

        with _server(pki.localhost_key, pki.localhost) as port:
            config = _config(f'couchbase2://localhost:{port}', _password(), trust_store_path=str(pki.ca))
            assert run_on_own_loop(call(config)).startswith(b'Basic ')
