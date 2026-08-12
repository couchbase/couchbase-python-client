import os
import tempfile
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Union

from acouchbase.cluster import AsyncCluster
from couchbase.auth import (CertificateAuthenticator,
                            JwtAuthenticator,
                            PasswordAuthenticator)
from couchbase.cluster import Cluster
from couchbase.observability.metrics import Meter
from couchbase.observability.tracing import RequestTracer
from couchbase.options import (ClusterOptions,
                               ClusterTimeoutOptions,
                               IpProtocol,
                               TLSVerifyMode)

from ..commands.sdk_commands import SdkCommandOptions


@dataclass
class ConnectionCache:
    hostname: str = None
    options: ClusterOptions = None
    mp_cluster_options: ClusterOptions = None
    obs_config: object = None
    cluster: Union[AsyncCluster, Cluster] = None
    tracer: Optional[RequestTracer] = None
    tracer_provider: object = None
    meter: Optional[Meter] = None
    meter_provider: object = None

    def close(self):
        if self.cluster:
            self.cluster.close()
        if self.tracer_provider:
            self.tracer_provider.shutdown()
        if self.meter_provider:
            self.meter_provider.shutdown()


class ClusterConnectOptions:
    # TODO Should these functions be moved to different file?

    @staticmethod
    def get_cluster_options(request):
        config = None
        if request.HasField('cluster_config'):
            config = request.cluster_config

        trust_store_path = ClusterConnectOptions._get_trust_store_path(config)
        authenticator = ClusterConnectOptions._get_authenticator(request, trust_store_path)

        # No config - return simple options with just the authenticator
        if config is None:
            return ClusterOptions(authenticator=authenticator)

        # TODO Add custom serializer
        kwargs = {
            'authenticator': authenticator,
            'timeout_options': ClusterConnectOptions.get_cluster_timeout_options(config),
            'enable_mutation_tokens': (config.enable_mutation_tokens
                                       if config.HasField('enable_mutation_tokens') else None),
            'enable_tcp_keep_alive': (config.enable_tcp_keep_alives
                                      if config.HasField('enable_tcp_keep_alives') else None),
            'ip_protocol': IpProtocol.ForceIPv4 if config.HasField('force_i_p_v4') else None,
            'config_poll_interval': ClusterConnectOptions.get_timeout_secs(config, 'config_poll_interval_secs'),
            'config_poll_floor': ClusterConnectOptions.get_timeout_secs(config, 'config_poll_floor_interval_secs'),
            'max_http_connections': config.max_http_connections if config.HasField('max_http_connections') else None,
            'transcoder': SdkCommandOptions.get_transcoder(config),
            'tls_verify': ClusterConnectOptions.get_tls_verify_mode(config),
        }

        # JwtAuthenticator has no slot for the trust cert (unlike Password/Certificate authenticators),
        # so pass it on the cluster options.
        if isinstance(authenticator, JwtAuthenticator) and trust_store_path is not None:
            kwargs['trust_store_path'] = trust_store_path

        if config.HasField('preferred_server_group'):
            kwargs['preferred_server_group'] = config.preferred_server_group
        if config.HasField('enable_app_telemetry'):
            kwargs['enable_app_telemetry'] = config.enable_app_telemetry
        if config.HasField('app_telemetry_endpoint'):
            kwargs['app_telemetry_endpoint'] = config.app_telemetry_endpoint
        if config.HasField('app_telemetry_backoff_secs'):
            kwargs['app_telemetry_backoff'] = ClusterConnectOptions.get_timeout_secs(config,
                                                                                     'app_telemetry_backoff_secs')
        if config.HasField('app_telemetry_ping_interval_secs'):
            ping_interval = ClusterConnectOptions.get_timeout_secs(config, 'app_telemetry_ping_interval_secs')
            kwargs['app_telemetry_ping_interval'] = ping_interval
        if config.HasField('app_telemetry_ping_timeout_secs'):
            ping_timeout = ClusterConnectOptions.get_timeout_secs(config, 'app_telemetry_ping_timeout_secs')
            kwargs['app_telemetry_ping_timeout'] = ping_timeout

        return ClusterOptions(**kwargs)

    @staticmethod
    def _get_trust_store_path(config) -> Optional[str]:
        if config is None:
            return None

        if config.HasField('cert_path'):
            return config.cert_path
        if config.HasField('cert'):
            cert_path = os.path.join(tempfile.gettempdir(), f'fit_cert_{os.getpid()}.pem')
            with open(cert_path, 'w') as f:
                f.write(config.cert)
            return cert_path
        return None

    @staticmethod
    def _write_temp_pem(contents: str, suffix: str) -> str:
        fd, path = tempfile.mkstemp(prefix='fit_auth_', suffix=suffix)
        with os.fdopen(fd, 'w') as f:
            f.write(contents)
        return path

    @staticmethod
    def create_authenticator(auth_message, trust_store_path: Optional[str] = None):
        """Create an authenticator from a proto authenticator message.

        This method can be used both during cluster connection and for
        dynamic credential updates.

        Args:
            auth_message: Proto authenticator message with password_auth or certificate_auth.
            trust_store_path: Optional path to trust store (CA cert) for TLS verification.

        Returns:
            PasswordAuthenticator or CertificateAuthenticator instance.

        Raises:
            NotImplementedError: If the authenticator type is not supported.
        """
        which_auth = auth_message.WhichOneof('authenticator')
        if which_auth == 'password_auth':
            password_auth = auth_message.password_auth
            return PasswordAuthenticator(password_auth.username,
                                         password_auth.password,
                                         cert_path=trust_store_path)

        if which_auth == 'jwt_auth':
            jwt_auth = auth_message.jwt_auth
            return JwtAuthenticator(jwt_auth.jwt)

        if which_auth == 'certificate_auth':
            certificate_auth = auth_message.certificate_auth
            cert_path = ClusterConnectOptions._write_temp_pem(certificate_auth.cert, '_client_cert.pem')
            key_path = ClusterConnectOptions._write_temp_pem(certificate_auth.key, '_client_key.pem')
            return CertificateAuthenticator(cert_path=cert_path,
                                            key_path=key_path,
                                            trust_store_path=trust_store_path)
        raise NotImplementedError(f"Authenticator type '{which_auth}' is not supported.")

    @staticmethod
    def _get_authenticator(request, trust_store_path: Optional[str]):
        if request.HasField('authenticator'):
            return ClusterConnectOptions.create_authenticator(request.authenticator, trust_store_path)

        return PasswordAuthenticator(request.cluster_username, request.cluster_password, cert_path=trust_store_path)

    @staticmethod
    def get_cluster_timeout_options(config):
        return ClusterTimeoutOptions(
            connect_timeout=ClusterConnectOptions.get_timeout_secs(config, 'kv_connect_timeout_secs'),
            kv_timeout=ClusterConnectOptions.get_timeout_millis(config, 'kv_timeout_millis'),
            kv_durable_timeout=ClusterConnectOptions.get_timeout_millis(config, 'kv_durable_timeout_millis'),
            views_timeout=ClusterConnectOptions.get_timeout_secs(config, 'view_timeout_secs'),
            query_timeout=ClusterConnectOptions.get_timeout_secs(config, 'query_timeout_secs'),
            analytics_timeout=ClusterConnectOptions.get_timeout_secs(config, 'analytics_timeout_secs'),
            search_timeout=ClusterConnectOptions.get_timeout_secs(config, 'search_timeout_secs'),
            management_timeout=ClusterConnectOptions.get_timeout_secs(config, 'management_timeout_secs'),
            idle_http_connection_timeout=ClusterConnectOptions.get_timeout_secs(
                config, 'idle_http_connection_timeout_secs'),
            config_idle_redial_timeout=ClusterConnectOptions.get_timeout_secs(config, 'config_idle_redial_timeout_secs')
        )

    @staticmethod
    def get_timeout_secs(config, field):
        if not config.HasField(field):
            return None
        return timedelta(seconds=getattr(config, field))

    @staticmethod
    def get_timeout_millis(config, field):
        if not config.HasField(field):
            return None
        return timedelta(milliseconds=getattr(config, field))

    @staticmethod
    def get_tls_verify_mode(config):
        if not config.HasField('insecure') or not config.insecure:
            return None
        return TLSVerifyMode.NO_VERIFY
