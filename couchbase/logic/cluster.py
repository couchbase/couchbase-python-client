from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)
from urllib.parse import parse_qs, urlparse

from couchbase.auth import CertificateAuthenticator
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import (ClusterOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               TLSVerifyMode,
                               forward_args,
                               get_valid_args)
from couchbase.pycbc_core import (close_connection,
                                  cluster_mgmt_operations,
                                  create_connection,
                                  diagnostics_operation,
                                  management_operation,
                                  mgmt_operations,
                                  operations)
from couchbase.result import (ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult)
from couchbase.transcoder import JSONTranscoder
from couchbase.options import TransactionConfig

if TYPE_CHECKING:
    from couchbase.options import DiagnosticsOptions, PingOptions
    from couchbase.transcoder import Transcoder


class ClusterLogic:
    def __init__(self,  # noqa: C901
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs  # type: Dict[str, Any]
                 ) -> ClusterLogic:

        self._connstr = connstr

        final_args = get_valid_args(ClusterOptions, kwargs, *options)
        authenticator = final_args.pop("authenticator", None)
        if not authenticator:
            raise InvalidArgumentException("Authenticator is mandatory")

        # lets only pass in the authenticator, no kwargs
        auth_kwargs = {k: v for k, v in final_args.items() if k in authenticator.valid_keys()}
        if isinstance(authenticator, CertificateAuthenticator) and 'trust_store_path' in auth_kwargs:
            # the trust_store_path _should_ be in the cluster opts, however < = 3.x SDK allowed it in
            # the CertificateAuthenticator, pop the trust_store_path from the auth_kwargs in case
            if 'trust_store_path' not in authenticator.as_dict():
                auth_kwargs.pop('trust_store_path')
        if len(auth_kwargs.keys()) > 0:
            raise InvalidArgumentException(
                "Authentication kwargs now allowed.  Only provide the Authenticator.")

        self._transaction_config = final_args.pop("transaction_config", TransactionConfig())
        self._transactions = None

        final_args = self._parse_connection_string(**final_args)
        conn_opts = self._validate_connect_options(**final_args)

        timeout_opts = {}
        for key in ClusterTimeoutOptions.get_allowed_option_keys(use_transform_keys=True):
            if key in conn_opts:
                timeout_opts[key] = conn_opts.pop(key)
        if timeout_opts:
            conn_opts['timeout_options'] = timeout_opts

        tracing_opts = {}
        for key in ClusterTracingOptions.get_allowed_option_keys(use_transform_keys=True):
            if key in conn_opts:
                tracing_opts[key] = conn_opts.pop(key)
        if tracing_opts:
            conn_opts['tracing_options'] = tracing_opts

        self._transcoder = conn_opts.pop("transcoder", None)
        if not self._transcoder:
            self._transcoder = JSONTranscoder()

        print(f'connection opts: {conn_opts}')
        self._auth = authenticator.as_dict()
        print(f'auth opts: {self._auth}')
        self._conn_opts = conn_opts
        self._connection = None
        self._cluster_info = None
        self._server_version = None

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        if not hasattr(self, "_connection"):
            self._connection = None
        return self._connection

    @property
    def transcoder(self) -> Transcoder:
        """
        **INTERNAL**
        """
        return self._transcoder

    @property
    def connected(self) -> bool:
        return hasattr(self, "_connection") and self._connection is not None

    @property
    def server_version(self) -> Optional[str]:
        if self._cluster_info:
            return self._cluster_info.server_version

        return None

    @property
    def server_version_short(self) -> Optional[float]:
        if self._cluster_info:
            return self._cluster_info.server_version_short

        return None

    @property
    def is_developer_preview(self) -> Optional[bool]:
        if self._cluster_info:
            return False
        return None

    def _parse_connection_string(self, **connect_kwargs):
        parsed_conn = urlparse(self._connstr)
        query_str = parsed_conn.query
        options = parse_qs(query_str)
        # @TODO:  issue warning if it is overriding cluster options?
        ssl = options.pop('ssl', None)
        if ssl is not None:
            connect_kwargs['tls_verify'] = TLSVerifyMode.from_str(ssl[0])
        valid_opts = ClusterOptions.get_allowed_option_keys()
        # @TODO:  any further validation??
        for k, v in options.items():
            if v is None:
                continue
            if k in valid_opts:
                connect_kwargs[k] = v[0]

        return connect_kwargs

    def _validate_connect_options(self, **connect_kwargs):
        final_opts = {}
        for k, v in connect_kwargs.items():
            if v is None:
                continue
            if k == 'tls_verify':
                final_opts[k] = v.value
            else:
                final_opts[k] = v

        return final_opts

    def _get_connection_opts(self, auth_only=False, conn_only=False):
        if auth_only is True:
            return self._auth
        if conn_only is True:
            return self._conn_opts

        return self._auth, self._conn_opts

    def _connect_cluster(self, **kwargs):

        connect_kwargs = {
            'auth': self._auth,
            'options': self._conn_opts
        }

        callback = kwargs.pop('callback', None)
        if callback:
            connect_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            connect_kwargs['errback'] = errback

        print(f'connect kwargs: {connect_kwargs}')

        return create_connection(
            self._connstr, **connect_kwargs,
        )

    def _close_cluster(self, **kwargs):

        # first close the transactions object, if any
        if self._transactions:
            self._transactions.close()

        close_kwargs = {}

        callback = kwargs.pop('callback', None)
        if callback:
            close_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            close_kwargs['errback'] = errback

        print("Closing connection: {}, kwargs: {}".format(self._connection, close_kwargs))

        return close_connection(
            self._connection, **close_kwargs
        )

    def _set_connection(self, conn):
        print("setting connection!")
        self._connection = conn

    def _destroy_connection(self):
        print("destroying connection!")
        del self._connection

    def _get_cluster_info(self, **kwargs) -> Optional[ClusterInfoResult]:

        cluster_info_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.CLUSTER.value,
            "op_type": cluster_mgmt_operations.GET_CLUSTER_INFO.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            cluster_info_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            cluster_info_kwargs['errback'] = errback

        return management_operation(**cluster_info_kwargs)

    def _enable_dp(self, **kwargs):
        """
            @TODO(jc):  cxx client:
                libc++abi: terminating with uncaught exception of type
                std::runtime_error: cannot map key: partition map is not available
        """

        enable_dp_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.CLUSTER.value,
            "op_type": cluster_mgmt_operations.ENABLE_DP.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            enable_dp_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            enable_dp_kwargs['errback'] = errback

        return management_operation(**enable_dp_kwargs)


    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Any
             ) -> Optional[PingResult]:

        ping_kwargs = {
            'conn': self._connection,
            'op_type': operations.PING.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            ping_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            ping_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *opts)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = list(
                map(lambda st: st.value, [ServiceType(st.value) for st in ServiceType]))

        if not isinstance(service_types, list):
            raise InvalidArgumentException("Service types must be a list/set.")

        service_types = list(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))
        final_args["service_types"] = service_types
        # TODO: tracing
        # final_args.pop("span", None)

        ping_kwargs.update(final_args)
        print("cluster ping args: {}".format(final_args))
        return diagnostics_operation(**ping_kwargs)

    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Optional[DiagnosticsResult]:

        diagnostics_kwargs = {
            'conn': self._connection,
            'op_type': operations.DIAGNOSTICS.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            diagnostics_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            diagnostics_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *opts)
        diagnostics_kwargs.update(final_args)
        return diagnostics_operation(**diagnostics_kwargs)
