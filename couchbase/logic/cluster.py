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

from __future__ import annotations

import logging
import warnings
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)
from urllib.parse import parse_qs, urlparse

from couchbase import PYCBC_VERSION
from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import (ClusterOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               TLSVerifyMode,
                               TransactionConfig,
                               forward_args,
                               get_valid_args)
from couchbase.pycbc_core import (close_connection,
                                  cluster_mgmt_operations,
                                  create_connection,
                                  diagnostics_operation,
                                  get_connection_info,
                                  management_operation,
                                  mgmt_operations,
                                  operations)
from couchbase.result import (ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult)
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.transcoder import JSONTranscoder, Transcoder

if TYPE_CHECKING:
    from couchbase.options import DiagnosticsOptions, PingOptions

log = logging.getLogger(__name__)


class ClusterLogic:

    _LEGACY_CONNSTR_QUERY_ARGS = {
        'ssl': {'tls_verify': TLSVerifyMode.to_str},
        'certpath': {'cert_path': lambda x: x},
        'cert_path': {'cert_path': lambda x: x},
        'truststorepath': {'trust_store_path': lambda x: x},
        'trust_store_path': {'trust_store_path': lambda x: x},
        'sasl_mech_force': {'sasl_mech_force': lambda x: x.split(',') if isinstance(x, str) else x}
    }

    def __init__(self,  # noqa: C901
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs  # type: Dict[str, Any]
                 ) -> ClusterLogic:

        # parse query string prior to parsing ClusterOptions
        connection_str, query_opts, legacy_opts = self._parse_connection_string(connstr)
        self._connstr = connection_str

        kwargs.update(query_opts)
        cluster_opts = get_valid_args(ClusterOptions, kwargs, *options)
        # add legacy options after parsing ClusterOptions to keep logic separate
        cluster_opts.update(self._parse_legacy_query_options(**legacy_opts))

        authenticator = cluster_opts.pop("authenticator", None)
        if not authenticator:
            raise InvalidArgumentException(message="Authenticator is mandatory.")

        # the cert_path _might_ be a part of the query options
        cert_path = cluster_opts.pop('cert_path', None)

        # lets only pass in the authenticator, no kwargs
        auth_kwargs = {k: v for k, v in cluster_opts.items() if k in authenticator.valid_keys()}
        if isinstance(authenticator, CertificateAuthenticator) and 'trust_store_path' in auth_kwargs:
            # the trust_store_path _should_ be in the cluster opts, however <= 3.x SDK allowed it in
            # the CertificateAuthenticator, pop the trust_store_path from the auth_kwargs in case
            if 'trust_store_path' not in authenticator.as_dict():
                auth_kwargs.pop('trust_store_path')
        elif (
            isinstance(authenticator, PasswordAuthenticator) and
            any(map(lambda mech: mech in auth_kwargs, ['sasl_mech_force', 'allowed_sasl_mechanisms']))
        ):
            # 3.x SDK allowed sasl_mech_force in the query string, 4.x is going to allow allowed_sasl_mechanisms
            # in the query string, if the PasswordAuthenticator's static ldap_compatible() method has been used
            # it takes precendence.
            # Set the PasswordAuthenticator's allowed_sasl_mechanisms, if not already set, to the value(s) found in the
            # connection string.  Note that allowed_sasl_mechanisms takes precendence over sasl_mech_force.
            for sasl_key in ['sasl_mech_force', 'allowed_sasl_mechanisms']:
                if sasl_key not in cluster_opts:
                    continue
                # pop from auth_kwargs to satisfy future check
                auth_kwargs.pop(sasl_key)
                if authenticator._allowed_sasl_mechanisms is not None:
                    continue
                if isinstance(cluster_opts[sasl_key], str):
                    authenticator._allowed_sasl_mechanisms = [cluster_opts[sasl_key]]
                else:
                    authenticator._allowed_sasl_mechanisms = cluster_opts[sasl_key]

        if len(auth_kwargs.keys()) > 0:
            raise InvalidArgumentException(
                message="Authentication kwargs now allowed.  Only provide the Authenticator.")

        self._auth = authenticator.as_dict()
        # add the cert_path to the authenticator if found
        if cert_path and 'cert_path' not in self._auth:
            self._auth['cert_path'] = cert_path

        # after cluster options have been parsed (both from the query string and provided
        # options/kwargs), separate into cluster options, timeout options and tracing options and txns config.

        self._transaction_config = cluster_opts.pop("transaction_config", TransactionConfig())
        self._transactions = None

        timeout_opts = {}
        for key in ClusterTimeoutOptions.get_allowed_option_keys(use_transform_keys=True):
            if key in cluster_opts:
                timeout_opts[key] = cluster_opts.pop(key)
        if timeout_opts:
            cluster_opts['timeout_options'] = timeout_opts

        tracing_opts = {}
        for key in ClusterTracingOptions.get_allowed_option_keys(use_transform_keys=True):
            if key in cluster_opts:
                tracing_opts[key] = cluster_opts.pop(key)
        if tracing_opts:
            cluster_opts['tracing_options'] = tracing_opts

        self._default_serializer = cluster_opts.pop("serializer", None)
        if not self._default_serializer:
            self._default_serializer = DefaultJsonSerializer()

        self._default_transcoder = cluster_opts.pop("transcoder", None)
        if not self._default_transcoder:
            self._default_transcoder = JSONTranscoder()

        cluster_opts['user_agent_extra'] = PYCBC_VERSION

        self._cluster_opts = cluster_opts
        self._connection = None
        self._cluster_info = None
        self._server_version = None

    def __del__(self):
        self._destroy_connection()

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        if not hasattr(self, "_connection"):
            self._connection = None
        return self._connection

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        """
        **INTERNAL** not intended for use in public API.
        """
        return self._default_transcoder

    @default_transcoder.setter
    def default_transcoder(self,
                           value  # type: Transcoder
                           ):
        """
        **INTERNAL** not intended for use in public API.
        """
        if not issubclass(value.__class__, Transcoder):
            raise InvalidArgumentException('Cannot set default transcoder to non Transcoder type.')

        self._default_transcoder = value

    @property
    def default_serializer(self) -> Optional[Serializer]:
        return self._default_serializer

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def connected(self) -> bool:
        """
            bool: Indicator on if the cluster has been connected or not.
        """
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
    def server_version_full(self) -> Optional[str]:
        if self._cluster_info:
            return self._cluster_info.server_version_full

        return None

    @property
    def is_developer_preview(self) -> Optional[bool]:
        if self._cluster_info:
            return False
        return None

    def _parse_connection_string(self, connection_str  # type: str
                                 ) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        """Parse the provided connection string

        The provided connection string will be parsed to split the connection string
        and the query options.  Query options will be split into legacy options
        and 'current' options.

        Args:
            connection_str (str): The connection string for the cluster.

        Returns:
            Tuple[str, Dict[str, Any], Dict[str, Any]]: The parsed connection string,
                current options and legacy options.
        """
        # handle possible lack of URL scheme
        if '//' not in connection_str:
            warning_msg = 'Connection string has deprecated format. Start connection string with: couchbase://'
            warnings.warn(warning_msg, DeprecationWarning, stacklevel=2)
            connection_str = f'//{connection_str}'

        parsed_conn = urlparse(connection_str)
        conn_str = ''
        if parsed_conn.scheme:
            conn_str = f'{parsed_conn.scheme}://{parsed_conn.netloc}{parsed_conn.path}'
        else:
            conn_str = f'{parsed_conn.netloc}{parsed_conn.path}'
        query_str = parsed_conn.query
        query_str_opts, legacy_query_str_opts = self._parse_query_string_options(query_str)

        return conn_str, query_str_opts, legacy_query_str_opts

    def _parse_query_string_options(self,
                                    query_str
                                    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Parse the query string options

        Query options will be split into legacy options and 'current' options. The values for the
        'current' options are cast to integers or booleans where applicable

        Args:
            query_str (str): The query string.

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: The parsed current options and legacy options.
        """
        options = parse_qs(query_str)

        # @TODO:  issue warning if it is overriding cluster options?
        legacy_query_str_opts = {}
        for k, v in options.items():
            if k not in self._LEGACY_CONNSTR_QUERY_ARGS.keys():
                continue
            if len(v) > 1:
                legacy_query_str_opts[k] = v
            else:
                legacy_query_str_opts[k] = v[0]

        query_str_opts = {}
        for k, v in options.items():
            if k in self._LEGACY_CONNSTR_QUERY_ARGS.keys():
                continue
            query_str_opts[k] = self._parse_query_string_value(v)

        return query_str_opts, legacy_query_str_opts

    def _parse_query_string_value(self,
                                  value  # type: List[str]
                                  ) -> Union[List[str], str, bool, int]:
        """Parse a query string value

        The provided value is a list of at least one element. Returns either a list of strings or a single element
        which might be cast to an integer or a boolean if that's appropriate.

        Args:
            value (List[str]): The query string value.

        Returns:
            Union[List[str], str, bool, int]: The parsed current options and legacy options.
        """

        if len(value) > 1:
            return value
        v = value[0]
        if v.isnumeric():
            return int(v)
        elif v.lower() in ['true', 'false']:
            return v.lower() == 'true'
        return v

    def _parse_legacy_query_options(self, **query_opts  # type: Dict[str, Any]
                                    ) -> Dict[str, Any]:
        """Parse legacy query string options

        See :attr:`~.ClusterLogic._LEGACY_CONNSTR_QUERY_ARGS`

        Returns:
            Dict[str, Any]: Representation of parsed query string parameters.
        """
        final_options = {}
        for opt_key, opt_value in query_opts.items():
            if opt_key not in self._LEGACY_CONNSTR_QUERY_ARGS:
                continue
            for final_key, transform in self._LEGACY_CONNSTR_QUERY_ARGS[opt_key].items():
                converted = transform(opt_value)
                if converted is not None:
                    final_options[final_key] = converted
        return final_options

    def _get_connection_opts(self, auth_only=False,  # type: Optional[bool]
                             conn_only=False  # type: Optional[bool]
                             ) -> Union[Dict[str, Any], Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
        """Get connection related options

        **INTERNAL** not intended for use in public API.

        Args:
            auth_only (bool, optional): Set to True to return only auth options. Defaults to False.
            conn_only (bool, optional): Set to True to return only cluster options. Defaults to False.

        Returns:
            Union[Dict[str, Any], Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]: Either the
                cluster auth, cluster options or a tuple of both the cluster auth and cluster options.
        """
        if auth_only is True:
            return self._auth
        if conn_only is True:
            return self._cluster_opts
        return self._auth, self._cluster_opts

    def _get_client_connection_info(self) -> Dict[str, Any]:
        """Get connection related options from the cxx client

        **INTERNAL** not intended for use in public API.

        Returns:
            Dict[str, Any]: All connection related information.
        """

        return get_connection_info(self._connection)

    def _connect_cluster(self, **kwargs):

        connect_kwargs = {
            'auth': self._auth,
            'options': self._cluster_opts
        }

        callback = kwargs.pop('callback', None)
        if callback:
            connect_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            connect_kwargs['errback'] = errback

        return create_connection(
            self._connstr, **connect_kwargs,
        )

    def _close_cluster(self, **kwargs):

        # first close the transactions object, if any
        log.debug("closing cluster")
        if self._transactions:
            self._transactions.close()
            del self._transactions

        close_kwargs = {}

        callback = kwargs.pop('callback', None)
        if callback:
            close_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            close_kwargs['errback'] = errback

        return close_connection(
            self._connection, **close_kwargs
        )

    def _set_connection(self, conn):
        self._connection = conn

    def _destroy_connection(self):
        if hasattr(self, '_connection'):
            self._connection = None

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
