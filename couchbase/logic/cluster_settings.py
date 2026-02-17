#  Copyright 2016-2023. Couchbase, Inc.
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

import warnings
from dataclasses import dataclass
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    TypedDict,
                    Union)
from urllib.parse import parse_qs, urlparse

from couchbase import USER_AGENT_EXTRA
from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import (ClusterMetricsOptions,
                               ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               TLSVerifyMode,
                               TransactionConfig,
                               get_valid_args)
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.transcoder import JSONTranscoder, Transcoder

LEGACY_CONNSTR_QUERY_ARGS = {
    'ssl': {'tls_verify': TLSVerifyMode.to_str},
    'certpath': {'cert_path': lambda x: x},
    'cert_path': {'cert_path': lambda x: x},
    'truststorepath': {'trust_store_path': lambda x: x},
    'trust_store_path': {'trust_store_path': lambda x: x},
    'sasl_mech_force': {'sasl_mech_force': lambda x: x.split(',') if isinstance(x, str) else x}
}


def parse_connection_string(connection_str: str) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
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
    query_str_opts, legacy_query_str_opts = parse_query_string_options(query_str)

    return conn_str, query_str_opts, legacy_query_str_opts


def parse_query_string_options(query_str: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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
        if k not in LEGACY_CONNSTR_QUERY_ARGS.keys():
            continue
        if len(v) > 1:
            legacy_query_str_opts[k] = v
        else:
            legacy_query_str_opts[k] = v[0]

    query_str_opts = {}
    for k, v in options.items():
        if k in LEGACY_CONNSTR_QUERY_ARGS.keys():
            continue
        query_str_opts[k] = parse_query_string_value(v)

    return query_str_opts, legacy_query_str_opts


def parse_query_string_value(value: List[str]) -> Union[List[str], str, bool, int]:
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


def parse_legacy_query_options(**query_opts: object) -> Dict[str, Any]:
    """Parse legacy query string options

    Returns:
        Dict[str, Any]: Representation of parsed query string parameters.
    """
    final_options = {}
    for opt_key, opt_value in query_opts.items():
        if opt_key not in LEGACY_CONNSTR_QUERY_ARGS:
            continue
        for final_key, transform in LEGACY_CONNSTR_QUERY_ARGS[opt_key].items():
            converted = transform(opt_value)
            if converted is not None:
                final_options[final_key] = converted
    return final_options


def build_authenticator(cluster_opts: Dict[str, Any]) -> Dict[str, Any]:  # noqa: C901
    authenticator = cluster_opts.pop('authenticator', None)
    if not authenticator:
        raise InvalidArgumentException(message='Authenticator is mandatory.')

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
        raise InvalidArgumentException(message='Authentication kwargs not allowed. Only provide the Authenticator.')

    auth = authenticator.as_dict()
    # add the cert_path to the authenticator if found
    if cert_path and 'cert_path' not in auth:
        auth['cert_path'] = cert_path

    return auth


def build_metrics_options(cluster_opts: Dict[str, Any]) -> Dict[str, Any]:
    metrics_opts = {}
    for key in ClusterMetricsOptions.get_allowed_option_keys(use_transform_keys=True):
        if key in cluster_opts:
            metrics_opts[key] = cluster_opts.pop(key)

    cluster_metrics_emit_interval = cluster_opts.pop('cluster_metrics_emit_interval', None)
    if cluster_metrics_emit_interval and 'metrics_emit_interval' not in metrics_opts:
        metrics_opts['metrics_emit_interval'] = cluster_metrics_emit_interval
    cluster_enable_metrics = cluster_opts.pop('cluster_enable_metrics', None)
    if cluster_enable_metrics is not None and 'enable_metrics' not in metrics_opts:
        metrics_opts['enable_metrics'] = cluster_enable_metrics
    if metrics_opts:
        cluster_opts['metrics_options'] = metrics_opts

    return metrics_opts


def build_timeout_options(cluster_opts: Dict[str, Any]) -> Dict[str, Any]:
    timeout_opts = {}
    for key in ClusterTimeoutOptions.get_allowed_option_keys(use_transform_keys=True):
        if key in cluster_opts:
            timeout_opts[key] = cluster_opts.pop(key)
    if timeout_opts:
        cluster_opts['timeout_options'] = timeout_opts

    return timeout_opts


def build_tracing_and_orphan_options(cluster_opts: Dict[str, Any]  # noqa: C901
                                     ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    tracing_opts = {}
    for key in ClusterTracingOptions.get_allowed_option_keys(use_transform_keys=True):
        if key in cluster_opts:
            tracing_opts[key] = cluster_opts.pop(key)

    orphan_opts = {}
    for key in ClusterOrphanReportingOptions.get_allowed_option_keys(use_transform_keys=True):
        if key in cluster_opts:
            orphan_opts[key] = cluster_opts.pop(key)

    # PYCBC-XXXX: C++ core split out orphan reporting from tracing, so we have separate options blocks now.
    #           However, we have to check for the old options in the tracing block for backwards compatibility.
    if tracing_opts:
        sample_size = tracing_opts.pop('orphan_sample_size', None)
        if sample_size and 'sample_size' not in orphan_opts:
            orphan_opts['orphan_sample_size'] = sample_size
        emit_interval = tracing_opts.pop('orphan_emit_interval', None)
        if emit_interval and 'emit_interval' not in orphan_opts:
            orphan_opts['orphan_emit_interval'] = emit_interval

    cluster_enable_tracing = cluster_opts.pop('cluster_enable_tracing', None)
    if cluster_enable_tracing is not None and 'enable_tracing' not in tracing_opts:
        tracing_opts['enable_tracing'] = cluster_enable_tracing
    if tracing_opts:
        cluster_opts['tracing_options'] = tracing_opts

    cluster_enable_orphan_reporting = cluster_opts.pop('cluster_enable_orphan_reporting', None)
    if cluster_enable_orphan_reporting is not None and 'enable_orphan_reporting' not in orphan_opts:
        orphan_opts['enable_orphan_reporting'] = cluster_enable_orphan_reporting
    if orphan_opts:
        cluster_opts['orphan_reporting_options'] = orphan_opts

    return tracing_opts, orphan_opts


class StreamingTimeouts(TypedDict):
    analytics_timeout: Optional[int] = None
    query_timeout: Optional[int] = None
    search_timeout: Optional[int] = None
    view_timeout: Optional[int] = None


@dataclass
class ClusterSettings:
    connstr: str
    auth: Dict[str, Any]
    cluster_options: Dict[str, Any]
    default_transcoder: Transcoder
    default_serializer: Serializer
    metrics_options: Dict[str, Any]
    orphan_options: Dict[str, Any]
    streaming_timeouts: StreamingTimeouts
    timeout_options: Dict[str, Any]
    tracing_options: Dict[str, Any]
    transaction_config: TransactionConfig

    @classmethod
    def build_cluster_settings(cls,
                               connstr,  # type: str
                               *options,  # type: ClusterOptions
                               **kwargs  # type: Dict[str, Any]
                               ) -> ClusterSettings:
        # parse query string prior to parsing ClusterOptions
        connection_str, query_opts, legacy_opts = parse_connection_string(connstr)

        kwargs.update(query_opts)
        cluster_opts = get_valid_args(ClusterOptions, kwargs, *options)
        # add legacy options after parsing ClusterOptions to keep logic separate
        cluster_opts.update(parse_legacy_query_options(**legacy_opts))

        auth = build_authenticator(cluster_opts)

        # after cluster options have been parsed (both from the query string and provided
        # options/kwargs), separate into cluster options, timeout options and tracing options and txns config.

        default_serializer = cluster_opts.pop('serializer', None)
        if not default_serializer:
            default_serializer = DefaultJsonSerializer()

        default_transcoder = cluster_opts.pop('transcoder', None)
        if not default_transcoder:
            default_transcoder = JSONTranscoder()

        timeout_opts = build_timeout_options(cluster_opts)
        streaming_timeouts: StreamingTimeouts = {
            'analytics_timeout': timeout_opts.get('analytics_timeout', None),
            'query_timeout': timeout_opts.get('query_timeout', None),
            'search_timeout': timeout_opts.get('search_timeout', None),
            'view_timeout': timeout_opts.get('view_timeout', None),
        }
        tracing_opts, orphan_opts = build_tracing_and_orphan_options(cluster_opts)
        metrics_opts = build_metrics_options(cluster_opts)
        transaction_cfg = cluster_opts.pop('transaction_config', TransactionConfig())
        cluster_opts['user_agent_extra'] = USER_AGENT_EXTRA
        return cls(connection_str,
                   auth,
                   cluster_opts,
                   default_transcoder,
                   default_serializer,
                   metrics_opts,
                   orphan_opts,
                   streaming_timeouts,
                   timeout_opts,
                   tracing_opts,
                   transaction_cfg)
