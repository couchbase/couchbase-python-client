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

"""One parser for a cluster's connection string and `ClusterOptions`, whatever the protocol.

Parsing, the authenticator, the tracer and meter and the per-concern blocks are the same work for
every protocol, so they are done once here.  What a protocol is handed at connect is its own
projection of `ClusterSettings`, and which keys it can act on is declared per protocol in the
option schema and reported by `check_options`.
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

from couchbase.common._observability import (LegacyTracerProtocol,
                                             MeterProtocol,
                                             NoOpMeter,
                                             NoOpTracer,
                                             ObservabilityInstruments,
                                             PercentileHistogramFactory,
                                             RequestTracerProtocol,
                                             WrappedTracer)
from couchbase.common._option_args import accepted_args
from couchbase.common._option_diagnostics import validated_mode
from couchbase.common._option_spec import BLOCKS
from couchbase.common._supportability import Supportability, _caller_stacklevel
from couchbase.common._transforms import (csv_or_list,
                                          timedelta_as_milliseconds,
                                          validate_bool)
from couchbase.common.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode, TLSVerifyMode
from couchbase.common.options import (ClusterMetricsOptions,
                                      ClusterOptions,
                                      ClusterOrphanReportingOptions,
                                      ClusterTimeoutOptions,
                                      ClusterTracingOptions,
                                      TransactionConfig)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.serializer import DefaultJsonSerializer, Serializer
from couchbase.common.transcoder import JSONTranscoder, Transcoder
from couchbase.common.warnings import CouchbaseDeprecationWarning, CouchbaseOptionWarning

LEGACY_CONNSTR_QUERY_ARGS = {
    'ssl': {'tls_verify': TLSVerifyMode.to_str},
    'certpath': {'cert_path': lambda x: x},
    'cert_path': {'cert_path': lambda x: x},
    'truststorepath': {'trust_store_path': lambda x: x},
    'trust_store_path': {'trust_store_path': lambda x: x},
    'sasl_mech_force': {'sasl_mech_force': lambda x: x.split(',') if isinstance(x, str) else x}
}

# The ClusterOptions keys that take a duration, which a connection string can only spell as text.
CONNSTR_DURATION_KEYS = frozenset(key for key, spec in BLOCKS['ClusterOptions'].keys.items() if spec.duration)
_DURATION_PART = r'(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|ms|s|m|h)'
_DURATION_MS = {'ns': 1e-6, 'us': 1e-3, 'µs': 1e-3, 'ms': 1, 's': 1e3, 'm': 6e4, 'h': 3.6e6}


def parse_connection_string_duration(key: str, value: object) -> timedelta:
    """A duration from the connection string: a whole number of milliseconds, or one or more
    number-and-unit parts as in ``2500ms``, ``2.5s`` or ``1m30s``.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the value is neither, or is
            longer than a `timedelta` can hold.
    """
    milliseconds: float
    if isinstance(value, int) and not isinstance(value, bool):
        milliseconds = value
    elif isinstance(value, str) and re.fullmatch(f'(?:{_DURATION_PART})+', value):
        milliseconds = sum(float(number) * _DURATION_MS[unit] for number, unit in re.findall(_DURATION_PART, value))
    else:
        raise InvalidArgumentException(message=(f'{key}={value} in the connection string is not a duration. '
                                                'Expected milliseconds, or a duration such as 2500ms or 2.5s.'))
    try:
        return timedelta(milliseconds=milliseconds)
    except OverflowError:
        raise InvalidArgumentException(message=(f'{key}={value} in the connection string is longer than '
                                                'a duration can be.')) from None


def parse_connection_string(connection_str: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Parse the provided connection string

    The provided connection string will be parsed to split the connection string
    and the query options.  Query options will be split into legacy options
    and 'current' options.

    Args:
        connection_str (str): The connection string for the cluster.

    Returns:
        tuple[str, dict[str, Any], dict[str, Any]]: The parsed connection string,
            current options and legacy options.
    """
    # handle possible lack of URL scheme
    if '//' not in connection_str:
        warning_msg = 'Connection string has deprecated format. Start connection string with: couchbase://'
        warnings.warn(warning_msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())
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


def parse_query_string_options(query_str: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Parse the query string options

    Query options will be split into legacy options and 'current' options. The values for the
    'current' options are cast to integers or booleans where applicable

    Args:
        query_str (str): The query string.

    Returns:
        tuple[dict[str, Any], dict[str, Any]]: The parsed current options and legacy options.
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


def parse_query_string_value(value: list[str]) -> list[str] | str | bool | int:
    """Parse a query string value

    The provided value is a list of at least one element. Returns either a list of strings or a single element
    which might be cast to an integer or a boolean if that's appropriate.

    Args:
        value (list[str]): The query string value.

    Returns:
        list[str] | str | bool | int: The parsed current options and legacy options.
    """

    if len(value) > 1:
        return value
    v = value[0]
    if v.isnumeric():
        return int(v)
    elif v.lower() in ['true', 'false']:
        return v.lower() == 'true'
    return v


def parse_legacy_query_options(**query_opts: object) -> dict[str, Any]:
    """Parse legacy query string options

    Returns:
        dict[str, Any]: Representation of parsed query string parameters.
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


def build_authenticator(cluster_opts: dict[str, Any]) -> dict[str, Any]:  # noqa: C901
    authenticator = cluster_opts.pop('authenticator', None)
    if not authenticator:
        raise InvalidArgumentException(message='Authenticator is mandatory.')

    # the cert_path _might_ be a part of the query options
    cert_path = cluster_opts.pop('cert_path', None)
    # Taken out whatever the authenticator, since they reach the binding only through the credentials.
    sasl_opts = {key: cluster_opts.pop(key)
                 for key in ('sasl_mech_force', 'allowed_sasl_mechanisms') if key in cluster_opts}

    # lets only pass in the authenticator, no kwargs
    auth_kwargs = {k: v for k, v in cluster_opts.items() if k in authenticator.valid_keys()}
    if isinstance(authenticator, CertificateAuthenticator) and 'trust_store_path' in auth_kwargs:
        # A cluster option, which a CertificateAuthenticator may also set, but not both.
        if 'trust_store_path' in authenticator.as_dict():
            raise InvalidArgumentException(message=('trust_store_path is set on both the CertificateAuthenticator '
                                                    'and the cluster options. Set it in one place.'))
        auth_kwargs.pop('trust_store_path')
    elif (
        isinstance(authenticator, PasswordAuthenticator) and
        sasl_opts and
        authenticator._allowed_sasl_mechanisms is None
    ):
        # 3.x SDK allowed sasl_mech_force in the query string, 4.x is going to allow allowed_sasl_mechanisms
        # in the query string, if the PasswordAuthenticator's static ldap_compatible() method has been used
        # it takes precendence.
        # Set the PasswordAuthenticator's allowed_sasl_mechanisms, if not already set, to the value(s) found in the
        # connection string.  Note that sasl_mech_force takes precedence over allowed_sasl_mechanisms.
        if len(sasl_opts) > 1:
            warnings.warn('sasl_mech_force and allowed_sasl_mechanisms are both set; sasl_mech_force applies.',
                          CouchbaseOptionWarning, stacklevel=_caller_stacklevel())
        mechanisms = sasl_opts.get('sasl_mech_force', sasl_opts.get('allowed_sasl_mechanisms'))
        authenticator._allowed_sasl_mechanisms = csv_or_list(mechanisms)

    if len(auth_kwargs.keys()) > 0:
        raise InvalidArgumentException(message='Authentication kwargs not allowed. Only provide the Authenticator.')

    auth = authenticator.as_dict()
    # add the cert_path to the authenticator if found
    if cert_path and 'cert_path' not in auth:
        auth['cert_path'] = cert_path

    return auth


def build_metrics_options(cluster_opts: dict[str, Any],
                          histogram_factory: PercentileHistogramFactory) -> tuple[dict[str, Any], MeterProtocol]:
    metrics_opts = {}
    for key in ClusterMetricsOptions.get_allowed_option_keys():
        if key in cluster_opts:
            metrics_opts[key] = cluster_opts.pop(key)

    logging_meter_emit_interval = cluster_opts.pop('logging_meter_emit_interval', None)
    if logging_meter_emit_interval and 'metrics_emit_interval' not in metrics_opts:
        metrics_opts['metrics_emit_interval'] = logging_meter_emit_interval
    cluster_enable_metrics = cluster_opts.pop('enable_metrics', None)

    # NOTE: we disable metrics in the C++ core as metrics logic resides (for now) on the wrapper side
    if cluster_enable_metrics is not None and 'metrics_enable_metrics' not in metrics_opts:
        metrics_opts['metrics_enable_metrics'] = cluster_enable_metrics

    meter = cluster_opts.pop('meter', None)
    # We want to disble metrics if we don't have a meter AND metrics is explicitly disabled
    if meter is None and validate_bool(metrics_opts.get('metrics_enable_metrics', True)) is False:
        enable_metrics = False
    else:
        enable_metrics = True

    metrics_opts['metrics_enable_metrics'] = enable_metrics
    if meter is None and enable_metrics is True:
        from couchbase.common._observability import LoggingMeter
        emit_interval = metrics_opts.get('metrics_emit_interval')
        emit_interval_ms = None if emit_interval is None else timedelta_as_milliseconds(emit_interval)
        meter = LoggingMeter(histogram_factory, emit_interval_ms=emit_interval_ms)

    if meter is None:
        from couchbase.common._observability import NoOpMeter
        meter = NoOpMeter()

    if meter and not isinstance(meter, MeterProtocol):
        raise InvalidArgumentException(message='Provided meter does not follow the MeterProtocol.')

    return metrics_opts, meter


def build_timeout_options(cluster_opts: dict[str, Any]) -> dict[str, Any]:
    timeout_opts = {}
    for key in ClusterTimeoutOptions.get_allowed_option_keys():
        if key in cluster_opts:
            timeout_opts[key] = cluster_opts.pop(key)
    return timeout_opts


# The tracing block's deprecated keys for the orphan reporter's queue, with the orphan block's keys
# that replaced them and win wherever both are written.
DEPRECATED_ORPHAN_KEYS = {'tracing_orphaned_queue_size': 'orphan_sample_size',
                          'tracing_orphaned_queue_flush_interval': 'orphan_emit_interval'}


def build_tracing_and_orphan_options(cluster_opts: dict[str, Any]  # noqa: C901
                                     ) -> tuple[dict[str, Any], dict[str, Any], WrappedTracer]:
    orphan_opts = {}
    for key in ClusterOrphanReportingOptions.get_allowed_option_keys():
        if key in cluster_opts:
            orphan_opts[key] = cluster_opts.pop(key)

    tracing_opts = {}
    for key in ClusterTracingOptions.get_allowed_option_keys():
        if key in cluster_opts:
            tracing_opts[key] = cluster_opts.pop(key)

    for old, new in DEPRECATED_ORPHAN_KEYS.items():
        if old in tracing_opts:
            orphan_opts.setdefault(new, tracing_opts.pop(old))

    cluster_enable_tracing = cluster_opts.pop('enable_tracing', None)
    if cluster_enable_tracing is not None and 'tracing_enable_tracing' not in tracing_opts:
        tracing_opts['tracing_enable_tracing'] = cluster_enable_tracing

    cluster_enable_orphan_reporting = cluster_opts.pop('enable_orphan_reporting', None)
    if cluster_enable_orphan_reporting is not None and 'orphan_enable_orphan_reporting' not in orphan_opts:
        orphan_opts['orphan_enable_orphan_reporting'] = cluster_enable_orphan_reporting

    tracer = cluster_opts.pop('tracer', None)
    # We want to disble tracing if we don't have a tracer AND tracing is explicitly disabled
    if tracer is None and validate_bool(tracing_opts.get('tracing_enable_tracing', True)) is False:
        enable_tracing = False
    else:
        enable_tracing = True

    tracing_opts['tracing_enable_tracing'] = enable_tracing
    if tracer is None and enable_tracing is True:
        from couchbase.common._observability import ThresholdLoggingTracer
        tracer = ThresholdLoggingTracer(config=(tracing_opts if tracing_opts else None))

    is_legacy = False
    if tracer is None:
        from couchbase.common._observability import NoOpTracer
        tracer = NoOpTracer()
    else:
        from couchbase.common._tracing import CouchbaseTracer
        if isinstance(tracer, CouchbaseTracer):
            # Once per cluster rather than per span: a legacy tracer makes a span on every
            # traced operation, and the spans it makes are the deprecated class's too.
            Supportability.type_deprecated('CouchbaseTracer', 'couchbase.observability.tracing.RequestTracer',
                                           'CouchbaseSpan is deprecated with it; use '
                                           'couchbase.observability.tracing.RequestSpan instead.')
            is_legacy = True

    if not isinstance(tracer, (RequestTracerProtocol, LegacyTracerProtocol)):
        msg = 'The tracer must implement the RequestTracerProtocol or LegacyTracerProtocol.'
        raise InvalidArgumentException(message=msg)

    return tracing_opts, orphan_opts, WrappedTracer(tracer, is_legacy)


@dataclass
class ClusterSettings:
    """A cluster's parsed settings, split by concern, each key under the spelling `ClusterOptions`
    stores it and each value as the caller gave it.

    `cluster_options` is the keys no block claimed.  The timeout, tracing, orphan and metrics
    blocks are their own fields, so a protocol decides how each reaches what it connects with,
    and maps each to what it sends.
    """

    connstr: str
    auth: dict[str, Any]
    cluster_options: dict[str, Any]
    default_transcoder: Transcoder
    default_serializer: Serializer
    option_validation: OptionValidationMode | None
    metrics_options: dict[str, Any]
    orphan_options: dict[str, Any]
    timeout_options: dict[str, Any]
    tracing_options: dict[str, Any]
    transaction_config: TransactionConfig
    observability_instruments: ObservabilityInstruments

    def set_observability_cluster_labels_callable(self, callable: Callable[[], Mapping[str, str]]) -> None:
        self.observability_instruments.get_cluster_labels_fn = callable

    @classmethod
    def build_cluster_settings(cls, connstr: str, *options: ClusterOptions, **kwargs: Any) -> ClusterSettings:
        histogram_factory = kwargs.pop('_histogram_factory')
        # parse query string prior to parsing ClusterOptions
        connection_str, query_opts, legacy_opts = parse_connection_string(connstr)

        kwargs.update({key: parse_connection_string_duration(key, value) if key in CONNSTR_DURATION_KEYS else value
                       for key, value in query_opts.items()})
        cluster_opts = accepted_args(ClusterOptions, kwargs, *options,
                                     protocol=CouchbaseProtocol.from_connstr(connstr))
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

        option_validation = validated_mode(cluster_opts.pop('option_validation', None))

        timeout_opts = build_timeout_options(cluster_opts)
        tracing_opts, orphan_opts, tracer = build_tracing_and_orphan_options(cluster_opts)
        metrics_opts, meter = build_metrics_options(cluster_opts, histogram_factory)
        # BRIDGE: the legacy TransactionConfig, whose core config the old transactions logic reads.
        # Removed by PYCBC-2007.
        from couchbase.logic._legacy_options import TransactionConfig as LegacyTransactionConfig
        transaction_cfg = cluster_opts.pop('transaction_config', LegacyTransactionConfig())
        return cls(connection_str,
                   auth,
                   cluster_opts,
                   default_transcoder,
                   default_serializer,
                   option_validation,
                   metrics_opts,
                   orphan_opts,
                   timeout_opts,
                   tracing_opts,
                   transaction_cfg,
                   ObservabilityInstruments(
                       tracer,
                       meter,
                       # allow ops can skip handler construction in no-op case
                       is_noop=isinstance(tracer.tracer, NoOpTracer) and isinstance(meter, NoOpMeter)))
