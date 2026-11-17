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

"""What every impl under one couchbase2 cluster reads, whichever API it serves.

The settings, the timeouts an operation falls back on, how its options are checked, the metadata
each call carries, and whether the cluster is closed.  The channel is not here: a sync cluster and an
async cluster each own their own kind.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from couchbase.common._cluster_settings import ClusterSettings
from couchbase.common._observability import (LoggingMeter,
                                             ObservabilityInstruments,
                                             ObservableRequestHandler,
                                             ThresholdLoggingTracer)
from couchbase.common._operations import Op
from couchbase.common._option_diagnostics import OptionsBlock, check_options
from couchbase.common._redaction import enable as enable_log_redaction
from couchbase.common._registry import CLOSED_CLUSTER_MSG
from couchbase.common.auth import (CertificateAuthenticator,
                                   JwtAuthenticator,
                                   PasswordAuthenticator)
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.common.options import ClusterOptions
from couchbase.protocol.couchbase2._channel import (ChannelConfig,
                                                    call_metadata,
                                                    with_credentials)
from couchbase.protocol.couchbase2._histogram import PercentileHistogram
from couchbase.protocol.couchbase2._request_context import RequestContext
from couchbase.protocol.couchbase2._tracing import GrpcTracing, Metadata

logger = logging.getLogger('couchbase.couchbase2')

# RFC 77's defaults, in seconds.  The durable timeout applies to any durability level but none.
KV_TIMEOUT = 2.5
KV_DURABLE_TIMEOUT = 10.0
QUERY_TIMEOUT = 75.0
SEARCH_TIMEOUT = 75.0
MANAGEMENT_TIMEOUT = 75.0


def unsupported(name: str) -> FeatureUnavailableException:
    """The exception every operation couchbase2 does not provide raises."""
    return FeatureUnavailableException(f'{name} is not available over the couchbase2 protocol.')


def _seconds(duration: timedelta | None, default: float) -> float:
    return default if duration is None else duration.total_seconds()


def _auth_kind(auth: dict[str, Any]) -> str:
    if 'jwt_token' in auth:
        return 'JWT'
    if 'username' in auth:
        return 'password'
    return 'certificate'


class ClusterCore:

    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Any) -> None:
        # The channel connects on the first call, so there is no connection to skip.
        kwargs.pop('skip_connect', None)
        kwargs['_histogram_factory'] = PercentileHistogram
        self.settings = ClusterSettings.build_cluster_settings(connstr, *options, **kwargs)
        if self.settings.cluster_options.get('log_redaction'):
            enable_log_redaction()
        self.channel_config = ChannelConfig.from_settings(self.settings)
        self.metadata: Metadata = call_metadata(self.channel_config)
        self.tracing = GrpcTracing(self.channel_config.endpoint)
        timeouts = self.settings.timeout_options
        self.kv_timeout = _seconds(timeouts.get('kv_timeout'), KV_TIMEOUT)
        self.kv_durable_timeout = _seconds(timeouts.get('kv_durable_timeout'), KV_DURABLE_TIMEOUT)
        self.query_timeout = _seconds(timeouts.get('query_timeout'), QUERY_TIMEOUT)
        self.search_timeout = _seconds(timeouts.get('search_timeout'), SEARCH_TIMEOUT)
        self.management_timeout = _seconds(timeouts.get('management_timeout'), MANAGEMENT_TIMEOUT)
        self.closed = False

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self.settings.observability_instruments

    def ensure_open(self) -> None:
        if self.closed:
            raise RuntimeError(CLOSED_CLUSTER_MSG)

    def update_credentials(self,
                           authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
                           ) -> None:
        """Send `authenticator`'s credentials on every call made from now on.

        Each call carries its credentials, so a password or JWT takes effect at once.  A client
        certificate is presented once, when the channel connects, so it cannot be replaced.
        """
        self.ensure_open()
        auth = authenticator.as_dict()
        current, new = _auth_kind(self.settings.auth), _auth_kind(auth)
        if new != current:
            raise InvalidArgumentException(
                f'The cluster authenticates with a {current} authenticator, so it cannot be replaced '
                f'with a {new} authenticator.')
        if new == 'certificate':
            raise unsupported('Replacing a certificate authenticator')
        self.settings.auth = auth
        self.channel_config = with_credentials(self.channel_config, auth)
        self.metadata = call_metadata(self.channel_config)

    def validate_options(self, block: OptionsBlock, opts: dict[str, Any]) -> None:
        check_options(block, opts, self.settings.option_validation, protocol=block.protocol)

    def context(self,
                op: Op,
                timeout: float,
                *,
                obs_handler: ObservableRequestHandler | None,
                readonly: bool | None = None) -> RequestContext:
        """A request's context, which cancels it if the cluster closes while it waits to retry."""
        return RequestContext(op,
                              timeout,
                              readonly=readonly,
                              obs_handler=obs_handler,
                              tracing=self.tracing,
                              closed=lambda: self.closed)

    def close(self) -> None:
        """Mark the cluster closed and stop the reporters; the caller closes its channel."""
        self.closed = True
        instruments = self.observability_instruments
        try:
            tracer = instruments.tracer.tracer
            if isinstance(tracer, ThresholdLoggingTracer):
                tracer.close()
            if isinstance(instruments.meter, LoggingMeter):
                instruments.meter.close()
        except Exception:
            logger.debug('Error stopping the observability reporters while closing the cluster.', exc_info=True)


def timeout_seconds(opts: dict[str, Any], default: float) -> float:
    """The operation's `timeout` option in seconds, or the cluster's for the operation."""
    timeout = opts.get('timeout')
    if timeout is None:
        return default
    if not isinstance(timeout, timedelta):
        raise InvalidArgumentException(f'Expected timeout to be a timedelta, got {timeout!r}.')
    return timeout.total_seconds()
