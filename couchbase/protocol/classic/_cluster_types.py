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

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Optional,
                    TypedDict)

from couchbase import USER_AGENT_EXTRA
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common._transforms import timedelta_as_milliseconds
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.logic.observability import ObservableRequestHandler as _OldObservableRequestHandler
from couchbase.logic.operation_types import StreamingOperationType
from couchbase.protocol.classic._bindings import ClusterMgmtBinding, ConnectionBinding
from couchbase.protocol.classic._wire import mapped

if TYPE_CHECKING:
    from couchbase.common._cluster_settings import ClusterSettings
    from couchbase.common.diagnostics import ClusterState
    from couchbase.protocol.classic._analytics import AnalyticsQuery
    from couchbase.protocol.classic._n1ql import N1QLQuery
    from couchbase.search import SearchQueryBuilder


class ClusterOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    Diagnostics = 'DiagnosticsOptions'
    Ping = 'PingOptions'
    WaitUntilReady = 'WaitUntilReadyOptions'


_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class ClusterRequest(ABC):

    def req_to_dict(self,
                    callback: Callable[..., None] | None = None,
                    errback: Callable[..., None] | None = None) -> dict[str, Any]:
        cls = type(self)
        cached_fields = _FIELDS_CACHE.get(cls)
        if cached_fields is None:
            cached_fields = list(fields(cls))
            _FIELDS_CACHE[cls] = cached_fields

        op_kwargs = {
            f.name: getattr(self, f.name)
            for f in cached_fields
            if getattr(self, f.name) is not None
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        return op_kwargs

    @property
    @abstractmethod
    def binding_method(self) -> str:
        raise NotImplementedError


@dataclass
class AnalyticsQueryRequest:
    analytics_query: AnalyticsQuery
    obs_handler: ObservableRequestHandler


@dataclass
class CloseConnectionRequest(ClusterRequest):
    @property
    def binding_method(self) -> str:
        return ConnectionBinding.Close.value


@dataclass
class ClusterInfoRequest(ClusterRequest):
    @property
    def binding_method(self) -> str:
        return ClusterMgmtBinding.ClusterDescribe.value


class StreamingTimeouts(TypedDict):
    """The cluster's timeout for each streaming service, in the milliseconds the binding reads."""
    analytics_timeout: int | None
    query_timeout: int | None
    search_timeout: int | None
    view_timeout: int | None


def timeout_ms(settings: ClusterSettings, key: str) -> int | None:
    """One of the cluster's timeouts in milliseconds, or `None` where the cluster sets none, which
    the binding answers with the core's default for the request."""
    timeout = settings.timeout_options.get(key)
    return None if timeout is None else timedelta_as_milliseconds(timeout)


def cluster_streaming_timeouts(settings: ClusterSettings) -> StreamingTimeouts:
    return {'analytics_timeout': timeout_ms(settings, 'analytics_timeout'),
            'query_timeout': timeout_ms(settings, 'query_timeout'),
            'search_timeout': timeout_ms(settings, 'search_timeout'),
            'view_timeout': timeout_ms(settings, 'views_timeout')}


def connect_options(settings: ClusterSettings) -> dict[str, Any]:
    """The options dict the binding reads at connect, nested the way it reads them.

    Every block is mapped through `ClusterOptions`' entry, since the settings hold each key under
    the spelling `ClusterOptions` stores it.  The tracing and metrics blocks stay on the Python
    side, which owns both: the binding is told only whether to forward spans, and to which tracer.
    """
    options = mapped(settings.cluster_options, 'ClusterOptions')
    if settings.timeout_options:
        options['timeout_options'] = mapped(settings.timeout_options, 'ClusterOptions')
    if settings.orphan_options:
        options['orphan_reporting_options'] = mapped(settings.orphan_options, 'ClusterOptions')
    if settings.tracing_options.get('tracing_enable_tracing'):
        options['setup_sdk_tracing'] = True
    tracer = settings.observability_instruments.tracer
    if tracer.is_legacy:
        options['legacy_tracer'] = tracer.tracer
    # The core appends this after its own fields, each separated by ';'.
    options['user_agent_extra'] = ';'.join(part for part in (USER_AGENT_EXTRA, options.get('user_agent_extra'))
                                           if part)
    return options


@dataclass
class CreateConnectionRequest:
    connstr: str
    auth: dict[str, Any]
    options: dict[str, Any]

    @classmethod
    def from_settings(cls, settings: ClusterSettings) -> CreateConnectionRequest:
        return cls(settings.connstr, settings.auth, connect_options(settings))

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.Connect.value

    def req_to_dict(self,
                    callback: Callable[..., None] | None = None,
                    errback: Callable[..., None] | None = None) -> dict[str, Any]:
        op_kwargs: dict[str, Any] = {
            'connstr': self.connstr,
            'auth': self.auth,
            'options': self.options
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        return op_kwargs


@dataclass
class DiagnosticsRequest(ClusterRequest):
    report_id: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.Diagnostics.value


@dataclass
class GetConnectionInfoRequest(ClusterRequest):
    @property
    def binding_method(self) -> str:
        return ConnectionBinding.GetConnectionInfo.value


@dataclass
class PingRequest(ClusterRequest):
    services: set[str]
    report_id: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.Ping.value


@dataclass
class QueryRequest:
    n1ql_query: N1QLQuery
    obs_handler: ObservableRequestHandler


@dataclass
class SearchQueryRequest:
    query_builder: SearchQueryBuilder
    obs_handler: _OldObservableRequestHandler
    num_workers: Optional[int] = None

    @property
    def binding_method(self) -> str:
        return StreamingOperationType.SearchQuery


@dataclass
class UpdateCredentialsRequest(ClusterRequest):
    auth: dict[str, Any]

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.UpdateCredentials.value


@dataclass
class WaitUntilReadyRequest:
    timeout: timedelta
    desired_state: ClusterState
    service_types: set[str]
