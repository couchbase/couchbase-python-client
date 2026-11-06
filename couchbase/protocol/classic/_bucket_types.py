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

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import ConnectionBinding

if TYPE_CHECKING:
    from couchbase.protocol.classic._views import ViewQuery


class BucketOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    Ping = 'PingOptions'
    View = 'ViewOptions'


_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class BucketRequest(ABC):

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
class CloseBucketRequest(BucketRequest):
    bucket_name: str

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.CloseBucket.value


@dataclass
class OpenBucketRequest(BucketRequest):
    bucket_name: str

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.OpenBucket.value


@dataclass
class PingRequest(BucketRequest):
    services: set[str]
    bucket_name: str | None = None
    report_id: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ConnectionBinding.Ping.value


@dataclass
class ViewQueryRequest:
    view_query: ViewQuery
    obs_handler: ObservableRequestHandler
