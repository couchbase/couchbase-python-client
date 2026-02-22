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

from dataclasses import dataclass, fields
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Optional,
                    Set)

from couchbase.logic.operation_types import BucketOperationType, StreamingOperationType

if TYPE_CHECKING:
    from couchbase.views import ViewQuery


@dataclass
class BucketRequest:

    def req_to_dict(self,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if getattr(self, field.name) is not None
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        return op_kwargs


@dataclass
class CloseBucketRequest(BucketRequest):
    bucket_name: str

    @property
    def op_name(self) -> str:
        return BucketOperationType.CloseBucket.value


@dataclass
class OpenBucketRequest(BucketRequest):
    bucket_name: str

    @property
    def op_name(self) -> str:
        return BucketOperationType.OpenBucket.value


@dataclass
class PingRequest(BucketRequest):
    services: Set[str]
    bucket_name: Optional[str] = None
    report_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketOperationType.Ping.value


@dataclass
class ViewQueryRequest:
    view_query: ViewQuery
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.ViewQuery.value
