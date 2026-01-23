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
from enum import IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    List,
                    Optional)

from couchbase.logic.operation_types import BucketOperationType

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import PyCapsuleType
    from couchbase.views import ViewQuery


class OpenOrCloseBucket(IntEnum):
    CLOSE = 0
    OPEN = 1


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['timeout']


@dataclass
class BucketRequest:
    # TODO: maybe timeout isn't optional, but defaults to default timeout?
    #       otherwise that makes inheritance tricky w/ child classes having required params

    def req_to_dict(self,
                    conn: PyCapsuleType,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = {
            'conn': conn,
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        if hasattr(self, 'timeout') and getattr(self, 'timeout') is not None:
            op_kwargs['timeout'] = getattr(self, 'timeout')

        op_kwargs.update(**{
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        })

        return op_kwargs


@dataclass
class CloseBucketRequest(BucketRequest):
    bucket_name: str
    open_bucket: int

    @property
    def op_name(self) -> str:
        return BucketOperationType.CloseBucket.value


@dataclass
class OpenBucketRequest(BucketRequest):
    bucket_name: str
    open_bucket: int

    @property
    def op_name(self) -> str:
        return BucketOperationType.OpenBucket.value


@dataclass
class PingRequest(BucketRequest):
    op_type: int
    service_types: List[str]
    report_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketOperationType.Ping.value


@dataclass
class ViewQueryRequest:
    view_query: ViewQuery
    num_workers: Optional[int] = None
