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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)

from couchbase.exceptions import (CouchbaseException,
                                  ErrorMapper,
                                  InternalSDKException)
from couchbase.exceptions import exception as BaseCouchbaseException
from couchbase.logic.binding_map import BindingMap
from couchbase.logic.bucket_types import CloseBucketRequest, OpenBucketRequest
from couchbase.logic.cluster_types import CloseConnectionRequest
from couchbase.logic.top_level_types import OpenOrCloseBucket, PyCapsuleType

if TYPE_CHECKING:
    from couchbase.logic.bucket_types import BucketRequest
    from couchbase.logic.cluster_types import ClusterRequest, CreateConnectionRequest
    from couchbase.logic.collection_types import CollectionRequest


class ClientAdapter:

    def __init__(self, connect_req: CreateConnectionRequest) -> None:
        self._connection: Optional[PyCapsuleType] = None
        self._connect_req = connect_req
        self._binding_map = BindingMap()
        self._execute_connect_request()

    @property
    def connected(self) -> bool:
        return self._connection is not None

    @property
    def connection(self) -> Optional[PyCapsuleType]:
        return self._connection

    def close_bucket(self, bucket_name: str) -> None:
        if not self.connected:
            raise RuntimeError('Cannot close a bucket if a connection has not been established.')
        self.execute_bucket_request(CloseBucketRequest(bucket_name, OpenOrCloseBucket.CLOSE))

    def close_connection(self) -> None:
        if not self.connected:
            raise RuntimeError('Cannot close a connection if one has not been established.')
        self.execute_cluster_request(CloseConnectionRequest())

    def execute_bucket_request(self, req: BucketRequest) -> Any:
        req_dict = req.req_to_dict(self._connection)
        ret = self._execute_req(req.op_name, req_dict)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        return ret

    def execute_collection_request(self, req: CollectionRequest) -> Any:
        req_dict = req.req_to_dict(self._connection)
        ret = self._execute_req(req.op_name, req_dict)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        return ret

    def execute_cluster_request(self, req: ClusterRequest) -> Any:
        req_dict = req.req_to_dict(self._connection)
        ret = self._execute_req(req.op_name, req_dict)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        return ret

    def open_bucket(self, bucket_name: str) -> None:
        if not self.connected:
            raise RuntimeError('Cannot open a bucket if a connection has not been established.')
        self.execute_bucket_request(OpenBucketRequest(bucket_name, OpenOrCloseBucket.OPEN))

    def _execute_connect_request(self) -> None:
        req_dict = self._connect_req.req_to_dict()
        ret = self._execute_req(self._connect_req.op_name, req_dict)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        self._connection = ret

    def _execute_req(self, op_name: str, req_dict: Dict[str, Any]) -> Any:
        try:
            return self._binding_map.op_map[op_name](**req_dict)
        except KeyError as e:
            msg = f'KeyError, most likely from op not found in binding_map.  Details: {e}'
            raise InternalSDKException(message=msg) from None
        except CouchbaseException:
            raise
        except Exception as ex:
            raise InternalSDKException(message=str(ex)) from None
