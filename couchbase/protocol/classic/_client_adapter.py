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

from collections.abc import Callable, Mapping
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._registry import CLOSED_CLUSTER_MSG
from couchbase.common.exceptions import CouchbaseException, InternalSDKException
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._binding_map import BindingMap
from couchbase.protocol.classic._bucket_types import CloseBucketRequest, OpenBucketRequest
from couchbase.protocol.classic._cluster_types import CloseConnectionRequest
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException

if TYPE_CHECKING:
    from couchbase.common.option_enums import OptionValidationMode
    from couchbase.protocol.classic._bucket_types import BucketRequest
    from couchbase.protocol.classic._cluster_types import ClusterRequest, CreateConnectionRequest
    from couchbase.protocol.classic._management._mgmt_req import MgmtRequest
    from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest


class ClientAdapter:

    def __init__(self, connect_req: CreateConnectionRequest, **kwargs: Any) -> None:
        num_io_threads = connect_req.options.get('num_io_threads', None)
        conn = pycbc_connection(num_io_threads) if num_io_threads is not None else pycbc_connection()
        self._connection: pycbc_connection | None = conn
        self._closed = False
        self._connect_req = connect_req
        self._binding_map = BindingMap(conn)
        self._couchbase_protocol = CouchbaseProtocol.CLASSIC
        self._option_validation: OptionValidationMode | None = kwargs.pop('option_validation', None)
        # for testing we sometimes want to skip the actual C++ core connection
        if not (kwargs.get('skip_connect', None) == 'TEST_SKIP_CONNECT'):
            self._execute_connect_request()

    @property
    def binding_map(self) -> BindingMap:
        """**INTERNAL**"""
        return self._binding_map

    @property
    def option_validation(self) -> OptionValidationMode | None:
        """**INTERNAL**"""
        return self._option_validation

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._couchbase_protocol

    @property
    def connected(self) -> bool:
        """**INTERNAL**"""
        return self._connection is not None and self._connection.connected

    @property
    def connection(self) -> pycbc_connection:
        """**INTERNAL**"""
        if self._connection is None:
            raise RuntimeError(CLOSED_CLUSTER_MSG)
        return self._connection

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise RuntimeError(CLOSED_CLUSTER_MSG)

    def _ensure_connected(self) -> None:
        if not self.connected:
            raise RuntimeError('Cannot perform operations without first establishing a connection.')

    def close_bucket(self, bucket_name: str) -> None:
        """**INTERNAL**"""
        if self._closed or not self.connected:
            return  # nothing was opened, so there is nothing to tear down
        self.execute_bucket_request(CloseBucketRequest(bucket_name))

    def close_connection(self) -> None:
        """**INTERNAL**"""
        if self._closed:
            return  # Already closed, idempotent behavior

        if not self.connected:
            # Not currently connected, but mark as closed anyway
            self._closed = True
            return

        self.execute_cluster_request(CloseConnectionRequest())
        self._closed = True
        self._connection = None

    def execute_bucket_request(self, req: BucketRequest) -> Any:
        """**INTERNAL**"""
        self._ensure_not_closed()
        req_dict = req.req_to_dict()
        ret = self._execute_req(req.binding_method, req_dict)
        if isinstance(ret, PycbcCoreException):
            raise ErrorMapper.build_exception(ret)
        return ret

    def execute_collection_request(self,
                                   opcode: int,
                                   req: list[PycbcCoreKeyValueRequest] | PycbcCoreKeyValueRequest,
                                   obs_handler: ObservableRequestHandler | None = None) -> Any:
        """**INTERNAL**"""
        self._ensure_not_closed()
        try:
            kv_op: Callable[[Any], Any] = self._binding_map.kv_ops[opcode]
            ret = kv_op(req)
            # pycbc_result and pycbc_exception have a core_span member
            if obs_handler and hasattr(ret, 'core_span'):
                obs_handler.process_core_span(ret.core_span)
            if isinstance(ret, PycbcCoreException):
                raise ErrorMapper.build_exception(ret)
            return ret
        except CouchbaseException:
            raise
        except Exception as ex:
            raise InternalSDKException(message=str(ex)) from None

    def execute_cluster_request(self, req: ClusterRequest) -> Any:
        """**INTERNAL**"""
        self._ensure_not_closed()
        req_dict = req.req_to_dict()
        ret = self._execute_req(req.binding_method, req_dict)
        if isinstance(ret, PycbcCoreException):
            raise ErrorMapper.build_exception(ret)
        return ret

    def execute_mgmt_request(self,
                             req: MgmtRequest,
                             obs_handler: ObservableRequestHandler | None = None) -> Any:
        """**INTERNAL**"""
        self._ensure_not_closed()
        req_dict = req.req_to_dict(obs_handler)
        ret = self._execute_req(req.binding_method, req_dict)
        # pycbc_result and pycbc_exception have a core_span member
        if obs_handler and hasattr(ret, 'core_span'):
            obs_handler.process_core_span(ret.core_span)
        if isinstance(ret, PycbcCoreException):
            raise ErrorMapper.build_exception(ret, mapping=req.error_map)
        return ret

    def open_bucket(self, bucket_name: str) -> None:
        """**INTERNAL**"""
        self._ensure_not_closed()
        self._ensure_connected()
        self.execute_bucket_request(OpenBucketRequest(bucket_name))

    def _execute_connect_request(self) -> None:
        req_dict = self._connect_req.req_to_dict()
        ret = self._execute_req(self._connect_req.binding_method, req_dict)
        if isinstance(ret, PycbcCoreException):
            raise ErrorMapper.build_exception(ret)

    def _execute_req(self, binding_method: str, req_dict: dict[str, Any]) -> Any:
        try:
            op_map = cast(Mapping[str, Callable[..., Any]], self._binding_map.op_map)
            return op_map[binding_method](**req_dict)
        except KeyError as e:
            msg = f'KeyError, most likely from op not found in binding_map.  Details: {e}'
            raise InternalSDKException(message=msg) from None
        except CouchbaseException:
            raise
        except Exception as ex:
            raise InternalSDKException(message=str(ex)) from None
