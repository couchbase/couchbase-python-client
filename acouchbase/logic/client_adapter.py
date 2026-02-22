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

from asyncio import Future
from functools import partial
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Optional)

from acouchbase import get_event_loop
from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap,
                                  InternalSDKException)
from couchbase.logic.binding_map import BindingMap
from couchbase.logic.bucket_types import CloseBucketRequest, OpenBucketRequest
from couchbase.logic.cluster_types import CloseConnectionRequest
from couchbase.logic.pycbc_core import pycbc_connection
from couchbase.logic.pycbc_core import pycbc_exception as PycbcCoreException

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase.logic.bucket_types import BucketRequest
    from couchbase.logic.cluster_types import ClusterRequest, CreateConnectionRequest
    from couchbase.logic.collection_types import CollectionRequest
    from couchbase.management.logic.mgmt_req import MgmtRequest


class AsyncClientAdapter:

    def __init__(self,
                 loop: AbstractEventLoop,
                 connect_req: CreateConnectionRequest,
                 loop_validator: Optional[Callable[[Optional[AbstractEventLoop]], AbstractEventLoop]] = None
                 ) -> None:
        num_io_threads = connect_req.options.get('num_io_threads', None)
        self._connection = pycbc_connection(num_io_threads) if num_io_threads is not None else pycbc_connection()
        if loop_validator:
            self._loop = loop_validator(loop)
        else:
            self._loop = self._get_loop(loop)
        self._connect_req = connect_req
        self._binding_map = BindingMap(self._connection)
        self._close_ft: Optional[Future[None]] = None
        self._connect_ft: Optional[Future[None]] = None
        self._closed = False
        self._create_connection()

    @property
    def connected(self) -> bool:
        return self._connection is not None and self._connection.connected

    @property
    def connection(self) -> pycbc_connection:
        return self._connection

    @property
    def connect_ft(self) -> Optional[Future[None]]:
        return self._connect_ft

    @property
    def loop(self) -> AbstractEventLoop:
        return self._loop

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise RuntimeError(
                'Cannot perform operations on a closed cluster. Create a new cluster instance to reconnect.')

    def _ensure_connected(self) -> None:
        if not self.connected:
            raise RuntimeError('Cannot perform operations without first establishing a connection.')

    async def close_connection(self) -> None:
        if self._closed:
            return  # Already closed, idempotent behavior

        self._close_ft = self._execute_close_connection_request()
        await self._close_ft
        self._closed = True

    def execute_bucket_request(self, req: BucketRequest) -> Future[Any]:
        self._ensure_not_closed()
        self._ensure_connected()

        ft = self._loop.create_future()

        def _callback(result) -> None:
            self._loop.call_soon_threadsafe(ft.set_result, result)

        def _errback(exc) -> None:
            excptn = ErrorMapper.build_exception(exc)
            self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = req.req_to_dict(callback=_callback, errback=_errback)
        self._execute_req(ft, req.op_name, req_dict)
        return ft

    def execute_close_bucket_request(self, bucket_name: str) -> Future[None]:
        req = CloseBucketRequest(bucket_name)
        return self.execute_bucket_request(req)

    def execute_cluster_request(self, req: ClusterRequest) -> Future[Any]:
        self._ensure_not_closed()

        ft = self._loop.create_future()

        def _callback(result) -> None:
            self._loop.call_soon_threadsafe(ft.set_result, result)

        def _errback(exc) -> None:
            excptn = ErrorMapper.build_exception(exc)
            self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = req.req_to_dict(callback=_callback, errback=_errback)
        if not self.connected:
            chained_ft = self._execute_connect_request() if self._connect_ft is None else self._connect_ft
            chained_ft.add_done_callback(partial(self._execute_chained_req, ft, req.op_name, req_dict))
        else:
            self._execute_req(ft, req.op_name, req_dict)
        return ft

    def execute_cluster_request_sync(self, req: ClusterRequest) -> Any:
        self._ensure_not_closed()
        req_dict = req.req_to_dict()
        ret = self._execute_req_sync(req.op_name, req_dict)
        if isinstance(ret, PycbcCoreException):
            raise ErrorMapper.build_exception(ret)
        return ret

    def execute_collection_request(self, req: CollectionRequest) -> Future[Any]:
        self._ensure_not_closed()
        self._ensure_connected()

        ft = self._loop.create_future()

        def _callback(result) -> None:
            self._loop.call_soon_threadsafe(ft.set_result, result)

        def _errback(exc) -> None:
            excptn = ErrorMapper.build_exception(exc)
            self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = req.req_to_dict(callback=_callback, errback=_errback)
        self._execute_req(ft, req.op_name, req_dict)
        return ft

    def execute_connect_bucket_request(self, bucket_name: str) -> Future[None]:
        self._ensure_not_closed()

        req = OpenBucketRequest(bucket_name)
        ft = self._loop.create_future()

        def _callback(_) -> None:
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_result, None)

        def _errback(ret: Any) -> None:
            excptn = ErrorMapper.build_exception(ret)
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = req.req_to_dict(callback=_callback, errback=_errback)
        if not self.connected:
            chained_ft = self._execute_connect_request() if self._connect_ft is None else self._connect_ft
            chained_ft.add_done_callback(partial(self._execute_chained_req, ft, req.op_name, req_dict))
        else:
            self._execute_req(ft, req.op_name, req_dict)
        return ft

    def execute_mgmt_request(self, req: MgmtRequest) -> Future[Any]:
        self._ensure_not_closed()

        ft = self._loop.create_future()

        def _callback(ret: Any) -> None:
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_result, ret)

        def _errback(ret: Any) -> None:
            excptn = ErrorMapper.build_exception(ret, mapping=req.error_map)
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = req.req_to_dict(callback=_callback, errback=_errback)
        if not self.connected:
            chained_ft = self._execute_connect_request() if self._connect_ft is None else self._connect_ft
            chained_ft.add_done_callback(partial(self._execute_chained_req, ft, req.op_name, req_dict))
        else:
            self._execute_req(ft, req.op_name, req_dict)
        return ft

    async def wait_until_connected(self) -> None:
        self._ensure_not_closed()
        if self.connected:
            return
        if self._connect_ft is None:
            self._create_connection()
        await self._connect_ft

    def _create_connection(self) -> None:
        self._ensure_not_closed()
        if self._close_ft is not None and self._close_ft.done() is not True:
            raise RuntimeError('Cannot attempt to connect when close attempt is pending.')

        if self._close_ft is not None:
            self._close_ft = None

        self._connect_ft = self._execute_connect_request()

    def _execute_chained_req(self,
                             ft: Future[Any],
                             op_name: str,
                             req_dict: Dict[str, Any],
                             chained_future: Future[Any]) -> None:
        if chained_future.cancelled():
            ft.cancel()
            return

        exc = chained_future.exception()
        if exc is not None:
            ft.set_exception(exc)
            return

        self._execute_req(ft, op_name, req_dict)

    def _execute_close_connection_request(self) -> Future[None]:
        req = CloseConnectionRequest()
        ft = self._loop.create_future()

        def _callback(_) -> None:
            if not ft.done():
                self._reset_connection()
                self._loop.call_soon_threadsafe(ft.set_result, None)

        def _errback(ret: Any) -> None:
            excptn = ErrorMapper.build_exception(ret)
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = req.req_to_dict(callback=_callback, errback=_errback)
        if not self.connected:
            # If we're closed, don't try to reconnect just to close again
            if self._closed:
                ft.set_result(None)
                return ft

            chained_ft = self._execute_connect_request() if self._connect_ft is None else self._connect_ft
            chained_ft.add_done_callback(partial(self._execute_chained_req, ft, req.op_name, req_dict))
        else:
            self._execute_req(ft, req.op_name, req_dict)
        return ft

    def _execute_connect_request(self) -> Future[None]:
        ft = self._loop.create_future()

        def _callback(_) -> None:
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_result, None)

        def _errback(ret: Any) -> None:
            excptn = ErrorMapper.build_exception(ret)
            if not ft.done():
                self._loop.call_soon_threadsafe(ft.set_exception, excptn)

        req_dict = self._connect_req.req_to_dict(callback=_callback, errback=_errback)
        self._execute_req(ft, self._connect_req.op_name, req_dict)
        return ft

    def _execute_req(self, ft: Future[Any], op_name: str, req_dict: Dict[str, Any]) -> None:
        try:
            self._binding_map.op_map[op_name](**req_dict)
        except KeyError as e:
            msg = f'KeyError, most likely from op not found in binding_map.  Details: {e}'
            ft.set_exception(InternalSDKException(message=msg))
        except CouchbaseException as e:
            ft.set_exception(e)
        except Exception as e:
            if isinstance(e, (TypeError, ValueError)):
                ft.set_exception(e)
            else:
                exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
                excptn = exc_cls(str(e))
                ft.set_exception(excptn)

    def _execute_req_sync(self, op_name: str, req_dict: Dict[str, Any]) -> Any:
        try:
            return self._binding_map.op_map[op_name](**req_dict)
        except KeyError as e:
            msg = f'KeyError, most likely from op not found in binding_map.  Details: {e}'
            raise InternalSDKException(message=msg) from None
        except CouchbaseException:
            raise
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(message=str(ex))
            raise excptn from None

    def _get_loop(self, loop: Optional[AbstractEventLoop] = None) -> AbstractEventLoop:
        if not loop:
            loop = get_event_loop()

        if not loop.is_running():
            raise RuntimeError('Event loop is not running.')

        return loop

    def _reset_connection(self):
        self._connect_ft = None
        self._connection = None
