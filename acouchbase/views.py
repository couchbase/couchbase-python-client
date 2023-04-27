#  Copyright 2016-2022. Couchbase, Inc.
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

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  AlreadyQueriedException,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap)
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.views import ViewQuery  # noqa: F401
from couchbase.logic.views import ViewRequestLogic, ViewRow


class AsyncViewRequest(ViewRequestLogic):
    def __init__(self,
                 connection,
                 loop,
                 encoded_query,
                 **kwargs
                 ):
        num_workers = kwargs.pop('num_workers', 2)
        super().__init__(connection, encoded_query, **kwargs)
        self._loop = loop
        self._tp_executor = ThreadPoolExecutor(num_workers)

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @classmethod
    def generate_view_request(cls, connection, loop, encoded_query, **kwargs):
        return cls(connection, loop, encoded_query, **kwargs)

    def _get_metadata(self):
        try:
            views_response = next(self._streaming_result)
            self._set_metadata(views_response)
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn

    def execute(self) -> Awaitable[None]:
        async def _execute():
            return [r async for r in self]

        return asyncio.create_task(_execute())

    def __aiter__(self):
        if self.done_streaming:
            raise AlreadyQueriedException()

        if not self.started_streaming:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        row = next(self._streaming_result)
        if isinstance(row, CouchbaseBaseException):
            raise ErrorMapper.build_exception(row)
        # should only be None one query request is complete and _no_ errors found
        if row is None:
            raise StopAsyncIteration

        # TODO:  until streaming, a dict is returned, no deserializing...
        # deserialized_row = self.serializer.deserialize(row)
        deserialized_row = row
        if issubclass(self.row_factory, ViewRow):
            return self.row_factory(**deserialized_row)
        else:
            return deserialized_row

    async def __anext__(self):
        try:
            return await self._loop.run_in_executor(self._tp_executor, self._get_next_row)
        except asyncio.QueueEmpty:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls('Unexpected QueueEmpty exception caught when doing Search query.')
            raise excptn
        except StopAsyncIteration:
            self._done_streaming = True
            self._get_metadata()
            raise
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn
