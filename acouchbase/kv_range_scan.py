# Copyright 2021, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  AlreadyQueriedException,
                                  CouchbaseException,
                                  ExceptionMap,
                                  RangeScanCompletedException)
from couchbase.logic.kv_range_scan import RangeScanRequestLogic


class AsyncRangeScanRequest(RangeScanRequestLogic):
    def __init__(self,
                 loop,
                 **kwargs,  # type: Dict[str, Any]
                 ):
        num_workers = kwargs.pop('num_workers', 2)
        super().__init__(**kwargs)
        self._loop = loop
        self._result_ftr = None
        self._tp_executor = ThreadPoolExecutor(num_workers)

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    def __aiter__(self):
        if self.done_streaming:
            raise AlreadyQueriedException()

        if not self.started_streaming:
            self._submit_scan()

        return self

    async def __anext__(self):
        try:
            return await self._loop.run_in_executor(self._tp_executor, self._get_next_row)
        # We can stop iterator when we receive RangeScanCompletedException
        except asyncio.QueueEmpty:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls('Unexpected QueueEmpty exception caught when doing N1QL query.')
            raise excptn
        except RangeScanCompletedException:
            self._done_streaming = True
            raise StopAsyncIteration
        except StopAsyncIteration:
            self._done_streaming = True
            raise
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn
