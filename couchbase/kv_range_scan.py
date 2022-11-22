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

from typing import Any, Dict

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  AlreadyQueriedException,
                                  CouchbaseException,
                                  ExceptionMap,
                                  RangeScanCompletedException)
from couchbase.logic.kv_range_scan import PrefixScan  # noqa: F401
from couchbase.logic.kv_range_scan import RangeScan  # noqa: F401
from couchbase.logic.kv_range_scan import SamplingScan  # noqa: F401
from couchbase.logic.kv_range_scan import ScanTerm  # noqa: F401
from couchbase.logic.kv_range_scan import ScanType  # noqa: F401
from couchbase.logic.kv_range_scan import RangeScanRequestLogic


class RangeScanRequest(RangeScanRequestLogic):
    def __init__(self,
                 **kwargs,  # type: Dict[str, Any]
                 ):
        super().__init__(**kwargs)

    def __iter__(self):
        if self.done_streaming:
            raise AlreadyQueriedException()

        if not self.started_streaming:
            self._submit_scan()

        return self

    def __next__(self):
        try:
            return self._get_next_row()
        # We can stop iterator when we receive RangeScanCompletedException
        except RangeScanCompletedException:
            self._done_streaming = True
            raise StopIteration
        except StopIteration:
            self._done_streaming = True
            raise
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn
