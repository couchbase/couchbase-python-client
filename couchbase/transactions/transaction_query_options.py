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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    overload)

from couchbase.pycbc_core import transaction_query_options

if TYPE_CHECKING:
    from datetime import timedelta

    from acouchbase.n1ql import QueryScanConsistency
    from couchbase._utils import JSONType
    from couchbase.scope import Scope


class TransactionQueryOptions:
    ALLOWED_KEYS = {"raw", "ad_hoc", "scan_consistency", "profile_mode", "client_context_id",
                    "scan_wait", "read_only", "scan_cap", "pipeline_batch", "pipeline_cap",
                    "scope", "metrics", "max_parallelism"}

    @overload
    def __init__(self,
                 raw=None,  # type: Optional[Dict[str, JSONType]]
                 ad_hoc=None,  # type: Optional[bool]
                 scan_consistency=None,  # type: Optional[QueryScanConsistency]
                 profile_mode=None,  # type: Optional[Any]
                 client_context_id=None,  # type: Optional[str]
                 scan_wait=None,  # type: Optional[timedelta]
                 read_only=None,  # type: Optional[bool]
                 scan_cap=None,  # type: Optional[int]
                 pipeline_batch=None,  # type: Optional[int]
                 pipeline_cap=None,  # type: Optional[int]
                 positional_args=None,  # type: Optional[Iterable[JSONType]]
                 named_args=None,  # type: Optional[Dict[str, JSONType]]
                 scope=None,  # type: Optional[Scope]
                 metrics=None,  # type: Optional[bool]
                 max_parallelism=None  # type: Optional[int]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, JSONType]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionQueryOptions.ALLOWED_KEYS}
        # TODO: mapping similar to the options elsewhere.
        scope = kwargs.pop("scope", None)
        if scope:
            kwargs["bucket"] = scope.bucket.name
            kwargs["scope"] = scope.name
        if kwargs.get("scan_wait", None):
            kwargs["scan_wait"] = kwargs["scan_wait"].total_seconds/1000
        if kwargs.get("scan_consistency", None):
            kwargs["scan_consistency"] = kwargs["scan_consistency"].value
        self._base = transaction_query_options(**kwargs)
