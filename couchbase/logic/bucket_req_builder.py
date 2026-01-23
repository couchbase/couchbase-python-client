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

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.bucket_types import PingRequest, ViewQueryRequest
from couchbase.options import forward_args
from couchbase.pycbc_core import operations
from couchbase.views import ViewQuery


class BucketRequestBuilder:

    def __init__(self, bucket_name: str) -> None:
        self._bucket_name = bucket_name

    def build_ping_request(self, *options: object, **kwargs: object) -> PingRequest:
        # TODO: OptionsProcessor
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)

        service_types = final_args.pop('service_types', None)
        if not service_types:
            service_types = list(
                map(lambda st: st.value, [ServiceType(st.value) for st in ServiceType]))

        if not isinstance(service_types, list):
            raise InvalidArgumentException('Service types must be a list/set.')

        service_types = list(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))

        req = PingRequest(operations.PING.value, service_types, **final_args)
        if timeout:
            req.timeout = timeout

        return req

    def build_view_query_request(self,
                                 design_doc: str,
                                 view_name: str,
                                 *options: object,
                                 **kwargs: object) -> ViewQueryRequest:
        num_workers = kwargs.pop('num_workers', None)
        req = ViewQueryRequest(ViewQuery.create_view_query_object(self._bucket_name,
                                                                  design_doc,
                                                                  view_name,
                                                                  *options,
                                                                  **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req
