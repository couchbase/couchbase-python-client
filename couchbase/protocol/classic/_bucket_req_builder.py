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

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common.diagnostics import ServiceType
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.options import PingOptions, ViewOptions
from couchbase.protocol.classic._bucket_types import (BucketOptionsBlock,
                                                      PingRequest,
                                                      ViewQueryRequest)
from couchbase.protocol.classic._views import ViewQuery
from couchbase.protocol.classic._wire import check_streaming_args, forward_args


class BucketRequestBuilder:

    def __init__(self, bucket_name: str, option_validation: OptionValidationMode | None = None) -> None:
        self._bucket_name = bucket_name
        self._option_validation = option_validation

    def build_ping_request(self, options: PingOptions | None = None, **kwargs: object) -> PingRequest:
        final_args = forward_args(kwargs, options, block=BucketOptionsBlock.Ping, mode=self._option_validation)
        timeout = final_args.pop('timeout', None)

        service_types = final_args.pop('service_types', None)
        if not service_types:
            service_types = list(
                map(lambda st: st.value, [ServiceType(st.value) for st in ServiceType]))

        if not isinstance(service_types, list):
            raise InvalidArgumentException('Service types must be a list/set.')

        services = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))

        req = PingRequest(services, bucket_name=self._bucket_name, **final_args)
        if timeout:
            req.timeout = timeout

        return req

    def build_view_query_request(self,
                                 design_doc: str,
                                 view_name: str,
                                 obs_handler: ObservableRequestHandler,
                                 options: ViewOptions | None = None,
                                 **kwargs: object) -> ViewQueryRequest:
        check_streaming_args((options,), kwargs, block=BucketOptionsBlock.View, options_type=ViewOptions,
                             mode=self._option_validation)
        return ViewQueryRequest(ViewQuery.create_view_query_object(self._bucket_name,
                                                                   design_doc,
                                                                   view_name,
                                                                   options,
                                                                   **kwargs), obs_handler)
