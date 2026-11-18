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

"""A sync bucket manager over the gateway's bucket admin service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._driver import call_unary
from couchbase.protocol.couchbase2._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._bucket_mgmt_types import (bucket_mgmt_errors,
                                                                          bucket_settings,
                                                                          check_no_durability_to_clear,
                                                                          named_bucket_settings)

if TYPE_CHECKING:
    from couchbase.common.management.buckets import (BucketDescribeResult,
                                                     BucketSettings,
                                                     CreateBucketSettings)
    from couchbase.common.management.options import (BucketDescribeOptions,
                                                     CreateBucketOptions,
                                                     DropBucketOptions,
                                                     FlushBucketOptions,
                                                     GetAllBucketOptions,
                                                     GetBucketOptions,
                                                     UpdateBucketOptions)
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl


class BucketMgmtImpl:

    def __init__(self, cluster_impl: ClusterImpl) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = BucketMgmtRequestBuilder(cluster_impl.core)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def create_bucket(self,
                      settings: CreateBucketSettings,
                      options: CreateBucketOptions | None = None,
                      **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.BucketCreate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_create_bucket_request(settings, obs_handler, options, **kwargs)
            ctx = core.context(Op.BucketCreate, req.timeout, obs_handler=req.obs_handler)
            with bucket_mgmt_errors():
                call_unary(self._cluster_impl.bucket_admin.CreateBucket, req.message, ctx, core.metadata)

    def update_bucket(self,
                      settings: BucketSettings,
                      options: UpdateBucketOptions | None = None,
                      **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.BucketUpdate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_update_bucket_request(settings, obs_handler, options, **kwargs)
            ctx = core.context(Op.BucketUpdate, req.timeout, obs_handler=req.obs_handler)
            if req.listing is not None:
                listed = call_unary(self._cluster_impl.bucket_admin.ListBuckets, req.listing, ctx, core.metadata)
                check_no_durability_to_clear(listed, req.message.bucket_name)
            with bucket_mgmt_errors():
                call_unary(self._cluster_impl.bucket_admin.UpdateBucket, req.message, ctx, core.metadata)

    def drop_bucket(self,
                    bucket_name: str,
                    options: DropBucketOptions | None = None,
                    **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.BucketDrop, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_bucket_request(bucket_name, obs_handler, options, **kwargs)
            ctx = core.context(Op.BucketDrop, req.timeout, obs_handler=req.obs_handler)
            with bucket_mgmt_errors():
                call_unary(self._cluster_impl.bucket_admin.DeleteBucket, req.message, ctx, core.metadata)

    def get_bucket(self,
                   bucket_name: str,
                   options: GetBucketOptions | None = None,
                   **kwargs: Any) -> BucketSettings:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.BucketGet, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_bucket_request(bucket_name, obs_handler, options, **kwargs)
            ctx = core.context(Op.BucketGet, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.bucket_admin.ListBuckets, req.message, ctx, core.metadata)
            return named_bucket_settings(response, req.bucket_name)

    def get_all_buckets(self,
                        options: GetAllBucketOptions | None = None,
                        **kwargs: Any) -> list[BucketSettings]:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.BucketGetAll, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_buckets_request(obs_handler, options, **kwargs)
            ctx = core.context(Op.BucketGetAll, req.timeout, obs_handler=req.obs_handler)
            return bucket_settings(call_unary(self._cluster_impl.bucket_admin.ListBuckets,
                                              req.message,
                                              ctx,
                                              core.metadata))

    def flush_bucket(self,
                     bucket_name: str,
                     options: FlushBucketOptions | None = None,
                     **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.BucketFlush, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_flush_bucket_request(bucket_name, obs_handler, options, **kwargs)
            ctx = core.context(Op.BucketFlush, req.timeout, obs_handler=req.obs_handler)
            with bucket_mgmt_errors():
                call_unary(self._cluster_impl.bucket_admin.FlushBucket, req.message, ctx, core.metadata)

    def bucket_describe(self,
                        bucket_name: str,
                        options: BucketDescribeOptions | None = None,
                        **kwargs: Any) -> BucketDescribeResult:
        raise unsupported('bucket_describe')
