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

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.management.logic.query_index_mgmt_req_types import QueryIndex
from txcouchbase.management.logic.query_index_mgmt_impl import TxQueryIndexMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                              CreatePrimaryQueryIndexOptions,
                                              CreateQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions,
                                              DropQueryIndexOptions,
                                              GetAllQueryIndexOptions,
                                              WatchQueryIndexOptions)


class QueryIndexManager:

    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._impl = TxQueryIndexMgmtImpl(client_adapter)
        self._collection_ctx = None

    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     keys,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs
                     ) -> Deferred[None]:
        req = self._impl.request_builder.build_create_index_request(bucket_name,
                                                                    index_name,
                                                                    keys,
                                                                    self._collection_ctx,
                                                                    *options,
                                                                    **kwargs)
        return self._impl.create_index_deferred(req)

    def create_primary_index(self,
                             bucket_name,   # type: str
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs
                             ) -> Deferred[None]:
        req = self._impl.request_builder.build_create_primary_index_request(bucket_name,
                                                                            self._collection_ctx,
                                                                            *options,
                                                                            **kwargs)
        return self._impl.create_primary_index_deferred(req)

    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs) -> Deferred[None]:
        req = self._impl.request_builder.build_drop_index_request(bucket_name,
                                                                  index_name,
                                                                  self._collection_ctx,
                                                                  *options,
                                                                  **kwargs)
        return self._impl.drop_index_deferred(req)

    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs) -> Deferred[None]:
        req = self._impl.request_builder.build_drop_primary_index_request(bucket_name,
                                                                          self._collection_ctx,
                                                                          *options,
                                                                          **kwargs)
        return self._impl.drop_primary_index_deferred(req)

    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Deferred[Iterable[QueryIndex]]:
        req = self._impl.request_builder.build_get_all_indexes_request(bucket_name,
                                                                       self._collection_ctx,
                                                                       *options,
                                                                       **kwargs)
        return self._impl.get_all_indexes_deferred(req)

    def build_deferred_indexes(self,
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ) -> Deferred[None]:
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param str bucket_name: name of the bucket.
        :param BuildDeferredQueryIndexOptions options: Options for building deferred indexes.
        :param Any kwargs: Override corresponding value in options.
        :raise: InvalidArgumentsException

        """
        req = self._impl.request_builder.build_build_deferred_indexes_request(bucket_name,
                                                                              self._collection_ctx,
                                                                              *options,
                                                                              **kwargs)
        return self._impl.build_deferred_indexes_deferred(req)

    def watch_indexes(self,
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      *options,     # type: WatchQueryIndexOptions
                      **kwargs      # type: Dict[str,Any]
                      ) -> Deferred[None]:
        """
        Watch polls indexes until they are online.

        :param str bucket_name: name of the bucket.
        :param Iterable[str] index_names: name(s) of the index(es).
        :param WatchQueryIndexOptions options: Options for request to watch indexes.
        :param Any kwargs: Override corresponding valud in options.
        :raises: QueryIndexNotFoundException
        :raises: WatchQueryIndexTimeoutException
        """
        req = self._impl.request_builder.build_watch_indexes_request(bucket_name,
                                                                     index_names,
                                                                     self._collection_ctx,
                                                                     *options,
                                                                     **kwargs)
        return self._impl.watch_indexes_deferred(req)
