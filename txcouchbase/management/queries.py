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

# from datetime import timedelta
# from time import perf_counter, sleep
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.management.logic.query_index_logic import QueryIndex, QueryIndexManagerLogic

# from couchbase.exceptions import QueryIndexNotFoundException, WatchQueryIndexTimeoutException
from couchbase.management.options import GetAllQueryIndexOptions

# from couchbase.options import forward_args

if TYPE_CHECKING:
    from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                              CreatePrimaryQueryIndexOptions,
                                              CreateQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions,
                                              DropQueryIndexOptions)


class QueryIndexManager(QueryIndexManagerLogic):
    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     fields,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs
                     ) -> Deferred[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating a secondary index.")
        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when creating a secondary index.")
        if not isinstance(fields, (list, tuple)):
            raise ValueError("fields must be provided when creating a secondary index.")

        return Deferred.fromFuture(super().create_index(bucket_name, index_name, fields, *options, **kwargs))

    def create_primary_index(self,
                             bucket_name,   # type: str
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs
                             ) -> Deferred[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating a primary index.")

        return Deferred.fromFuture(super().create_primary_index(bucket_name, *options, **kwargs))

    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs) -> Deferred[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a secondary index.")
        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when dropping a secondary index.")

        return Deferred.fromFuture(super().drop_index(bucket_name, index_name, *options, **kwargs))

    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs) -> Deferred[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a primary index.")

        return Deferred.fromFuture(super().drop_primary_index(bucket_name, *options, **kwargs))

    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Deferred[Iterable[QueryIndex]]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a secondary index.")

        return Deferred.fromFuture(super().get_all_indexes(bucket_name, *options, **kwargs))

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

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when building deferred indexes.")

        return Deferred.fromFuture(super().build_deferred_indexes(bucket_name, *options, **kwargs))

    # def watch_indexes(self,   # noqa: C901
    #                   bucket_name,  # type: str
    #                   index_names,  # type: Iterable[str]
    #                   *options,     # type: WatchQueryIndexOptions
    #                   **kwargs      # type: Dict[str,Any]
    #                   ) -> Deferred[None]:
    #     """
    #     Watch polls indexes until they are online.

    #     :param str bucket_name: name of the bucket.
    #     :param Iterable[str] index_names: name(s) of the index(es).
    #     :param WatchQueryIndexOptions options: Options for request to watch indexes.
    #     :param Any kwargs: Override corresponding valud in options.
    #     :raises: QueryIndexNotFoundException
    #     :raises: WatchQueryIndexTimeoutException
    #     """
    #     final_args = forward_args(kwargs, *options)

    #     if final_args.get("watch_primary", False):
    #         index_names.append("#primary")

    #     timeout = final_args.get("timeout", None)
    #     if not timeout:
    #         raise ValueError(
    #             'Must specify a timeout condition for watch indexes')

    #     def check_indexes(index_names, indexes):
    #         for idx_name in index_names:
    #             match = next((i for i in indexes if i.name == idx_name), None)
    #             if not match:
    #                 raise QueryIndexNotFoundException(
    #                     "Cannot find index with name: {}".format(idx_name))

    #         return all(map(lambda i: i.state == "online", indexes))

    #     # timeout is converted to microsecs via final_args()
    #     timeout_millis = timeout / 1000

    #     interval_millis = float(50)
    #     start = perf_counter()
    #     time_left = timeout_millis
    #     while True:

    #         opts = GetAllQueryIndexOptions(
    #             timeout=timedelta(milliseconds=time_left))

    #         indexes = self.get_all_indexes(bucket_name, opts)

    #         all_online = check_indexes(index_names, indexes)
    #         if all_online:
    #             break

    #         interval_millis += 500
    #         if interval_millis > 1000:
    #             interval_millis = 1000

    #         time_left = timeout_millis - ((perf_counter() - start) * 1000)
    #         if interval_millis > time_left:
    #             interval_millis = time_left

    #         if time_left <= 0:
    #             raise WatchQueryIndexTimeoutException(
    #                 "Failed to find all indexes online within the alloted time.")

    #         sleep(interval_millis / 1000)
