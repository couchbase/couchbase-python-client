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

from couchbase.management.logic.query_index_mgmt_impl import QueryIndex, QueryIndexMgmtImpl

# @TODO:  lets deprecate import of options from couchbase.management.queries
from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                          CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions,
                                          WatchQueryIndexOptions)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter


class QueryIndexManager:
    """
    Performs management operations on query indexes.

    For managing query indexes at the collection level, :class:`.CollectionQueryIndexManager` should be used.
    """

    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._impl = QueryIndexMgmtImpl(client_adapter)
        self._collection_ctx = None

    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     keys,          # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> None:
        """Creates a new query index.

        Args:
            bucket_name (str): The name of the bucket this index is for.
            index_name (str): The name of the index.
            keys (Iterable[str]): The keys which this index should cover.
            options (:class:`~couchbase.management.options.CreateQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name, index_name or keys
                are invalid types.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        req = self._impl.request_builder.build_create_index_request(bucket_name,
                                                                    index_name,
                                                                    keys,
                                                                    self._collection_ctx,
                                                                    *options,
                                                                    **kwargs)
        self._impl.create_index(req)

    def create_primary_index(self,
                             bucket_name,   # type: str
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs       # type: Dict[str, Any]
                             ) -> None:
        """Creates a new primary query index.

        Args:
            bucket_name (str): The name of the bucket this index is for.
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        req = self._impl.request_builder.build_create_primary_index_request(bucket_name,
                                                                            self._collection_ctx,
                                                                            *options,
                                                                            **kwargs)
        self._impl.create_primary_index(req)

    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> None:
        """Drops an existing query index.

        Args:
            bucket_name (str): The name of the bucket containing the index to drop.
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_name are
                invalid types.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        req = self._impl.request_builder.build_drop_index_request(bucket_name,
                                                                  index_name,
                                                                  self._collection_ctx,
                                                                  *options,
                                                                  **kwargs)
        self._impl.drop_index(req)

    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs         # type: Dict[str, Any]
                           ) -> None:
        """Drops an existing primary query index.

        Args:
            bucket_name (str): The name of the bucket this index to drop.
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        req = self._impl.request_builder.build_drop_primary_index_request(bucket_name,
                                                                          self._collection_ctx,
                                                                          *options,
                                                                          **kwargs)
        self._impl.drop_primary_index(req)

    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Iterable[QueryIndex]:
        """Returns a list of indexes for a specific bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch indexes for.
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`.QueryIndex`]: A list of indexes for a specific bucket.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        req = self._impl.request_builder.build_get_all_indexes_request(bucket_name,
                                                                       self._collection_ctx,
                                                                       *options,
                                                                       **kwargs)
        return self._impl.get_all_indexes(req)

    def build_deferred_indexes(self,
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs         # type: Dict[str, Any]
                               ) -> None:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            bucket_name (str): The name of the bucket to perform build on.
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        req = self._impl.request_builder.build_build_deferred_indexes_request(bucket_name,
                                                                              self._collection_ctx,
                                                                              *options,
                                                                              **kwargs)
        self._impl.build_deferred_indexes(req)

    def watch_indexes(self,   # noqa: C901
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      *options,     # type: WatchQueryIndexOptions
                      **kwargs      # type: Dict[str,Any]
                      ) -> None:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
            bucket_name (str): The name of the bucket to watch for indexes on.
            index_names (Iterable[str]): The names of the indexes to watch.
            options (:class:`~couchbase.management.options.WatchQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_names are
                invalid types.
            :class:`~couchbase.exceptions.WatchQueryIndexTimeoutException`: If the specified timeout is reached
                before all the specified indexes are ready to use.
        """
        req = self._impl.request_builder.build_watch_indexes_request(bucket_name,
                                                                     index_names,
                                                                     self._collection_ctx,
                                                                     *options,
                                                                     **kwargs)
        self._impl.watch_indexes(req)


class CollectionQueryIndexManager:
    """
    Performs management operations on query indexes at the collection level.
    """

    def __init__(self, client_adapter: ClientAdapter, bucket_name: str, scope_name: str, collection_name: str) -> None:
        self._bucket_name = bucket_name
        self._impl = QueryIndexMgmtImpl(client_adapter)
        self._collection_ctx = (collection_name, scope_name)

    def create_index(self,
                     index_name,    # type: str
                     keys,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> None:
        """Creates a new query index.

        Args:
            index_name (str): The name of the index.
            keys (Iterable[str]): The keys which this index should cover.
            options (:class:`~couchbase.management.options.CreateQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name or keys are invalid types.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        req = self._impl.request_builder.build_create_index_request(self._bucket_name,
                                                                    index_name,
                                                                    keys,
                                                                    self._collection_ctx,
                                                                    *options,
                                                                    **kwargs)
        self._impl.create_index(req)

    def create_primary_index(self,
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs       # type: Dict[str, Any]
                             ) -> None:
        """Creates a new primary query index.

        Args:
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        req = self._impl.request_builder.build_create_primary_index_request(self._bucket_name,
                                                                            self._collection_ctx,
                                                                            *options,
                                                                            **kwargs)
        self._impl.create_primary_index(req)

    def drop_index(self,
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> None:
        """Drops an existing query index.

        Args:
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name is an invalid types.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        req = self._impl.request_builder.build_drop_index_request(self._bucket_name,
                                                                  index_name,
                                                                  self._collection_ctx,
                                                                  *options,
                                                                  **kwargs)
        self._impl.drop_index(req)

    def drop_primary_index(self,
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs         # type: Dict[str, Any]
                           ) -> None:
        """Drops an existing primary query index.

        Args:
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        req = self._impl.request_builder.build_drop_primary_index_request(self._bucket_name,
                                                                          self._collection_ctx,
                                                                          *options,
                                                                          **kwargs)
        self._impl.drop_primary_index(req)

    def get_all_indexes(self,
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Iterable[QueryIndex]:
        """Returns a list of indexes for a specific collection.

        Args:
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`.QueryIndex`]: A list of indexes.

        """
        req = self._impl.request_builder.build_get_all_indexes_request(self._bucket_name,
                                                                       self._collection_ctx,
                                                                       *options,
                                                                       **kwargs)
        return self._impl.get_all_indexes(req)

    def build_deferred_indexes(self,
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs         # type: Dict[str, Any]
                               ) -> None:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        """
        req = self._impl.request_builder.build_build_deferred_indexes_request(self._bucket_name,
                                                                              self._collection_ctx,
                                                                              *options,
                                                                              **kwargs)
        self._impl.build_deferred_indexes(req)

    def watch_indexes(self,
                      index_names,  # type: Iterable[str]
                      *options,     # type: WatchQueryIndexOptions
                      **kwargs      # type: Dict[str, Any]
                      ) -> None:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
            index_names (Iterable[str]): The names of the indexes to watch.
            options (:class:`~couchbase.management.options.WatchQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_names are invalid types.
            :class:`~couchbase.exceptions.WatchQueryIndexTimeoutException`: If the specified timeout is reached
                before all the specified indexes are ready to use.
        """
        req = self._impl.request_builder.build_watch_indexes_request(self._bucket_name,
                                                                     index_names,
                                                                     self._collection_ctx,
                                                                     *options,
                                                                     **kwargs)
        self._impl.watch_indexes(req)
