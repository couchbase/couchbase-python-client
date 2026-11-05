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

__all__ = [
    'CollectionQueryIndexManager',
    'QueryIndex',
    'QueryIndexManager',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING

from couchbase.common.management.queries import QueryIndex

# @TODO:  lets deprecate import of options from couchbase.management.queries
from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                          CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions,
                                          WatchQueryIndexOptions)

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._query_index_mgmt import QueryIndexMgmtInterface
    from couchbase.common.management.options import (BuildDeferredQueryIndexOptionsKwargs,
                                                     CreatePrimaryQueryIndexOptionsKwargs,
                                                     CreateQueryIndexOptionsKwargs,
                                                     DropPrimaryQueryIndexOptionsKwargs,
                                                     DropQueryIndexOptionsKwargs,
                                                     GetAllQueryIndexOptionsKwargs,
                                                     WatchQueryIndexOptionsKwargs)


class QueryIndexManager:
    """Performs management operations on query indexes.

    For managing query indexes at the collection level, :class:`.CollectionQueryIndexManager` should be used.
    """

    def __init__(self, impl: QueryIndexMgmtInterface) -> None:
        self._impl = impl

    def create_index(self,
                     bucket_name: str,
                     index_name: str,
                     keys: Iterable[str],
                     options: CreateQueryIndexOptions | None = None,
                     **kwargs: Unpack[CreateQueryIndexOptionsKwargs]) -> None:
        """Creates a new query index.

        Args:
            bucket_name (str): The name of the bucket this index is for.
            index_name (str): The name of the index.
            keys (Iterable[str]): The keys which this index should cover.
            options (:class:`~couchbase.management.options.CreateQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name, index_name or keys
                are invalid types.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        self._impl.create_index(bucket_name, index_name, keys, options, **kwargs)

    def create_primary_index(self,
                             bucket_name: str,
                             options: CreatePrimaryQueryIndexOptions | None = None,
                             **kwargs: Unpack[CreatePrimaryQueryIndexOptionsKwargs]) -> None:
        """Creates a new primary query index.

        Args:
            bucket_name (str): The name of the bucket this index is for.
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        self._impl.create_primary_index(bucket_name, options, **kwargs)

    def drop_index(self,
                   bucket_name: str,
                   index_name: str,
                   options: DropQueryIndexOptions | None = None,
                   **kwargs: Unpack[DropQueryIndexOptionsKwargs]) -> None:
        """Drops an existing query index.

        Args:
            bucket_name (str): The name of the bucket containing the index to drop.
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_name are
                invalid types.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        self._impl.drop_index(bucket_name, index_name, options, **kwargs)

    def drop_primary_index(self,
                           bucket_name: str,
                           options: DropPrimaryQueryIndexOptions | None = None,
                           **kwargs: Unpack[DropPrimaryQueryIndexOptionsKwargs]) -> None:
        """Drops an existing primary query index.

        Args:
            bucket_name (str): The name of the bucket this index to drop.
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        self._impl.drop_primary_index(bucket_name, options, **kwargs)

    def get_all_indexes(self,
                        bucket_name: str,
                        options: GetAllQueryIndexOptions | None = None,
                        **kwargs: Unpack[GetAllQueryIndexOptionsKwargs]) -> Iterable[QueryIndex]:
        """Returns a list of indexes for a specific bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch indexes for.
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`.QueryIndex`]: A list of indexes for a specific bucket.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        return self._impl.get_all_indexes(bucket_name, options, **kwargs)

    def build_deferred_indexes(self,
                               bucket_name: str,
                               options: BuildDeferredQueryIndexOptions | None = None,
                               **kwargs: Unpack[BuildDeferredQueryIndexOptionsKwargs]) -> None:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            bucket_name (str): The name of the bucket to perform build on.
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        self._impl.build_deferred_indexes(bucket_name, options, **kwargs)

    def watch_indexes(self,
                      bucket_name: str,
                      index_names: Iterable[str],
                      options: WatchQueryIndexOptions | None = None,
                      **kwargs: Unpack[WatchQueryIndexOptionsKwargs]) -> None:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
            bucket_name (str): The name of the bucket to watch for indexes on.
            index_names (Iterable[str]): The names of the indexes to watch.
            options (:class:`~couchbase.management.options.WatchQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_names are
                invalid types.
            :class:`~couchbase.exceptions.WatchQueryIndexTimeoutException`: If the specified timeout is reached
                before all the specified indexes are ready to use.
        """
        self._impl.watch_indexes(bucket_name, index_names, options, **kwargs)


class CollectionQueryIndexManager:
    """Performs management operations on query indexes at the collection level."""

    def __init__(self, impl: QueryIndexMgmtInterface, bucket_name: str) -> None:
        self._impl = impl
        self._bucket_name = bucket_name

    def create_index(self,
                     index_name: str,
                     keys: Iterable[str],
                     options: CreateQueryIndexOptions | None = None,
                     **kwargs: Unpack[CreateQueryIndexOptionsKwargs]) -> None:
        """Creates a new query index.

        Args:
            index_name (str): The name of the index.
            keys (Iterable[str]): The keys which this index should cover.
            options (:class:`~couchbase.management.options.CreateQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name or keys are invalid types.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        self._impl.create_index(self._bucket_name, index_name, keys, options, **kwargs)

    def create_primary_index(self,
                             options: CreatePrimaryQueryIndexOptions | None = None,
                             **kwargs: Unpack[CreatePrimaryQueryIndexOptionsKwargs]) -> None:
        """Creates a new primary query index.

        Args:
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        self._impl.create_primary_index(self._bucket_name, options, **kwargs)

    def drop_index(self,
                   index_name: str,
                   options: DropQueryIndexOptions | None = None,
                   **kwargs: Unpack[DropQueryIndexOptionsKwargs]) -> None:
        """Drops an existing query index.

        Args:
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name is an invalid types.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        self._impl.drop_index(self._bucket_name, index_name, options, **kwargs)

    def drop_primary_index(self,
                           options: DropPrimaryQueryIndexOptions | None = None,
                           **kwargs: Unpack[DropPrimaryQueryIndexOptionsKwargs]) -> None:
        """Drops an existing primary query index.

        Args:
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        self._impl.drop_primary_index(self._bucket_name, options, **kwargs)

    def get_all_indexes(self,
                        options: GetAllQueryIndexOptions | None = None,
                        **kwargs: Unpack[GetAllQueryIndexOptionsKwargs]) -> Iterable[QueryIndex]:
        """Returns a list of indexes for a specific collection.

        Args:
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`.QueryIndex`]: A list of indexes.

        """
        return self._impl.get_all_indexes(self._bucket_name, options, **kwargs)

    def build_deferred_indexes(self,
                               options: BuildDeferredQueryIndexOptions | None = None,
                               **kwargs: Unpack[BuildDeferredQueryIndexOptionsKwargs]) -> None:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        """
        self._impl.build_deferred_indexes(self._bucket_name, options, **kwargs)

    def watch_indexes(self,
                      index_names: Iterable[str],
                      options: WatchQueryIndexOptions | None = None,
                      **kwargs: Unpack[WatchQueryIndexOptionsKwargs]) -> None:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
            index_names (Iterable[str]): The names of the indexes to watch.
            options (:class:`~couchbase.management.options.WatchQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_names are invalid types.
            :class:`~couchbase.exceptions.WatchQueryIndexTimeoutException`: If the specified timeout is reached
                before all the specified indexes are ready to use.
        """
        self._impl.watch_indexes(self._bucket_name, index_names, options, **kwargs)
