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

import asyncio
from datetime import timedelta
from time import perf_counter
from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  InvalidArgumentException,
                                  QueryIndexNotFoundException,
                                  WatchQueryIndexTimeoutException)
from couchbase.management.logic import ManagementType
from couchbase.management.logic.query_index_logic import QueryIndex, QueryIndexManagerLogic
from couchbase.management.options import GetAllQueryIndexOptions
from couchbase.options import forward_args

if TYPE_CHECKING:
    from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                              CreatePrimaryQueryIndexOptions,
                                              CreateQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions,
                                              DropQueryIndexOptions,
                                              WatchQueryIndexOptions)


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

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     keys,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs
                     ) -> Awaitable[None]:
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

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when creating a secondary index.')
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided when creating a secondary index.')
        if not isinstance(keys, (list, tuple)):
            raise InvalidArgumentException('Index keys must be provided when creating a secondary index.')

        super().create_index(bucket_name, index_name, keys, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_primary_index(self,
                             bucket_name,   # type: str
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs
                             ) -> Awaitable[None]:
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
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when creating a primary index.')

        super().create_primary_index(bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs) -> Awaitable[None]:
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
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when dropping a secondary index.')
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided when dropping a secondary index.')

        super().drop_index(bucket_name, index_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs) -> Awaitable[None]:
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

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when dropping a primary index.')

        super().drop_primary_index(bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(QueryIndex, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Awaitable[Iterable[QueryIndex]]:
        """Returns a list of indexes for a specific bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch indexes for.
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Awaitable[Iterable[:class:`.QueryIndex`]]: A list of indexes for a specific bucket.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when retrieving all indexes.')

        super().get_all_indexes(bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def build_deferred_indexes(self,
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ) -> Awaitable[None]:
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
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when building deferred indexes.')

        super().build_deferred_indexes(bucket_name, *options, **kwargs)

    async def watch_indexes(self,   # noqa: C901
                            bucket_name,  # type: str
                            index_names,  # type: Iterable[str]
                            *options,     # type: WatchQueryIndexOptions
                            **kwargs      # type: Dict[str,Any]
                            ) -> Awaitable[None]:
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

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when watching indexes.')
        if not isinstance(index_names, (list, tuple)):
            raise InvalidArgumentException('One or more index_names must be provided when watching indexes.')

        final_args = forward_args(kwargs, *options)

        scope_name = final_args.get('scope_name', None)
        collection_name = final_args.get('collection_name', None)

        if final_args.get('watch_primary', False):
            index_names.append('#primary')

        timeout = final_args.get('timeout', None)
        if not timeout:
            raise InvalidArgumentException('Must specify a timeout condition for watch indexes')

        def check_indexes(index_names, indexes):
            for idx_name in index_names:
                match = next((i for i in indexes if i.name == idx_name), None)
                if not match:
                    raise QueryIndexNotFoundException(f'Cannot find index with name: {idx_name}')

            return all(map(lambda i: i.state == 'online', indexes))

        # timeout is converted to microsecs via final_args()
        timeout_millis = timeout / 1000

        interval_millis = float(50)
        start = perf_counter()
        time_left = timeout_millis
        while True:

            opts = GetAllQueryIndexOptions(
                timeout=timedelta(milliseconds=time_left))
            if scope_name:
                opts['scope_name'] = scope_name
                opts['collection_name'] = collection_name

            try:
                indexes = await self.get_all_indexes(bucket_name, opts)
            except AmbiguousTimeoutException:
                pass  # go ahead and move on, raise WatchQueryIndexTimeoutException later if needed

            all_online = check_indexes(index_names, indexes)
            if all_online:
                break

            interval_millis += 500
            if interval_millis > 1000:
                interval_millis = 1000

            time_left = timeout_millis - ((perf_counter() - start) * 1000)
            if interval_millis > time_left:
                interval_millis = time_left

            if time_left <= 0:
                raise WatchQueryIndexTimeoutException('Failed to find all indexes online within the alloted time.')

            await asyncio.sleep(interval_millis / 1000)


class CollectionQueryIndexManager(QueryIndexManagerLogic):
    def __init__(self, connection, loop, bucket_name, scope_name, collection_name):
        super().__init__(connection)
        self._bucket_name = bucket_name
        self._scope_name = scope_name
        self._collection_name = collection_name
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_index(self,
                     index_name,    # type: str
                     keys,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs
                     ) -> Awaitable[None]:
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
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided when creating a secondary index.')
        if not isinstance(keys, (list, tuple)):
            raise InvalidArgumentException('Index keys must be provided when creating a secondary index.')

        if not kwargs:
            kwargs = {}

        if kwargs.get('scope_name') or (options and options[0].get('scope_name')):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if kwargs.get('collection_name') or (options and options[0].get('collection_name')):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        kwargs['scope_name'] = self._scope_name
        kwargs['collection_name'] = self._collection_name
        super().create_index(self._bucket_name, index_name, keys, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_primary_index(self,
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs
                             ) -> Awaitable[None]:
        """Creates a new primary query index.

        Args:
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        if not kwargs:
            kwargs = {}

        if kwargs.get('scope_name') or (options and options[0].get('scope_name')):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if kwargs.get('collection_name') or (options and options[0].get('collection_name')):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        kwargs['scope_name'] = self._scope_name
        kwargs['collection_name'] = self._collection_name
        super().create_primary_index(self._bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs) -> Awaitable[None]:
        """Drops an existing query index.

        Args:
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided when dropping a secondary index.')

        if not kwargs:
            kwargs = {}

        if kwargs.get('scope_name') or (options and options[0].get('scope_name')):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if kwargs.get('collection_name') or (options and options[0].get('collection_name')):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        kwargs['scope_name'] = self._scope_name
        kwargs['collection_name'] = self._collection_name
        super().drop_index(self._bucket_name, index_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_primary_index(self,
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs) -> Awaitable[None]:
        """Drops an existing primary query index.

        Args:
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        if not kwargs:
            kwargs = {}

        if kwargs.get('scope_name') or (options and options[0].get('scope_name')):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if kwargs.get('collection_name') or (options and options[0].get('collection_name')):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        kwargs['scope_name'] = self._scope_name
        kwargs['collection_name'] = self._collection_name
        super().drop_primary_index(self._bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(QueryIndex, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Awaitable[Iterable[QueryIndex]]:
        """Returns a list of indexes for a specific collection.

        Args:
            bucket_name (str): The name of the bucket to fetch indexes for.
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Awaitable[Iterable[:class:`.QueryIndex`]]: A list of indexes.
        """
        if not kwargs:
            kwargs = {}

        if kwargs.get('scope_name') or (options and options[0].get('scope_name')):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if kwargs.get('collection_name') or (options and options[0].get('collection_name')):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        kwargs['scope_name'] = self._scope_name
        kwargs['collection_name'] = self._collection_name
        super().get_all_indexes(self._bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.QueryIndexMgmt, QueryIndexManagerLogic._ERROR_MAPPING)
    def build_deferred_indexes(self,
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ) -> Awaitable[None]:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        """
        if not kwargs:
            kwargs = {}

        if kwargs.get('scope_name') or (options and options[0].get('scope_name')):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if kwargs.get('collection_name') or (options and options[0].get('collection_name')):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        kwargs['scope_name'] = self._scope_name
        kwargs['collection_name'] = self._collection_name
        super().build_deferred_indexes(self._bucket_name, *options, **kwargs)

    async def watch_indexes(self,   # noqa: C901
                            index_names,  # type: Iterable[str]
                            *options,     # type: WatchQueryIndexOptions
                            **kwargs      # type: Dict[str,Any]
                            ) -> Awaitable[None]:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
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

        if not isinstance(index_names, (list, tuple)):
            raise InvalidArgumentException('One or more index_names must be provided when watching indexes.')

        final_args = forward_args(kwargs, *options)

        if final_args.get('watch_primary', False):
            index_names.append('#primary')

        if final_args.get('scope_name'):
            raise InvalidArgumentException('scope_name cannot be set in the options when using the collection-level '
                                           'query index manager')
        if final_args.get('collection_name'):
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

        timeout = final_args.get('timeout', None)
        if not timeout:
            raise InvalidArgumentException('Must specify a timeout condition for watch indexes')

        def check_indexes(index_names, indexes):
            for idx_name in index_names:
                match = next((i for i in indexes if i.name == idx_name), None)
                if not match:
                    raise QueryIndexNotFoundException(f'Cannot find index with name: {idx_name}')

            return all(map(lambda i: i.state == 'online', indexes))

        # timeout is converted to microsecs via final_args()
        timeout_millis = timeout / 1000

        interval_millis = float(50)
        start = perf_counter()
        time_left = timeout_millis
        while True:
            opts = GetAllQueryIndexOptions(
                timeout=timedelta(milliseconds=time_left))

            try:
                indexes = await self.get_all_indexes(opts)
            except AmbiguousTimeoutException:
                pass  # go ahead and move on, raise WatchQueryIndexTimeoutException later if needed

            all_online = check_indexes(index_names, indexes)
            if all_online:
                break

            interval_millis += 500
            if interval_millis > 1000:
                interval_millis = 1000

            time_left = timeout_millis - ((perf_counter() - start) * 1000)
            if interval_millis > time_left:
                interval_millis = time_left

            if time_left <= 0:
                raise WatchQueryIndexTimeoutException('Failed to find all indexes online within the alloted time.')

            await asyncio.sleep(interval_millis / 1000)
