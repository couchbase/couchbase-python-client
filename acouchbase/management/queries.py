import asyncio
from datetime import timedelta
from time import perf_counter
from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable)

from acouchbase.management.logic import QueryIndexMgmtWrapper
from couchbase.exceptions import QueryIndexNotFoundException, WatchQueryIndexTimeoutException
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

    @QueryIndexMgmtWrapper.inject_callbacks(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     fields,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs
                     ) -> Awaitable[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating a secondary index.")
        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when creating a secondary index.")
        if not isinstance(fields, (list, tuple)):
            raise ValueError("fields must be provided when creating a secondary index.")

        super().create_index(bucket_name, index_name, fields, *options, **kwargs)

    @QueryIndexMgmtWrapper.inject_callbacks(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_primary_index(self,
                             bucket_name,   # type: str
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs
                             ) -> Awaitable[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating a primary index.")

        super().create_primary_index(bucket_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.inject_callbacks(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs) -> Awaitable[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a secondary index.")
        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when dropping a secondary index.")

        super().drop_index(bucket_name, index_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.inject_callbacks(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs) -> Awaitable[None]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a primary index.")

        super().drop_primary_index(bucket_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.inject_callbacks(QueryIndex, QueryIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Awaitable[Iterable[QueryIndex]]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a secondary index.")

        super().get_all_indexes(bucket_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.inject_callbacks(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def build_deferred_indexes(self,
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ) -> Awaitable[None]:
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param str bucket_name: name of the bucket.
        :param BuildDeferredQueryIndexOptions options: Options for building deferred indexes.
        :param Any kwargs: Override corresponding value in options.
        :raise: InvalidArgumentsException

        """
        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when building deferred indexes.")

        super().build_deferred_indexes(bucket_name, *options, **kwargs)

    async def watch_indexes(self,   # noqa: C901
                            bucket_name,  # type: str
                            index_names,  # type: Iterable[str]
                            *options,     # type: WatchQueryIndexOptions
                            **kwargs      # type: Dict[str,Any]
                            ) -> Awaitable[None]:
        """
        Watch polls indexes until they are online.

        :param str bucket_name: name of the bucket.
        :param Iterable[str] index_names: name(s) of the index(es).
        :param WatchQueryIndexOptions options: Options for request to watch indexes.
        :param Any kwargs: Override corresponding valud in options.
        :raises: QueryIndexNotFoundException
        :raises: WatchQueryIndexTimeoutException
        """
        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when watching indexes.")
        if not isinstance(index_names, (list, tuple)):
            raise ValueError("index_names must be provided when watching indexes.")

        final_args = forward_args(kwargs, *options)

        if final_args.get("watch_primary", False):
            index_names.append("#primary")

        timeout = final_args.get("timeout", None)
        if not timeout:
            raise ValueError(
                'Must specify a timeout condition for watch indexes')

        def check_indexes(index_names, indexes):
            for idx_name in index_names:
                match = next((i for i in indexes if i.name == idx_name), None)
                if not match:
                    raise QueryIndexNotFoundException(
                        "Cannot find index with name: {}".format(idx_name))

            return all(map(lambda i: i.state == "online", indexes))

        # timeout is converted to microsecs via final_args()
        timeout_millis = timeout / 1000

        interval_millis = float(50)
        start = perf_counter()
        time_left = timeout_millis
        while True:

            opts = GetAllQueryIndexOptions(
                timeout=timedelta(milliseconds=time_left))

            indexes = await self.get_all_indexes(bucket_name, opts)

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
                raise WatchQueryIndexTimeoutException(
                    "Failed to find all indexes online within the alloted time.")

            await asyncio.sleep(interval_millis / 1000)
