from datetime import timedelta
from time import perf_counter, sleep
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    overload)

from couchbase.exceptions import QueryIndexNotFoundException, WatchQueryIndexTimeoutException
from couchbase.management.logic.query_index_logic import QueryIndex, QueryIndexManagerLogic
from couchbase.management.logic.wrappers import QueryIndexMgmtWrapper
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
    def __init__(self, connection):
        super().__init__(connection)

    @QueryIndexMgmtWrapper.block(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     fields,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs
                     ) -> None:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating a secondary index.")
        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when creating a secondary index.")
        if not isinstance(fields, (list, tuple)):
            raise ValueError("fields must be provided when creating a secondary index.")

        super().create_index(bucket_name, index_name, fields, *options, **kwargs)

    @QueryIndexMgmtWrapper.block(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def create_primary_index(self,
                             bucket_name,   # type: str
                             *options,      # type: CreatePrimaryQueryIndexOptions
                             **kwargs
                             ) -> None:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating a primary index.")

        super().create_primary_index(bucket_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.block(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs) -> None:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a secondary index.")
        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when dropping a secondary index.")

        super().drop_index(bucket_name, index_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.block(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs) -> None:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a primary index.")

        super().drop_primary_index(bucket_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.block(QueryIndex, QueryIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Iterable[QueryIndex]:

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when dropping a secondary index.")

        return super().get_all_indexes(bucket_name, *options, **kwargs)

    @QueryIndexMgmtWrapper.block(None, QueryIndexManagerLogic._ERROR_MAPPING)
    def build_deferred_indexes(self,
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ) -> None:
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

    def watch_indexes(self,   # noqa: C901
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      *options,     # type: WatchQueryIndexOptions
                      **kwargs      # type: Dict[str,Any]
                      ) -> None:
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

            indexes = self.get_all_indexes(bucket_name, opts)

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

            sleep(interval_millis / 1000)


"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead all options should be imported from couchbase.management.options.

"""


class GetAllQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 scope_name=None,       # type: str
                 collection_name=None   # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Get all query indexes options

        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreateQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 ignore_if_exists=None,  # type: bool
                 num_replicas=None,     # type: int
                 deferred=None,         # type: bool
                 condition=None,        # type: str
                 scope_name=None,       # type: str
                 collection_name=None   # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Query Index creation options

        :param timeout: operation timeout in seconds
        :param ignore_if_exists: don't throw an exception if index already exists
        :param num_replicas: number of replicas
        :param deferred: whether the index creation should be deferred
        :param condition: 'where' condition for partial index creation
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        if 'ignore_if_exists' not in kwargs:
            kwargs['ignore_if_exists'] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreatePrimaryQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 index_name=None,        # type: str
                 timeout=None,           # type: timedelta
                 ignore_if_exists=None,  # type: bool
                 num_replicas=None,      # type: int
                 deferred=None,          # type: bool
                 condition=None,         # type: str
                 scope_name=None,        # type: str
                 collection_name=None    # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Query Primary Index creation options

        :param index_name: name of primary index
        :param timeout: operation timeout in seconds
        :param ignore_if_exists: don't throw an exception if index already exists
        :param num_replicas: number of replicas
        :param deferred: whether the index creation should be deferred
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        if 'ignore_if_exists' not in kwargs:
            kwargs['ignore_if_exists'] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 ignore_if_not_exists=None,   # type: bool
                 timeout=None,                # type: timedelta
                 scope_name=None,             # type: str
                 collection_name=None         # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Drop query index options

        :param ignore_if_exists: don't throw an exception if index already exists
        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(DropQueryIndexOptions, self).__init__(**kwargs)


class DropPrimaryQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 index_name=None,            # str
                 ignore_if_not_exists=None,  # type: bool
                 timeout=None,               # type: timedelta
                 scope_name=None,            # type: str
                 collection_name=None        # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Drop primary index options

        :param index_name: name of primary index
        :param timeout: operation timeout in seconds
        :param ignore_if_exists: don't throw an exception if index already exists
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(DropPrimaryQueryIndexOptions, self).__init__(**kwargs)


class WatchQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 watch_primary=None,      # type: bool
                 timeout=None,            # type: timedelta
                 scope_name=None,         # type: str
                 collection_name=None     # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Watch query index options

        :param watch_primary: If True, watch primary indexes
        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class BuildDeferredQueryIndexOptionsDeprecated(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 scope_name=None,       # type: str
                 collection_name=None   # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Build deferred query indexes options

        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(BuildDeferredQueryIndexOptions, self).__init__(**kwargs)


GetAllQueryIndexOptions = GetAllQueryIndexOptionsDeprecated  # noqa: F811
CreateQueryIndexOptions = CreateQueryIndexOptionsDeprecated  # noqa: F811
CreatePrimaryQueryIndexOptions = CreatePrimaryQueryIndexOptionsDeprecated  # noqa: F811
DropQueryIndexOptions = DropQueryIndexOptionsDeprecated  # noqa: F811
DropPrimaryQueryIndexOptions = DropPrimaryQueryIndexOptionsDeprecated  # noqa: F811
WatchQueryIndexOptions = WatchQueryIndexOptionsDeprecated  # noqa: F811
BuildDeferredQueryIndexOptions = BuildDeferredQueryIndexOptionsDeprecated  # noqa: F811
