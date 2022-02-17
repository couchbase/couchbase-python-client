import asyncio
import time
from typing import *
from datetime import timedelta
import couchbase_core._libcouchbase as LCB
from couchbase.management.admin import Admin
from couchbase_core import mk_formstr
from couchbase.options import forward_args
from couchbase.management.admin import METHMAP


from couchbase.exceptions import (HTTPException, QueryIndexAlreadyExistsException,
                                  WatchQueryIndexTimeoutException, QueryIndexNotFoundException,
                                  InvalidArgumentException)

from couchbase.management.queries import (QueryErrorMapper, QueryIndex,
                                          GetAllQueryIndexOptions, CreateQueryIndexOptions,
                                          CreatePrimaryQueryIndexOptions, DropQueryIndexOptions,
                                          DropPrimaryQueryIndexOptions, BuildDeferredQueryIndexOptions,
                                          WatchQueryIndexOptions)


@QueryErrorMapper.wrap
class AQueryIndexManager(object):
    _HANDLE_ERRORS_ASYNC = True

    def __init__(self,         # type: "AQueryIndexManager"
                 admin_bucket  # type: Admin
                 ):
        """
        Query Index Manager
        The Query Index Manager interface contains the means for managing indexes used for queries.
        :param parent_cluster: Parent cluster
        """
        self._admin_bucket = admin_bucket

    def _http_request(self,      # type: "AQueryIndexManager"
                      **kwargs,  # type: Dict[str,Any]
                      ) -> Awaitable:
        # the kwargs can override the defaults
        imeth = None
        method = kwargs.get('method', 'GET')
        if not method in METHMAP:
            raise InvalidArgumentException("Unknown HTTP Method", method)

        imeth = METHMAP[method]
        return self._admin_bucket._http_request(
            type=LCB.LCB_HTTP_TYPE_QUERY,
            path=kwargs['path'],
            method=imeth,
            content_type=kwargs.get('content_type', 'application/json'),
            post_data=kwargs.get('content', None),
            response_format=LCB.FMT_JSON,
            timeout=kwargs.get('timeout', None))

    def _on_create_possibly_raise(self,     # type: "AQueryIndexManager"
                                  error,    # type: Union[str, Any]
                                  **kwargs  # type: Dict[str,Any]
                                  ) -> None:
        if isinstance(error, list) and "msg" in error[0] and "already exists" in error[0]["msg"]:
            if not kwargs.get('ignore_if_exists', False):
                return True

    def _on_drop_possibly_raise(self,       # type: "AQueryIndexManager"
                                error,      # type: Union[str, Any]
                                **kwargs    # type: Any
                                ) -> None:
        if isinstance(error, list) and "msg" in error[0] and "not found" in error[0]["msg"]:
            if not kwargs.get('ignore_if_not_exists', False):
                return True

    def _validate_scope_and_collection(self,  # type: "AQueryIndexManager"
                                       scope=None,  # type: str
                                       collection=None  # type: str
                                       ) -> bool:
        if not (scope and scope.split()) and (collection and collection.split()):
            raise InvalidArgumentException(
                "Both scope and collection must be set.  Invalid scope.")
        if (scope and scope.split()) and not (collection and collection.split()):
            raise InvalidArgumentException(
                "Both scope and collection must be set.  Invalid collection.")

    def _build_keyspace(self,  # type: "AQueryIndexManager"
                        bucket,  # type: str
                        scope=None,  # type: str
                        collection=None  # type: str
                        ) -> str:

        # None AND empty check done in validation, only check for None
        if scope and collection:
            return "`{}`.`{}`.`{}`".format(bucket, scope, collection)

        if scope:
            return "`{}`.`{}`".format(bucket, scope)

        return "`{}`".format(bucket)

    def _create_index(self,             # type: "AQueryIndexManager"
                      bucket_name,      # type: str
                      fields,           # type: Iterable[str]
                      index_name=None,  # type: str
                      **kwargs          # type: Dict[str,Any]
                      ) -> Awaitable:

        scope_name = kwargs.get("scope_name", None)
        collection_name = kwargs.get("collection_name", None)
        self._validate_scope_and_collection(scope_name, collection_name)

        primary = kwargs.get("primary", False)
        condition = kwargs.get("condition", None)

        if primary and fields:
            raise TypeError('Cannot create primary index with explicit fields')
        elif not primary and not fields:
            raise ValueError('Fields required for non-primary index')

        if condition and primary:
            raise ValueError('cannot specify condition for primary index')

        query_str = ""

        if not fields:
            query_str += "CREATE PRIMARY INDEX"
        else:
            query_str += "CREATE INDEX"

        if index_name and index_name.split():
            query_str += " `{}` ".format(index_name)

        query_str += " ON {} ".format(self._build_keyspace(
            bucket_name, scope_name, collection_name))

        if fields:
            field_names = ["`{}`".format(f) for f in fields]
            query_str += "({})".format(", ".join(field_names))

        if condition:
            query_str += " WHERE {}".format(condition)

        options = {}
        deferred = kwargs.get("deferred", False)
        if deferred:
            options["defer_build"] = deferred

        num_replicas = kwargs.get("num_replicas", None)
        if num_replicas:
            options["num_replica"] = num_replicas

        if options:
            query_str += " WITH {{{}}}".format(
                ", ".join(["'{0}':{1}".format(k, v) for k, v in options.items()]))

        result = self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **kwargs)

        ft = asyncio.Future()

        def on_ok(res):
            response = res.value
            if "errors" in response and self._on_create_possibly_raise(response["errors"], **kwargs):
                msg = response["errors"][0].get("msg", "Index already exists")
                ft.set_exception(QueryIndexAlreadyExistsException.pyexc(
                    msg, response["errors"]))
            else:
                ft.set_result(response)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            if isinstance(err, HTTPException):
                error = getattr(
                    getattr(
                        err,
                        'objextra',
                        None),
                    'value',
                    {}).get(
                    'errors',
                    "")
                if self._on_create_possibly_raise(error, **kwargs):
                    ft.set_exception(err)
                else:
                    ft.set_result(True)
            else:
                ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def _drop_index(self,             # type: "AQueryIndexManager"
                    bucket_name,      # type: str
                    index_name=None,  # type: str
                    **kwargs          # type: Dict[str,Any]
                    ) -> Awaitable:

        scope_name = kwargs.get("scope_name", None)
        collection_name = kwargs.get("collection_name", None)
        self._validate_scope_and_collection(scope_name, collection_name)

        # previous ignore_missing was a viable kwarg - should only have ignore_if_not_exists
        ignore_missing = kwargs.pop("ignore_missing", None)
        if ignore_missing:
            kwargs["ignore_if_not_exists"] = ignore_missing

        query_str = ""
        keyspace = self._build_keyspace(
            bucket_name, scope_name, collection_name)
        if not index_name:
            query_str += "DROP PRIMARY INDEX ON {}".format(keyspace)
        else:
            if scope_name and collection_name:
                query_str += "DROP INDEX `{0}` ON {1}".format(
                    index_name, keyspace)
            else:
                query_str += "DROP INDEX {0}.`{1}`".format(
                    keyspace, index_name)

        result = self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **kwargs)

        ft = asyncio.Future()

        def on_ok(res):
            response = res.value
            if "errors" in response and self._on_drop_possibly_raise(response["errors"], **kwargs):
                msg = response["errors"][0].get("msg", "Index not found")
                ft.set_exception(QueryIndexNotFoundException.pyexc(
                    msg, response["errors"]))
            else:
                ft.set_result(response)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            if isinstance(err, HTTPException):
                error = getattr(
                    getattr(
                        err,
                        'objextra',
                        None),
                    'value',
                    {}).get(
                    'errors',
                    "")
                if self._on_drop_possibly_raise(error, **kwargs):
                    ft.set_exception(err)
                else:
                    ft.set_result(True)
            else:
                ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def get_all_indexes(self,           # type: "AQueryIndexManager"
                        bucket_name,    # type: str
                        *options,       # type: "GetAllQueryIndexOptions"
                        **kwargs        # type: Dict[str,Any]
                        ):
        # type: (...) -> List[QueryIndex]
        """
        Fetches all indexes from the server.

        :param str bucket_name: the name of the bucket.
        :param GetAllQueryIndexOptions options: Options to use for getting all indexes.
        :param Any kwargs: Override corresponding value in options.
        :return: A list of QueryIndex objects.
        :raises: InvalidArgumentsException
        """

        final_args = forward_args(kwargs, *options)

        scope_name = final_args.get("scope_name", None)
        collection_name = final_args.get("collection_name", None)

        if scope_name and collection_name:
            query_str = """
            SELECT idx.* FROM system:indexes AS idx
            WHERE `bucket_id`="{0}" AND `scope_id`="{1}"
                AND `keyspace_id`="{2}" AND `using`="gsi"
            ORDER BY is_primary DESC, name ASC
            """.format(bucket_name, scope_name, collection_name)
        elif scope_name:
            query_str = """
            SELECT idx.* FROM system:indexes AS idx
            WHERE `bucket_id`="{0}" AND `scope_id`="{1}" AND `using`="gsi"
            ORDER BY is_primary DESC, name ASC
            """.format(bucket_name, scope_name)
        else:
            query_str = """
            SELECT idx.* FROM system:indexes AS idx
            WHERE (
                (`bucket_id` IS MISSING AND `keyspace_id`="{0}")
                OR `bucket_id`="{0}"
            ) AND `using`="gsi"
            ORDER BY is_primary DESC, name ASC
            """.format(bucket_name)

        result = self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **final_args)

        ft = asyncio.Future()

        def on_ok(res):
            response = res.value
            indexes = []
            if response and "results" in response:
                indexes = list(
                    map(QueryIndex.from_server, response.get("results")))

            ft.set_result(indexes)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    async def create_index(self,    # type: "AQueryIndexManager"
                           bucket_name,   # type: str
                           index_name,    # type: str
                           fields,        # type: Iterable[str]
                           *options,      # type: "CreateQueryIndexOptions"
                           **kwargs       # type: Dict[str,Any]
                           ):
        # type: (...) -> None
        """
        Creates a new index.

        :param str bucket_name: name of the bucket.
        :param str index_name: the name of the index.
        :param Iterable[str] fields: Fields over which to create the index.
        :param CreateQueryIndexOptions options: Options to use when creating index.
        :param Any kwargs: Override corresponding value in options.
        :raises: QueryIndexAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # CREATE INDEX index_name ON bucket_name WITH { "num_replica": 2 }
        #         https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/createindex.html
        #

        final_args = forward_args(kwargs, *options)
        await self._create_index(bucket_name, fields, index_name, **final_args)

    async def create_primary_index(self,    # type: "AQueryIndexManager"
                                   bucket_name,   # type: str
                                   *options,      # type: "CreatePrimaryQueryIndexOptions"
                                   **kwargs       # type: Dict[str,Any]
                                   ):
        """
        Creates a new primary index.

        :param str bucket_name:  name of the bucket.
        :param str index_name: name of the index.
        :param CreatePrimaryQueryIndexOptions options: Options to use when creating primary index
        :param Any kwargs: Override corresponding values in options.
        :raises: QueryIndexAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # CREATE INDEX index_name ON bucket_name WITH { "num_replica": 2 }
        #         https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/createindex.html
        #

        kwargs['primary'] = True
        final_args = forward_args(kwargs, *options)
        index_name = final_args.pop("index_name", None)
        await self._create_index(bucket_name, [], index_name, **final_args)

    async def drop_index(self,      # type: "AQueryIndexManager"
                         bucket_name,     # type: str
                         index_name,      # type: str
                         *options,        # type: "DropQueryIndexOptions"
                         **kwargs         # type: Dict[str,Any]
                         ) -> None:
        """
        Drops an index.

        :param str bucket_name: name of the bucket.
        :param str index_name: name of the index.
        :param DropQueryIndexOptions options: Options for dropping index.
        :param Any kwargs: Override corresponding value in options.
        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        final_args = forward_args(kwargs, *options)
        await self._drop_index(bucket_name, index_name, **final_args)

    async def drop_primary_index(self,      # type: "AQueryIndexManager"
                                 bucket_name,     # type: str
                                 *options,        # type: "DropPrimaryQueryIndexOptions"
                                 **kwargs         # type: Dict[str,Any]
                                 ) -> None:
        """
        Drops a primary index.

        :param bucket_name: name of the bucket.
        :param index_name:  name of the index.
        :param ignore_if_not_exists: Don't error/throw if the index does not exist.
        :param timeout:  the time allowed for the operation to be terminated. This is controlled by the client.

        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        final_args = forward_args(kwargs, *options)
        index_name = final_args.pop("index_name", None)
        await self._drop_index(bucket_name, index_name, **final_args)

    def _get_build_deferred_indexes_future(self,         # type: "AQueryIndexManager"
                                           query_str,    # type: str
                                           **final_args  # type: Dict[str,Any]
                                           ) -> Awaitable:
        result = self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **final_args
        )

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    async def _build_deferred_prior_6_5(self, bucket_name, **final_args):
        """
        ** INTERNAL **
        """
        indexes = await self.get_all_indexes(bucket_name, GetAllQueryIndexOptions(
            timeout=final_args.get("timeout", None)))
        deferred_indexes = [
            idx.name for idx in indexes if idx.state in ["deferred", "pending"]]
        query_str = "BUILD INDEX ON `{}` ({})".format(
            bucket_name, ", ".join(["`{}`".format(di) for di in deferred_indexes]))

        await self._get_build_deferred_indexes_future(query_str, **final_args)

    async def _build_deferred_6_5_plus(self, bucket_name, **final_args):
        """
        ** INTERNAL **
        """
        scope_name = final_args.get("scope_name", None)
        collection_name = final_args.get("collection_name", None)

        self._validate_scope_and_collection(scope_name, collection_name)

        keyspace = self._build_keyspace(
            bucket_name, scope_name, collection_name)

        if scope_name and collection_name:
            inner_query_str = """
            SELECT RAW idx.name FROM system:indexes AS idx
            WHERE `bucket_id`="{0}" AND `scope_id`="{1}"
                AND `keyspace_id`="{2}" AND state="deferred"
            """.format(bucket_name, scope_name, collection_name)
        else:
            inner_query_str = """
            SELECT RAW idx.name FROM system:indexes AS idx
            WHERE (
                (`bucket_id` IS MISSING AND `keyspace_id`="{0}")
                OR `bucket_id`="{0}"
            ) AND state="deferred"
            """.format(bucket_name)

        query_str = "BUILD INDEX ON {} (({}))".format(
            keyspace, inner_query_str)

        await self._get_build_deferred_indexes_future(query_str, **final_args)

    async def build_deferred_indexes(self,            # type: "AQueryIndexManager"
                                     bucket_name,     # type: str
                                     *options,        # type: "BuildDeferredQueryIndexOptions"
                                     **kwargs         # type: Dict[str,Any]
                                     ) -> Awaitable:
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param str bucket_name: name of the bucket.
        :param BuildDeferredQueryIndexOptions options: Options for building deferred indexes.
        :param Any kwargs: Override corresponding value in options.
        :raise: InvalidArgumentsException

        """

        final_args = forward_args(kwargs, *options)
        is_6_5_plus = await self._admin_bucket._is_6_5_plus_async()
        if is_6_5_plus:
            await self._build_deferred_6_5_plus(bucket_name, **final_args)
        else:
            await self._build_deferred_prior_6_5(bucket_name, **final_args)

    async def watch_indexes(self,         # type: "AQueryIndexManager"
                            bucket_name,  # type: str
                            index_names,  # type: Iterable[str]
                            *options,     # type: "WatchQueryIndexOptions"
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
        final_args = forward_args(kwargs, *options)

        scope_name = final_args.get("scope_name", None)
        collection_name = final_args.get("collection_name", None)

        self._validate_scope_and_collection(scope_name, collection_name)

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
        start = time.perf_counter()
        time_left = timeout_millis
        while True:

            opts = GetAllQueryIndexOptions(
                timeout=timedelta(milliseconds=time_left))
            if scope_name:
                opts["scope_name"] = scope_name
                opts["collection_name"] = collection_name

            indexes = await self.get_all_indexes(bucket_name, opts)

            all_online = check_indexes(index_names, indexes)
            if all_online:
                break

            interval_millis += 500
            if interval_millis > 1000:
                interval_millis = 1000

            time_left = timeout_millis - ((time.perf_counter() - start) * 1000)
            if interval_millis > time_left:
                interval_millis = time_left

            if time_left <= 0:
                raise WatchQueryIndexTimeoutException(
                    "Failed to find all indexes online within the alloted time.")

            await asyncio.sleep(interval_millis / 1000)
