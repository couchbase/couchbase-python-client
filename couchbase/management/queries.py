import json
import time
import attr
from attr.validators import instance_of as io, optional
from typing import *

import couchbase_core._libcouchbase as LCB
from couchbase_core import mk_formstr
from couchbase.options import OptionBlock, OptionBlockTimeOut, forward_args, timedelta
from couchbase.management.admin import METHMAP
from couchbase.management.generic import GenericManager

from couchbase.exceptions import (ErrorMapper, HTTPException, QueryIndexAlreadyExistsException,
                                  WatchQueryIndexTimeoutException, QueryIndexNotFoundException,
                                  InvalidArgumentException)

try:
    from typing import Protocol
except BaseException:
    from typing_extensions import Protocol


class QueryErrorMapper(ErrorMapper):
    @staticmethod
    def mapping():
        # type: (...) -> Dict[CBErrorType,Dict[Any, CBErrorType]]
        return {HTTPException: {".*[iI]ndex.*already exists.*": QueryIndexAlreadyExistsException,
                                ".*[iI]ndex.*[nN]ot [fF]ound.*": QueryIndexNotFoundException}}


def is_null_or_empty(
    value  # type: str
) -> bool:
    return not (value and value.split())


@QueryErrorMapper.wrap
class QueryIndexManager(GenericManager):
    def __init__(self, parent_cluster):
        """
        Query Index Manager
        The Query Index Manager interface contains the means for managing indexes used for queries.
        :param parent_cluster: Parent cluster
        """
        super(QueryIndexManager, self).__init__(parent_cluster)

    def _http_request(self, **kwargs):
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

    def _validate_scope_and_collection(self,  # type: "QueryIndexManager"
                                       scope=None,  # type: str
                                       collection=None  # type: str
                                       ) -> bool:
        if not (scope and scope.split()) and (collection and collection.split()):
            raise InvalidArgumentException(
                "Both scope and collection must be set.  Invalid scope.")
        if (scope and scope.split()) and not (collection and collection.split()):
            raise InvalidArgumentException(
                "Both scope and collection must be set.  Invalid collection.")

    def _build_keyspace(self,  # type: "QueryIndexManager"
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

    def _create_index(self, bucket_name, fields,
                      index_name=None, **kwargs):

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

        def possibly_raise(error):
            if isinstance(error, list) and "msg" in error[0] and "already exists" in error[0]["msg"]:
                if not kwargs.get('ignore_if_exists', False):
                    raise

        try:
            resp = self._http_request(
                path="",
                method="POST",
                content=mk_formstr({"statement": query_str}),
                content_type='application/x-www-form-urlencoded',
                **kwargs
            ).value
            if "errors" in resp and possibly_raise(resp["errors"]):
                msg = resp["errors"][0].get("msg", "Index already exists")
                raise QueryIndexAlreadyExistsException.pyexc(
                    msg, resp["errors"])
        except HTTPException as h:
            error = getattr(
                getattr(
                    h,
                    'objextra',
                    None),
                'value',
                {}).get(
                'errors',
                "")
            if possibly_raise(error):
                raise

    def _drop_index(self, bucket_name, index_name=None, **kwargs):

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

        def possibly_raise(error):
            if isinstance(error, list) and "msg" in error[0] and "not found" in error[0]["msg"]:
                if not kwargs.get('ignore_if_not_exists', False):
                    return True
        try:
            resp = self._http_request(
                path="",
                method="POST",
                content=mk_formstr({"statement": query_str}),
                content_type='application/x-www-form-urlencoded',
                **kwargs
            ).value
            if "errors" in resp and possibly_raise(resp["errors"]):
                msg = resp["errors"][0].get("msg", "Index not found")
                raise QueryIndexNotFoundException.pyexc(msg, resp["errors"])
        except HTTPException as h:
            error = getattr(
                getattr(
                    h,
                    'objextra',
                    None),
                'value',
                {}).get(
                'errors',
                "")
            if possibly_raise(error):
                raise

    def get_all_indexes(self,           # type: "QueryIndexManager"
                        bucket_name,    # type: str
                        *options,       # type: "GetAllQueryIndexOptions"
                        **kwargs        # type: Any
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

        response = self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **final_args
        ).value

        if response and "results" in response:
            results = response.get("results")
            res = list(map(QueryIndex.from_server, results))
            return res

        return []

    def create_index(self,          # type: "QueryIndexManager"
                     bucket_name,   # type: str
                     index_name,    # type: str
                     fields,        # type: Iterable[str]
                     *options,      # type: "CreateQueryIndexOptions"
                     **kwargs
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
        self._create_index(bucket_name, fields, index_name, **final_args)

    def create_primary_index(self,  # type: "QueryIndexManager"
                             bucket_name,  # type: str
                             *options,  # type: "CreatePrimaryQueryIndexOptions"
                             **kwargs
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
        self._create_index(bucket_name, [], index_name, **final_args)

    def drop_index(self,            # type: "QueryIndexManager"
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: "DropQueryIndexOptions"
                   **kwargs):
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
        self._drop_index(bucket_name, index_name, **final_args)

    def drop_primary_index(self,            # type: "QueryIndexManager"
                           bucket_name,     # type: str
                           *options,        # type: "DropPrimaryQueryIndexOptions"
                           **kwargs):
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
        self._drop_index(bucket_name, index_name, **final_args)

    def watch_indexes(self,         # type: "QueryIndexManager"
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      *options,     # type: "WatchQueryIndexOptions"
                      **kwargs):
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

            indexes = self.get_all_indexes(bucket_name, opts)

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

            time.sleep(interval_millis / 1000)

    def _build_deferred_prior_6_5(self, bucket_name, **final_args):
        """
        ** INTERNAL **
        """
        indexes = self.get_all_indexes(bucket_name, GetAllQueryIndexOptions(
            timeout=final_args.get("timeout", None)))
        deferred_indexes = [
            idx.name for idx in indexes if idx.state in ["deferred", "pending"]]
        query_str = "BUILD INDEX ON `{}` ({})".format(
            bucket_name, ", ".join(["`{}`".format(di) for di in deferred_indexes]))

        self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **final_args
        )

    def _build_deferred_6_5_plus(self, bucket_name, **final_args):
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

        self._http_request(
            path="",
            method="POST",
            content=mk_formstr({"statement": query_str}),
            content_type='application/x-www-form-urlencoded',
            **final_args
        )

    def build_deferred_indexes(self,            # type: "QueryIndexManager"
                               bucket_name,     # type: str
                               *options,        # type: "BuildDeferredQueryIndexOptions"
                               **kwargs
                               ):
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param str bucket_name: name of the bucket.
        :param BuildDeferredQueryIndexOptions options: Options for building deferred indexes.
        :param Any kwargs: Override corresponding value in options.
        :raise: InvalidArgumentsException

        """
        final_args = forward_args(kwargs, *options)
        if self._admin_bucket._is_6_5_plus():
            self._build_deferred_6_5_plus(bucket_name, **final_args)
        else:
            self._build_deferred_prior_6_5(bucket_name, **final_args)


class IndexType(object):
    pass


@attr.s
class QueryIndex(Protocol):
    """The QueryIndex protocol provides a means of mapping a query index into an object."""

    name = attr.ib(validator=io(str))  # type: str
    is_primary = attr.ib(validator=io(bool))  # type: bool
    type = attr.ib(validator=io(IndexType), type=IndexType)  # type: IndexType
    state = attr.ib(validator=io(str))  # type: str
    namespace = attr.ib(validator=io(str))  # type: str
    keyspace = attr.ib(validator=io(str))  # type: str
    index_key = attr.ib(validator=io(Iterable))  # type: Iterable[str]
    condition = attr.ib(validator=io(str))  # type: str
    bucket_name = attr.ib(validator=optional(io(str)))  # type: Optional[str]
    scope_name = attr.ib(validator=optional(io(str)))  # type: Optional[str]
    collection_name = attr.ib(
        validator=optional(io(str)))  # type: Optional[str]
    partition = attr.ib(validator=optional(
        validator=io(str)))  # type: Optional[str]

    @classmethod
    def from_server(cls,
                    json_data  # type: Dict[str, Any]
                    ):

        return cls(json_data.get("name"),
                   bool(json_data.get("is_primary")),
                   IndexType(),
                   json_data.get("state"),
                   json_data.get("keyspace_id"),
                   json_data.get("namespace_id"),
                   [],
                   json_data.get("condition", ""),
                   json_data.get(
                       "bucket_id", json_data.get("keyspace_id", "")),
                   json_data.get("scope_id", ""),
                   json_data.get("keyspace_id", ""),
                   json_data.get("partition", None)
                   )


class GetAllQueryIndexOptions(OptionBlockTimeOut):
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
        super(GetAllQueryIndexOptions, self).__init__(**kwargs)


class CreateQueryIndexOptions(OptionBlockTimeOut):
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
        super(CreateQueryIndexOptions, self).__init__(**kwargs)


class CreatePrimaryQueryIndexOptions(CreateQueryIndexOptions):
    @overload
    def __init__(self,
                 index_name=None,        # type: str
                 timeout=None,           # type: timedelta
                 ignore_if_exists=None,  # type: bool
                 num_replicas=None,      # type: int
                 deferred=None,          # type: bool
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
        super(CreatePrimaryQueryIndexOptions, self).__init__(**kwargs)


class DropQueryIndexOptions(OptionBlockTimeOut):
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
        super(DropQueryIndexOptions, self).__init__(**kwargs)


class DropPrimaryQueryIndexOptions(OptionBlockTimeOut):
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
        super(DropPrimaryQueryIndexOptions, self).__init__(**kwargs)


class WatchQueryIndexOptions(OptionBlock):
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
        super(WatchQueryIndexOptions, self).__init__(**kwargs)


class BuildDeferredQueryIndexOptions(OptionBlockTimeOut):
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
        super(BuildDeferredQueryIndexOptions, self).__init__(**kwargs)
