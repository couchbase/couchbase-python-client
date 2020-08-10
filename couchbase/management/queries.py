from couchbase_core._ixmgmt import N1QL_PRIMARY_INDEX, IxmgmtRequest, N1qlIndex
from couchbase_core.bucketmanager import BucketManager
from couchbase.options import OptionBlock, OptionBlockTimeOut, forward_args, timedelta
from typing import *
from couchbase.management.generic import GenericManager
import attr
from attr.validators import instance_of as io, deep_mapping as dm
from couchbase_core._pyport import Protocol
from couchbase.exceptions import HTTPException, ErrorMapper, AnyPattern, QueryIndexAlreadyExistsException, \
    QueryIndexNotFoundException, DocumentNotFoundException, DocumentExistsException


class QueryErrorMapper(ErrorMapper):
    @staticmethod
    def mapping():
        # type: (...) -> Dict[CBErrorType,Dict[Any, CBErrorType]]
        return {DocumentNotFoundException: {AnyPattern(): QueryIndexNotFoundException},
                DocumentExistsException: {AnyPattern(): QueryIndexAlreadyExistsException}}


@QueryErrorMapper.wrap
class QueryIndexManager(GenericManager):
    def __init__(self, parent_cluster):
        """
        Query Index Manager
        The Query Index Manager interface contains the means for managing indexes used for queries.
        :param parent_cluster: Parent cluster
        """
        super(QueryIndexManager,self).__init__(parent_cluster)

    def get_all_indexes(self,           # type: QueryIndexManager
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
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
        # N1QL
        # SELECT idx.* FROM system:indexes AS idx
        # WHERE keyspace_id = "bucket_name"
        # ORDER BY is_primary DESC, name ASC
        info = N1qlIndex()
        info.keyspace = bucket_name
        response = IxmgmtRequest(self._admin_bucket, 'list', info, **forward_args(kwargs, *options)).execute()
        return list(map(QueryIndex.from_n1qlindex, response))

    def _mk_index_def(self, bucket_name, ix, primary=False):
        if isinstance(ix, N1qlIndex):
            return N1qlIndex(ix)

        info = N1qlIndex()
        info.keyspace = bucket_name
        info.primary = primary

        if ix:
            info.name = ix
        elif not primary:
            raise ValueError('Missing name for non-primary index')

        return info

    def _n1ql_index_create(self, bucket_name, ix, defer=False, ignore_exists=False, primary=False, fields=None, cond = None, timeout=None, **kwargs):
        """
        Create an index for use with N1QL.

        :param str ix: The name of the index to create
        :param bool defer: Whether the building of indexes should be
            deferred. If creating multiple indexes on an existing
            dataset, using the `defer` option in conjunction with
            :meth:`build_deferred_indexes` and :meth:`watch_indexes` may
            result in substantially reduced build times.
        :param bool ignore_exists: Do not throw an exception if the index
            already exists.
        :param Iterable[str] fields: A list of fields that should be supplied
            as keys for the index. For non-primary indexes, this must
            be specified and must contain at least one field name.
        :param bool primary: Whether this is a primary index. If creating
            a primary index, the name may be an empty string and `fields`
            must be empty.
        :param str condition: Specify a condition for indexing. Using
            a condition reduces an index size
        :raise: :exc:`~.DocumentExistsException` if the index already exists

        .. seealso:: :meth:`n1ql_index_create_primary`
        """
        fields = fields or []

        if kwargs:
            raise TypeError('Unknown keyword arguments', kwargs)

        info = self._mk_index_def(bucket_name, ix, primary)

        if primary and fields:
            raise TypeError('Cannot create primary index with explicit fields')
        elif not primary and not fields:
            raise ValueError('Fields required for non-primary index')

        if fields:
            info.fields = fields

        if primary and info.name is N1QL_PRIMARY_INDEX:
            del info.name

        if cond:
            if primary:
                raise ValueError('cannot specify condition for primary index')
            info.condition = cond

        options = {
            'ignore_exists': ignore_exists,
            'defer': defer
        }

        if timeout:
            options['timeout']=timeout
        # Now actually create the indexes
        return IxmgmtRequest(self._admin_bucket, 'create', info, **options).execute()

    def create_index(self,          # type: QueryIndexManager
                     bucket_name,   # type: str
                     index_name,    # type: str
                     fields,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
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
        self._create_index(bucket_name, fields, index_name, *options, **kwargs)

    def _create_index(self, bucket_name, fields, index_name, *options, **kwargs):
        final_args = {
            k.replace('deferred', 'defer').replace('condition', 'cond').replace('ignore_if_exists', 'ignore_exists'): v
            for k, v in forward_args(kwargs, *options).items()}
        try:
            self._n1ql_index_create(bucket_name, index_name, fields=fields, **final_args)
        except QueryIndexAlreadyExistsException:
            if not final_args.get('ignore_exists', False):
                raise


    def create_primary_index(self,  # type: QueryIndexManager
                             bucket_name,  # type: str
                             *options,  # type: CreatePrimaryQueryIndexOptions
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
        index_name = ""
        if options and options[0] :
            index_name = options[0].pop("index_name", "")
        fields = []
        self._create_index(bucket_name, fields, index_name, *options, **kwargs)

    def _drop_index(self, bucket_name, index_name, *options, **kwargs):
        info = BucketManager._mk_index_def(bucket_name, index_name, primary=kwargs.pop('primary',False))
        final_args = {k.replace('ignore_if_not_exists','ignore_missing'):v for k,v in  forward_args(kwargs, *options).items()}
        try:
            IxmgmtRequest(self._admin_bucket, 'drop', info, **final_args).execute()
        except QueryIndexNotFoundException:
            if not final_args.get("ignore_missing", False):
                raise

    def drop_index(self,            # type: QueryIndexManager
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
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

    def drop_primary_index(self,            # type: QueryIndexManager
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
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
        final_args=forward_args(kwargs, *options)
        final_args['primary'] = True
        index_name = final_args.pop("index_name", "")
        self._drop_index(bucket_name, index_name, **final_args)

    def watch_indexes(self,         # type: QueryIndexManager
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      *options,     # type: WatchQueryIndexOptions
                      **kwargs):
        """
        Watch polls indexes until they are online.

        :param str bucket_name: name of the bucket.
        :param Iterable[str] index_names: name(s) of the index(es).
        :param WatchQueryIndexOptions options: Options for request to watch indexes.
        :param Any kwargs: Override corresponding valud in options.
        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        final_args=forward_args(kwargs, *options)
        BucketManager(self._admin_bucket).n1ql_index_watch(index_names, **final_args)

    def build_deferred_indexes(self,            # type: QueryIndexManager
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ):
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param str bucket_name: name of the bucket.
        :param BuildDeferredQueryIndexOptions options: Options for building deferred indexes.
        :param Any kwargs: Override corresponding value in options.
        :raise: InvalidArgumentsException

        """
        final_args=forward_args(kwargs, *options)
        return BucketManager._n1ql_index_build_deferred(bucket_name, self._admin_bucket, **final_args)


class IndexType(object):
    pass


@attr.s
class QueryIndex(Protocol):
    """The QueryIndex protocol provides a means of mapping a query index into an object."""

    name = attr.ib(validator=io(str))  # type: str
    is_primary = attr.ib(validator=io(bool))  # type: bool
    type = attr.ib(validator=io(IndexType), type=IndexType)  # type: IndexType
    state = attr.ib(validator=io(str))  # type: str
    keyspace = attr.ib(validator=io(str))  # type: str
    index_key = attr.ib(validator=io(Iterable))  # type: Iterable[str]
    condition = attr.ib(validator=io(str))  # type: str
    @classmethod
    def from_n1qlindex(cls,
                       n1qlindex  # type: N1qlIndex
                       ):
        return cls(n1qlindex.name, bool(n1qlindex.primary), IndexType(), n1qlindex.state, n1qlindex.keyspace, [], n1qlindex.condition or "")


class GetAllQueryIndexOptions(OptionBlockTimeOut):
    pass


class CreateQueryIndexOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,           # type: timedelta
                 ignore_if_exists=None,  # type: bool
                 num_replicas=None,      # type: int
                 deferred=None,          # type: bool
                 condition=None,         # type: str
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
                 ):
        pass

    def __init__(self, **kwargs):
        super(CreatePrimaryQueryIndexOptions, self).__init__(**kwargs)


class DropQueryIndexOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 ignore_if_not_exists=None,  # type: bool
                 timeout=None                # type: timedelta
                 ):
        pass

    def __init__(self, **kwargs):
        super(DropQueryIndexOptions, self).__init__(**kwargs)


class DropPrimaryQueryIndexOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 index_name=None,            # str
                 ignore_if_not_exists=None,  # type: bool
                 timeout=None                # type: timedelta
                 ):
        pass

    def __init__(self, **kwargs):
        super(DropPrimaryQueryIndexOptions, self).__init__(**kwargs)


class WatchQueryIndexOptions(OptionBlock):
    @overload
    def __init__(self,
                 watch_primary=None  # type: bool
                 ):
        pass

    def __init__(self, **kwargs):
        super(WatchQueryIndexOptions, self).__init__(**kwargs)


class BuildDeferredQueryIndexOptions(OptionBlockTimeOut):
    pass
