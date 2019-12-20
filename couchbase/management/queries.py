from couchbase.exceptions import KeyNotFoundException, KeyExistsException
from couchbase_core._ixmgmt import N1QL_PRIMARY_INDEX, IxmgmtRequest, N1qlIndex
from couchbase_core.bucketmanager import BucketManager
from couchbase.options import (OptionBlockTimeOut, timeout_forward_args as forward_args, timedelta)
from typing import *
from couchbase.management.generic import GenericManager
import attr
from attr.validators import instance_of as io, deep_mapping as dm
from couchbase_core._pyport import Protocol

from couchbase_core.exceptions import HTTPError, ErrorMapper, AnyPattern


class QueryIndexNotFoundException(KeyNotFoundException):
    pass


class IndexAlreadyExistsException(KeyExistsException):
    pass


class QueryErrorMapper(ErrorMapper):
    @staticmethod
    def mapping():
        # type: (...) -> Dict[CBErrorType,Dict[Any, CBErrorType]]
        return {KeyNotFoundException: {AnyPattern(): QueryIndexNotFoundException},
                KeyExistsException: {AnyPattern(): IndexAlreadyExistsException}}


@QueryErrorMapper.wrap
class QueryIndexManager(GenericManager):
    def __init__(self, parent_cluster):
        """
        Query Index Manager
        The Query Index Manager interface contains the means for managing indexes used for queries.
        :param parent_cluster: Parent cluster
        """
        super(QueryIndexManager,self).__init__(parent_cluster)

    @overload
    def get_all_indexes(self,  # type: QueryIndexManager
                        bucket_name,  # type: str
                        timeout=None,  # type: timedelta
                        ):
        pass

    @overload
    def get_all_indexes(self,  # type: QueryIndexManager
                        bucket_name,  # type: str
                        options,  # type: GetAllQueryIndexOptions
                        ):
        pass

    def get_all_indexes(self,  # type: QueryIndexManager
                        bucket_name,  # type: str
                        *options,  # type: GetAllQueryIndexOptions
                        **kwargs
                        ):
        # type: (...) -> List[QueryIndex]
        """
        Fetches all indexes from the server.

        :param str bucket_name: the name of the bucket.
        :param timedelta timeout: the time allowed for the operation to be terminated. This is controlled by the client.
        :return: A list of QueryIndex objects.


        :raises: InvalidArgumentsException
        """
        # N1QL
        # SELECT idx.* FROM system:indexes AS idx
        # WHERE keyspace_id = "bucket_name"
        # ORDER BY is_primary DESC, name ASC
        info = N1qlIndex()
        info.keyspace = bucket_name
        response = IxmgmtRequest(self._admin_bucket, 'list', info).execute()
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
        :raise: :exc:`~.KeyExistsError` if the index already exists

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

    @overload
    def create_index(self,  # type: QueryIndexManager
                     bucket_name,  # type: str
                     index_name,  # type: str
                     fields,  # type: Iterable[str]
                     options  # type: CreateQueryIndexOptions
                     ):
        pass

    @overload
    def create_index(self,  # type: QueryIndexManager
                     bucket_name,  # type: str
                     index_name,  # type: str
                     fields,  # type: Iterable[str]
                     ignore_if_exists=None,  # type: bool
                     num_replicas=0,  # type: int
                     deferred=False,  # type: bool
                     timeout=None,  # type: timedelta
                     condition=None  # type: str
                     ):
        pass

    def create_index(self,  # type: QueryIndexManager
                     bucket_name,  # type: str
                     index_name,  # type: str
                     fields,  # type: Iterable[str]
                     *options,  # type: CreateQueryIndexOptions
                     **kwargs
                     ):
        """
        Creates a new index.

        :param: str bucket_name: name of the bucket.
        :param str index_name: the name of the index.
        :param Iterable[str] fields: the fields to create the index over.
        :param bool ignore_if_exists: Don't error/throw if the index already exists.
        :param int num_replicas: The number of replicas that this index should have. Uses the WITH keyword and num_replica.
        :param bool deferred: Whether the index should be created as a deferred index.
        :param timedelta timeout:  the time allowed for the operation to be terminated. This is controlled by the client.
        :param condition: condition on which to filter
        :raises: IndexAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # CREATE INDEX index_name ON bucket_name WITH { "num_replica": 2 }
        #         https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/createindex.html
        #
        self._create_index(bucket_name, fields, index_name, **kwargs)

    def _create_index(self, bucket_name, fields, index_name, *options, **kwargs):
        final_args = {
            k.replace('deferred', 'defer').replace('condition', 'cond').replace('ignore_if_exists', 'ignore_exists'): v
            for k, v in forward_args(kwargs, *options).items()}
        self._n1ql_index_create(bucket_name, index_name, fields=fields, **final_args)

    @overload
    def create_primary_index(self,  # type: QueryIndexManager
                             bucket_name,  # type: str
                             index_name="",  # type: str
                             deferred=False,  # type: bool
                             ignore_if_exists=False,  # type: bool
                             num_replicas=None,  # type: int
                             timeout=None  # type: timedelta
                             ):
        pass

    def create_primary_index(self,  # type: QueryIndexManager
                             bucket_name,  # type: str
                             *options,  # type: CreatePrimaryQueryIndexOptions
                             **kwargs
        ):
        """
        Creates a new primary index.

        :param str bucket_name:  name of the bucket.
        :param str index_name: name of the index.
        :param book deferred: Whether the index should be created as a deferred index.
        :param bool ignore_if_exists:  Don't error/throw if the index already exists.
        :param int num_replicas: The number of replicas that this index should have. Uses the WITH keyword and num_replica.
        :param: timedelta timeout: the time allowed for the operation to be terminated. This is controlled by the client.

        :raises: QueryIndexAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # CREATE INDEX index_name ON bucket_name WITH { "num_replica": 2 }
        #         https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/createindex.html
        #
        self._create_index(bucket_name, "", [], primary=True, *options, **kwargs)

    def _drop_index(self, bucket_name, index_name, *options, **kwargs):
        info = BucketManager._mk_index_def(bucket_name, index_name, primary=kwargs.pop('primary',False))
        final_args = {k.replace('ignore_if_not_exists','ignore_missing'):v for k,v in  forward_args(kwargs, *options).items()}
        IxmgmtRequest(self._admin_bucket, 'drop', info, **final_args).execute()

    @overload
    def drop_index(self,  # type: QueryIndexManager
                   bucket_name,  # type: str
                   index_name,  # type: str
                   ignore_if_not_exists=False,  # type: bool
                   timeout=None  # type: timedelta
                   ):
        pass

    def drop_index(self,  # type: QueryIndexManager
                   bucket_name,  # type: str
                   index_name,  # type: str
                   *options,  # type: DropQueryIndexOptions
                   **kwargs):
        """
        Drops an index.

        :param str bucket_name: name of the bucket.
        :param str index_name: name of the index.
        :param bool ignore_if_not_exists: Don't error/throw if the index does not exist.
        :param timedelta timeout: the time allowed for the operation to be terminated. This is controlled by the client.

        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        self._drop_index(bucket_name, index_name, *options, **kwargs)

    @overload
    def drop_primary_index(self,  # type: QueryIndexManager
                           bucket_name,  # type: str
                           index_name="",  # type: str
                           ignore_if_not_exists=False,  # type: str
                           timeout=None  # type: timedelta
                           ):
        pass

    def drop_primary_index(self,  # type: QueryIndexManager
                           bucket_name,  # type: str
                           *options,  # type: DropPrimaryQueryIndexOptions
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
        self._drop_index(bucket_name, "", primary=True, **kwargs)

    @overload
    def watch_indexes(self,  # type: QueryIndexManager
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      timeout=None,  # type: timedelta
                      ):
        pass

    def watch_indexes(self,  # type: QueryIndexManager
                      bucket_name,  # type: str
                      index_names,  # type: Iterable[str]
                      *options,  # type: WatchQueryIndexOptions
                      **kwargs):
        """
        Watch polls indexes until they are online.

        :param str bucket_name: name of the bucket.
        :param Iterable[str] index_names: name(s) of the index(es).
        :param timedelta timeout: the time allowed for the operation to be terminated. This is controlled by the client.
        :param: bool watch_primary: whether or not to watch the primary index.

        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        final_args=forward_args(kwargs, *options)
        BucketManager(self._admin_bucket).n1ql_index_watch(index_names, **final_args)

    @overload
    def build_deferred_indexes(self,  # type: QueryIndexManager
                               bucket_name,  # type: str
                               timeout=None  # type: timedelta
                               ):
        pass

    def build_deferred_indexes(self,  # type: QueryIndexManager
                               bucket_name,  # type: str
                               *options,  # type: BuildQueryIndexOptions
                               **kwargs
                               ):
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param bucket_name: name of the bucket.
        :param timeout: the time allowed for the operation to be terminated. This is controlled by the client.

        :raise: InvalidArgumentsException

        """
        return BucketManager._n1ql_index_build_deferred(bucket_name, self._admin_bucket, **kwargs)


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
    pass


class CreatePrimaryQueryIndexOptions(OptionBlockTimeOut):
    pass


class DropQueryIndexOptions(OptionBlockTimeOut):
    pass


class DropPrimaryQueryIndexOptions(OptionBlockTimeOut):
    pass


class WatchQueryIndexOptions(OptionBlockTimeOut):
    pass


class BuildQueryIndexOptions(OptionBlockTimeOut):
    pass
