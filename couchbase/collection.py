from couchbase_core.supportability import uncommitted, volatile
from couchbase_core import abstractmethod, JSON

from boltons.funcutils import wraps
from mypy_extensions import VarArg, KwArg, Arg

from .subdocument import LookupInSpec, MutateInSpec, MutateInOptions, \
    gen_projection_spec
from .result import GetResult, GetReplicaResult, ExistsResult, get_result_wrapper, CoreResult, ResultPrecursor, LookupInResult, MutateInResult, \
    MutationResult, _wrap_in_mutation_result, AsyncGetResult, get_replica_result_wrapper, get_mutation_result, get_multi_mutation_result
from .options import forward_args, timedelta, OptionBlockTimeOut, OptionBlockDeriv, ConstrainedInt, SignedInt64, \
    AcceptableInts
from .options import OptionBlock, AcceptableInts, OptionBlockBase
from .durability import ReplicateTo, PersistTo, ClientDurability, ServerDurability, DurabilityType, \
    ServerDurableOptionBlock, DurabilityOptionBlock
from couchbase_core._libcouchbase import Collection as _Base
import couchbase.exceptions
from couchbase_core.exceptions import NotSupportedError
from couchbase_core.client import Client as CoreClient
import copy

from typing import *
from couchbase.durability import Durability, DurabilityType, ServerDurability, ClientDurability, ServerDurableOptionBlock, ClientDurableOptionBlock, DurabilityOptionBlock
from couchbase_core.asynchronous.bucket import AsyncClientFactory
from datetime import timedelta

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict


class DeltaValue(ConstrainedInt):
    def __init__(self,
                 value  # type: AcceptableInts
                 ):
        # type: (...) -> None
        """
        A non-negative integer between 0 and +0x7FFFFFFFFFFFFFFF inclusive.
        Used as an argument for :meth:`Collection.increment` and :meth:`Collection.decrement`

        :param couchbase.options.AcceptableInts value: the value to initialise this with.

        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super(DeltaValue,self).__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return 0


class ReplaceOptions(DurabilityOptionBlock):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 durability=None,    # type: DurabilityType
                 cas=0               # type: int
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        super(ReplaceOptions, self).__init__(**kwargs)


class AppendOptions(OptionBlockTimeOut):
    pass


class RemoveOptions(DurabilityOptionBlock):
    @overload
    def __init__(self,
                 durability=None,  # type: DurabilityType
                 cas=0,            # type: int
                 **kwargs):
        """
        Remove Options

        :param DurabilityType durability: durability type

        :param int cas: The CAS to use for the removal operation.
        If specified, the key will only be removed from the server
        if it has the same CAS as specified. This is useful to
        remove a key only if its value has not been changed from the
        version currently visible to the client. If the CAS on the
        server does not match the one specified, an exception is
        thrown.
        """
        pass

    def __init__(self,
                 **kwargs
                 ):
        super(RemoveOptions, self).__init__(**kwargs)


class PrependOptions(OptionBlockTimeOut):
    pass


class UnlockOptions(OptionBlockTimeOut):
    pass


class CounterOptions(ServerDurableOptionBlock):
    pass


class CollectionOptions(OptionBlock):
    pass


class ExistsOptions(OptionBlockTimeOut):
    pass


class GetOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,  # type: timedelta
                 with_expiry=None,  # type: bool
                 project=None  # type: Iterable[str]
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(GetOptions, self).__init__(**kwargs)

    @property
    def with_expiry(self):
      # type: (...) -> bool
      return self.get('with_expiry', False)

    @property
    def project(self):
        # type: (...) -> Iterable[str]
        return self.get('project', [])


class GetAndTouchOptions(GetOptions):
    pass


class GetAndLockOptions(GetOptions):
    pass


class GetAnyReplicaOptions(GetOptions):
    pass


class GetAllReplicasOptions(GetOptions):
    pass


class InsertOptions(DurabilityOptionBlock):
    pass


class UpsertOptions(DurabilityOptionBlock):
    pass


T = TypeVar('T', bound='CBCollection')
R = TypeVar("R")

RawCollectionMethodDefault = Callable[
    [Arg('CBCollection', 'self'), Arg(str, 'key'), VarArg(OptionBlockDeriv), KwArg(Any)], R]
RawCollectionMethodInt = Callable[
    [Arg('CBCollection', 'self'), Arg(str, 'key'), int, VarArg(OptionBlockDeriv), KwArg(Any)], R]
RawCollectionMethod = Union[RawCollectionMethodDefault, RawCollectionMethodInt]
RawCollectionMethodSpecial = TypeVar('RawCollectionMethodSpecial', bound=RawCollectionMethod)

CoreBucketOpRead = TypeVar("CoreBucketOpRead", Callable[[Any], CoreResult], Callable[[Any], GetResult])


class BinaryCollection(object):
    pass


class TouchOptions(OptionBlockTimeOut):
    pass


class LookupInOptions(OptionBlockTimeOut):
    pass


CoreBucketOp = TypeVar("CoreBucketOp", Callable[[Any], CoreResult], Callable[[Any], MutationResult])


def _wrap_multi_mutation_result(wrapped  # type: CoreBucketOp
                               ):
    # type: (...) -> CoreBucketOp
    @wraps(wrapped)
    def wrapper(target, keys, *options, **kwargs
                ):
        return get_multi_mutation_result(target, wrapped, keys, *options, **kwargs)
    return volatile(wrapper)


class CBCollection(CoreClient):
    def __init__(self,
                 *args,
                 **kwargs
                 ):
        # type: (...) -> None
        """
        Couchbase collection. Should only be invoked by internal API, e.g.
        by :meth:`couchbase.collection.scope.Scope.collection` or
        :meth:`couchbase.bucket.Bucket.default_collection`.

        Args as for CoreClient, plus:

        :param couchbase.collections.Scope parent: parent scope
        :param str name: name of collection
        :param CollectionOptions options: miscellaneous options
        """
        name = kwargs.pop('name', None)
        parent = kwargs.pop('parent', None)
        args = list(args)
        connstr = kwargs.pop('connection_string', kwargs.pop('connstr', None))
        connstr = connstr or args.pop(0)
        final_args = [connstr] + args
        if parent:
            kwargs['scope']=parent.name
            kwargs['collection']=name
        super(CBCollection, self).__init__(*final_args, **kwargs)

    def _wrap_dsop(self, sdres, has_value=False, **kwargs):
        return getattr(super(CBCollection, self)._wrap_dsop(sdres, has_value), 'value')

    @classmethod
    def cast(cls,
             parent,    # type: Scope
             name,      # type: Optional[str]
             *options   # type: CollectionOptions
             ):
        # type: (...) -> CBCollection
        coll_args = copy.deepcopy(parent.bucket._bucket_args)
        coll_args.update(name=name, parent=parent)
        result = parent.bucket._corebucket_class(parent.bucket._connstr, **coll_args)
        return result

    @property
    def bucket(self):
        # type: (...) -> CoreClient
        return super(CBCollection,self)

    MAX_GET_OPS = 16

    def _get_generic(self, key, kwargs, options):
        opts = forward_args(kwargs, *options)
        opts.pop('key', None)
        spec = opts.pop('spec', [])
        project = opts.pop('project', None)
        with_expiry = opts.pop('with_expiry', False)
        if project:
            if len(project) <= CBCollection.MAX_GET_OPS:
                spec = gen_projection_spec(project)
            else:
                raise couchbase.exceptions.ArgumentError(
                    "Project only accepts {} operations or less".format(CBCollection.MAX_GET_OPS))
        if not project and not opts.get('with_expiry', False):
            x = super(CBCollection,self).get(key, **opts)
        else:
            # if you want the expiry, or a projection, need to do a subdoc lookup
            # NOTE: this currently doesn't work for with_expiry.  We need to add that
            x = super(CBCollection,self).lookup_in(key, spec, **opts)

        # NOTE: there is no reason for the options in the ResultPrecursor below.  Once
        # we get expiry done correctly, lets eliminate that as well.  Previously the
        # expiry you passed in was just duplicated into the result, which of course made
        # no sense since expiry should have been with_expiry (a bool) in the options.
        return ResultPrecursor(x, options)

    @get_result_wrapper
    def get(self,
            key,        # type: str
            *options,   # type: GetOptions
            **kwargs    # type: Any
            ):
        # type: (...) -> GetResult
        """Obtain an object stored in Couchbase by given key.

        :param string key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`
        :param: GetOptions options: The options to use for this get request.
        :param: Any kwargs: Override corresponding value in options.

        :raise: :exc:`.NotFoundError` if the key does not exist
        :return: A :class:`couchbase.result.GetResult` object

        Simple get::

            value = cb.get('key').content_as[str]

        Inspect CAS value::

            rv = cb.get("key")
            value, cas = rv.content_as[str], rv.cas

        Request the expiry::
            rv = cb.get("key", GetOptions(with_expiry=True))
            value, expiry = rv.content_as[str], rv.expiry
        """
        return self._get_generic(key, kwargs, options)

    @get_result_wrapper
    def get_and_touch(self,
                      key,          # type: str
                      expiry,       # type: int
                      *options,     # type: GetAndTouchOptions
                      **kwargs
                      ):
        # type: (...) -> GetResult
        """
        Get the document with the specified key, and update the expiry.
        :param str key: Key of document to get and touch.
        :param timedelta expiry: New expiry for document.  Set to timedelta(seconds=0) to never expire.
        :param GetAndTouchOptions options: Options for request.
        :param kwargs: Override corresponding value in options.
        :return: A :class:`couchbase.result.GetResult` object representing the document for this key.
        """
        # we place the expiry in the kwargs...
        kwargs['expiry'] = expiry
        return self._get_generic(key, kwargs, options)

    @get_result_wrapper
    def get_and_lock(self,
                     key,       # type: str
                     expiry,    # type: timedelta
                     *options,  # type: GetAndLockOptions
                     **kwargs
                     ):
        # type: (...) -> GetResult
        """
        Get a document with the specified key, locking it from mutation.
        :param str key: Key of document to lock
        :param timedelta expiry: Time at which the lock expires.  Note you can always unlock it, and the server unlocks
                it after 15 seconds.
        :param GetAndLockOptions options: Options for the get and lock operation.
        :param Any kwargs: Override corresponding value in options.
        :return: A :class:`couchbase.result.GetResult` object representing the document for this key.
        """
        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(_Base.lock(self, key, int(expiry.total_seconds()), **final_options), final_options)

    @get_replica_result_wrapper
    def get_any_replica(self,
                        key,        # type: str
                        *options,   # type: GetFromReplicaOptions
                        **kwargs
                        ):
        # type: (...) -> GetReplicaResult
        """Obtain an object stored in Couchbase by given key, from a replica.

        :param string key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`
        :param: GetFromReplicaOptions options: The options to use for this get request.
        :param: Any kwargs: Override corresponding value in options.

        :raise: :exc:`.NotFoundError` if the key does not exist
                :exc:`.DocumentUnretrievableError` if no replicas exist
        :return: A :class:`couchbase.result.GetReplicaResult` object

        """
        final_options = forward_args(kwargs, *options)
        return super(CBCollection, self).rget(key, **final_options)

    @get_replica_result_wrapper
    def get_all_replicas(self,
                         key,        # type: str
                         *options,   # type: GetAllReplicasOptions
                         **kwargs    # type: Any
                         ):
      # type: (...) -> Iterable[GetReplicaResult]
      """Obtain an object stored in Couchbase by given key, from every replica.

      :param string key: The key to fetch. The type of key is the same
          as mentioned in :meth:`upsert`
      :param: GetFromReplicaOptions options: The options to use for this get request.
      :param: Any kwargs: Override corresponding value in options.

      :raise: :exc:`.NotFoundError` if the key does not exist
              :exc:`.DocumentUnretrievableError` if no replicas exist
      :return: A list(:class:`couchbase.result.GetReplicaResult`) object
      """
      return super(CBCollection, self).rgetall(key, **forward_args(kwargs, *options))

    @volatile
    def get_multi(self,         # type: CBCollection
                  keys,         # type: Iterable[str]
                  *options,     # type: GetOptions
                  **kwargs
                  ):
        # type: (...) -> Dict[str,GetResult]
        """
        Get multiple keys from the collection

        :param keys: list of keys to get
        :type Iterable[str] keys: list of keys to get
        :return: a dictionary of :class:`~.GetResult` objects by key
        :rtype: dict
        """
        raw_result = super(CBCollection,self).get_multi(keys, **forward_args(kwargs, *options))
        return {k: AsyncGetResult(v) for k, v in raw_result.items()}

    @overload
    def upsert_multi(self,  # type: CBCollection
                     keys,  # type: Mapping[str,Any]
                     ttl=0,  # type: int
                     format=None,  # type: int
                     durability=None  # type: DurabilityType
                     ):
        pass

    @volatile
    def upsert_multi(self,  # type: CBCollection
                     keys,  # type: Dict[str,JSON]
                     *options,  # type: GetOptions
                     **kwargs
                     ):
        # type: (...) -> Dict[str,MutationResult]
        """
        Write multiple items to the cluster. Multi version of :meth:`upsert`

        :param dict keys: A dictionary of keys to set. The keys are the
            keys as they should be on the server, and the values are the
            values for the keys to be stored.

            `keys` may also be a :class:`~.ItemCollection`. If using a
            dictionary variant for item collections, an additional
            `ignore_cas` parameter may be supplied with a boolean value.
            If not specified, the operation will fail if the CAS value
            on the server does not match the one specified in the
            `Item`'s `cas` field.
        :param int ttl: If specified, sets the expiry value
            for all keys
        :param int format: If specified, this is the conversion format
            which will be used for _all_ the keys.
        :param int persist_to: Durability constraint for persistence.
            Note that it is more efficient to use :meth:`endure_multi`
            on the returned :class:`~couchbase_v2.result.MultiResult` than
            using these parameters for a high volume of keys. Using
            these parameters however does save on latency as the
            constraint checking for each item is performed as soon as it
            is successfully stored.
        :param int replicate_to: Durability constraints for replication.
            See notes on the `persist_to` parameter for usage.
        :param Durability durability_level: Sync replication durability level.
            You should either use this or the old-style durability params above,
            but not both.

        :return: A :class:`~.MultiResult` object, which is a
            `dict`-like object

        The multi methods are more than just a convenience, they also
        save on network performance by batch-scheduling operations,
        reducing latencies. This is especially noticeable on smaller
        value sizes.

        .. seealso:: :meth:`upsert`
        """
        return get_multi_mutation_result(self, CoreClient.upsert_multi, keys, *options, **kwargs)

    @volatile
    def insert_multi(self,  # type: CBCollection
                     keys,  # type: Dict[str,JSON]
                     *options,  # type: GetOptions
                     **kwargs
                     ):
        # type: (...) -> Dict[str, MutationResult]
        """
        Insert multiple items into the collection.

        :param dict keys: dictionary of items to insert, by key
        :return: a dictionary of :class:`~.MutationResult` objects by key
        :rtype: dict

        .. seealso:: :meth:`upsert_multi` - for other optional arguments
        """
        return get_multi_mutation_result(self, CoreClient.insert_multi, keys, *options, **kwargs)

    @volatile
    def remove_multi(self,  # type: CBCollection
                     keys,  # type: Iterable[str]
                     *options,  # type: GetOptions
                     **kwargs
                     ):
        # type: (...) -> Dict[str, MutationResult]
        """
        Remove multiple items from the collection.

        :param list keys: list of items to remove, by key
        :return: a dictionary of :class:`~.MutationResult` objects by key
        :rtype: dict

        .. seealso:: :meth:`upsert_multi` - for other optional arguments
        """
        # See CCBC-1199 - LCB support for client durability not there for remove
        # for now, we raise NotSupported so nobody is surprised.
        persist_to = kwargs.get('persist_to', 0)
        replicate_to = kwargs.get('replicate_to', 0)
        if persist_to > 0 or replicate_to > 0:
            raise NotSupportedError("Client durability not supported yet for remove")
        return get_multi_mutation_result(self, CoreClient.remove_multi, keys, *options, **kwargs)

    replace_multi = _wrap_multi_mutation_result(CoreClient.replace_multi)
    touch_multi = _wrap_multi_mutation_result(CoreClient.touch_multi)
    lock_multi = _wrap_multi_mutation_result(CoreClient.lock_multi)
    unlock_multi = _wrap_multi_mutation_result(CoreClient.unlock_multi)
    append_multi = _wrap_multi_mutation_result(CoreClient.unlock_multi)
    prepend_multi = _wrap_multi_mutation_result(CoreClient.prepend_multi)
    counter_multi = _wrap_multi_mutation_result(CoreClient.counter_multi)

    def touch(self,
              key,          # type: str
              expiry,       # type: timedelta
              *options,     # type: TouchOptions
              **kwargs):
        # type: (...) -> MutationResult
        """Update a key's expiry time

        :param string key: The key whose expiry time should be
            modified
        :param timedelta expiry: The new expiry time. If the expiry time
            is timedelta(seconds=0), then the key never expires (and any existing
            expiry is removed)
        :param TouchOptions options: Options for touch command.
        :param Any kwargs: Override corresponding value in options.
        :return: :class:`.MutationResult`
        :raise: The same things that :meth:`get` does

        .. seealso:: :meth:`get_and_touch` - which can be used to get *and* update the
            expiry
        """
        # lets just pop the expiry into the kwargs.  If one was present, we override it
        kwargs['expiry'] = expiry
        args = forward_args(kwargs, *options)
        print(args)
        return _Base.touch(self, key, **forward_args(kwargs, *options))

    @_wrap_in_mutation_result
    def unlock(self,
               key,         # type: str
               cas,         # type: int
               *options,    # type: UnlockOptions
               **kwargs
               ):
        # type: (...) -> MutationResult
        """Unlock a Locked Key in Couchbase.

        This unlocks an item previously locked by :meth:`lock`

        :param str key: The key to unlock
        :param int cas: The cas, which you got when you used get_and_lock.
        :param UnlockOptions options: Options for the unlock operation.
        :param Any kwargs: Override corresponding value in options.

        See :meth:`lock` for an example.

        :raise: :exc:`.TempFailException` if the CAS supplied does not
            match the CAS on the server (possibly because it was
            unlocked by previous call).

        .. seealso:: :meth:`lock`
        """
        # pop the cas into the kwargs
        kwargs['cas'] = cas
        return _Base.unlock(self, key, **forward_args(kwargs, *options))

    def exists(self,      # type: CBCollection
               key,       # type: str
               *options,  # type: ExistsOptions
               **kwargs   # type: Any
                ):
        # type: (...) -> ExistsResult
        """Check to see if a key exists in this collection.

        :param str key: the id of the document.
        :param ExistsOptions options: options for checking if a key exists.
        :return: An ExistsResult object with a boolean value indicating the presence of the document.
        :raise: Any exceptions raised by the underlying platform.
        """
        return ExistsResult(self.bucket.exists(key, **forward_args(kwargs, *options)))

    @_wrap_in_mutation_result
    def upsert(self,
               key,         # type: str
               value,       # type: Any
               *options,    # type: UpsertOptions
               **kwargs     # type: Any
               ):
        # type: (...) -> MutationResult
        """Unconditionally store the object in Couchbase.

        :param key:
            The key to set the value with. By default, the key must be
            either a :class:`bytes` or :class:`str` object encodable as
            UTF-8. If a custom `transcoder` class is used (see
            :meth:`~__init__`), then the key object is passed directly
            to the transcoder, which may serialize it how it wishes.
        :type id: string or bytes

        :param value: The value to set for the key.
            This should be a native Python value which will be transparently
            serialized to JSON by the library. Do not pass already-serialized
            JSON as the value or it will be serialized again.

            If you are using a different `format` setting (see `format`
            parameter), and/or a custom transcoder then value for this
            argument may need to conform to different criteria.

        :param UpsertOptions options: Options for the upsert operation.
        :param Any kwargs: Override corresponding value in options.

        :raise: :exc:`.ArgumentError` if an argument is supplied that is
            not applicable in this context. For example setting the CAS
            as a string.
        :raise: :exc`.CouchbaseNetworkError`
        :raise: :exc:`.KeyExistsError` if the key already exists on the
            server with a different CAS value.
        :raise: :exc:`.ValueFormatError` if the value cannot be
            serialized with chosen encoder, e.g. if you try to store a
            dictionary in plain mode.
        :return: :class:`~.Result`.

        Simple set::

            cb.upsert('key', 'value')

        Force JSON document format for value::

            cb.upsert('foo', {'bar': 'baz'}, format=couchbase_core.FMT_JSON)

        Insert JSON from a string::

            JSONstr = '{"key1": "value1", "key2": 123}'
            JSONobj = json.loads(JSONstr)
            cb.upsert("documentID", JSONobj, format=couchbase_core.FMT_JSON)

        Force UTF8 document format for value::

            cb.upsert('foo', "<xml></xml>", format=couchbase_core.FMT_UTF8)

        Perform optimistic locking by specifying last known CAS version::

            cb.upsert('foo', 'bar', cas=8835713818674332672)

        Simple set with durability::

            cb.upsert('key', 'value', durability_level=Durability.MAJORITY_AND_PERSIST_TO_ACTIVE)

        """

        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(self.bucket.upsert(key, value, **final_options), final_options)

    @_wrap_in_mutation_result
    def insert(self,
               key,         # type: str
               value,       # type: Any
               *options,    # type InsertOptions
               **kwargs):
        # type: (...) -> ResultPrecursor
        """Store an object in Couchbase unless it already exists.

        Follows the same conventions as :meth:`upsert` but the value is
        stored only if it does not exist already. Conversely, the value
        is not stored if the key already exists.

        Notably missing from this method is the `cas` parameter, this is
        because `insert` will only succeed if a key does not already
        exist on the server (and thus can have no CAS)

        :param str key: Key of document to insert
        :param Any value: The document itself.
        :param InsertOptions options: Options for the insert request.
        :param Any kwargs: Override corresponding value in the options.
        :raise: :exc:`.KeyExistsError` if the key already exists

        .. seealso:: :meth:`upsert`
        """

        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(_Base.insert(self, key, value, **final_options), final_options)

    @_wrap_in_mutation_result
    def replace(self,
                key,        # type: str
                value,      # type: Any
                *options,   # type: ReplaceOptions
                **kwargs    # type: Any
                ):
        # type: (...) -> MutationResult
        """Store an object in Couchbase only if it already exists.

           Follows the same conventions as :meth:`upsert`, but the value is
           stored only if a previous value already exists.

           :param str key: Key of document to replace
           :param Any value: The document itself.
           :param ReplaceOptions options: Options for the replace request.
           :param Any kwargs: Override corresponding value in the options.

           :raise: :exc:`.NotFoundError` if the key does not exist

           .. seealso:: :meth:`upsert`
        """

        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(_Base.replace(self, key, value, **final_options), final_options)

    @_wrap_in_mutation_result
    def remove(self,        # type: CBCollection
               key,         # type: str
               *options,    # type: RemoveOptions
               **kwargs
               ):
        # type: (...) -> MutationResult
        """Remove the key-value entry for a given key in Couchbase.

        :param str key: A string which is the key to remove. The format and
            type of the key follows the same conventions as in
            :meth:`upsert`
        :param RemoveOptions options: Options for removing key.
        :param Any kwargs: Override corresponding value in options
        :raise: :exc:`.NotFoundError` if the key does not exist.
        :raise: :exc:`.KeyExistsError` if a CAS was specified, but
            the CAS on the server had changed
        :return: A :class:`~.MutationResult` object.

        Simple remove::

            ok = cb.remove("key").success

        Don't complain if key does not exist::

            ok = cb.remove("key", quiet=True)

        Only remove if CAS matches our version::

            rv = cb.get("key")
            cb.remove("key", cas=rv.cas)
        """
        final_options = forward_args(kwargs, *options)
        # See CCBC-1199 - LCB support for client durability not there for remove
        # for now, we raise NotSupported so nobody is surprised.
        persist_to = final_options.get('persist_to', 0)
        replicate_to = final_options.get('replicate_to', 0)
        if persist_to > 0 or replicate_to > 0:
            raise NotSupportedError("Client durability not supported yet for remove")
        return ResultPrecursor(self.bucket.remove(key, **final_options), final_options)

    def lookup_in(self,         # type: CBCollection
                  key,          # type: str
                  spec,         # type: LookupInSpec
                  *options,     # type: LookupInOptions
                  **kwargs      # type: Any
                  ):
        # type: (...) -> LookupInResult

        """Atomically retrieve one or more paths from a document.

        :param str key: The key of the document to lookup
        :param LookupInSpec spec: An iterable sequence of Specs (see :mod:`.couchbase_core.subdocument`)
        :param LookupInOptions options: Options for the lookup_in operation.
        :param Any kwargs: Override corresponding value in options.

        :return: A :class:`.couchbase.LookupInResult` object.
            This object contains the results and any errors of the
            operation.

        Example::

            import couchbase_core.subdocument as SD
            rv = cb.lookup_in('user',
                              SD.get('email'),
                              SD.get('name'),
                              SD.exists('friends.therock'))

            email = rv[0]
            name = rv[1]
            friend_exists = rv.exists(2)

        """
        final_options = forward_args(kwargs, *options)
        return LookupInResult(self.bucket.lookup_in(key, spec, **final_options))

    def mutate_in(self,  # type: CBCollection
                  key,  # type: str
                  spec,  # type: MutateInSpec
                  *options,  # type: MutateInOptions
                  **kwargs  # type: Any
                  ):
        # type: (...) -> MutateInResult
        """Perform multiple atomic modifications within a document.

        :param key: The key of the document to modify
        :param MutateInSpec spec: An iterable of specs (See :mod:`.couchbase.mutate_in.MutateInSpecItemBase`)
        :param MutateInOptions options: Options for the mutate_in operation.
        :param kwargs: Override corresponding value in options.
        :return: A :class:`~.couchbase.MutationResult` object.

        Here's an example of adding a new tag to a "user" document
        and incrementing a modification counter::

            import couchbase_core.subdocument as SD
            # ....
            cb.mutate_in('user',
                          SD.array_addunique('tags', 'dog'),
                          SD.counter('updates', 1))

        .. note::

            The `insert_doc` and `upsert_doc` options are mutually exclusive.
            Use `insert_doc` when you wish to create a new document with
            extended attributes (xattrs).

        .. seealso:: :mod:`.couchbase_core.subdocument`
        """
        final_options = forward_args(kwargs, *options)
        return MutateInResult(self.bucket.mutate_in(key, spec, **final_options), **final_options)

    def binary(self):
        # type: (...) -> BinaryCollection
        pass

    @_wrap_in_mutation_result
    def append(self,
               key,  # type: str
               value,  # type: str
               *options,  # type: Any
               **kwargs  # type: Any
               ):
        # type: (...) -> ResultPrecursor
        """Append a string to an existing value in Couchbase.
        :param str key: Key for the value to append
        :param str value: The data to append to the existing value.

        Other parameters follow the same conventions as :meth:`upsert`.

        The `format` argument must be one of
        :const:`~couchbase_core.FMT_UTF8` or :const:`~couchbase_core.FMT_BYTES`.
        If not specified, it will be :const:`~.FMT_UTF8` (overriding the
        :attr:`default_format` attribute). This is because JSON or
        Pickle formats will be nonsensical when random data is appended
        to them. If you wish to modify a JSON or Pickle encoded object,
        you will need to retrieve it (via :meth:`get`), modify it, and
        then store it again (using :meth:`upsert`).

        Additionally, you must ensure the value (and flags) for the
        current value is compatible with the data to be appended. For an
        example, you may append a :const:`~.FMT_BYTES` value to an
        existing :const:`~couchbase_core.FMT_JSON` value, but an error will
        be thrown when retrieving the value using :meth:`get` (you may
        still use the :attr:`data_passthrough` to overcome this).

        :raise: :exc:`.NotStoredError` if the key does not exist
        """
        x = _Base.append(self, key, value, forward_args(kwargs, *options))
        return ResultPrecursor(x, options)

    def prepend(self,
                key,  # type: str
                value,  # type: str
                *options,  # type: PrependOptions
                **kwargs  # type: Any
                ):
        # type: (...) -> ResultPrecursor
        """Prepend a string to an existing value in Couchbase.

        .. seealso:: :meth:`append`
        """
        x = _Base.prepend(self, key, value, **forward_args(kwargs, *options))
        return ResultPrecursor(x, options)

    @_wrap_in_mutation_result
    def increment(self,
                  key,  # type: str
                  delta,  # type: DeltaValue
                  *options,  # type: CounterOptions
                  **kwargs
                  ):
        # type: (...) -> ResultPrecursor
        """Increment the numeric value of an item.

        This method instructs the server to treat the item stored under
        the given key as a numeric counter.

        Counter operations require that the stored value
        exists as a string representation of a number (e.g. ``123``). If
        storing items using the :meth:`upsert` family of methods, and
        using the default :const:`couchbase_core.FMT_JSON` then the value
        will conform to this constraint.

        :param string key: A key whose counter value is to be modified
        :param DeltaValue delta: an amount by which the key should be incremented.
        :param CounterOptions options: Options for the increment operation.
        :param Any kwargs: Overrides corresponding value in the options
        :raise: :exc:`.NotFoundError` if the key does not exist on the
           bucket (and `initial` was `None`)
        :raise: :exc:`.DeltaBadvalError` if the key exists, but the
           existing value is not numeric
        :return: A :class:`couchbase.result.MutationResult` object.

        Simple increment::

           rv = cb.increment("key")
           cb.get("key").content_as[int]
           # 42

        Increment by 10::

           rv = cb.increment("key", DeltaValue(10))


        Increment by 20, set initial value to 5 if it does not exist::

           rv = cb.increment("key", DeltaValue(20), initial=SignedInt64(5))

        """
        final_opts = self._check_delta_initial(kwargs, *options)
        x = _Base.counter(self, key, delta=int(DeltaValue.verified(delta)), **final_opts)
        return ResultPrecursor(x, final_opts)

    @_wrap_in_mutation_result
    def decrement(self,
                  key,          # type: str
                  delta,        # type: DeltaValue
                  *options,     # type: CounterOptions
                  **kwargs
                  ):
        # type: (...) -> ResultPrecursor
        """Decrement the numeric value of an item.

        This method instructs the server to treat the item stored under
        the given key as a numeric counter.

        Counter operations require that the stored value
        exists as a string representation of a number (e.g. ``123``). If
        storing items using the :meth:`upsert` family of methods, and
        using the default :const:`couchbase_core.FMT_JSON` then the value
        will conform to this constraint.

        :param string key: A key whose counter value is to be modified
        :param DeltaValue delta: an amount by which the key should be decremented.
        :param CounterOptions options: Options for the decrement operation.
        :param Any kwargs: Overrides corresponding value in the options
        :raise: :exc:`.NotFoundError` if the key does not exist on the
           bucket (and `initial` was `None`)
        :raise: :exc:`.DeltaBadvalError` if the key exists, but the
           existing value is not numeric
        :return: A :class:`couchbase.result.MutationResult` object.

        Simple decrement::

           rv = cb.decrement("key")
           cb.get("key").content_as[int]
           # 42

        Decrement by 10::

           rv = cb.decrement("key", DeltaValue(10))


        Decrement by 20, set initial value to 5 if it does not exist::

           rv = cb.decrement("key", DeltaValue(20), initial=SignedInt64(5))

        """

        final_opts = self._check_delta_initial(kwargs, *options)
        x = super(CBCollection, self).counter(key, delta=-int(DeltaValue.verified(delta)), **final_opts)
        return ResultPrecursor(x, final_opts)

    @staticmethod
    def _check_delta_initial(kwargs, *options):
        final_opts = forward_args(kwargs, *options)
        init_arg = final_opts.get('initial')
        initial = None if init_arg is None else int(SignedInt64.verified(init_arg))
        if initial is not None:
            final_opts['initial'] = initial
        return final_opts


class Scope(object):
    def __init__(self,  # type: Scope
                 parent,  # type: couchbase.bucket.Bucket
                 name=None  # type: str
                 ):
        # type: (...) -> Any
        """
        Collection scope representation.
        Constructor should only be invoked internally.

        :param parent: parent bucket.
        :param name: name of scope to open
        """
        self._name = name
        self.bucket = parent

    def __deepcopy__(self, memodict={}):
        result = copy.copy(self)
        return result

    @property
    def _realbucket(self):
        # type: (...) -> CoreClient
        return self.bucket._bucket

    @property
    def name(self):
        # type (...)->str
        """

        :return:    A string value that is the name of the collection.
        :except     ScopeNotFoundException
        :except     AuthorizationException
        """
        return self._name

    def default_collection(self,  # type: Scope
                           *options,  # type: Any
                           **kwargs  # type: Any
                           ):
        # type: (...) -> CBCollection
        """
        Returns the default collection for this bucket.

        :param collection_name: string identifier for a given collection.
        :param options: collection options
        :return: A :class:`.Collection` for a collection with the given name.

        :raise: CollectionNotFoundException
        :raise: AuthorizationException
        """
        return self._gen_collection(None, *options)

    def _gen_collection(self,
                        collection_name,  # type: Optional[str]
                        *options  # type: CollectionOptions
                        ):
        # type: (...) -> CBCollection
        return CBCollection.cast(self, collection_name, *options)
    @volatile
    def collection(self,
                        collection_name,  # type: str
                        *options  # type: CollectionOptions
                        ):
        # type: (...) -> CBCollection
        """
        Gets the named collection for this bucket.

        :param collection_name: string identifier for a given collection.
        :param options: collection options
        :return: A :class:`.Collection` for a collection with the given name.

        :raise: CollectionNotFoundException
        :raise: AuthorizationException

        """
        return self._gen_collection(collection_name, *options)


Collection = CBCollection


class AsyncCBCollection(AsyncClientFactory.gen_async_client(CBCollection)):
    def __init__(self, *args, **kwargs):
        super(AsyncCBCollection, self).__init__(*args, **kwargs)
