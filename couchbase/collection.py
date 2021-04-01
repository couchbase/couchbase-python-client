import copy
from datetime import timedelta
from functools import wraps
from typing import *

from couchbase_core._libcouchbase import Bucket as _Base
from couchbase_core._libcouchbase import FMT_UTF8
from mypy_extensions import VarArg, KwArg, Arg

import couchbase.exceptions
from couchbase.durability import DurabilityType, DurabilityOptionBlock
from couchbase.exceptions import NotSupportedException, DocumentNotFoundException, PathNotFoundException, QueueEmpty, \
    PathExistsException, DocumentExistsException
from couchbase_core import JSON, operation_mode
from couchbase_core.asynchronous.client import AsyncClientMixin
from couchbase_core.client import Client as CoreClient
from couchbase_core.supportability import volatile, internal
from .options import AcceptableInts
from .options import forward_args, OptionBlockTimeOut, OptionBlockDeriv, ConstrainedInt, SignedInt64
import couchbase.options
from .result import GetResult, GetReplicaResult, ExistsResult, get_result_wrapper, CoreResult, ResultPrecursor, \
    LookupInResult, MutateInResult, \
    MutationResult, _wrap_in_mutation_result, get_replica_result_wrapper, get_multi_mutation_result, \
    get_multi_get_result, lookup_in_result_wrapper, mutate_in_result_wrapper
from .subdocument import LookupInSpec, MutateInSpec, MutateInOptions, \
    gen_projection_spec

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict
import os
from couchbase_core import abstractmethod, ABCMeta, with_metaclass
import wrapt

import couchbase_core.subdocument as SD


class DeltaValue(ConstrainedInt):
    def __init__(self,
                 value  # type: AcceptableInts
                 ):
        # type: (...) -> None
        """
        A non-negative integer between 0 and +0x7FFFFFFFFFFFFFFF inclusive.
        Used as an argument for :meth:`Collection.increment` and :meth:`Collection.decrement`

        :param value: the value to initialise this with.

        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super(DeltaValue, self).__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return 0


class ReplaceOptions(DurabilityOptionBlock):
    def __init__(self,
                 timeout=None,       # type: timedelta
                 durability=None,    # type: DurabilityType
                 cas=0               # type: int
                 ):
        """

        :param timeout:
        :param durability:
        :param cas:
        """
        super(ReplaceOptions, self).__init__(timeout=timeout, durability=durability, cas=cas)


class RemoveOptions(DurabilityOptionBlock):
    @overload
    def __init__(self,
                 durability=None,  # type: DurabilityType
                 cas=0,            # type: int
                 **kwargs):
        """
        Remove Options

        :param durability: durability type

        :param cas: The CAS to use for the removal operation.
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


class ExtensionOptions(DurabilityOptionBlock):
    def __init__(self,  # type: ExtensionOptions
                 durability=None,   # type: DurabilityType,
                 cas=None,          # type: int
                 timeout=None       # type: timedelta
                 ):
        """
        Options for {extending} an item

        :param cas: CAS value
        :param timeout: Timeout for operation
        :param durability: Durability settings
        """
        super(ExtensionOptions, self).__init__(cas=cas, timeout=timeout, durability=durability)


class PrependOptions(ExtensionOptions._wrap_docs(extending='prepending')):
    pass


class AppendOptions(ExtensionOptions._wrap_docs(extending='appending')):
    pass


class CounterOptions(DurabilityOptionBlock):
    def __init__(self,  # type: CounterOptions
                 durability=None,   # type: DurabilityType
                 cas=None,          # type: int
                 timeout=None,      # type: timedelta
                 expiry=None,       # type: timedelta
                 initial=SignedInt64(0),      # type: SignedInt64
                 delta=DeltaValue(1)         # type: DeltaValue
                 ):
        # type: (...) -> None
        """
        Settings for {counter_type} operations

        :param durability: Durability settings
        :param cas: the CAS value
        :param timeout: Timeout for the operation
        :param expiry: Expiration time
        :param initial: Initial value
        :param delta: Variation
        """
        super(CounterOptions, self).__init__(durability=durability, cas=cas, timeout=timeout, expiry=expiry, delta=delta, initial=initial)


class IncrementOptions(CounterOptions._wrap_docs(counter_type='increment')):
    pass


class DecrementOptions(CounterOptions._wrap_docs(counter_type='decrement')):
    pass


class UnlockOptions(OptionBlockTimeOut):
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


def _get_result_and_inject(func  # type: RawCollectionMethod
                           ):
    # type: (...) ->RawCollectionMethod
    result = _inject_scope_and_collection(get_result_wrapper(func))
    operation_mode.operate_on_doc(result, lambda x: func.__doc__)
    result.__name__ = func.__name__
    return result


def _mutate_result_and_inject(func  # type: RawCollectionMethod
                              ):
    # type: (...) ->RawCollectionMethod
    result = _inject_scope_and_collection(_wrap_in_mutation_result(func))
    operation_mode.operate_on_doc(result, lambda x: func.__doc__)
    result.__name__ = func.__name__
    return result


def _inject_scope_and_collection(func  # type: RawCollectionMethodSpecial
                                 ):
    # type: (...) -> RawCollectionMethod
    @wraps(func)
    def wrapped(self,  # type: CBCollection
                *args,  # type: Any
                **kwargs  # type:  Any
                ):
        # type: (...)->Any
        self._inject_scope_collection_kwargs(kwargs)
        return func(self, *args, **kwargs)

    if getattr(func, 'coll_injected', False):
        return func
    wrapped.coll_injected = True

    return wrapped


CoreBucketOpRead = TypeVar("CoreBucketOpRead", Callable[[Any], CoreResult], Callable[[Any], GetResult])


def _wrap_get_result(func  # type: CoreBucketOpRead
                     ):
    # type: (...) -> CoreBucketOpRead
    @wraps(func)
    def wrapped(self,  # type: CBCollection
                *args,  # type: Any
                **kwargs  # type:  Any
                ):
        # type: (...)->Any
        return _inject_scope_and_collection(get_result_wrapper(func))(self, *args, **kwargs)

    return wrapped


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
        return get_multi_mutation_result(target.bucket, wrapped, keys, *options, **kwargs)

    return _inject_scope_and_collection(wrapper)



def _dsop(create_type=None, wrap_missing_path=True):
    import functools

    def real_decorator(fn):
        @functools.wraps(fn)
        def newfn(self, key, *args, **kwargs):
            try:
                return fn(self, key, *args, **kwargs)
            except DocumentNotFoundException:
                if kwargs.get('create'):
                    try:
                        self.insert(key, create_type())
                    except DocumentExistsException:
                        pass
                    return fn(self, key, *args, **kwargs)
                else:
                    raise
            except PathNotFoundException:
                if wrap_missing_path:
                    raise IndexError(args[0])

        return newfn

    return real_decorator


class CBCollection(wrapt.ObjectProxy):
    def __reduce_ex__(self, protocol):
        raise NotImplementedError()

    def __reduce__(self):
        raise NotImplementedError()

    @classmethod
    def _wrap_collections_class(cls):
        if not hasattr(cls, 'coll_wrapped'):
            for name in cls._MEMCACHED_OPERATIONS:
                meth = getattr(cls, name)
                if not name.startswith('_'):
                    setattr(cls, name, _inject_scope_and_collection(meth))
            cls.coll_wrapped = True

    def _inject_scope_collection_kwargs(self, kwargs):
        # NOTE: BinaryCollection, for instance, contains a collection and has an interface
        # which uses this annotation.  So -- anything this depends on must be supported by
        # that interface.  If we add/remove something that depends on self from here, we need
        # to do same in BinaryCollection (and any other object that does likewise).
        if self.true_collections:
            if self._self_name and not self._self_scope:
                raise couchbase.exceptions.CollectionNotFoundException
            if self._self_scope and self._self_name:
                kwargs['scope'] = self._self_scope.name
                kwargs['collection'] = self._self_name

    @internal
    def __init__(self,  # type: CBCollection
                 name=None,  # type: str
                 parent_scope=None,  # type: Scope
                 *options,
                 **kwargs
                 ):
        # type: (...) -> None
        """
        Couchbase collection.

        :param parent_scope: parent scope
        :param name: name of collection
        :param options: miscellaneous options
        """
        assert issubclass(type(parent_scope.bucket), CoreClientDatastructureWrap)
        self._wrap_collections_class()
        wrapt.ObjectProxy.__init__(self, parent_scope.bucket)
        self._self_scope = parent_scope  # type: Scope
        self._self_name = name  # type: Optional[str]
        self._self_true_collections = name and parent_scope

    def __copy__(self):
        raise NotImplementedError()

    def __deepcopy__(self, memo):
        raise NotImplementedError()

    @property
    def bucket(self  # type: CBCollection
               ):
        # type: (...) -> CoreClient
        return self._self_scope.bucket

    def __str__(self):
        return "CBCollection of {}".format(str(self.bucket))

    def __repr__(self):
        return "CBCollection of {}".format(repr(self.bucket))

    @classmethod
    def _gen_memd_wrappers(cls, factory):
        return CoreClient._gen_memd_wrappers_retarget(cls, factory)

    @property
    def true_collections(self):
        return self._self_true_collections

    def _get_content(self, result):
        return getattr(result, '_original', result)

    def _wrap_dsop(self,
                   sdres,  # type: SubdocResult
                   has_value=False, **kwargs):
        from couchbase_core.items import Item
        sdres = self._get_content(sdres)
        it = Item(sdres.key)
        it.cas = sdres.cas
        if has_value:
            it.value = sdres[0]
        return getattr(it, 'value', it)

    @classmethod
    def _cast(cls,
              parent_scope,  # type: Scope
              name           # type: Optional[str]
              ):
        # type: (...) -> CBCollection
        coll_args = dict(**parent_scope.bucket._bucket_args)
        coll_args.update(name=name, parent_scope=parent_scope)
        result = parent_scope.bucket._collection_factory(connection_string=parent_scope.bucket._connstr, **coll_args)
        return result

    def _get_generic(self, key, kwargs, options):
        opts = forward_args(kwargs, *options)
        opts.pop('key', None)
        project = opts.pop('project', [])
        with_expiry = opts.pop('with_expiry', False)
        if project or with_expiry:
            spec = gen_projection_spec(project, with_expiry)
            x = CoreClient.lookup_in(self.bucket, key, spec, **opts)
        else:
            x = CoreClient.get(self.bucket, key, **opts)
        # NOTE: there is no reason for the options in the ResultPrecursor below.  Once
        # we get expiry done correctly, lets eliminate that as well.  Previously the
        # expiry you passed in was just duplicated into the result, which of course made
        # no sense since expiry should have been with_expiry (a bool) in the options.
        return ResultPrecursor(x, options)

    @_get_result_and_inject
    def get(self,
            key,        # type: str
            *options,   # type: GetOptions
            **kwargs    # type: Any
            ):
        # type: (...) -> GetResult
        """Obtain an object stored in Couchbase by given key.

        :param key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`
        :param: GetOptions options: The options to use for this get request.
        :param: Any kwargs: Override corresponding value in options.

        :raise: :exc:`.DocumentNotFoundException` if the key does not exist

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

    @_get_result_and_inject
    def get_and_touch(self,         # type: CBCollection
                      key,          # type: str
                      expiry,       # type: timedelta
                      *options,     # type: GetAndTouchOptions
                      **kwargs
                      ):
        # type: (...) -> GetResult
        """
        Get the document with the specified key, and update the expiry.

        :param key: Key of document to get and touch.
        :param expiry: New expiry for document.  Set to timedelta(seconds=0) to never expire.
        :param options: Options for request.
        :param kwargs: Override corresponding value in options.
        :return: A :class:`couchbase.result.GetResult` object representing the document for this key.
        """
        # we place the expiry in the kwargs...
        kwargs['expiry'] = expiry
        return self._get_generic(key, kwargs, options)

    @_get_result_and_inject
    def get_and_lock(self,
                     key,       # type: str
                     expiry,    # type: timedelta
                     *options,  # type: GetAndLockOptions
                     **kwargs
                     ):
        # type: (...) -> GetResult
        """
        Get a document with the specified key, locking it from mutation.

        :param key: Key of document to lock
        :param expiry: Time at which the lock expires.  Note you can always unlock it, and the server unlocks
                it after 15 seconds.
        :param options: Options for the get and lock operation.
        :param kwargs: Override corresponding value in options.
        """
        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(CoreClient.lock(self.bucket, key, int(expiry.total_seconds()), **final_options),
                               final_options)

    @_inject_scope_and_collection
    @get_replica_result_wrapper
    def get_any_replica(self,
                        key,  # type: str
                        *options,  # type: GetFromReplicaOptions
                        **kwargs
                        ):
        # type: (...) -> GetReplicaResult
        """Obtain an object stored in Couchbase by given key, from a replica.

        :param key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`
        :param: GetFromReplicaOptions options: The options to use for this get request.
        :param: Any kwargs: Override corresponding value in options.

        :raise: :exc:`.DocumentNotFoundException` if the key does not exist
                :exc:`.DocumentUnretrievableException` if no replicas exist
        :return: A :class:`couchbase.result.GetReplicaResult` object
        """
        final_options = forward_args(kwargs, *options)
        return CoreClient.rget(self.bucket, key, **final_options)

    @_inject_scope_and_collection
    @get_replica_result_wrapper
    def get_all_replicas(self,
                         key,  # type: str
                         *options,  # type: GetAllReplicasOptions
                         **kwargs  # type: Any
                         ):
        # type: (...) -> Iterable[GetReplicaResult]
        """Obtain an object stored in Couchbase by given key, from every replica.

        :param key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`
        :param: options: The options to use for this get request.
        :param: kwargs: Override corresponding value in options.

        :raise: :exc:`.DocumentNotFoundException` if the key does not exist
              :exc:`.DocumentUnretrievableException` if no replicas exist
        """
        return CoreClient.rgetall(self.bucket, key, **forward_args(kwargs, *options))

    @_inject_scope_and_collection
    @volatile
    def get_multi(self,         # type: CBCollection
                  keys,         # type: Iterable[str]
                  *options,     # type: GetOptions
                  **kwargs      # type: Any
                  ):
        # type: (...) -> Dict[str,GetResult]
        """
        Get multiple keys from the collection

        :param keys: list of keys to get
        :type keys: list of keys to get
        :return: a dictionary of :class:`~.GetResult` objects by key
        """
        return get_multi_get_result(self.bucket, _Base.get_multi, keys, **kwargs)

    @overload
    def upsert_multi(self,  # type: CBCollection
                     keys,  # type: Mapping[str,Any]
                     ttl=0,  # type: int
                     format=None,  # type: int
                     durability=None  # type: DurabilityType
                     ):
        pass

    @_inject_scope_and_collection
    @volatile
    def upsert_multi(self,  # type: CBCollection
                     keys,  # type: Dict[str,JSON]
                     *options,  # type: GetOptions
                     **kwargs
                     ):
        # type: (...) -> Dict[str,MutationResult]
        """
        Write multiple items to the cluster. Multi version of :meth:`upsert`

        :param keys: A dictionary of keys to set. The keys are the
            keys as they should be on the server, and the values are the
            values for the keys to be stored.

            `keys` may also be a :class:`~.ItemCollection`. If using a
            dictionary variant for item collections, an additional
            `ignore_cas` parameter may be supplied with a boolean value.
            If not specified, the operation will fail if the CAS value
            on the server does not match the one specified in the
            `Item`'s `cas` field.
        :param ttl: If specified, sets the expiry value
            for all keys
        :param format: If specified, this is the conversion format
            which will be used for _all_ the keys.
        :param persist_to: Durability constraint for persistence.
            Note that it is more efficient to use :meth:`endure_multi`
            on the returned :class:`~couchbase_v2.result.MultiResult` than
            using these parameters for a high volume of keys. Using
            these parameters however does save on latency as the
            constraint checking for each item is performed as soon as it
            is successfully stored.
        :param replicate_to: Durability constraints for replication.
            See notes on the `persist_to` parameter for usage.
        :param durability_level: Sync replication durability level.
            You should either use this or the old-style durability params above,
            but not both.
        :returns: a dictionary of :class:`~.MutationResult` objects by key

        The multi methods are more than just a convenience, they also
        save on network performance by batch-scheduling operations,
        reducing latencies. This is especially noticeable on smaller
        value sizes.

        .. seealso:: :meth:`upsert`
        """
        return get_multi_mutation_result(self.bucket, CoreClient.upsert_multi, keys, *options, **kwargs)

    @_inject_scope_and_collection
    @volatile
    def insert_multi(self,      # type: CBCollection
                     keys,      # type: Dict[str,JSON]
                     *options,  # type: GetOptions
                     **kwargs
                     ):
        # type: (...) -> Dict[str, MutationResult]
        """
        Insert multiple items into the collection.

        :param keys: dictionary of items to insert, by key
        :return: a dictionary of :class:`~.MutationResult` objects by key

        .. seealso:: :meth:`upsert_multi` - for other optional arguments
        """
        return get_multi_mutation_result(self.bucket, _Base.insert_multi, keys, *options, **kwargs)

    @_inject_scope_and_collection
    @volatile
    def remove_multi(self,      # type: CBCollection
                     keys,      # type: Iterable[str]
                     *options,  # type: GetOptions
                     **kwargs
                     ):
        # type: (...) -> Dict[str, MutationResult]
        """
        Remove multiple items from the collection.

        :param keys: list of items to remove, by key
        :return: a dictionary of :class:`~.MutationResult` objects by key

        .. seealso:: :meth:`upsert_multi` - for other optional arguments
        """
        # See CCBC-1199 - LCB support for client durability not there for remove
        # for now, we raise NotSupported so nobody is surprised.
        persist_to = kwargs.get('persist_to', 0)
        replicate_to = kwargs.get('replicate_to', 0)
        if persist_to > 0 or replicate_to > 0:
            raise NotSupportedException("Client durability not supported yet for remove")
        return get_multi_mutation_result(self.bucket, CoreClient.remove_multi, keys, *options, **kwargs)

    replace_multi = _wrap_multi_mutation_result(_Base.replace_multi)
    touch_multi = _wrap_multi_mutation_result(_Base.touch_multi)
    lock_multi = _wrap_multi_mutation_result(_Base.lock_multi)
    unlock_multi = _wrap_multi_mutation_result(_Base.unlock_multi)
    append_multi = _wrap_multi_mutation_result(_Base.append_multi)
    prepend_multi = _wrap_multi_mutation_result(_Base.prepend_multi)

    @_inject_scope_and_collection
    def touch(self,         # type: CBCollection
              key,          # type: str
              expiry,       # type: timedelta
              *options,     # type: TouchOptions
              **kwargs):
        # type: (...) -> MutationResult
        """Update a key's expiry time

        :param key: The key whose expiry time should be
            modified
        :param expiry: The new expiry time. If the expiry time
            is timedelta(seconds=0), then the key never expires (and any existing
            expiry is removed)
        :param options: Options for touch command.
        :param kwargs: Override corresponding value in options.
        :raise: The same things that :meth:`get` does

        .. seealso:: :meth:`get_and_touch` - which can be used to get *and* update the
            expiry
        """
        # lets just pop the expiry into the kwargs.  If one was present, we override it
        kwargs['expiry'] = expiry
        return CoreClient.touch(self.bucket, key, **forward_args(kwargs, *options))

    @_mutate_result_and_inject
    def unlock(self,
               key,         # type: str
               cas,         # type: int
               *options,    # type: UnlockOptions
               **kwargs     # type: Any
               ):
        # type: (...) -> MutationResult
        """Unlock a Locked Key in Couchbase.

        This unlocks an item previously locked by :meth:`lock`

        :param key: The key to unlock
        :param cas: The cas, which you got when you used get_and_lock.
        :param options: Options for the unlock operation.
        :param kwargs: Override corresponding value in options.

        See :meth:`get_and_lock` for an example.

        :raise: :exc:`.TemporaryFailException` if the CAS supplied does not
            match the CAS on the server (possibly because it was
            unlocked by previous call).

        .. seealso:: :meth:`get_and_lock`
        """
        # pop the cas into the kwargs
        kwargs['cas'] = cas
        return CoreClient.unlock(self.bucket, key, **forward_args(kwargs, *options))

    @_inject_scope_and_collection
    def exists(self,      # type: CBCollection
               key,       # type: str
               *options,  # type: ExistsOptions
               **kwargs   # type: Any
               ):
        # type: (...) -> ExistsResult
        """Check to see if a key exists in this collection.

        :param key: the id of the document.
        :param options: options for checking if a key exists.
        :return: An object with a boolean value indicating the presence of the document.
        :raise: Any exceptions raised by the underlying platform.
        """
        return ExistsResult(CoreClient.exists(self.bucket, key, **forward_args(kwargs, *options)))

    @_mutate_result_and_inject
    def upsert(self,        # type: CBCollection
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

        :param value: The value to set for the key.
            This should be a native Python value which will be transparently
            serialized to JSON by the library. Do not pass already-serialized
            JSON as the value or it will be serialized again.

            If you are using a different `format` setting (see `format`
            parameter), and/or a custom transcoder then value for this
            argument may need to conform to different criteria.

        :param options: Options for the upsert operation.
        :param kwargs: Override corresponding value in options.

        :raise: :exc:`.InvalidArgumentException` if an argument is supplied that is
            not applicable in this context. For example setting the CAS
            as a string.
        :raise:
            :exc`.CouchbaseNetworkException`
        :raise:
            :exc:`.DocumentExistsException` if the key already exists on the
            server with a different CAS value.
        :raise:
            :exc:`.ValueFormatException` if the value cannot be
            serialized with chosen encoder, e.g. if you try to store a
            dictionary in plain mode.

        Simple set::

            cb.upsert('key', 'value')

        Force JSON document format for value::

            cb.upsert('foo', {'bar': 'baz'})

        Insert JSON from a string::

            JSONstr = '{"key1": "value1", "key2": 123}'
            JSONobj = json.loads(JSONstr)
            cb.upsert("documentID", JSONobj)

        Force UTF8 document format for value::

            cb.upsert('foo', "<xml></xml>", format=couchbase_core.FMT_UTF8)

        Simple set with durability::

            cb.upsert('key', 'value', durability_level=Durability.MAJORITY_AND_PERSIST_TO_ACTIVE)

        """

        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(CoreClient.upsert(self.bucket, key, value, **final_options), final_options)

    @_mutate_result_and_inject
    def insert(self,
               key,  # type: str
               value,  # type: Any
               *options,  # type InsertOptions
               **kwargs):
        # type: (...) -> MutationResult
        """Store an object in Couchbase unless it already exists.

        Follows the same conventions as :meth:`upsert` but the value is
        stored only if it does not exist already. Conversely, the value
        is not stored if the key already exists.

        Notably missing from this method is the `cas` parameter, this is
        because `insert` will only succeed if a key does not already
        exist on the server (and thus can have no CAS)

        :param key: Key of document to insert
        :param value: The document itself.
        :param options: Options for the insert request.
        :param kwargs: Override corresponding value in the options.
        :raise: :exc:`.DocumentExistsException` if the key already exists

        .. seealso:: :meth:`upsert`
        """

        final_options = forward_args(kwargs, *options)
        return CoreClient.insert(self.bucket, key, value, **final_options)

    @_mutate_result_and_inject
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

           :param key: Key of document to replace
           :param value: The document itself.
           :param options: Options for the replace request.
           :param kwargs: Override corresponding value in the options.

           :raise: :exc:`.DocumentNotFoundException` if the key does not exist

           .. seealso:: :meth:`upsert`
        """

        final_options = forward_args(kwargs, *options)
        return ResultPrecursor(CoreClient.replace(self.bucket, key, value, **final_options), final_options)

    @_mutate_result_and_inject
    def remove(self,        # type: CBCollection
               key,         # type: str
               *options,    # type: RemoveOptions
               **kwargs
               ):
        # type: (...) -> MutationResult
        """Remove the key-value entry for a given key in Couchbase.

        :param key: A string which is the key to remove. The format and
            type of the key follows the same conventions as in
            :meth:`upsert`
        :param options: Options for removing key.
        :param kwargs: Override corresponding value in options
        :raise: :exc:`.DocumentNotFoundException` if the key does not exist.
        :raise: :exc:`.DocumentExistsException` if a CAS was specified, but
            the CAS on the server had changed

        Simple remove::

            ok = cb.remove("key").success

        Don't complain if key does not exist::

            ok = cb.remove("key", quiet=True)

        Only remove if CAS matches our version::

            rv = cb.get("key")
            cb.remove("key", RemoveOptions(cas=rv.cas))
        """
        final_options = forward_args(kwargs, *options)
        # See CCBC-1199 - LCB support for client durability not there for remove
        # for now, we raise NotSupported so nobody is surprised.
        persist_to = final_options.get('persist_to', 0)
        replicate_to = final_options.get('replicate_to', 0)
        if persist_to > 0 or replicate_to > 0:
            raise NotSupportedException("Client durability not supported yet for remove")
        return ResultPrecursor(CoreClient.remove(self.bucket, key, **final_options), final_options)

    @_inject_scope_and_collection
    @lookup_in_result_wrapper
    def lookup_in(self,         # type: CBCollection
                  key,          # type: str
                  spec,         # type: LookupInSpec
                  *options,     # type: LookupInOptions
                  **kwargs      # type: Any
                  ):
        # type: (...) -> LookupInResult

        """Atomically retrieve one or more paths from a document.

        :param key: The key of the document to lookup
        :param spec: An iterable sequence of Specs (see :mod:`.couchbase_core.subdocument`)
        :param options: Options for the lookup_in operation.
        :param kwargs: Override corresponding value in options.

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
        return ResultPrecursor(CoreClient.lookup_in(self.bucket, key, spec, **final_options), final_options)

    @_inject_scope_and_collection
    @mutate_in_result_wrapper
    def mutate_in(self,  # type: CBCollection
                  key,  # type: str
                  spec,  # type: MutateInSpec
                  *options,  # type: MutateInOptions
                  **kwargs  # type: Any
                  ):
        # type: (...) -> MutateInResult
        """Perform multiple atomic modifications within a document.

        :param key: The key of the document to modify
        :param spec: An iterable of specs (See :mod:`.couchbase.mutate_in.MutateInSpecItemBase`)
        :param options: Options for the mutate_in operation.
        :param kwargs: Override corresponding value in options.

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
        return ResultPrecursor(CoreClient.mutate_in(self.bucket, key, spec, **final_options), final_options)

    def binary(self):
        # type: (...) -> BinaryCollection
        return BinaryCollection(self)

    @_dsop(create_type=dict)
    def map_add(self, key, mapkey, value, create=False, **kwargs):
        """
        Set a value for a key in a map.

        These functions are all wrappers around the :meth:`mutate_in` or
        :meth:`lookup_in` methods.

        :param key: The document ID of the map
        :param mapkey: The key in the map to set
        :param value: The value to use (anything serializable to JSON)
        :param create: Whether the map should be created if it does not exist
        :param kwargs: Additional arguments passed to :meth:`mutate_in`
        :raise: :cb_exc:`Document.DocumentNotFoundException` if the document does not exist.
            and `create` was not specified

        .. Initialize a map and add a value

            cb.upsert('a_map', {})
            cb.map_add('a_map', 'some_key', 'some_value')
            cb.map_get('a_map', 'some_key').value  # => 'some_value'
            cb.get('a_map').value  # => {'some_key': 'some_value'}

        """
        op = SD.upsert(mapkey, value)
        sdres = self.mutate_in(key, (op,), **kwargs)
        return self._wrap_dsop(sdres, **kwargs)

    @_dsop()
    def map_get(self, key, mapkey, **kwargs):
        """
        Retrieve a value from a map.

        :param key: The document ID
        :param mapkey: Key within the map to retrieve
        :return: :class:`~.ValueResult`
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add` for an example
        """
        op = SD.get(mapkey)
        sdres = self.lookup_in(key, (op,), **kwargs)
        return self._wrap_dsop(sdres, True)

    @_dsop()
    def map_remove(self, key, mapkey, **kwargs):
        """
        Remove an item from a map.

        :param key: The document ID
        :param mapkey: The key in the map
        :param See:meth:`mutate_in` for options
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. Remove a map key-value pair:

            cb.map_remove('a_map', 'some_key')

        .. seealso:: :meth:`map_add`
        """
        op = SD.remove(mapkey)
        sdres = self.mutate_in(key, (op,), **kwargs)
        return self._wrap_dsop(sdres, **kwargs)

    @_dsop()
    def map_size(self, key, **kwargs):
        """
        Get the number of items in the map.

        :param key: The document ID of the map
        :return int: The number of items in the map
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """

        return self._get_content(self.lookup_in(key, (SD.get_count(''),), **kwargs))[0]

    @_dsop(create_type=list)
    def list_append(self, key, value, create=False, **kwargs):
        """
        Add an item to the end of a list.

        :param key: The document ID of the list
        :param value: The value to append
        :param create: Whether the list should be created if it does not
               exist. Note that this option only works on servers >= 4.6
        :param kwargs: Additional arguments to :meth:`mutate_in`
        :return: :class:`~.OperationResult`.
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.
            and `create` was not specified.

        example::

            cb.list_append('a_list', 'hello')
            cb.list_append('a_list', 'world')

        .. seealso:: :meth:`map_add`
        """
        op = SD.array_append('', value)
        sdres = self.mutate_in(key, (op,), **kwargs)
        return self._wrap_dsop(sdres, **kwargs)

    @_dsop(create_type=list)
    def list_prepend(self, key, value, create=False, **kwargs):
        """
        Add an item to the beginning of a list.

        :param key: Document ID
        :param value: Value to prepend
        :param create:
            Whether the list should be created if it does not exist
        :param kwargs: Additional arguments to :meth:`mutate_in`.
        :return: :class:`OperationResult`.
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.
            and `create` was not specified.

        This function is identical to :meth:`list_append`, except for prepending
        rather than appending the item

        .. seealso:: :meth:`list_append`, :meth:`map_add`
        """
        op = SD.array_prepend('', value)
        sdres = self.mutate_in(key, (op,), **kwargs)
        return self._wrap_dsop(sdres, **kwargs)

    @_dsop()
    def list_set(self, key, index, value, **kwargs):
        """
        Sets an item within a list at a given position.

        :param key: The key of the document
        :param index: The position to replace
        :param value: The value to be inserted
        :param kwargs: Additional arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        :raise: :exc:`IndexError` if the index is out of bounds

        example::

            cb.upsert('a_list', ['hello', 'world'])
            cb.list_set('a_list', 1, 'good')
            cb.get('a_list').value # => ['hello', 'good']

        .. seealso:: :meth:`map_add`, :meth:`list_append`
        """
        op = SD.replace('[{0}]'.format(index), value)
        sdres = self.mutate_in(key, (op,), **kwargs)
        return self._wrap_dsop(sdres, **kwargs)

    @_dsop(create_type=list)
    def set_add(self, key, value, create=False, **kwargs):
        """
        Add an item to a set if the item does not yet exist.

        :param key: The document ID
        :param value: Value to add
        :param create: Create the set if it does not exist
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: A :class:`~.OperationResult` if the item was added,
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist
            and `create` was not specified.

        .. seealso:: :meth:`map_add`
        """
        op = SD.array_addunique('', value)
        try:
            sdres = self.mutate_in(key, (op,), **kwargs)
            return self._wrap_dsop(sdres, **kwargs)
        except PathExistsException:
            pass

    @_dsop()
    def set_remove(self, key, value, **kwargs):
        """
        Remove an item from a set.

        :param key: The docuent ID
        :param value: Value to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: A :class:`OperationResult` if the item was removed, false
                 otherwise
        :raise: :cb_exc:`DocumentNotFoundException` if the set does not exist.

        .. seealso:: :meth:`set_add`, :meth:`map_add`
        """
        while True:
            rv = self.bucket.get(key, **kwargs)
            try:
                ix = rv.value.index(value)
                kwargs['cas'] = rv.cas
                return self.list_remove(key, ix, **kwargs)
            except DocumentExistsException:
                pass
            except ValueError:
                return

    def set_size(self, key, **kwargs):
        """
        Get the length of a set.

        :param key: The document ID of the set
        :return: The length of the set
        :raise: :cb_exc:`DocumentNotFoundException` if the set does not exist.

        """
        return self.list_size(key, **kwargs)

    def set_contains(self, key, value, **kwargs):
        """
        Determine if an item exists in a set
        :param key: The document ID of the set
        :param value: The value to check for
        :return: True if `value` exists in the set
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist
        """
        rv = self.bucket.get(key, **kwargs)
        return value in rv.value

    @_dsop()
    def list_get(self, key, index, **kwargs):
        """
        Get a specific element within a list.

        :param key: The document ID
        :param index: The index to retrieve
        :return: :class:`ValueResult` for the element
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        return self.map_get(key, '[{0}]'.format(index), **kwargs)

    @_dsop()
    def list_remove(self, key, index, **kwargs):
        """
        Remove the element at a specific index from a list.

        :param key: The document ID of the list
        :param index: The index to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        return self.map_remove(key, '[{0}]'.format(index), **kwargs)

    @_dsop()
    def list_size(self, key, **kwargs):
        """
        Retrieve the number of elements in the list.

        :param key: The document ID of the list
        :return: The number of elements within the list
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        return self.map_size(key, **kwargs)

    @_dsop(create_type=list)
    def queue_push(self, key, value, create=False, **kwargs):
        """
        Add an item to the end of a queue.

        :param key: The document ID of the queue
        :param value: The item to add to the queue
        :param create: Whether the queue should be created if it does not exist
        :param kwargs: Arguments to pass to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :cb_exc:`DocumentNotFoundException` if the queue does not exist and
            `create` was not specified.

        example::

            # Ensure it's removed first

            cb.remove('a_queue')
            cb.queue_push('a_queue', 'job9999', create=True)
            cb.queue_pop('a_queue').value  # => job9999
        """
        return self.list_prepend(key, value, **kwargs)

    @_dsop()
    def queue_pop(self, key, **kwargs):
        """
        Remove and return the first item queue.

        :param key: The document ID
        :param kwargs: Arguments passed to :meth:`mutate_in`
        :return: A :class:`ValueResult`
        :raise: :cb_exc:`QueueEmpty` if there are no items in the queue.
        :raise: :cb_exc:`DocumentNotFoundException` if the queue does not exist.
        """
        while True:
            try:
                itm = self.list_get(key, -1, **kwargs)
            except IndexError:
                raise QueueEmpty

            kwargs.update({k: v for k, v in getattr(itm, '__dict__', {}).items() if k in {'cas'}})
            try:
                self.list_remove(key, -1, **kwargs)
                return itm
            except DocumentExistsException:
                pass
            except IndexError:
                raise QueueEmpty

    @_dsop()
    def queue_size(self, key):
        """
        Get the length of the queue.

        :param key: The document ID of the queue
        :return: The length of the queue
        :raise: :cb_exc:`DocumentNotFoundException` if the queue does not exist.
        """
        return self.list_size(key)

    dsops = (map_get,
             map_add,
             map_remove,
             queue_push,
             list_size,
             map_size,
             queue_pop,

             list_set,
             list_remove,
             list_prepend,
             list_get,
             queue_size,

             list_append,
             set_add,
             set_contains,
             set_remove,
             set_size)

    dsop_strs = tuple(map(lambda x: x.__name__, dsops))
    _MEMCACHED_NOMULTI = CoreClient._MEMCACHED_NOMULTI + dsop_strs
    _MEMCACHED_OPERATIONS = CoreClient._MEMCACHED_OPERATIONS + dsop_strs


class BinaryCollection(object):
    def __init__(self,
                 collection  # type: CBCollection
                 ):
        self._collection = collection
        # The following are needed for the @_mutate_result_and_inject annotation.
        # If that implementation is changed, we must maintain that here.  That could be
        # improved, later.
        self._self_name = self._collection._self_name
        self._self_scope = self._collection._self_scope
        self.true_collections = self._collection.true_collections
        self._inject_scope_collection_kwargs = collection._inject_scope_collection_kwargs

    _MEMCACHED_OPERATIONS = ('append', 'prepend', 'increment', 'decrement')
    _MEMCACHED_NOMULTI = _MEMCACHED_OPERATIONS

    @_mutate_result_and_inject
    def append(self,
               key,         # type: str
               value,       # type: Union[str|bytes]
               *options,    # type: AppendOptions
               **kwargs     # type: Any
               ):
        # type: (...) -> MutationResult
        """Append a string to an existing value in Couchbase.

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


        :param key: Key for the value to append
        :param value: The data to append to the existing value.
        :param options: Options for the append operation.
        :raise: :exc:`.NotStoredException` if the key does not exist
        """
        final_options = {"format": FMT_UTF8}
        final_options.update(forward_args(kwargs, *options))
        x = CoreClient.append(self._collection.bucket, key, value, **final_options)
        return ResultPrecursor(x, options)

    @_mutate_result_and_inject
    def prepend(self,
                key,  # type: str
                value,  # type: str
                *options,  # type: PrependOptions
                **kwargs  # type: Any
                ):
        # type: (...) -> MutationResult
        """Prepend a string to an existing value in Couchbase.

        :param key: Key for the value to append
        :param value: The data to append to the existing value.
        :param options: Options for the prepend operation.
        :raise: :exc:`.NotStoredException` if the key does not exist

        .. seealso:: :meth:`append`
        """
        final_options = {"format": FMT_UTF8}
        final_options.update(forward_args(kwargs, *options))
        x = CoreClient.prepend(self._collection.bucket, key, value, **final_options)
        return ResultPrecursor(x, options)

    @_mutate_result_and_inject
    def increment(self,
                  key,  # type: str
                  *options,  # type: IncrementOptions
                  **kwargs
                  ):
        # type: (...) -> MutationResult
        """Increment the numeric value of an item.

        This method instructs the server to treat the item stored under
        the given key as a numeric counter.

        Counter operations require that the stored value
        exists as a string representation of a number (e.g. ``123``). If
        storing items using the :meth:`upsert` family of methods, and
        using the default :const:`couchbase_core.FMT_JSON` then the value
        will conform to this constraint.

        :param key: A key whose counter value is to be modified
        :param delta: an amount by which the key should be incremented.
        :param options: Options for the increment operation.
        :param kwargs: Overrides corresponding value in the options
        :raise: :exc:`.DocumentNotFoundException` if the key does not exist on the
            bucket (and `initial` was `None`)
        :raise: :exc:`.DeltaBadvalException` if the key exists, but the
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
        x = CoreClient.counter(self._collection.bucket, key, **final_opts)
        return ResultPrecursor(x, final_opts)

    @_mutate_result_and_inject
    def decrement(self,
                  key,          # type: str
                  *options,     # type: IncrementOptions
                  **kwargs
                  ):
        # type: (...) -> MutationResult
        """Decrement the numeric value of an item.

        This method instructs the server to treat the item stored under
        the given key as a numeric counter.

        Counter operations require that the stored value
        exists as a string representation of a number (e.g. ``123``). If
        storing items using the :meth:`upsert` family of methods, and
        using the default :const:`couchbase_core.FMT_JSON` then the value
        will conform to this constraint.

        :param key: A key whose counter value is to be modified
        :param delta: an amount by which the key should be decremented.
        :param options: Options for the decrement operation.
        :param kwargs: Overrides corresponding value in the options
        :raise: :exc:`.DocumentNotFoundException` if the key does not exist on the
            bucket (and `initial` was `None`)
        :raise: :exc:`.DeltaBadvalException` if the key exists, but the
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
        final_opts['delta'] = -final_opts.get('delta', DeltaValue(1))
        x = CoreClient.counter(self._collection.bucket, key, **final_opts)
        return ResultPrecursor(x, final_opts)

    @volatile
    def increment_multi(self, keys, *options, **kwargs):
        func = _wrap_multi_mutation_result(_Base.counter_multi)
        final_opts = self._check_delta_initial(kwargs, *options)
        return func(self._collection, keys, **final_opts)

    @volatile
    def decrement_multi(self, keys, *options, **kwargs):
        func = _wrap_multi_mutation_result(_Base.counter_multi)
        final_opts = self._check_delta_initial(kwargs, *options)
        delta = final_opts.pop('delta')
        if delta:
            final_opts['delta'] = -delta
        return func(self._collection, keys, **final_opts)

    @staticmethod
    def _check_delta_initial(kwargs, *options):
        final_opts = forward_args(kwargs, *options)
        init_arg = final_opts.get('initial')
        initial = None if init_arg is None else int(SignedInt64.verified(init_arg))
        if initial is not None:
            final_opts['initial'] = initial
        delta_arg = final_opts.get('delta')
        delta = None if delta_arg is None else int(DeltaValue.verified(delta_arg))
        if delta is not None:
            final_opts['delta'] = delta
        return final_opts


class Scope(object):
    def __init__(self,  # type: Scope
                 parent,  # type: couchbase.bucket.Bucket
                 name=None  # type: str
                 ):
        # type: (...) -> None
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
    def name(self):
        # type (...)->str
        """

        :return:    A string value that is the name of the collection.
        """
        return self._name

    def default_collection(self):
        # type: (...) -> CBCollection
        """
        Returns the default collection for this bucket.
        :return: A :class:`.Collection` for a collection with the given name.
        """
        return self._gen_collection(None)

    def _gen_collection(self,
                        collection_name  # type: Optional[str]
                        ):
        # type: (...) -> CBCollection
        return CBCollection._cast(self, collection_name)

    def collection(self,
                   collection_name  # type: str
                   ):
        # type: (...) -> CBCollection
        """
        Gets the named collection for this bucket.

        :param collection_name: string identifier for a given collection.
        :return: A :class:`.Collection` for a collection with the given name.

        :raise: CollectionNotFoundException
        """
        return self._gen_collection(collection_name)


class CoreClientDatastructureWrap(CoreClient):
    def _wrap_dsop(self, sdres, has_value=False, **kwargs):
        return getattr(CoreClient._wrap_dsop(self, sdres, has_value), 'value')

    @property
    def lockmode(self):
        return couchbase.options.LockMode(super(CoreClientDatastructureWrap, self).lockmode)


class AsyncCBCollection(AsyncClientMixin, CBCollection):
    pass


Collection = CBCollection
