from .subdoc import LookupInSpec, spec_to_SDK2
from .subdoc import gen_projection_spec
from .result import GetResult, get_result_wrapper, SDK2Result
from .options import forward_args, Seconds, OptionBlockTimeOut
from .mutate_in import mutation_result, MutationResult, MutateInSpec, MutateInOptions
from .options import OptionBlock
from .durability import ReplicateTo, PersistTo, ClientDurableOption
from couchbase_core._libcouchbase import Bucket as _Base
import couchbase_v3.exceptions
from couchbase_core.bucket import Bucket as CoreBucket
import copy
import pyrsistent
from typing import *


class ReplaceOptions(OptionBlockTimeOut, ClientDurableOption):
    def __init__(self, *args, **kwargs):
        super(ReplaceOptions, self).__init__(*args, **kwargs)

    def cas(self, cas  # type: int
           ):
        # type: (...)->ReplaceOptions
        self.cas = cas
        return self


class AppendOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(AppendOptions, self).__init__(*args, **kwargs)


class RemoveOptionsBase(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(RemoveOptionsBase, self).__init__(*args, **kwargs)


class RemoveOptions(RemoveOptionsBase, ClientDurableOption):

    ServerDurable=RemoveOptionsBase
    ClientDurable=RemoveOptionsBase

    def __init__(self, *args, **kwargs):
        super(RemoveOptions, self).__init__(*args, **kwargs)


class PrependOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(PrependOptions, self).__init__(*args, **kwargs)


class UnlockOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(UnlockOptions, self).__init__(*args, **kwargs)


class CounterOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(CounterOptions, self).__init__(*args, **kwargs)


class CollectionOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(CollectionOptions, self).__init__(*args, **kwargs)


class GetAndTouchOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(GetAndTouchOptions, self).__init__(*args, **kwargs)


class LockOptions(OptionBlock):
    pass


class GetOptionsProject(OptionBlock):
    def __init__(self, parent, *args):
        self['project']=args
        super(GetOptionsProject,self).__init__(**parent)


class GetOptionsNonProject(OptionBlock):
    def __init__(self, parent):
        super(GetOptionsNonProject,self).__init__(**parent)


class GetOptions(OptionBlock):
    def __init__(self, *args, **kwargs):
        super(GetOptions, self).__init__(*args, **kwargs)

    def project(self,
                *args):
        # type: (...)->GetOptionsProject
        return GetOptionsProject(self, *args)

    def timeout(self,
                duration  # type: Seconds
                ):
        # type: (...)->GetOptionsNonProject
        self['timeout']=duration.__int__()
        return GetOptionsNonProject(self)

    def __copy__(self):
        return GetOptionsNonProject(**self)


class GetAndLockOptions(GetOptions, LockOptions):
    pass


RawCollectionMethod = TypeVar('T', bound=Callable[[Tuple['CBCollection',...]], Any])


def _inject_scope_and_collection(func  # type: RawCollectionMethod
                                 ):
    # type: (...) ->RawCollectionMethod

    def wrapped(self, *args, **kwargs):
        if self.true_collections:
            if self.name and not self._scope:
                raise couchbase_v3.exceptions.CollectionMissingException
            if self._scope and self.name:
                kwargs['scope'] = self._scope
                kwargs['collection'] = self.name
        return func(self, *args, **kwargs)

    return wrapped


def _get_result_and_inject(func  # type: RawCollectionMethod
                           ):
    # type: (...) ->RawCollectionMethod
    return _inject_scope_and_collection(get_result_wrapper(func))


def _mutate_result_and_inject(func  # type: RawCollectionMethod
                              ):
    # type: (...) ->RawCollectionMethod
    return _inject_scope_and_collection(mutation_result(func))


ResultPrecursor = Tuple[SDK2Result, Any]


class InsertOptions(OptionBlock, ClientDurableOption):
    pass


class GetFromReplicaOptions(OptionBlock):
    pass


class CBCollection(object):
    def __init__(self,
                 parent,  # type: Scope
                 name=None,  # type: Optional[str]
                 options=None  # type: CollectionOptions
                 ):
        # type: (...)->None
        super(CBCollection, self).__init__()
        self.parent = parent  # type: Scope
        self.name = name
        self.true_collections = self.name and self._scope

    @property
    def bucket(self):
        # type: (...) -> CoreBucket
        return self.parent.bucket._bucket

    @property
    def _scope(self):
        return self.parent.name


    def _get_generic(self, key, kwargs, options):
        options = forward_args(kwargs, *options)
        options.pop('key', None)
        spec = options.pop('spec', [])
        project = options.pop('project', None)
        if project:
            if len(project) < 17:
                spec = gen_projection_spec(project)
        if not project:
            x = _Base.get(self.bucket, key, **options)
        else:
            x = self.bucket.lookup_in(key, *spec, **options)
        return x, options


    @overload
    def get(self,
            key,  # type:str
            options=None,  # type: GetOptions
            ):
        # type: (...) -> GetResult
        pass

    @overload
    def get(self,
            key,  # type:str
            project=None,  # type: couchbase_core.JSON
            timeout=None,  # type: Seconds
            quiet=None,  # type: bool
            replica=False,  # type: bool
            no_format=False  # type: bool
            ):
        # type: (...) -> GetResult
        pass

    @_get_result_and_inject
    def get(self,
            key,  # type: str
            *options,  # type: GetOptions
            **kwargs  # type: Any
            ):
        # type: (...) -> GetResult
        """Obtain an object stored in Couchbase by given key.

        :param string key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`

        :param Durat timeout: If specified, indicates that the key's expiration
            time should be *modified* when retrieving the value.

        :param boolean quiet: causes `get` to return None instead of
            raising an exception when the key is not found. It defaults
            to the value set by :attr:`~quiet` on the instance. In
            `quiet` mode, the error may still be obtained by inspecting
            the :attr:`~.Result.rc` attribute of the :class:`.Result`
            object, or checking :attr:`.Result.success`.

            Note that the default value is `None`, which means to use
            the :attr:`quiet`. If it is a boolean (i.e. `True` or
            `False`) it will override the `couchbase_core.bucket.Bucket`-level
            :attr:`quiet` attribute.

        :param bool replica: Whether to fetch this key from a replica
            rather than querying the master server. This is primarily
            useful when operations with the master fail (possibly due to
            a configuration change). It should normally be used in an
            exception handler like so

            Using the ``replica`` option::

                try:
                    res = c.get("key", quiet=True) # suppress not-found errors
                catch CouchbaseError:
                    res = c.get("key", replica=True, quiet=True)

        :param bool no_format: If set to ``True``, then the value will
            always be delivered in the :class:`~couchbase_core.result.Result`
            object as being of :data:`~couchbase_core.FMT_BYTES`. This is a
            item-local equivalent of using the :attr:`data_passthrough`
            option

        :raise: :exc:`.NotFoundError` if the key does not exist
        :raise: :exc:`.CouchbaseNetworkError`
        :raise: :exc:`.ValueFormatError` if the value cannot be
            deserialized with chosen decoder, e.g. if you try to
            retreive an object stored with an unrecognized format
        :return: A :class:`~.Result` object

        Simple get::

            value = cb.get('key').value

        Get multiple values::

            cb.get_multi(['foo', 'bar'])
            # { 'foo' : <Result(...)>, 'bar' : <Result(...)> }

        Inspect the flags::

            rv = cb.get("key")
            value, flags, cas = rv.value, rv.flags, rv.cas

        Update the expiration time::

            rv = cb.get("key", timeout=10)
            # Expires in ten seconds

        .. seealso:: :meth:`get_multi`
        """
        return self._get_generic(key, kwargs, options)

    @overload
    def get_and_touch(self,
                      id,  # type: str
                      expiration,  # type: int
                      options=None  # type: GetAndTouchOptions
                      ):
        # type: (...)->IGetResult
        pass

    @_get_result_and_inject
    def get_and_touch(self,
                      id,  # type: str
                      expiration,  # type: int
                      *options,  # type: Tuple[GetAndTouchOptions]
                      **kwargs  # type: Any
                      ):
        # type: (...)->Tuple[SDK2Result, Tuple[Tuple[GetAndTouchOptions]]]
        kwargs_final = forward_args(kwargs, *options)
        if 'durability' in kwargs_final.keys():
            raise couchbase_v3.exceptions.ReplicaNotAvailableException()
        cb = self._bucket  # type: CoreBucket
        kwargs_final['ttl'] = 0
        x = cb.get(id, **kwargs_final)
        return x, options

    @_get_result_and_inject
    def get_and_lock(self,
                     id,  # type: str
                     expiration,  # type: int
                     *options,  # type: GetAndLockOptions
                     **kwargs
                     ):
        # type: (...)->IGetResult
        x = _Base.get(self.bucket, id, expiration, **forward_args(kwargs, *options))
        _Base.lock(self.bucket, id, options)
        return x, options

    @_get_result_and_inject
    def get_from_replica(self,
                         id,  # type: str
                         replica_index,  # type: ReplicaMode
                         *options,  # type: GetFromReplicaOptions
                         **kwargs  # type: any
                         ):
        # type: (...)->IGetResult
        return self.bucket.rget(id, replica_index, **forward_args(kwargs, *options))

    def touch(self,
              id,  # type: str
              *options,  # type: TouchOptions
              **kwargs):
        # type: (...)->MutationResult
        """Update a key's expiration time

        :param string key: The key whose expiration time should be
            modified
        :param int timeout: The new expiration time. If the expiration time
            is `0` then the key never expires (and any existing
            expiration is removed)
        :return: :class:`.OperationResult`

        Update the expiration time of a key ::

            cb.upsert("key", timeout=100)
            # expires in 100 seconds
            cb.touch("key", timeout=0)
            # key should never expire now

        :raise: The same things that :meth:`get` does

        .. seealso:: :meth:`get` - which can be used to get *and* update the
            expiration, :meth:`touch_multi`
        """
        return _Base.touch(self.bucket,id, **forward_args(kwargs, *options))

    @mutation_result
    def unlock(self,
               id,  # type: str
               *options  # type: UnlockOptions
               ):
        # type: (...)->MutationResult
        """Unlock a Locked Key in Couchbase.

        This unlocks an item previously locked by :meth:`lock`

        :param key: The key to unlock
        :param cas: The cas returned from :meth:`lock`'s
            :class:`.Result` object.

        See :meth:`lock` for an example.

        :raise: :exc:`.TemporaryFailError` if the CAS supplied does not
            match the CAS on the server (possibly because it was
            unlocked by previous call).

        .. seealso:: :meth:`lock` :meth:`unlock_multi`
        """
        return _Base.unlock(self.bucket, id, **forward_args({}, *options))

    def exists(self, id,  # type: str,
               timeout=None,  # type: Seconds
               ):
        # type: (...)->IExistsResult
        """
        Any exceptions raised by the underlying platform

        :param id: the id of the document
        :type: str
        :param timeout: the time allowed for the operation to be terminated. This is controlled by the client.
        :type: str
        :return: An IExistsResult object with a boolean value indicating the presence of the document.
        :raises: Any exceptions raised by the underlying platform
        """

    class UpsertOptions(OptionBlock, ClientDurableOption):
        def __init__(self, *args, **kwargs):
            super(CBCollection.UpsertOptions, self).__init(*args, **kwargs)

    @overload
    def upsert(self, key, value, *options  # type: UpsertOptions
               ):
        pass

    @overload
    def upsert(self,
               id,  # type: str
               value,  # type: Any
               cas=0,  # type: int
               timeout=0,  # type: Seconds
               format=None,
               persist_to=PersistTo.NONE,  # type: PersistTo.Value
               replicate_to=ReplicateTo.NONE  # type: ReplicateTo.Value
               ):
        # type: (...) -> MutationResult
        pass

    @_mutate_result_and_inject
    def upsert(self,
               id,  # type: str
               value,  # type: Any
               *options,  # type: UpsertOptions
               **kwargs  # type: Any
               ):
        # type: (...)->MutationResult
        """Unconditionally store the object in Couchbase.

        :param key:
            The key to set the value with. By default, the key must be
            either a :class:`bytes` or :class:`str` object encodable as
            UTF-8. If a custom `transcoder` class is used (see
            :meth:`~__init__`), then the key object is passed directly
            to the transcoder, which may serialize it how it wishes.
        :type key: string or bytes

        :param value: The value to set for the key.
            This should be a native Python value which will be transparently
            serialized to JSON by the library. Do not pass already-serialized
            JSON as the value or it will be serialized again.

            If you are using a different `format` setting (see `format`
            parameter), and/or a custom transcoder then value for this
            argument may need to conform to different criteria.

        :param int cas: The _CAS_ value to use. If supplied, the value
            will only be stored if it already exists with the supplied
            CAS

        :param Seconds timeout: If specified, the key will expire after this
            many seconds

        :param int format: If specified, indicates the `format` to use
            when encoding the value. If none is specified, it will use
            the `default_format` For more info see
            :attr:`~.default_format`

        :param int persist_to:
            Perform durability checking on this many nodes nodes for
            persistence to disk. See :meth:`endure` for more information

        :param int replicate_to: Perform durability checking on this
            many replicas for presence in memory. See :meth:`endure` for
            more information.

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

        Several sets at the same time (mutli-set)::

            cb.upsert_multi({'foo': 'bar', 'baz': 'value'})

        .. seealso:: :meth:`upsert_multi`
        """

        return _Base.upsert(self.bucket, id, value, **forward_args(kwargs, *options))

    def insert(self,
               id,  # type: str
               value,  # type: Any
               *options  # type: InsertOptions
               ):
        # type: (...)->MutationResult
        pass

    @overload
    def insert(self,
               id,  # type: str
               value,  # type: Any
               timeout=Seconds(0),  # type: Seconds
               format=None,  # type: str
               persist_to=PersistTo.NONE,  # type: PersistTo.Value
               replicate_to=ReplicateTo.NONE  # type: ReplicateTo.Value
               ):
        pass

    @_mutate_result_and_inject
    def insert(self, key, value, *options, **kwargs):
        """Store an object in Couchbase unless it already exists.

        Follows the same conventions as :meth:`upsert` but the value is
        stored only if it does not exist already. Conversely, the value
        is not stored if the key already exists.

        Notably missing from this method is the `cas` parameter, this is
        because `insert` will only succeed if a key does not already
        exist on the server (and thus can have no CAS)

        :raise: :exc:`.KeyExistsError` if the key already exists

        .. seealso:: :meth:`upsert`, :meth:`insert_multi`
        """

        return _Base.insert(self.bucket, key, value, **forward_args(kwargs, *options))

    @overload
    def replace(self,
                id,  # type: str
                value,  # type: Any
                cas=0,  # type: int
                timeout=None,  # type: Seconds
                format=None,  # type: bool
                persist_to=PersistTo.NONE,  # type: PersistTo.Value
                replicate_to=ReplicateTo.NONE  # type: ReplicateTo.Value
                ):
        # type: (...)->MutationResult
        pass

    @overload
    def replace(self,
                id,  # type: str
                value,  # type: Any
                options,  # type: ReplaceOptions
                ):
        # type: (...)->MutationResult
        pass

    def replace(self,
                id,  # type: str
                value,  # type: Any
                *options,
                **kwargs
                ):
        # type: (...)->MutationResult
        """Store an object in Couchbase only if it already exists.

           Follows the same conventions as :meth:`upsert`, but the value is
           stored only if a previous value already exists.

           :raise: :exc:`.NotFoundError` if the key does not exist

           .. seealso:: :meth:`upsert`, :meth:`replace_multi`"""

        return _Base.replace(self.bucket, id, value, **forward_args(kwargs, *options))

    @overload
    def remove(self,  # type: CBCollection
               id,  # type: str
               cas=0,  # type: int
               persist_to=PersistTo.NONE,  # type: PersistTo.Value
               replicate_to=ReplicateTo.NONE  # type: ReplicateTo.Value
               ):
        # type: (...)->MutationResult
        pass

    @overload
    def remove(self,  # type: CBCollection
               id,  # type: str
               *options  # type: RemoveOptions
               ):
        # type: (...)->MutationResult
        pass

    @_mutate_result_and_inject
    def remove(self,  # type: CBCollection
               id,  # type: str
               *options,  # type: RemoveOptions
               **kwargs
               ):
        # type: (...)->MutationResult
        """Remove the key-value entry for a given key in Couchbase.

        :param key: A string which is the key to remove. The format and
            type of the key follows the same conventions as in
            :meth:`upsert`
        :type key: string, dict, or tuple/list

        :param int cas: The CAS to use for the removal operation.
            If specified, the key will only be removed from the server
            if it has the same CAS as specified. This is useful to
            remove a key only if its value has not been changed from the
            version currently visible to the client. If the CAS on the
            server does not match the one specified, an exception is
            thrown.
        :param boolean quiet:
            Follows the same semantics as `quiet` in :meth:`get`
        :param int persist_to: If set, wait for the item to be removed
            from the storage of at least these many nodes
        :param int replicate_to: If set, wait for the item to be removed
            from the cache of at least these many nodes
            (excluding the master)
        :raise: :exc:`.NotFoundError` if the key does not exist.
        :raise: :exc:`.KeyExistsError` if a CAS was specified, but
            the CAS on the server had changed
        :return: A :class:`~.Result` object.

        Simple remove::

            ok = cb.remove("key").success

        Don't complain if key does not exist::

            ok = cb.remove("key", quiet=True)

        Only remove if CAS matches our version::

            rv = cb.get("key")
            cb.remove("key", cas=rv.cas)

        Remove multiple keys::

            oks = cb.remove_multi(["key1", "key2", "key3"])

        Remove multiple keys with CAS::

            oks = cb.remove({
                "key1" : cas1,
                "key2" : cas2,
                "key3" : cas3
            })

        .. seealso:: :meth:`remove_multi`, :meth:`endure`
            for more information on the ``persist_to`` and
            ``replicate_to`` options.
        """
        return _Base.remove(self.bucket, id, **forward_args(kwargs, *options))

    @overload
    def lookup_in(self,
                  id,  # type: str,
                  spec,  # type: LookupInSpec
                  *options  # type: LookupInOptions
                  ):
        # type: (...)->ILookupInResult
        pass

    @_get_result_and_inject
    def lookup_in(self,
                  id,  # type: str,
                  spec,  # type: LookupInSpec
                  *options,  # type: LookupInOptions
                  **kwargs
                  ):
        # type: (...)->ILookupInResult
        return self.bucket.lookup_in(id, *(spec_to_SDK2(spec)), **forward_args(kwargs, *options))

    @_mutate_result_and_inject
    def mutate_in(self,
                  id,  # type: str,
                  spec,  # type: MutateInSpec
                  *options,  # type: MutateInOptions
                  **kwargs
                  ):
        # type: (...)->MutateInResult
        return self.bucket.mutate_in(id, *spec, **forward_args(kwargs, *options))

    def binary(self):
        # type: (...)->IBinaryCollection
        pass

    @overload
    def append(self,
               id,  # type: str
               value,  # type: str
               options=None,  # type: AppendOptions
               ):
        # type: (...)->MutationResult
        pass

    @overload
    def append(self,
               id,  # type: str
               value,  # type: str
               cas=0,  # type: int
               format=None,  # type: long
               persist_to=PersistTo.NONE,  # type: PersistTo.Value
               replicate_to=ReplicateTo.NONE  # type: ReplicateTo.Value
               ):
        pass

    @_mutate_result_and_inject
    def append(self,
               id,  # type: str
               value,  # type: str
               *options,  # type: Any
               **kwargs  # type: Any
               ):
        # type: (...)->ResultPrecursor
        """Append a string to an existing value in Couchbase.

        :param string value: The data to append to the existing value.

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

        .. seealso:: :meth:`upsert`, :meth:`append_multi`)
        """
        x = _Base.append(self.bucket, id, value, forward_args(kwargs, *options))
        return x, options

    @overload
    def prepend(self,
                id,  # type: str
                value,  # type: Any,
                cas=0,  # type: int
                format=None,  # type: int
                persist_to=PersistTo.NONE,  # type: PersistTo.Value
                replicate_to=ReplicateTo.NONE  # type: ReplicateTo.Value
                ):
        # type: (...)->MutationResult
        pass

    @overload
    def prepend(self,
                id,  # type: str
                value,  # type: str
                *options  # type: PrependOptions
                ):
        # type: (...)->MutationResult
        pass

    def prepend(self,
                id,  # type: str
                value,  # type: str
                *options,  # type: PrependOptions
                **kwargs  # type: Any
                ):
        # type: (...)->ResultPrecursor
        x = _Base.prepend(self.bucket, id, value, **forward_args(kwargs, *options))
        return x, options

    @overload
    def increment(self,
                  id,  # type: str
                  delta,  # type: int
                  initial=None,  # type: int
                  timeout=Seconds(0)  # type: Seconds
                  ):
        # type: (...)->ResultPrecursor
        pass

    @overload
    def increment(self,
                  id,  # type: str
                  delta,  # type: int
                  *options,  # type: CounterOptions
                  **kwargs
                  ):
        # type: (...)->ResultPrecursor
        pass

    def increment(self,
                  id,  # type: str
                  delta,  # type: int
                  *options,  # type: CounterOptions
                  **kwargs
                  ):
        # type: (...)->ResultPrecursor
        x = _Base.counter(self.bucket, id, delta, **forward_args(kwargs, *options))
        return x, options

    @overload
    def decrement(self,
                  id,  # type: str
                  delta,  # type: int
                  initial=None,  # type: int
                  timeout=Seconds(0)  # type: Seconds
                  ):
        # type: (...)->ResultPrecursor
        pass

    @overload
    def decrement(self,
                  id,  # type: str
                  delta,  # type: int
                  *options,  # type: CounterOptions
                  **kwargs
                  ):
        # type: (...)->ResultPrecursor
        pass

    def decrement(self,
                  id,  # type: str
                  delta,  # type: int
                  *options,  # type: CounterOptions
                  **kwargs
                  ):
        # type: (...)->ResultPrecursor
        x = _Base.counter(self.bucket, id, -delta, **forward_args(kwargs, *options))
        return x, options


class Scope(object):
    def __init__(self,
                 parent,  # type: couchbase_v3.bucket.Bucket
                 name=None  # type: str
                 ):
        if name:
            self._name = name
        self.record = pyrsistent.PRecord()
        self.bucket = parent

    def __deepcopy__(self, memodict={}):
        result = copy.copy(self)
        return result

    @property
    def name(self):
        # type (...)->str
        """

        :return:    A string value that is the name of the collection.
        :except     ScopeNotFoundException
        :except     AuthorizationException
        """
        return self._name

    def default_collection(self, *options, **kwargs):
        return CBCollection(self, name=None, **forward_args(kwargs,*options))

    def open_collection(self,
                        collection_name,  # type: str
                        *options  # type: CollectionOptions
                        ):
        # type: (...) -> CBCollection
        """

        :param collection_name:
        :param options:
        :return:
        """
        """
        Gets an ICollection instance given a collection name.
        
        Response
        A ICollection implementation for a collection with a given name.
        Throws
        Any exceptions raised by the underlying platform
        CollectionNotFoundException
        AuthorizationException

        :param collection_name: string identifier for a given collection.
        :param options: collection options
        :return:
        """
        return CBCollection(self, collection_name, *options)


UpsertOptions = CBCollection.UpsertOptions