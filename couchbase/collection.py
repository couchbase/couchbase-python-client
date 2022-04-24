from __future__ import annotations

from copy import copy
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    Union,
                    overload)

from couchbase.binary_collection import BinaryCollection
from couchbase.datastructures import (CouchbaseList,
                                      CouchbaseMap,
                                      CouchbaseQueue,
                                      CouchbaseSet)
from couchbase.exceptions import (DocumentExistsException,
                                  ErrorMapper,
                                  InvalidArgumentException,
                                  PathExistsException,
                                  QueueEmpty)
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic import BlockingWrapper, decode_value
from couchbase.logic.collection import CollectionLogic
from couchbase.options import (AppendMultiOptions,
                               DecrementMultiOptions,
                               ExistsMultiOptions,
                               GetMultiOptions,
                               IncrementMultiOptions,
                               InsertMultiOptions,
                               LockMultiOptions,
                               PrependMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UnlockMultiOptions,
                               UpsertMultiOptions,
                               forward_args,
                               get_valid_multi_args)
from couchbase.pycbc_core import (binary_multi_operation,
                                  kv_multi_operation,
                                  operations)
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetResult,
                              LookupInResult,
                              MultiCounterResult,
                              MultiExistsResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutateInResult,
                              MutationResult,
                              OperationResult)
from couchbase.subdocument import (array_addunique,
                                   array_append,
                                   array_prepend,
                                   count)
from couchbase.subdocument import get as subdoc_get
from couchbase.subdocument import remove as subdoc_remove
from couchbase.subdocument import replace
from couchbase.subdocument import upsert as subdoc_upsert
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType
    from couchbase.durability import DurabilityType
    from couchbase.options import (AcceptableInts,
                                   AppendOptions,
                                   DecrementOptions,
                                   ExistsOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetOptions,
                                   IncrementOptions,
                                   InsertOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   MutationMultiOptions,
                                   NoValueMultiOptions,
                                   PrependOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.result import MultiResultType
    from couchbase.subdocument import Spec, StoreSemantics


class Collection(CollectionLogic):

    def __init__(self, scope, name):
        super().__init__(scope, name)

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Any
            ) -> GetResult:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Any
    ) -> GetResult:
        return super().get(key, **kwargs)

    @BlockingWrapper.block(ExistsResult)
    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Any
    ) -> ExistsResult:
        return super().exists(key, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def insert(
        self,  # type: "Collection"
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        return super().insert(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        return super().upsert(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Any
                ) -> MutationResult:
        return super().replace(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Any
               ) -> MutationResult:
        return super().remove(key, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Any
              ) -> MutationResult:
        return super().touch(key, expiry, *opts, **kwargs)

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Any
                      ) -> GetResult:
        # add to kwargs for conversion to int
        kwargs["expiry"] = expiry
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_touch_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_and_touch_internal(self,
                                key,  # type: str
                                **kwargs,  # type: Any
                                ) -> GetResult:
        return super().get_and_touch(key, **kwargs)

    def get_and_lock(
        self,
        key,  # type: str
        lock_time,  # type: timedelta
        *opts,  # type: GetAndLockOptions
        **kwargs,  # type: Any
    ) -> GetResult:
        # add to kwargs for conversion to int
        kwargs["lock_time"] = lock_time
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_lock_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_and_lock_internal(self,
                               key,  # type: str
                               **kwargs,  # type: Any
                               ) -> GetResult:
        return super().get_and_lock(key, **kwargs)

    @BlockingWrapper.block(None)
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Any
               ) -> None:
        return super().unlock(key, cas, *opts, **kwargs)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Any
    ) -> LookupInResult:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_internal(key, spec, **final_args)

    @BlockingWrapper.block_and_decode(LookupInResult)
    def _lookup_in_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Any
    ) -> LookupInResult:
        return super().lookup_in(key, spec, **kwargs)

    @BlockingWrapper.block(MutateInResult)
    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Any
    ) -> MutateInResult:
        return super().mutate_in(key, spec, *opts, **kwargs)

    def binary(self) -> BinaryCollection:
        return BinaryCollection(self)

    @BlockingWrapper.block(MutationResult)
    def _append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        return super().append(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def _prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        return super().prepend(key, value, *opts, **kwargs)

    @BlockingWrapper.block(CounterResult)
    def _increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> CounterResult:
        return super().increment(key, *opts, **kwargs)

    @BlockingWrapper.block(CounterResult)
    def _decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> CounterResult:
        return super().decrement(key, *opts, **kwargs)

    def couchbase_list(self, key  # type: str
                       ) -> CouchbaseList:
        return CouchbaseList(key, self)

    @BlockingWrapper._dsop(create_type='list')
    def list_append(self, key,  # type: str
                    value,  # type: JSONType
                    create=False,  # type: Optional[bool]
                    **kwargs,  # type: Dict[str, Any]
                    ) -> OperationResult:
        """
        Add an item to the end of a list.

        :param key: The document ID of the list
        :param value: The value to append
        :param create: Whether the list should be created if it does not
               exist. Note that this option only works on servers >= 4.6
        :param kwargs: Additional arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`.
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.
            and `create` was not specified.

        example::

            cb.list_append('a_list', 'hello')
            cb.list_append('a_list', 'world')

        .. seealso:: :meth:`map_add`
        """
        op = array_append('', value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop(create_type='list')
    def list_prepend(self, key,  # type: str
                     value,  # type: JSONType
                     create=False,  # type: Optional[bool]
                     **kwargs,  # type: Dict[str, Any]
                     ) -> OperationResult:
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
        op = array_prepend('', value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def list_set(self, key,  # type: str
                 index,  # type: int
                 value,  # type: JSONType
                 **kwargs  # type: Dict[str, Any]
                 ) -> OperationResult:
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

        op = replace(f'[{index}]', value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def list_get(self, key,  # type: str
                 index,  # type: int
                 **kwargs  # type: Dict[str, Any]
                 ) -> Any:
        """
        Get a specific element within a list.

        :param key: The document ID
        :param index: The index to retrieve
        :return: value for the element
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        op = subdoc_get(f'[{index}]')
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    @BlockingWrapper._dsop()
    def list_remove(self, key,  # type: str
                    index,  # type: int
                    **kwargs  # type: Dict[str, Any]
                    ) -> OperationResult:
        """
        Remove the element at a specific index from a list.

        :param key: The document ID of the list
        :param index: The index to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """

        op = subdoc_remove(f'[{index}]')
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def list_size(self, key,  # type: str
                  **kwargs  # type: Dict[str, Any]
                  ) -> int:
        """
        Get a specific element within a list.

        :param key: The document ID
        :param index: The index to retrieve
        :return: value for the element
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        op = count('')
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    def couchbase_map(self, key  # type: str
                      ) -> CouchbaseMap:
        return CouchbaseMap(key, self)

    @BlockingWrapper._dsop(create_type='dict')
    def map_add(self, key,  # type: str
                mapkey,  # type: str
                value,  # type: Any
                create=False,  # type: Optional[bool]
                **kwargs  # type: Dict[str, Any]
                ) -> OperationResult:
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
        op = subdoc_upsert(mapkey, value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def map_get(self, key,  # type: str
                mapkey,  # type: str
                **kwargs  # type: Dict[str, Any]
                ) -> Any:
        """
        Retrieve a value from a map.

        :param key: The document ID
        :param mapkey: Key within the map to retrieve
        :return: :class:`~.ValueResult`
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add` for an example
        """
        op = subdoc_get(mapkey)
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    @BlockingWrapper._dsop()
    def map_remove(self, key,  # type: str
                   mapkey,  # type: str
                   **kwargs  # type: Dict[str, Any]
                   ) -> OperationResult:
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
        op = subdoc_remove(mapkey)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def map_size(self, key,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> int:
        """
        Get the number of items in the map.

        :param key: The document ID of the map
        :return int: The number of items in the map
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    def couchbase_set(self, key  # type: str
                      ) -> CouchbaseSet:
        return CouchbaseSet(key, self)

    @BlockingWrapper._dsop(create_type='list')
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
        op = array_addunique('', value)
        try:
            sd_res = self.mutate_in(key, (op,), **kwargs)
            return OperationResult(sd_res.cas, sd_res.mutation_token())
        except PathExistsException:
            pass

    @BlockingWrapper._dsop()
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
            rv = self.get(key, **kwargs)
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
        rv = self.get(key, **kwargs)
        return value in rv.value

    def couchbase_queue(self, key  # type: str
                        ) -> CouchbaseQueue:
        return CouchbaseQueue(key, self)

    @BlockingWrapper._dsop(create_type='list')
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

    @BlockingWrapper._dsop()
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

            kwargs.update({k: v for k, v in getattr(
                itm, '__dict__', {}).items() if k in {'cas'}})
            try:
                self.list_remove(key, -1, **kwargs)
                return itm
            except DocumentExistsException:
                pass
            except IndexError:
                raise QueueEmpty

    @BlockingWrapper._dsop()
    def queue_size(self, key):
        """
        Get the length of the queue.

        :param key: The document ID of the queue
        :return: The length of the queue
        :raise: :cb_exc:`DocumentNotFoundException` if the queue does not exist.
        """
        return self.list_size(key)

    def _get_multi_mutation_transcoded_op_args(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: MutationMultiOptions
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool]:

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        per_key_args = final_args.pop('per_key_options', None)
        op_transcoder = final_args.pop('transcoder', self.default_transcoder)
        op_args = {}
        for key, value in keys_and_docs.items():
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                key_transcoder = per_key_args.pop('transcoder', op_transcoder)
                op_args[key].update(per_key_args[key])
                transcoded_value = key_transcoder.encode_value(value)
            else:
                transcoded_value = op_transcoder.encode_value(value)
            op_args[key]['value'] = transcoded_value

        if isinstance(opts_type, ReplaceMultiOptions):
            for k, v in op_args.items():
                expiry = v.get('expiry', None)
                preserve_expiry = v.get('preserve_expiry', False)
                if expiry and preserve_expiry is True:
                    raise InvalidArgumentException(
                        message=("The expiry and preserve_expiry options cannot "
                                 f"both be set for replace operations.  Multi-op key: {k}.")
                    )

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions

    def _get_multi_op_args(
        self,
        keys,  # type: List[str]
        *opts,  # type: NoValueMultiOptions
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool, Dict[str, Transcoder]]:
        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        op_transcoder = final_args.pop('transcoder', self.default_transcoder)
        per_key_args = final_args.pop('per_key_options', None)
        op_args = {}
        key_transcoders = {}
        for key in keys:
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                key_transcoder = per_key_args.pop('transcoder', op_transcoder)
                key_transcoders[key] = key_transcoder
                op_args[key].update(per_key_args[key])
            else:
                key_transcoders[key] = op_transcoder

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions, key_transcoders

    def get_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: GetMultiOptions
        **kwargs,  # type: Any
    ) -> MultiGetResult:
        op_args, return_exceptions, transcoders = self._get_multi_op_args(keys,
                                                                          *opts,
                                                                          opts_type=GetMultiOptions,
                                                                          **kwargs)
        op_type = operations.GET.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                continue
            value = v.raw_result.get('value', None)
            flags = v.raw_result.get('flags', None)
            tc = transcoders[k]
            v.raw_result['value'] = decode_value(tc, value, flags)

        return MultiGetResult(res, return_exceptions)

    def lock_multi(
        self,
        keys,  # type: List[str]
        lock_time,  # type: timedelta
        *opts,  # type: LockMultiOptions
        **kwargs,  # type: Any
    ) -> MultiGetResult:
        kwargs["lock_time"] = lock_time
        op_args, return_exceptions, transcoders = self._get_multi_op_args(keys,
                                                                          *opts,
                                                                          opts_type=LockMultiOptions,
                                                                          **kwargs)
        op_type = operations.GET_AND_LOCK.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                continue
            value = v.raw_result.get('value', None)
            flags = v.raw_result.get('flags', None)
            tc = transcoders[k]
            v.raw_result['value'] = decode_value(tc, value, flags)

        return MultiGetResult(res, return_exceptions)

    def exists_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: ExistsMultiOptions
        **kwargs,  # type: Any
    ) -> MultiExistsResult:
        op_args, return_exceptions, _ = self._get_multi_op_args(keys,
                                                                *opts,
                                                                opts_type=ExistsMultiOptions,
                                                                **kwargs)
        op_type = operations.EXISTS.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiExistsResult(res, return_exceptions)

    def insert_multi(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: InsertMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_mutation_transcoded_op_args(keys_and_docs,
                                                                                 *opts,
                                                                                 opts_type=InsertMultiOptions,
                                                                                 **kwargs)
        op_type = operations.INSERT.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def upsert_multi(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: UpsertMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_mutation_transcoded_op_args(keys_and_docs,
                                                                                 *opts,
                                                                                 opts_type=UpsertMultiOptions,
                                                                                 **kwargs)
        op_type = operations.UPSERT.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def replace_multi(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: ReplaceMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_mutation_transcoded_op_args(keys_and_docs,
                                                                                 *opts,
                                                                                 opts_type=ReplaceMultiOptions,
                                                                                 **kwargs)
        op_type = operations.REPLACE.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def remove_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: RemoveMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        op_args, return_exceptions, _ = self._get_multi_op_args(keys,
                                                                *opts,
                                                                opts_type=RemoveMultiOptions,
                                                                **kwargs)
        op_type = operations.REMOVE.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def touch_multi(
        self,
        keys,  # type: List[str]
        expiry,  # type: timedelta
        *opts,  # type: TouchMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        kwargs['expiry'] = expiry
        op_args, return_exceptions, _ = self._get_multi_op_args(keys,
                                                                *opts,
                                                                opts_type=TouchMultiOptions,
                                                                **kwargs)
        op_type = operations.TOUCH.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def unlock_multi(  # noqa: C901
        self,
        keys,  # type: Union[MultiResultType, Dict[str, int]]
        *opts,  # type: UnlockMultiOptions
        **kwargs,  # type: Any
    ) -> Dict[str, Union[None, CouchbaseBaseException]]:

        op_keys_cas = {}
        if isinstance(keys, dict):
            if not all(map(lambda k: isinstance(k, str), keys.keys())):
                raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
            if not all(map(lambda v: isinstance(v, int), keys.values())):
                raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
            op_keys_cas = copy(keys)
        elif isinstance(keys, (MultiGetResult, MultiMutationResult)):
            for k, v in keys.results.items():
                op_keys_cas[k] = v.cas
        else:
            raise InvalidArgumentException(
                'keys type must be Union[MultiGetResult, MultiMutationResult, Dict[str, int].')

        op_args, return_exceptions, _ = self._get_multi_op_args(list(op_keys_cas.keys()),
                                                                *opts,
                                                                opts_type=UnlockMultiOptions,
                                                                **kwargs)

        for k, v in op_args.items():
            v['cas'] = op_keys_cas[k]

        op_type = operations.UNLOCK.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        output = {}
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                if not return_exceptions:
                    raise ErrorMapper.build_exception(v)
                else:
                    output[k] = ErrorMapper.build_exception(v)
            else:
                output[k] = None

        return output

    def _get_multi_counter_op_args(
        self,
        keys,  # type: List[str]
        *opts,  # type: Union[IncrementMultiOptions, DecrementMultiOptions]
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool]:
        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)

        global_delta, global_initial = self._get_and_validate_delta_initial(final_args)
        final_args['delta'] = int(global_delta)
        final_args['initial'] = int(global_initial)

        per_key_args = final_args.pop('per_key_options', None)
        op_args = {}
        for key in keys:
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                # need to validate delta/initial if provided per key
                delta = per_key_args[key].get('delta', None)
                initial = per_key_args[key].get('initial', None)
                self._validate_delta_initial(delta=delta, initial=initial)
                if delta:
                    per_key_args[key]['delta'] = int(delta)
                if initial:
                    per_key_args[key]['initial'] = int(initial)
                op_args[key].update(per_key_args[key])

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions

    def _get_multi_binary_mutation_op_args(
        self,
        keys_and_docs,  # type: Dict[str, Union[str, bytes, bytearray]]
        *opts,  # type: Union[AppendMultiOptions, PrependMultiOptions]
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool]:

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        parsed_keys_and_docs = {}
        for k, v in keys_and_docs.items():
            if isinstance(v, str):
                value = v.encode("utf-8")
            elif isinstance(v, bytearray):
                value = bytes(v)
            else:
                value = v

            if not isinstance(value, bytes):
                raise ValueError(
                    "The value provided must of type str, bytes or bytearray.")

            parsed_keys_and_docs[k] = value

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        per_key_args = final_args.pop('per_key_options', None)
        op_args = {}
        for key, value in parsed_keys_and_docs.items():
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                op_args[key].update(per_key_args[key])
            op_args[key]['value'] = value

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions

    def _append_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: AppendMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_binary_mutation_op_args(keys_and_values,
                                                                             *opts,
                                                                             opts_type=AppendMultiOptions,
                                                                             **kwargs)
        op_type = operations.APPEND.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def _prepend_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: PrependMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_binary_mutation_op_args(keys_and_values,
                                                                             *opts,
                                                                             opts_type=PrependMultiOptions,
                                                                             **kwargs)
        op_type = operations.PREPEND.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def _increment_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: IncrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        op_args, return_exceptions = self._get_multi_counter_op_args(keys,
                                                                     *opts,
                                                                     opts_type=IncrementMultiOptions,
                                                                     **kwargs)
        op_type = operations.INCREMENT.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiCounterResult(res, return_exceptions)

    def _decrement_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: DecrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        op_args, return_exceptions = self._get_multi_counter_op_args(keys,
                                                                     *opts,
                                                                     opts_type=DecrementMultiOptions,
                                                                     **kwargs)
        op_type = operations.DECREMENT.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiCounterResult(res, return_exceptions)

    @staticmethod
    def default_name():
        return "_default"


"""
@TODO:  remove the code below for the 4.1 release

Everything below should be removed in the 4.1 release.
All options should come from couchbase.options, or couchbase.management.options

"""


class OptionsTimeoutDeprecated(dict):
    def __init__(
        self,
        timeout=None,  # type: timedelta
        span=None,  # type: Any
        **kwargs  # type: Any
    ):
        """
        Base options with timeout and span options
        :param timeout: Timeout for this operation
        :param span: Parent tracing span to use for this operation
        """
        if timeout:
            kwargs["timeout"] = timeout

        if span:
            kwargs["span"] = span
        super().__init__(**kwargs)

    def timeout(
        self,
        timeout,  # type: timedelta
    ):
        self["timeout"] = timeout
        return self

    def span(
        self,
        span,  # type: Any
    ):
        self["span"] = span
        return self


class DurabilityOptionBlockDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        durability=None,  # type: DurabilityType
        expiry=None,  # type: timedelta
    ):
        # type: (...) -> None
        """
        Options for operations with any type of durability

        :param durability: Durability setting
        :param expiry: When any mutation should expire
        :param timeout: Timeout for operation
        """
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def expiry(self):
        return self.get("expiry", None)


class InsertOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        """Insert Options

        **DEPRECATED** User `couchbase.options.InsertOptions`
        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UpsertOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        preserve_expiry=False,  # type: bool
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ReplaceOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        cas=0,  # type: int
        preserve_expiry=False,  # type: bool
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class RemoveOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        cas=0,  # type: int
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetOptionsDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        with_expiry=None,  # type: bool
        project=None,  # type: Iterable[str]
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def with_expiry(self):
        # type: (...) -> bool
        return self.get("with_expiry", False)

    @property
    def project(self):
        # type: (...) -> Iterable[str]
        return self.get("project", [])


class ExistsOptionsDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TouchOptionsDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndTouchOptionsDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndLockOptionsDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UnlockOptionsDeprecated(OptionsTimeoutDeprecated):
    @overload
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class LookupInOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,  # type: LookupInOptions
        timeout=None,  # type: timedelta
        access_deleted=None  # type: bool
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class MutateInOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,  # type: MutateInOptions
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        cas=0,          # type: int
        durability=None,  # type: DurabilityType
        store_semantics=None,  # type: StoreSemantics
        access_deleted=None,  # type: bool
        preserve_expiry=None  # type: bool
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class IncrementOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        expiry=None,       # type: timedelta
        durability=None,   # type: DurabilityType
        delta=None,         # type: DeltaValue
        initial=None,      # type: SignedInt64
        span=None         # type: Any

    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DecrementOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        expiry=None,       # type: timedelta
        durability=None,   # type: DurabilityType
        delta=None,         # type: DeltaValue
        initial=None,      # type: SignedInt64
        span=None         # type: Any
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class AppendOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        durability=None,   # type: DurabilityType
        cas=None,          # type: int
        span=None         # type: Any
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PrependOptionsDeprecated(DurabilityOptionBlockDeprecated):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        durability=None,   # type: DurabilityType
        cas=None,          # type: int
        span=None         # type: Any
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ConstrainedIntDeprecated():
    def __init__(self, value):
        """
        A signed integer between cls.min() and cls.max() inclusive

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        self.value = type(self).verify_value(value)

    @classmethod
    def verify_value(cls, item  # type: AcceptableInts
                     ):
        # type: (...) -> int
        value = getattr(item, 'value', item)
        if not isinstance(value, int) or not (cls.min() <= value <= cls.max()):
            raise InvalidArgumentException(
                "Integer in range {} and {} inclusiverequired".format(cls.min(), cls.max()))
        return value

    @classmethod
    def is_valid(cls,
                 item  # type: AcceptableInts
                 ):
        return isinstance(item, cls)

    def __neg__(self):
        return -self.value

    # Python 3.8 deprecated the implicit conversion to integers using __int__
    # use __index__ instead
    # still needed for Python 3.7
    def __int__(self):
        return self.value

    # __int__ falls back to __index__
    def __index__(self):
        return self.value

    def __add__(self, other):
        if not (self.min() <= (self.value + int(other)) <= self.max()):
            raise InvalidArgumentException(
                "{} + {} would be out of range {}-{}".format(self.value, other, self.min(), self.min()))

    @classmethod
    def max(cls):
        raise NotImplementedError()

    @classmethod
    def min(cls):
        raise NotImplementedError()

    def __str__(self):
        return "{cls_name} with value {value}".format(
            cls_name=type(self), value=self.value)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.value == other.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value


class SignedInt64Deprecated(ConstrainedIntDeprecated):
    def __init__(self, value):
        """
        A signed integer between -0x8000000000000000 and +0x7FFFFFFFFFFFFFFF inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return -0x8000000000000000


class UnsignedInt64Deprecated(ConstrainedIntDeprecated):
    def __init__(self, value):
        """
        An unsigned integer between 0x0000000000000000 and +0x8000000000000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def min(cls):
        return 0x0000000000000000

    @classmethod
    def max(cls):
        return 0x8000000000000000


class DeltaValueDeprecated(ConstrainedIntDeprecated):
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
        super().__init__(value)

    @ classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @ classmethod
    def min(cls):
        return 0


InsertOptions = InsertOptionsDeprecated  # noqa: F811
UpsertOptions = UpsertOptionsDeprecated  # noqa: F811
ReplaceOptions = RemoveOptionsDeprecated  # noqa: F811
RemoveOptions = RemoveOptionsDeprecated  # noqa: F811
GetOptions = GetOptionsDeprecated  # noqa: F811
ExistsOptions = ExistsOptionsDeprecated  # noqa: F811
TouchOptions = TouchOptionsDeprecated  # noqa: F811
GetAndTouchOptions = GetAndTouchOptionsDeprecated  # noqa: F811
GetAndLockOptions = GetAndLockOptionsDeprecated  # noqa: F811
UnlockOptions = UnlockOptionsDeprecated  # noqa: F811
IncrementOptions = IncrementOptionsDeprecated  # noqa: F811
DecrementOptions = DecrementOptionsDeprecated  # noqa: F811
PrependOptions = PrependOptionsDeprecated  # noqa: F811
AppendOptions = AppendOptionsDeprecated  # noqa: F811
LookupInOptions = LookupInOptionsDeprecated  # noqa: F811
MutateInOptions = MutateInOptionsDeprecated  # noqa: F811

SignedInt64 = SignedInt64Deprecated  # noqa: F811
UnsignedInt64 = UnsignedInt64Deprecated  # noqa: F811
DeltaValue = DeltaValueDeprecated  # noqa: F811
