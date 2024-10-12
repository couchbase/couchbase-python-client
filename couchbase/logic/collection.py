#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import json
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Union)

from couchbase._utils import timedelta_as_microseconds
from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan)
from couchbase.logic.options import DeltaValueBase, SignedInt64Base
from couchbase.mutation_state import MutationState
from couchbase.options import forward_args
from couchbase.pycbc_core import (binary_operation,
                                  kv_operation,
                                  operations,
                                  subdoc_operation)
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)
from couchbase.subdocument import (Spec,
                                   StoreSemantics,
                                   SubDocOp)
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase._utils import JSONType
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   ExistsOptions,
                                   IncrementOptions,
                                   InsertOptions,
                                   MutateInOptions,
                                   MutationOptions,
                                   PrependOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)


class CollectionLogic:
    def __init__(self, scope, name):
        if not scope:
            raise InvalidArgumentException(message="Collection must be given a scope")
        # if not scope.connection:
        #     raise RuntimeError("No connection provided")
        self._scope = scope
        self._collection_name = name
        self._connection = scope.connection

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._connection

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._scope.default_transcoder

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Collection` instance.
        """
        return self._collection_name

    def _set_connection(self):
        """
        **INTERNAL**
        """
        self._connection = self._scope.connection

    def _get_connection_args(self) -> Dict[str, Any]:
        return {
            "conn": self._connection,
            "bucket": self._scope.bucket_name,
            "scope": self._scope.name,
            "collection_name": self.name
        }

    def _get_mutation_options(self,
                              *opts,  # type: MutationOptions
                              **kwargs  # type: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """**INTERNAL**
        Parses the mutaiton operation options.  If synchronous durability has been set and no timeout provided, the
        default timeout will be set to the default KV durable timeout (10 seconds).
        """
        args = forward_args(kwargs, *opts)
        if 'durability' in args and isinstance(args['durability'], int) and 'timeout' not in args:
            args['timeout'] = timedelta_as_microseconds(timedelta(seconds=10))

        return args

    def get(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Optional[GetResult]:
        """**INTERNAL**

        Key-Value *get* operation.  Should only be called by classes that inherit from the base
            class :class:`~couchbase.logic.CollectionLogic`.

        Args:
            key (str): document key
            kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~.options.GetOptions`

        Raises:
            :class:`~.exceptions.DocumentNotFoundException`: If the provided document key does not exist.
        """
        op_type = operations.GET.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def get_any_replica(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Optional[GetReplicaResult]:
        """**INTERNAL**

        Key-Value *get_any_replica* operation.  Should only be called by classes that inherit from the base
            class :class:`~couchbase.logic.CollectionLogic`.

        Args:
            key (str): document key
            kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~.options.GetAnyReplicaOptions`

        Raises:
            :class:`~.exceptions.DocumentNotFoundException`: If the provided document key does not exist.
        """
        op_type = operations.GET_ANY_REPLICA.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def get_all_replicas(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Optional[Iterable[GetReplicaResult]]:
        """**INTERNAL**

        Key-Value *get_all_replicas* operation.  Should only be called by classes that inherit from the base
            class :class:`~couchbase.logic.CollectionLogic`.

        Args:
            key (str): document key
            kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~.options.GetAllReplicasOptions`

        Raises:
            :class:`~.exceptions.DocumentNotFoundException`: If the provided document key does not exist.
        """
        op_type = operations.GET_ALL_REPLICAS.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Any
    ) -> Optional[ExistsResult]:
        op_type = operations.EXISTS.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=forward_args(kwargs, *opts)
        )

    def insert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Any
    ) -> Optional[MutationResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)
        op_type = operations.INSERT.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )

    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Any
    ) -> Optional[MutationResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)

        op_type = operations.UPSERT.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )

    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Any
                ) -> Optional[MutationResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        expiry = final_args.get("expiry", None)
        preserve_expiry = final_args.get("preserve_expiry", False)
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                "The expiry and preserve_expiry options cannot both be set for replace operations."
            )

        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)

        op_type = operations.REPLACE.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )

    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Any
               ) -> Optional[MutationResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        op_type = operations.REMOVE.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=final_args
        )

    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Any
              ) -> Optional[MutationResult]:
        kwargs["expiry"] = expiry
        op_type = operations.TOUCH.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=forward_args(kwargs, *opts)
        )

    def get_and_touch(self,
                      key,  # type: str
                      **kwargs,  # type: Any
                      ) -> Optional[GetResult]:
        op_type = operations.GET_AND_TOUCH.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=kwargs
        )

    def get_and_lock(self,
                     key,  # type: str
                     **kwargs,  # type: Any
                     ) -> Optional[GetResult]:
        op_type = operations.GET_AND_LOCK.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=kwargs
        )

    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Any
               ) -> None:
        op_type = operations.UNLOCK.value
        final_args = forward_args(kwargs, *opts)
        final_args['cas'] = cas
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            op_type=op_type,
            op_args=final_args
        )

    def lookup_in(self,
                  key,  # type: str
                  spec,  # type: Iterable[Spec]
                  **kwargs,  # type: Any
                  ) -> Optional[LookupInResult]:
        op_type = operations.LOOKUP_IN.value
        return subdoc_operation(
            **self._get_connection_args(),
            key=key,
            spec=spec,
            op_type=op_type,
            op_args=kwargs
        )

    def lookup_in_all_replicas(self,
                               key,  # type: str
                               spec,  # type: Iterable[Spec]
                               **kwargs,  # type: Any
                               ) -> Optional[Iterable[LookupInReplicaResult]]:
        op_type = operations.LOOKUP_IN_ALL_REPLICAS.value
        return subdoc_operation(
            **self._get_connection_args(),
            key=key,
            spec=spec,
            op_type=op_type,
            op_args=kwargs
        )

    def lookup_in_any_replica(self,
                              key,  # type: str
                              spec,  # type: Iterable[Spec]
                              **kwargs,  # type: Any
                              ) -> Optional[LookupInReplicaResult]:
        op_type = operations.LOOKUP_IN_ANY_REPLICA.value
        return subdoc_operation(
            **self._get_connection_args(),
            key=key,
            spec=spec,
            op_type=op_type,
            op_args=kwargs
        )

    def mutate_in(   # noqa: C901
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Any
    ) -> Optional[MutateInResult]:   # noqa: C901
        # no tc for sub-doc, use default JSON
        final_args = self._get_mutation_options(*opts, **kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)

        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)

        spec_ops = [s[0] for s in spec]
        if SubDocOp.DICT_ADD in spec_ops and preserve_expiry is True:
            raise InvalidArgumentException(
                'The preserve_expiry option cannot be set for mutate_in with insert operations.')

        if SubDocOp.REPLACE in spec_ops and expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for mutate_in with replace operations.')

        """
            @TODO(jc): document that the kwarg will override option:
            await cb.mutate_in(key,
                (SD.upsert('new_path', 'im new'),),
                MutateInOptions(store_semantics=SD.StoreSemantics.INSERT),
                upsert_doc=True)

                will set store_semantics to be UPSERT
        """

        insert_semantics = final_args.pop('insert_doc', None)
        upsert_semantics = final_args.pop('upsert_doc', None)
        replace_semantics = final_args.pop('replace_doc', None)
        if insert_semantics is not None and (upsert_semantics is not None or replace_semantics is not None):
            raise InvalidArgumentException("Cannot set multiple store semantics.")
        if upsert_semantics is not None and (insert_semantics is not None or replace_semantics is not None):
            raise InvalidArgumentException("Cannot set multiple store semantics.")

        if insert_semantics is not None:
            final_args["store_semantics"] = StoreSemantics.INSERT
        if upsert_semantics is not None:
            final_args["store_semantics"] = StoreSemantics.UPSERT
        if replace_semantics is not None:
            final_args["store_semantics"] = StoreSemantics.REPLACE

        final_spec = []
        allowed_multi_ops = [SubDocOp.ARRAY_PUSH_FIRST,
                             SubDocOp.ARRAY_PUSH_LAST,
                             SubDocOp.ARRAY_INSERT]

        for s in spec:
            if len(s) == 6:
                tmp = list(s[:5])
                if s[0] in allowed_multi_ops:
                    new_value = json.dumps(s[5], ensure_ascii=False)
                    # this is an array, need to remove brackets
                    tmp.append(new_value[1:len(new_value)-1].encode('utf-8'))
                else:
                    # no need to propagate the flags
                    tmp.append(transcoder.encode_value(s[5])[0])
                final_spec.append(tuple(tmp))
            else:
                final_spec.append(s)

        op_type = operations.MUTATE_IN.value
        return subdoc_operation(
            **self._get_connection_args(),
            key=key,
            spec=final_spec,
            op_type=op_type,
            op_args=final_args
        )

    def _validate_delta_initial(self, delta=None, initial=None) -> None:
        # @TODO: remove deprecation next .minor
        # from couchbase.collection import DeltaValueDeprecated, SignedInt64Deprecated
        if delta is not None:
            if not DeltaValueBase.is_valid(delta):
                raise InvalidArgumentException("Argument is not valid DeltaValue")
        if initial is not None:
            if not SignedInt64Base.is_valid(initial):
                raise InvalidArgumentException("Argument is not valid SignedInt64")

    def _get_and_validate_delta_initial(self, final_args):
        initial = final_args.get('initial', None)
        delta = final_args.get('delta', None)
        if not initial:
            initial = SignedInt64Base(0)
        if not delta:
            delta = DeltaValueBase(1)

        self._validate_delta_initial(delta=delta, initial=initial)

        return delta, initial

    def increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> Optional[CounterResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        if not final_args.get('initial', None):
            final_args['initial'] = SignedInt64Base(0)
        if not final_args.get('delta', None):
            final_args['delta'] = DeltaValueBase(1)

        self._validate_delta_initial(delta=final_args['delta'],
                                     initial=final_args['initial'])

        op_type = operations.INCREMENT.value
        final_args['initial'] = int(final_args['initial'])
        final_args['delta'] = int(final_args['delta'])

        if final_args['initial'] < 0:
            # Negative 'initial' means no initial value
            del final_args['initial']

        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                op_args=final_args)

    def decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> Optional[CounterResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        if not final_args.get('initial', None):
            final_args['initial'] = SignedInt64Base(0)
        if not final_args.get('delta', None):
            final_args['delta'] = DeltaValueBase(1)

        self._validate_delta_initial(delta=final_args['delta'],
                                     initial=final_args['initial'])

        op_type = operations.DECREMENT.value
        final_args['initial'] = int(final_args['initial'])
        final_args['delta'] = int(final_args['delta'])

        if final_args['initial'] < 0:
            # Negative 'initial' means no initial value
            del final_args['initial']

        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                op_args=final_args)

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> Optional[MutationResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError(
                "The value provided must of type str, bytes or bytearray.")

        op_type = operations.APPEND.value
        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                value=value,
                                op_args=final_args)

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> Optional[MutationResult]:
        final_args = self._get_mutation_options(*opts, **kwargs)
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError(
                "The value provided must of type str, bytes or bytearray.")

        op_type = operations.PREPEND.value
        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                value=value,
                                op_args=final_args)

    def build_scan_args(self,  # noqa: C901
                        scan_type,  # type: Union[RangeScan, PrefixScan, SamplingScan]
                        **kwargs,  # type: Dict[str, Any]
                        ) -> Dict[str, Any]:
        """** INTERNAL **

        Args:
            scan_type (Union[RangeScan, PrefixScan, SamplingScan]): Either a :class:`~couchbase.kv_range_scan.RangeScan`, a
              :class:`~couchbase.kv_range_scan.PrefixScan` or a :class:`~couchbase.kv_range_scan.SamplingScan` instance.
            kwargs (Dict[str, Any]): Options for scan operation.

        Raises:
            InvalidArgumentException: If scan_type is not either a RangeScan, PrefixScan or SamplingScan instance.
            InvalidArgumentException: If sort option is provided and is incorrect type.
            InvalidArgumentException: If consistent_with option is provided and is not a valid state
            InvalidArgumentException: If concurrency is not positive
            InvalidArgumentException: If sampling scan limit is not positive

        Returns:
            Dict[str, Any]: Parsed and processed scan operation arguments.
        """  # noqa: E501
        op_type = None
        if 'concurrency' in kwargs and kwargs['concurrency'] < 1:
            raise InvalidArgumentException('Concurrency option must be positive')

        if isinstance(scan_type, RangeScan):
            op_type = operations.KV_RANGE_SCAN.value
            if scan_type.start is not None:
                kwargs['start'] = scan_type.start.to_dict()
            if scan_type.end is not None:
                kwargs['end'] = scan_type.end.to_dict()
        elif isinstance(scan_type, PrefixScan):
            op_type = operations.KV_PREFIX_SCAN.value
            kwargs['prefix'] = scan_type.prefix
        elif isinstance(scan_type, SamplingScan):
            op_type = operations.KV_SAMPLING_SCAN.value
            if scan_type.limit <= 0:
                raise InvalidArgumentException('Sampling scan limit must be positive')
            kwargs['limit'] = scan_type.limit
            if scan_type.seed is not None:
                kwargs['seed'] = scan_type.seed
        else:
            raise InvalidArgumentException('scan_type must be Union[RangeScan, PrefixScan, SamplingScan]')

        transcoder = kwargs.pop('transcoder', None)

        consistent_with = kwargs.pop('consistent_with', None)
        if consistent_with:
            if not (isinstance(consistent_with, MutationState) and len(consistent_with._sv) > 0):
                raise InvalidArgumentException('Passed empty or invalid mutation state')
            else:
                kwargs['consistent_with'] = list(token.as_dict() for token in consistent_with._sv)

        return_args = {
            'transcoder': transcoder,
            'op_type': op_type,
            'op_args': kwargs,
        }
        return_args.update(**self._get_connection_args())
        return return_args
