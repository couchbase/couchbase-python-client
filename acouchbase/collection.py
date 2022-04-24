from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable,
                    Union)

from acouchbase.datastructures import (CouchbaseList,
                                       CouchbaseMap,
                                       CouchbaseQueue,
                                       CouchbaseSet)
from acouchbase.logic import AsyncWrapper
from couchbase.binary_collection import BinaryCollection
from couchbase.logic.collection import CollectionLogic
from couchbase.options import forward_args
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   ExistsOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetOptions,
                                   IncrementOptions,
                                   InsertOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   PrependOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.subdocument import Spec


class AsyncCollection(CollectionLogic):

    def __init__(self, scope, name):
        super().__init__(scope, name)
        self._loop = scope.loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Any
            ) -> Awaitable[GetResult]:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_internal(key, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Awaitable[GetResult]:
        super().get(key, **kwargs)

    @AsyncWrapper.inject_callbacks(ExistsResult)
    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Any
    ) -> Awaitable[ExistsResult]:
        super().exists(key, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def insert(
        self,  # type: "Collection"
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        super().insert(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        super().upsert(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Any
                ) -> MutationResult:
        super().replace(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Any
               ) -> MutationResult:
        super().remove(key, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Any
              ) -> MutationResult:
        super().touch(key, expiry, *opts, **kwargs)

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

    @AsyncWrapper.inject_callbacks_and_decode(GetResult)
    def _get_and_touch_internal(self,
                                key,  # type: str
                                **kwargs,  # type: Any
                                ) -> GetResult:
        super().get_and_touch(key, **kwargs)

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

    @AsyncWrapper.inject_callbacks_and_decode(GetResult)
    def _get_and_lock_internal(self,
                               key,  # type: str
                               **kwargs,  # type: Any
                               ) -> GetResult:
        super().get_and_lock(key, **kwargs)

    @AsyncWrapper.inject_callbacks(None)
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Any
               ) -> None:
        super().unlock(key, cas, *opts, **kwargs)

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

    @AsyncWrapper.inject_callbacks_and_decode(LookupInResult)
    def _lookup_in_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Any
    ) -> LookupInResult:
        super().lookup_in(key, spec, **kwargs)

    @AsyncWrapper.inject_callbacks(MutateInResult)
    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Any
    ) -> MutateInResult:
        super().mutate_in(key, spec, *opts, **kwargs)

    def binary(self) -> BinaryCollection:
        return BinaryCollection(self)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def _append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        super().append(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def _prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        super().prepend(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(CounterResult)
    def _increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> Awaitable[CounterResult]:
        super().increment(key, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(CounterResult)
    def _decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> Awaitable[CounterResult]:
        super().decrement(key, *opts, **kwargs)

    def couchbase_list(self, key  # type: str
                       ) -> CouchbaseList:
        return CouchbaseList(key, self)

    def couchbase_map(self, key  # type: str
                      ) -> CouchbaseMap:
        return CouchbaseMap(key, self)

    def couchbase_set(self, key  # type: str
                      ) -> CouchbaseSet:
        return CouchbaseSet(key, self)

    def couchbase_queue(self, key  # type: str
                        ) -> CouchbaseQueue:
        return CouchbaseQueue(key, self)

    @staticmethod
    def default_name():
        return "_default"


Collection = AsyncCollection
