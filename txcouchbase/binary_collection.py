
from typing import (TYPE_CHECKING,
                    Any,
                    Union)

from twisted.internet.defer import Deferred

from couchbase.result import CounterResult, MutationResult

if TYPE_CHECKING:
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   IncrementOptions,
                                   PrependOptions)


class BinaryCollection:

    def __init__(self, collection):
        self._collection = collection

    def increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> Deferred[CounterResult]:
        return self._collection._increment(key, *opts, **kwargs)

    def decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> Deferred[CounterResult]:
        return self._collection._decrement(key, *opts, **kwargs)

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> Deferred[MutationResult]:
        return self._collection._append(key, value, *opts, **kwargs)

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> Deferred[MutationResult]:
        return self._collection._prepend(key, value, *opts, **kwargs)
