
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Union)

from couchbase.result import (CounterResult,
                              MultiCounterResult,
                              MultiMutationResult,
                              MutationResult)

if TYPE_CHECKING:
    from couchbase.options import (AppendMultiOptions,
                                   AppendOptions,
                                   DecrementMultiOptions,
                                   DecrementOptions,
                                   IncrementMultiOptions,
                                   IncrementOptions,
                                   PrependMultiOptions,
                                   PrependOptions)


class BinaryCollection:

    def __init__(self, collection):
        self._collection = collection

    def increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> CounterResult:
        return self._collection._increment(key, *opts, **kwargs)

    def decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> CounterResult:
        return self._collection._decrement(key, *opts, **kwargs)

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        return self._collection._append(key, value, *opts, **kwargs)

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        return self._collection._prepend(key, value, *opts, **kwargs)

    def append_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: AppendMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        return self._collection._append_multi(keys_and_values, *opts, **kwargs)

    def prepend_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: PrependMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        return self._collection._prepend_multi(keys_and_values, *opts, **kwargs)

    def increment_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: IncrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        return self._collection._increment_multi(keys, *opts, **kwargs)

    def decrement_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: DecrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        return self._collection._decrement_multi(keys, *opts, **kwargs)
