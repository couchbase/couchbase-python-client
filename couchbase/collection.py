from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Iterable,
                    Union,
                    overload)

from couchbase.binary_collection import BinaryCollection
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic import BlockingWrapper
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
                                   PrependOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.subdocument import Spec, StoreSemantics
    from couchbase.transcoder import Transcoder


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
            transcoder = self.transcoder
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
            transcoder = self.transcoder
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
            transcoder = self.transcoder
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
            transcoder = self.transcoder
        final_args['transcoder'] = transcoder
        print(f'lookup in kwargs: {final_args}')
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
