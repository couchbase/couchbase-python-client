from __future__ import annotations

import json
import logging
import uuid
from collections.abc import (Callable,
                             Iterable,
                             Iterator)
from datetime import timedelta
from functools import wraps
from random import randint
from time import perf_counter_ns, time
from typing import (TYPE_CHECKING,
                    Any,
                    Concatenate,
                    ParamSpec,
                    Protocol,
                    TypeAlias,
                    TypedDict,
                    TypeVar)

from google.protobuf import timestamp_pb2 as timestamp

from couchbase import subdocument as subdoc
from couchbase.durability import (ClientDurability,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import CouchbaseException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.options import (AppendOptions,
                               DecrementOptions,
                               DeltaValue,
                               ExistsOptions,
                               GetAllReplicasOptions,
                               GetAndLockOptions,
                               GetAndTouchOptions,
                               GetAnyReplicaOptions,
                               GetOptions,
                               IncrementOptions,
                               InsertOptions,
                               LookupInAllReplicasOptions,
                               LookupInAnyReplicaOptions,
                               LookupInOptions,
                               MutateInOptions,
                               PrependOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               ScanOptions,
                               SignedInt64,
                               TouchOptions,
                               UnlockOptions,
                               UpsertOptions)
from couchbase.replica_reads import ReadPreference
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult,
                              ScanResult,
                              ScanResultIterable)
from couchbase.subdocument import (MutationMacro,
                                   Spec,
                                   StoreSemantics)

from run import top_level_pb2 as run_pb
from sdk import workload_pb2 as sdk_pb
from sdk.kv import commands_pb2 as kv_commands_pb
from sdk.kv import lookup_in_pb2 as kv_lookup_in_pb
from sdk.kv import mutate_in_pb2 as kv_mutate_in_pb
from sdk.kv.binary import commands_pb2 as kv_commands_binary_pb
from sdk.kv.rangescan import top_level_pb2 as kv_range_scan_pb
from shared import basic_pb2 as basic_pb
from shared import content_pb2 as content_pb
from shared import exceptions_pb2 as exceptions_pb
from streams import top_level_pb2 as streams_pb

from .sdk_commands import (DURABILITY_LEVEL_MAP,
                           SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

if TYPE_CHECKING:
    from google.protobuf import duration_pb2

    from couchbase.cluster import Cluster
    from couchbase.collection import Collection

    from sdk.kv import options_pb2 as kv_options_pb
    from sdk.kv.binary import options_pb2 as kv_binary_options_pb
    from shared import collection_pb2 as collection_pb
    from shared import doc_location_pb2 as doc_location_pb

    from ..telemetry.span_owner import SpanOwner
    from ..workloads import Counters

    # What a command's `execute_command` returns: one result, or a stream of them.
    StreamedResult: TypeAlias = run_pb.Result | Iterator[run_pb.Result | exceptions_pb.Exception]

logger = logging.getLogger(__name__)

P = ParamSpec('P')


# What the result decorators read from the command they wrap.
class _ReturnsResult(Protocol):
    @property
    def _initiated(self) -> timestamp.Timestamp: ...
    @property
    def _return_result(self) -> bool: ...


class _ReturnsContent(_ReturnsResult, Protocol):
    @property
    def _content_as(self) -> content_pb.ContentAs: ...


class _ReturnsLookupIn(_ReturnsResult, Protocol):
    @property
    def _raw_specs(self) -> Iterable[kv_lookup_in_pb.LookupInSpec]: ...


class _ReturnsMutateIn(_ReturnsResult, Protocol):
    @property
    def _raw_specs(self) -> Iterable[kv_mutate_in_pb.MutateInSpec]: ...


class _StreamsContent(Protocol):
    @property
    def _initiated(self) -> timestamp.Timestamp: ...
    @property
    def _stream_config(self) -> streams_pb.Config: ...
    @property
    def _content_as(self) -> content_pb.ContentAs: ...


class _StreamsScan(Protocol):
    @property
    def _initiated(self) -> timestamp.Timestamp: ...
    @property
    def _stream_config(self) -> streams_pb.Config: ...
    @property
    def _content_as(self) -> content_pb.ContentAs | None: ...


class _StreamsLookupIn(Protocol):
    @property
    def _initiated(self) -> timestamp.Timestamp: ...
    @property
    def _stream_config(self) -> streams_pb.Config: ...
    @property
    def _raw_specs(self) -> Iterable[kv_lookup_in_pb.LookupInSpec]: ...


R = TypeVar('R', bound=_ReturnsResult)
RC = TypeVar('RC', bound=_ReturnsContent)
RL = TypeVar('RL', bound=_ReturnsLookupIn)
RM = TypeVar('RM', bound=_ReturnsMutateIn)
SC = TypeVar('SC', bound=_StreamsContent)
SS = TypeVar('SS', bound=_StreamsScan)
SL = TypeVar('SL', bound=_StreamsLookupIn)


class Keyspace(TypedDict):
    doc_id: str | None
    bucket_name: str
    scope_name: str
    collection_name: str


VALID_KV_COMMAND_ARGS = {
    'collection': lambda c: c is not None,
    'content': lambda c: True,
    'doc_id': lambda i: isinstance(i, str),
    'return_result': lambda rr: isinstance(rr, bool),
    'initiated': lambda i: isinstance(i, timestamp.Timestamp),
    'options': lambda o: True,
    'scan_type': lambda s: True,
    'stream_config': lambda s: True,
    'content_as': lambda c: True,
    'specs': lambda s: True,
    'raw_specs': lambda s: True,
    'expiry': lambda e: e is None or isinstance(e, timedelta),
    'cas': lambda c: isinstance(c, int),
    'duration': lambda d: isinstance(d, timedelta),
    'bytes': lambda c: True,
    'span_owner': lambda s: True,
}

STORE_SEMANTICS_MAP = {
    kv_mutate_in_pb.StoreSemantics.INSERT: StoreSemantics.INSERT,
    kv_mutate_in_pb.StoreSemantics.REPLACE: StoreSemantics.REPLACE,
    kv_mutate_in_pb.StoreSemantics.UPSERT: StoreSemantics.UPSERT,
}

PERSIST_TO_MAP = {
    basic_pb.PersistTo.PERSIST_TO_NONE: PersistToExtended.NONE,
    basic_pb.PersistTo.PERSIST_TO_ACTIVE: PersistToExtended.ACTIVE,
    basic_pb.PersistTo.PERSIST_TO_ONE: PersistToExtended.ONE,
    basic_pb.PersistTo.PERSIST_TO_TWO: PersistToExtended.TWO,
    basic_pb.PersistTo.PERSIST_TO_THREE: PersistToExtended.THREE,
    basic_pb.PersistTo.PERSIST_TO_FOUR: PersistToExtended.FOUR,
}

REPLICATE_TO_MAP = {
    basic_pb.ReplicateTo.REPLICATE_TO_NONE: ReplicateTo.NONE,
    basic_pb.ReplicateTo.REPLICATE_TO_ONE: ReplicateTo.ONE,
    basic_pb.ReplicateTo.REPLICATE_TO_TWO: ReplicateTo.TWO,
    basic_pb.ReplicateTo.REPLICATE_TO_THREE: ReplicateTo.THREE,
}

READ_PREFERENCE_MAP = {
    basic_pb.ReadPreference.NO_PREFERENCE: ReadPreference.NO_PREFERENCE,
    basic_pb.ReadPreference.SELECTED_SERVER_GROUP: ReadPreference.SELECTED_SERVER_GROUP,
}


class KvCommandOptions(SdkCommandOptions):
    @staticmethod
    def get_with_expiry(options: Any) -> bool | None:
        if not options.HasField("with_expiry"):
            return None

        return options.with_expiry

    @staticmethod
    def get_projection(options: Any) -> list[str] | None:
        if len(options.projection) == 0:
            return None

        return list(options.projection)

    @staticmethod
    def get_cas(options: Any) -> int | None:
        if not options.HasField("cas"):
            return None

        return options.cas

    @staticmethod
    def convert_duration(duration: duration_pb2.Duration) -> timedelta:
        return timedelta(seconds=duration.seconds)

    @staticmethod
    def convert_expiry(expiry: basic_pb.Expiry) -> timedelta | None:
        exp_type = expiry.WhichOneof("expiryType")
        if exp_type == "relativeSecs":
            return timedelta(seconds=expiry.relativeSecs)
        elif exp_type == "absoluteEpochSecs":
            return timedelta(seconds=(expiry.absoluteEpochSecs - int(time())))
        return None

    @staticmethod
    def get_expiry(options: Any) -> timedelta | None:
        if not options.HasField("expiry"):
            return None

        return KvCommandOptions.convert_expiry(options.expiry)

    @staticmethod
    def get_durability(options: Any) -> ServerDurability | ClientDurability | None:
        if not options.HasField("durability"):
            return None

        durability_type = options.durability.WhichOneof("durability")
        if durability_type == "durabilityLevel":
            return ServerDurability(DURABILITY_LEVEL_MAP[options.durability.durabilityLevel])
        elif durability_type == "observe":
            return ClientDurability(
                replicate_to=REPLICATE_TO_MAP[options.durability.observe.replicateTo],
                persist_to=PERSIST_TO_MAP[options.durability.observe.persistTo]
            )
        return None

    @staticmethod
    def get_store_semantics(options: Any) -> StoreSemantics | None:
        if not options.HasField("store_semantics"):
            return None

        return STORE_SEMANTICS_MAP.get(options.store_semantics, None)

    @staticmethod
    def get_create_as_deleted(options: Any) -> bool | None:
        if not options.HasField("create_as_deleted"):
            return None

        return options.create_as_deleted

    @staticmethod
    def get_initial(options: Any) -> SignedInt64 | None:
        if not options.HasField("initial"):
            return None
        return SignedInt64(options.initial)

    @staticmethod
    def get_delta(options: Any) -> DeltaValue | None:
        if not options.HasField("delta"):
            return None
        return DeltaValue(options.delta)

    @staticmethod
    def get_access_deleted(options: Any) -> bool | None:
        return KvCommandOptions.get_simple_option(options, 'access_deleted')

    @staticmethod
    def get_ids_only(options: Any) -> bool | None:
        return KvCommandOptions.get_simple_option(options, 'ids_only')

    @staticmethod
    def get_batch_byte_limit(options: Any) -> int | None:
        return KvCommandOptions.get_simple_option(options, 'batch_byte_limit')

    @staticmethod
    def get_batch_item_limit(options: Any) -> int | None:
        return KvCommandOptions.get_simple_option(options, 'batch_item_limit')

    @staticmethod
    def get_concurrency(options: Any) -> int | None:
        return KvCommandOptions.get_simple_option(options, 'concurrency')

    @staticmethod
    def get_read_preference(options: Any) -> ReadPreference | None:
        if not options.HasField('read_preference'):
            return None

        if options.read_preference not in READ_PREFERENCE_MAP:
            raise ValueError(f'Unexpected read preference: {options.read_preference}')

        return READ_PREFERENCE_MAP[options.read_preference]


class KvCommandResult(SdkCommandResult):
    @classmethod
    def as_get_result(cls,
                      fn: Callable[Concatenate[RC, P], GetResult],
                      ) -> Callable[Concatenate[RC, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: RC, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(get_result=cls.to_get_result(res, self._content_as))
                else:
                    sdk_result = sdk_pb.Result(success=res.success)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_exists_result(cls,
                         fn: Callable[Concatenate[R, P], ExistsResult],
                         ) -> Callable[Concatenate[R, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: R, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(exists_result=cls.to_exists_result(res))
                else:
                    sdk_result = sdk_pb.Result(success=res.success)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_get_replica_result(cls,
                              fn: Callable[Concatenate[RC, P], GetReplicaResult],
                              ) -> Callable[Concatenate[RC, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: RC, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(get_replica_result=cls.to_get_replica_result(res, self._content_as))
                else:
                    sdk_result = sdk_pb.Result(success=res.success)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_mutate_in_result(cls,
                            fn: Callable[Concatenate[RM, P], MutateInResult],
                            ) -> Callable[Concatenate[RM, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: RM, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(mutate_in_result=cls.to_mutate_in_result(res, self._raw_specs))
                else:
                    sdk_result = sdk_pb.Result(success=res.success)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_mutation_result(cls,
                           fn: Callable[Concatenate[R, P], MutationResult],
                           ) -> Callable[Concatenate[R, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: R, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(mutation_result=cls.to_mutation_result(res))
                else:
                    sdk_result = sdk_pb.Result(success=res.success)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_counter_result(cls,
                          fn: Callable[Concatenate[R, P], CounterResult],
                          ) -> Callable[Concatenate[R, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: R, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(counter_result=cls.to_counter_result(res))
                else:
                    sdk_result = sdk_pb.Result(success=res.success)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_scan_result_stream(cls,
                              fn: Callable[Concatenate[SS, P], ScanResultIterable],
                              ) -> Callable[Concatenate[SS, P], StreamedResult]:
        @wraps(fn)
        def wrapped_fn(self: SS, *args: P.args, **kwargs: P.kwargs) -> StreamedResult:
            try:
                result = fn(self, *args, **kwargs)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

            return cls.to_scan_result_iterator(
                result, self._initiated, self._stream_config.stream_id, self._content_as
            )

        return wrapped_fn

    @classmethod
    def as_lookup_in_result(cls,
                            fn: Callable[Concatenate[RL, P], LookupInResult],
                            ) -> Callable[Concatenate[RL, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: RL, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(lookup_in_result=cls.to_lookup_in_result(res, self._raw_specs))
                else:
                    sdk_result = sdk_pb.Result(success=isinstance(res, LookupInResult))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_lookup_in_any_replica_result(cls,
                                        fn: Callable[Concatenate[RL, P], LookupInReplicaResult],
                                        ) -> Callable[Concatenate[RL, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: RL, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(
                        lookup_in_any_replica_result=cls.to_lookup_in_replica_result(res, self._raw_specs))
                else:
                    sdk_result = sdk_pb.Result(success=isinstance(res, LookupInReplicaResult))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_lookup_in_all_replicas_result_stream(cls,
                                                fn: Callable[Concatenate[SL, P], Iterable[LookupInReplicaResult]],
                                                ) -> Callable[Concatenate[SL, P], StreamedResult]:
        @wraps(fn)
        def wrapped_fn(self: SL, *args: P.args, **kwargs: P.kwargs) -> StreamedResult:
            try:
                result = fn(self, *args, **kwargs)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

            return cls.to_lookup_in_all_replicas_result_iterator(
                result, self._initiated, self._stream_config.stream_id, self._raw_specs
            )

        return wrapped_fn

    @classmethod
    def as_get_all_replicas_result_stream(cls,
                                          fn: Callable[Concatenate[SC, P], Iterable[GetReplicaResult]],
                                          ) -> Callable[Concatenate[SC, P], StreamedResult]:
        @wraps(fn)
        def wrapped_fn(self: SC, *args: P.args, **kwargs: P.kwargs) -> StreamedResult:
            try:
                result = fn(self, *args, **kwargs)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

            return cls.to_get_all_replicas_result_iterator(
                result, self._initiated, self._stream_config.stream_id, self._content_as
            )

        return wrapped_fn

    @staticmethod
    def to_mutation_result(sdk_result: MutationResult) -> kv_commands_pb.MutationResult:
        """  Converts a Couchbase MutationResult object to the equivalent Protobuf MutationResult
        """
        res = kv_commands_pb.MutationResult()
        res.cas = sdk_result.cas
        cb_mutation_token = sdk_result.mutation_token()
        if cb_mutation_token is not None:
            res.mutation_token.partition_id = cb_mutation_token.partition_id
            res.mutation_token.partition_uuid = cb_mutation_token.partition_uuid
            res.mutation_token.sequence_number = cb_mutation_token.sequence_number
            res.mutation_token.bucket_name = cb_mutation_token.bucket_name
        return res

    @staticmethod
    def to_get_result(sdk_result: GetResult, content_as: content_pb.ContentAs) -> kv_commands_pb.GetResult:
        """ Converts a Couchbase GetResult object to the equivalent Protobuf GetResult object
        """
        res = kv_commands_pb.GetResult(cas=sdk_result.cas, content=KvCommandResult.to_content(sdk_result, content_as))

        expiry = sdk_result.expiry_time
        if expiry is not None:
            res.expiry_time = int(expiry.timestamp())

        return res

    @staticmethod
    def to_get_replica_result(
            sdk_result: GetReplicaResult,
            content_as: content_pb.ContentAs,
            stream_id: str | None = None
    ) -> kv_commands_pb.GetReplicaResult:
        """ Converts a Couchbase GetReplicaResult object to the equivalent Protobuf GetReplicaResult
        """
        res = kv_commands_pb.GetReplicaResult(cas=sdk_result.cas,
                                              content=KvCommandResult.to_content(sdk_result, content_as),
                                              is_replica=sdk_result.is_replica)
        if stream_id is not None:
            res.stream_id = stream_id

        return res

    @staticmethod
    def to_exists_result(sdk_result: ExistsResult) -> kv_commands_pb.ExistsResult:
        """ Converts a Couchbase ExistsResult object to the equivalent Protobuf ExistsResult object
        """
        return kv_commands_pb.ExistsResult(cas=sdk_result.cas, exists=sdk_result.exists)

    @staticmethod
    def to_counter_result(sdk_result: CounterResult) -> kv_commands_binary_pb.CounterResult:
        """  Converts a Couchbase CounterResult object to the equivalent Protobuf CounterResult
        """
        res = kv_commands_binary_pb.CounterResult()
        res.cas = sdk_result.cas
        cb_mutation_token = sdk_result.mutation_token()
        if cb_mutation_token is not None:
            res.mutation_token.partition_id = cb_mutation_token.partition_id
            res.mutation_token.partition_uuid = cb_mutation_token.partition_uuid
            res.mutation_token.sequence_number = cb_mutation_token.sequence_number
            res.mutation_token.bucket_name = cb_mutation_token.bucket_name
        res.content = sdk_result.content
        return res

    @classmethod
    def to_mutate_in_result(cls,
                            sdk_result: MutateInResult,
                            raw_specs: Iterable[kv_mutate_in_pb.MutateInSpec]
                            ) -> kv_mutate_in_pb.MutateInResult:
        """  Converts a Couchbase MutateInResult object to the equivalent Protobuf MutateInResult
        """
        res = kv_mutate_in_pb.MutateInResult(cas=sdk_result.cas)
        cb_mutation_token = sdk_result.mutation_token()
        if cb_mutation_token is not None:
            res.mutation_token.partition_id = cb_mutation_token.partition_id
            res.mutation_token.partition_uuid = cb_mutation_token.partition_uuid
            res.mutation_token.sequence_number = cb_mutation_token.sequence_number
            res.mutation_token.bucket_name = cb_mutation_token.bucket_name

        for idx, raw_spec in enumerate(raw_specs):
            res.results.append(cls.to_mutate_in_spec_result(sdk_result, idx, raw_spec))

        return res

    @classmethod
    def to_mutate_in_spec_result(cls,
                                 sdk_result: MutateInResult,
                                 idx: int,
                                 raw_spec: kv_mutate_in_pb.MutateInSpec
                                 ) -> kv_mutate_in_pb.MutateInSpecResult:
        if not raw_spec.HasField('content_as'):
            return kv_mutate_in_pb.MutateInSpecResult()

        try:
            content_or_error = content_pb.ContentOrError(content=cls.to_content(sdk_result, raw_spec.content_as, idx))
        except Exception as e:
            content_or_error = content_pb.ContentOrError(exception=cls.to_exception(e))

        return kv_mutate_in_pb.MutateInSpecResult(content_as_result=content_or_error)

    @classmethod
    def to_scan_result_iterator(cls,
                                result: ScanResultIterable,
                                initiated: timestamp.Timestamp,
                                stream_id: str,
                                content_as: content_pb.ContentAs | None,
                                ) -> Iterator[run_pb.Result | exceptions_pb.Exception]:
        try:
            iterator = result.rows()
            cnt = 0
            while True:
                try:
                    start = perf_counter_ns()
                    row = next(iterator)
                    end = perf_counter_ns()
                    cnt += 1

                    yield run_pb.Result(
                        sdk=sdk_pb.Result(range_scan_result=cls.to_scan_result(row, stream_id, content_as)),
                        initiated=initiated,
                        elapsedNanos=(end - start)
                    )
                except StopIteration:
                    logger.info(f"Got {cnt} results from range scan")
                    return
        except Exception as e:
            if not isinstance(e, CouchbaseException):
                logger.warning(f"Caught {type(e).__name__} exception ({str(e)})", exc_info=True)
            exception = cls.to_exception(e)
            yield exception
            return

    @staticmethod
    def to_scan_result(sdk_result: ScanResult,
                       stream_id: str,
                       content_as: content_pb.ContentAs | None,
                       ) -> kv_range_scan_pb.ScanResult:
        """ Converts an SDK ScanResult object to the equivalent Protobuf ScanResult object
        """
        res = kv_range_scan_pb.ScanResult(id=sdk_result.id, stream_id=stream_id, id_only=sdk_result.ids_only)

        if sdk_result.ids_only is not None and not sdk_result.ids_only:
            res.cas = sdk_result.cas
            # The SDK reports the expiry as a datetime and the message carries epoch seconds.
            expiry = sdk_result.expiry_time
            if expiry is not None:
                res.expiry_time = int(expiry.timestamp())

            if content_as is not None:
                res.content.CopyFrom(KvCommandResult.to_content(sdk_result, content_as))

        return res

    @classmethod
    def to_lookup_in_result(cls,
                            result: LookupInResult,
                            raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec],
                            ) -> kv_lookup_in_pb.LookupInResult:
        res = kv_lookup_in_pb.LookupInResult(cas=result.cas)
        for idx, raw_spec in enumerate(raw_specs):
            res.results.append(cls.to_lookup_in_spec_result(result, idx, raw_spec))

        return res

    @classmethod
    def to_lookup_in_spec_result(cls,
                                 result: LookupInResult | LookupInReplicaResult,
                                 idx: int,
                                 raw_spec: kv_lookup_in_pb.LookupInSpec
                                 ) -> kv_lookup_in_pb.LookupInSpecResult:
        try:
            content_or_error = content_pb.ContentOrError(content=cls.to_content(result, raw_spec.content_as, idx))
        except Exception as e:
            content_or_error = content_pb.ContentOrError(exception=cls.to_exception(e))
        try:
            exists_or_error = kv_lookup_in_pb.BooleanOrError(value=result.exists(idx))
        except Exception as e:
            exists_or_error = kv_lookup_in_pb.BooleanOrError(exception=cls.to_exception(e))
        return kv_lookup_in_pb.LookupInSpecResult(content_as_result=content_or_error, exists_result=exists_or_error)

    @classmethod
    def to_lookup_in_replica_result(cls,
                                    result: LookupInReplicaResult,
                                    raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec],
                                    ) -> kv_lookup_in_pb.LookupInReplicaResult:
        res = kv_lookup_in_pb.LookupInReplicaResult(cas=result.cas, is_replica=result.is_replica)
        for idx, raw_spec in enumerate(raw_specs):
            res.results.append(cls.to_lookup_in_spec_result(result, idx, raw_spec))

        return res

    @classmethod
    def to_lookup_in_all_replicas_result(cls,
                                         replica_result: LookupInReplicaResult,
                                         raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec],
                                         stream_id: str
                                         ) -> kv_lookup_in_pb.LookupInAllReplicasResult:
        return kv_lookup_in_pb.LookupInAllReplicasResult(
            lookup_in_replica_result=cls.to_lookup_in_replica_result(replica_result, raw_specs),
            stream_id=stream_id
        )

    @classmethod
    def to_lookup_in_all_replicas_result_iterator(cls,
                                                  result: Iterable[LookupInReplicaResult],
                                                  initiated: timestamp.Timestamp,
                                                  stream_id: str,
                                                  raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec],
                                                  ) -> Iterator[run_pb.Result | exceptions_pb.Exception]:
        try:
            iterator = iter(result)
            cnt = 0
            while True:
                try:
                    start = perf_counter_ns()
                    replica_result = next(iterator)
                    end = perf_counter_ns()
                    cnt += 1

                    lookup_in_all_replicas_result = cls.to_lookup_in_all_replicas_result(
                        replica_result, raw_specs, stream_id
                    )

                    yield run_pb.Result(
                        sdk=sdk_pb.Result(lookup_in_all_replicas_result=lookup_in_all_replicas_result),
                        initiated=initiated,
                        elapsedNanos=(end - start)
                    )
                except StopIteration:
                    logger.info(f"Got {cnt} results from lookup-in all replicas")
                    return
        except Exception as e:
            if not isinstance(e, CouchbaseException):
                logger.warning(f"Caught {type(e).__name__} exception ({str(e)})")
            exception = cls.to_exception(e)
            yield exception
            return

    @classmethod
    def to_get_all_replicas_result_iterator(cls,
                                            result: Iterable[GetReplicaResult],
                                            initiated: timestamp.Timestamp,
                                            stream_id: str,
                                            content_as: content_pb.ContentAs,
                                            ) -> Iterator[run_pb.Result | exceptions_pb.Exception]:
        try:
            iterator = iter(result)
            cnt = 0
            while True:
                try:
                    start = perf_counter_ns()
                    get_result = next(iterator)
                    end = perf_counter_ns()

                    get_all_replicas_result = cls.to_get_replica_result(
                        get_result, content_as, stream_id
                    )

                    cnt += 1
                    yield run_pb.Result(
                        sdk=sdk_pb.Result(get_replica_result=get_all_replicas_result),
                        initiated=initiated,
                        elapsedNanos=(end - start)
                    )
                except StopIteration:
                    logger.info(f"Got {cnt} results from get all replicas")
                    return
        except Exception as e:
            if not isinstance(e, CouchbaseException):
                logger.warning(f"Caught {type(e).__name__} exception ({str(e)})")
            exception = cls.to_exception(e)
            yield exception
            return


class KvCommandBuilder:
    @staticmethod
    def get_collection(cluster: Cluster, keyspace: Keyspace | collection_pb.Collection) -> Collection:
        if isinstance(keyspace, dict):
            return (cluster
                    .bucket(keyspace['bucket_name'])
                    .scope(keyspace['scope_name'])
                    .collection(keyspace['collection_name']))
        else:
            return (cluster
                    .bucket(keyspace.bucket_name)
                    .scope(keyspace.scope_name)
                    .collection(keyspace.collection_name))

    @staticmethod
    def get_location_details(doc_location: doc_location_pb.DocLocation, counters: Counters) -> Keyspace:
        doc_location_type = doc_location.WhichOneof('location')
        if doc_location_type is None:
            raise ValueError('Document location has no location type set')
        location = getattr(doc_location, doc_location_type)
        doc_id: str | None = None
        if doc_location_type == 'specific':
            doc_id = location.id
        elif doc_location_type == 'uuid':
            doc_id = str(uuid.uuid4())
        elif doc_location_type == "pool":
            p_type = location.WhichOneof("poolSelectionStrategy")
            if p_type == "random":
                doc_id = location.id_preface + str(randint(1, location.pool_size - 1))
            elif p_type == "counter":
                value = counters.increment_and_get(location.counter.counter)
                doc_id = location.id_preface + str(value % location.pool_size)
        else:
            raise ValueError(f"Get location type '{doc_location_type}' is not recognized")

        return {
            'doc_id': doc_id,
            'bucket_name': location.collection.bucket_name,
            'scope_name': location.collection.scope_name,
            'collection_name': location.collection.collection_name
        }

    @staticmethod
    def get_content_or_macro(content: kv_mutate_in_pb.ContentOrMacro) -> Any:
        content_type = content.WhichOneof('content_or_macro')
        if content_type == 'content':
            return KvCommandBuilder.get_content(content.content)
        if content_type == 'macro':
            return KvCommandBuilder.get_macro(content.macro)
        raise ValueError(f"The content format '{content_type}' is not recognized")

    @staticmethod
    def get_content(content: content_pb.Content) -> Any:
        content_type = content.WhichOneof("content")
        if content_type == "passthrough_string":
            return content.passthrough_string
        if content_type == "convert_to_json":
            return json.loads(content.convert_to_json.decode('utf-8'))
        if content_type == "byte_array":
            return content.byte_array
        if content_type == "null":
            return None
        else:
            raise ValueError(f"The content format '{content_type}' is not recognized")

    @staticmethod
    def get_macro(macro: kv_mutate_in_pb.MutateInMacro.ValueType) -> MutationMacro:
        if macro == kv_mutate_in_pb.MutateInMacro.CAS:
            return MutationMacro.cas()
        if macro == kv_mutate_in_pb.MutateInMacro.SEQ_NO:
            return MutationMacro.seq_no()
        if macro == kv_mutate_in_pb.MutateInMacro.VALUE_CRC_32C:
            return MutationMacro.value_crc32c()
        raise ValueError(f"The macro format '{macro}' is not recognized")

    @staticmethod
    def get_lookup_in_specs(proto_specs: Iterable[kv_lookup_in_pb.LookupInSpec]) -> list[Spec]:
        res = []
        for proto_spec in proto_specs:
            spec_type = proto_spec.WhichOneof('operation')
            if spec_type == 'exists':
                if proto_spec.exists.HasField('xattr'):
                    res.append(subdoc.exists(proto_spec.exists.path, proto_spec.exists.xattr))
                else:
                    res.append(subdoc.exists(proto_spec.exists.path))
            elif spec_type == 'get':
                if len(proto_spec.get.path) > 0:
                    if proto_spec.get.HasField('xattr'):
                        res.append(subdoc.get(proto_spec.get.path, proto_spec.get.xattr))
                    else:
                        res.append(subdoc.get(proto_spec.get.path))
                else:
                    if proto_spec.get.HasField('xattr'):
                        # TODO: what to do here?
                        raise NotImplementedError("Can't set xattr with get_full")
                    else:
                        res.append(subdoc.get_full())
            elif spec_type == 'count':
                if proto_spec.count.HasField('xattr'):
                    res.append(subdoc.count(proto_spec.count.path, proto_spec.count.xattr))
                else:
                    res.append(subdoc.count(proto_spec.count.path))
            else:
                raise NotImplementedError(f'LookupIn spec type {spec_type} not supported')
        return res

    @classmethod
    def get_mutate_in_specs(cls,  # noqa: C901
                            proto_specs: Iterable[kv_mutate_in_pb.MutateInSpec],
                            ) -> list[Spec]:
        res = []
        for proto_spec in proto_specs:
            spec_type = proto_spec.WhichOneof('operation')
            if spec_type is None:
                raise NotImplementedError('MutateIn spec has no operation set')
            proto_op = getattr(proto_spec, spec_type)
            opt_kwargs: dict[str, Any] = {}
            if proto_op.HasField('xattr'):
                opt_kwargs['xattr'] = proto_op.xattr
            if hasattr(proto_op, 'create_path') and proto_op.HasField('create_path'):
                opt_kwargs['create_parents'] = proto_op.create_path
            if spec_type == 'upsert':
                res.append(subdoc.upsert(proto_op.path, cls.get_content_or_macro(proto_op.content), **opt_kwargs))
            elif spec_type == 'insert':
                res.append(subdoc.insert(proto_op.path, cls.get_content_or_macro(proto_op.content), **opt_kwargs))
            elif spec_type == 'replace':
                res.append(subdoc.replace(proto_op.path, cls.get_content_or_macro(proto_op.content), **opt_kwargs))
            elif spec_type == 'remove':
                res.append(subdoc.remove(proto_op.path, **opt_kwargs))
            elif spec_type == 'array_append':
                res.append(
                    subdoc.array_append(proto_op.path, *map(cls.get_content_or_macro, proto_op.content), **opt_kwargs))
            elif spec_type == 'array_prepend':
                res.append(
                    subdoc.array_prepend(proto_op.path, *map(cls.get_content_or_macro, proto_op.content), **opt_kwargs))
            elif spec_type == 'array_insert':
                res.append(
                    subdoc.array_insert(proto_op.path, *map(cls.get_content_or_macro, proto_op.content), **opt_kwargs))
            elif spec_type == 'array_add_unique':
                res.append(
                    subdoc.array_addunique(proto_op.path, cls.get_content_or_macro(proto_op.content), **opt_kwargs))
            elif spec_type == 'increment':
                res.append(subdoc.increment(proto_op.path, proto_op.delta, **opt_kwargs))
            elif spec_type == 'decrement':
                res.append(subdoc.decrement(proto_op.path, proto_op.delta, **opt_kwargs))
            else:
                raise NotImplementedError(f'MutateIn spec type {spec_type} not supported')
        return res

    # `kv_cmd` is whichever of the unrelated command messages `cmd_type` names.
    @classmethod
    def build_command(cls,  # noqa: C901
                      cluster: Cluster,
                      kv_cmd: Any,
                      cmd_type: str,
                      counters: Counters,
                      **cmd_kwargs: Any,
                      ) -> SdkCommand:
        if cmd_type not in ['binary']:
            cmd_kwargs['options'] = kv_cmd.options if kv_cmd.HasField('options') else None

        if cmd_type not in ['range_scan', 'binary']:
            keyspace = cls.get_location_details(kv_cmd.location, counters)
            cmd_kwargs.update({
                'doc_id': keyspace['doc_id'],
                'collection': cls.get_collection(cluster, keyspace)
            })

        if cmd_type == 'insert':
            cmd_kwargs['content'] = cls.get_content(kv_cmd.content)
            return InsertCommand.create_command(**cmd_kwargs)
        if cmd_type == 'get':
            cmd_kwargs['content_as'] = kv_cmd.content_as
            return GetCommand.create_command(**cmd_kwargs)
        if cmd_type == 'replace':
            cmd_kwargs['content'] = cls.get_content(kv_cmd.content)
            return ReplaceCommand.create_command(**cmd_kwargs)
        if cmd_type == 'upsert':
            cmd_kwargs['content'] = cls.get_content(kv_cmd.content)
            return UpsertCommand.create_command(**cmd_kwargs)
        if cmd_type == 'remove':
            return RemoveCommand.create_command(**cmd_kwargs)

        elif cmd_type == 'range_scan':
            cmd_kwargs.update({
                'collection': cls.get_collection(cluster, kv_cmd.collection),
                'scan_type': kv_cmd.scan_type,
                'stream_config': kv_cmd.stream_config,
                'content_as': kv_cmd.content_as if kv_cmd.HasField('content_as') else None,
            })
            return ScanCommand.create_command(**cmd_kwargs)
        if cmd_type == 'lookup_in':
            cmd_kwargs['specs'] = cls.get_lookup_in_specs(kv_cmd.spec)
            cmd_kwargs['raw_specs'] = kv_cmd.spec
            return LookupInCommand.create_command(**cmd_kwargs)
        if cmd_type == 'lookup_in_any_replica':
            cmd_kwargs['specs'] = cls.get_lookup_in_specs(kv_cmd.spec)
            cmd_kwargs['raw_specs'] = kv_cmd.spec
            return LookupInAnyReplicaCommand.create_command(**cmd_kwargs)
        if cmd_type == 'lookup_in_all_replicas':
            cmd_kwargs['specs'] = cls.get_lookup_in_specs(kv_cmd.spec)
            cmd_kwargs['raw_specs'] = kv_cmd.spec
            cmd_kwargs['stream_config'] = kv_cmd.stream_config
            return LookupInAllReplicasCommand.create_command(**cmd_kwargs)
        if cmd_type == 'get_and_touch':
            cmd_kwargs['content_as'] = kv_cmd.content_as
            cmd_kwargs['expiry'] = KvCommandOptions.convert_expiry(kv_cmd.expiry)
            return GetAndTouchCommand.create_command(**cmd_kwargs)
        if cmd_type == 'touch':
            cmd_kwargs['expiry'] = KvCommandOptions.convert_expiry(kv_cmd.expiry)
            return TouchCommand.create_command(**cmd_kwargs)
        if cmd_type == 'get_and_lock':
            cmd_kwargs['content_as'] = kv_cmd.content_as
            cmd_kwargs['duration'] = KvCommandOptions.convert_duration(kv_cmd.duration)
            return GetAndLockCommand.create_command(**cmd_kwargs)
        if cmd_type == 'unlock':
            cmd_kwargs['cas'] = kv_cmd.cas
            return UnlockCommand.create_command(**cmd_kwargs)
        if cmd_type == 'get_any_replica':
            cmd_kwargs['content_as'] = kv_cmd.content_as
            return GetAnyReplicaCommand.create_command(**cmd_kwargs)
        if cmd_type == 'get_all_replicas':
            cmd_kwargs['content_as'] = kv_cmd.content_as
            cmd_kwargs['stream_config'] = kv_cmd.stream_config
            return GetAllReplicasCommand.create_command(**cmd_kwargs)
        if cmd_type == 'exists':
            return ExistsCommand.create_command(**cmd_kwargs)
        if cmd_type == 'mutate_in':
            cmd_kwargs['specs'] = cls.get_mutate_in_specs(kv_cmd.spec)
            cmd_kwargs['raw_specs'] = kv_cmd.spec
            return MutateInCommand.create_command(**cmd_kwargs)
        if cmd_type == 'binary':
            binary_cmd_type = kv_cmd.WhichOneof('command')
            binary_cmd = getattr(kv_cmd, binary_cmd_type)

            cmd_kwargs['options'] = binary_cmd.options if binary_cmd.HasField('options') else None

            keyspace = cls.get_location_details(binary_cmd.location, counters)
            cmd_kwargs.update({
                'doc_id': keyspace['doc_id'],
                'collection': cls.get_collection(cluster, keyspace)
            })

            # I have referred to the grpc content attribute as bytes here to avoid confusion
            if binary_cmd_type == 'append':
                cmd_kwargs['bytes'] = binary_cmd.content
                return AppendCommand.create_command(**cmd_kwargs)
            if binary_cmd_type == 'prepend':
                cmd_kwargs['bytes'] = binary_cmd.content
                return PrependCommand.create_command(**cmd_kwargs)
            if binary_cmd_type == 'decrement':
                return DecrementCommand.create_command(**cmd_kwargs)
            if binary_cmd_type == 'increment':
                return IncrementCommand.create_command(**cmd_kwargs)

        raise NotImplementedError(f'KV operation `{cmd_type}` not supported')


class GetCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.GetOptions | None = kwargs['options']
        self._options: GetOptions | None = None
        self._content_as: content_pb.ContentAs = kwargs['content_as']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'with_expiry': KvCommandOptions.get_with_expiry(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'project': KvCommandOptions.get_projection(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = GetOptions(**opt_kwargs)

    @KvCommandResult.as_get_result
    def execute_command(self) -> GetResult:
        return self._collection.get(self._doc_id, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetCommand:
        command = GetCommand(**kwargs)
        command.set_options()
        return command


class InsertCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._content = kwargs['content']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.InsertOptions | None = kwargs['options']
        self._options: InsertOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'expiry': KvCommandOptions.get_expiry(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = InsertOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.insert(self._doc_id, self._content, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> InsertCommand:
        command = InsertCommand(**kwargs)
        command.set_options()
        return command


class ReplaceCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._content = kwargs['content']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.ReplaceOptions | None = kwargs['options']
        self._options: ReplaceOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'expiry': KvCommandOptions.get_expiry(self._raw_options),
            'preserve_expiry': KvCommandOptions.get_preserve_expiry(self._raw_options),
            'cas': KvCommandOptions.get_cas(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        }

        self._options = ReplaceOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.replace(self._doc_id, self._content, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> ReplaceCommand:
        command = ReplaceCommand(**kwargs)
        command.set_options()
        return command


class RemoveCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.RemoveOptions | None = kwargs['options']
        self._options: RemoveOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'cas': KvCommandOptions.get_cas(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = RemoveOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.remove(self._doc_id, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> RemoveCommand:
        command = RemoveCommand(**kwargs)
        command.set_options()
        return command


class UpsertCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._content = kwargs['content']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.UpsertOptions | None = kwargs['options']
        self._options: UpsertOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'expiry': KvCommandOptions.get_expiry(self._raw_options),
            'preserve_expiry': KvCommandOptions.get_preserve_expiry(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = UpsertOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.upsert(self._doc_id, self._content, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> UpsertCommand:
        command = UpsertCommand(**kwargs)
        command.set_options()
        return command


class ScanCommand(SdkCommand):
    _STREAM_TYPE = streams_pb.Type.STREAM_KV_RANGE_SCAN

    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._raw_options: kv_range_scan_pb.ScanOptions | None = kwargs['options']
        self._raw_scan_type: kv_range_scan_pb.ScanType = kwargs['scan_type']
        self._content_as: content_pb.ContentAs | None = kwargs['content_as']
        self._stream_config: streams_pb.Config = kwargs['stream_config']
        self._return_result: bool = kwargs['return_result']
        self._initiated = kwargs['initiated']
        self._options: ScanOptions | None = None
        self._scan_type: RangeScan | PrefixScan | SamplingScan | None = None
        self._logger = logging.getLogger(__name__)
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    @property
    def stream_type(self) -> streams_pb.Type.ValueType:
        return self._STREAM_TYPE

    @property
    def stream_config(self) -> streams_pb.Config:
        return self._stream_config

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'ids_only': KvCommandOptions.get_ids_only(self._raw_options),
            'batch_byte_limit': KvCommandOptions.get_batch_byte_limit(self._raw_options),
            'batch_item_limit': KvCommandOptions.get_batch_item_limit(self._raw_options),
            'consistent_with': KvCommandOptions.get_consistent_with(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'concurrency': KvCommandOptions.get_concurrency(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = ScanOptions(**opt_kwargs)

    def set_scan_type(self) -> None:
        if self._raw_scan_type is None:
            raise ValueError('Scan type is required for Scan command')

        scan_type_str = self._raw_scan_type.WhichOneof('type')
        if scan_type_str == 'range':
            self._set_range_or_prefix_scan()
        elif scan_type_str == 'sampling':
            self._set_sampling_scan()
        else:
            raise ValueError(f"Scan type {scan_type_str} not supported")

    def _set_range_or_prefix_scan(self) -> None:
        rng = self._raw_scan_type.range
        range_type = rng.WhichOneof('range')

        if range_type == 'from_to':
            self._scan_type = RangeScan(
                start=self._get_scan_term(getattr(rng.from_to, 'from')),
                end=self._get_scan_term(rng.from_to.to)
            )

        elif range_type == 'doc_id_prefix':
            self._scan_type = PrefixScan(rng.doc_id_prefix)

        else:
            raise NotImplementedError(f'Range type {range_type} for KV range scan not supported')

    def _set_sampling_scan(self) -> None:
        sampling = self._raw_scan_type.sampling
        self._scan_type = SamplingScan(
            limit=sampling.limit,
            seed=sampling.seed if sampling.HasField('seed') else None
        )

    @staticmethod
    def _get_scan_term(term_choice: kv_range_scan_pb.ScanTermChoice) -> ScanTerm | None:
        choice_type = term_choice.WhichOneof('choice')
        if choice_type == 'term':
            scan_term = term_choice.term
            exclusive = scan_term.exclusive if scan_term.HasField('exclusive') else None
            term_type = scan_term.WhichOneof('term')
            if term_type is None:
                raise ValueError('Scan term has no term set')
            return ScanTerm(getattr(scan_term, term_type), exclusive)
        elif choice_type == 'default':
            return None
        else:
            raise ValueError(f"Scan term choice '{choice_type}' not supported")

    @KvCommandResult.as_scan_result_stream
    def execute_command(self) -> ScanResultIterable:
        if self._scan_type is None:
            raise ValueError('Scan type has not been set, call set_scan_type first.')
        return self._collection.scan(self._scan_type, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> ScanCommand:
        command = ScanCommand(**kwargs)
        command.set_options()
        command.set_scan_type()
        return command


class LookupInCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_lookup_in_pb.LookupInOptions | None = kwargs['options']
        self._specs: list[Spec] = kwargs['specs']
        self._raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec] = kwargs['raw_specs']
        self._options: LookupInOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'access_deleted': KvCommandOptions.get_access_deleted(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = LookupInOptions(**opt_kwargs)

    @KvCommandResult.as_lookup_in_result
    def execute_command(self) -> LookupInResult:
        return self._collection.lookup_in(self._doc_id, self._specs, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> LookupInCommand:
        command = LookupInCommand(**kwargs)
        command.set_options()
        return command


class LookupInAnyReplicaCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_lookup_in_pb.LookupInAnyReplicaOptions | None = kwargs['options']
        self._specs: list[Spec] = kwargs['specs']
        self._raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec] = kwargs['raw_specs']
        self._options: LookupInAnyReplicaOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'read_preference': KvCommandOptions.get_read_preference(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = LookupInAnyReplicaOptions(**opt_kwargs)

    @KvCommandResult.as_lookup_in_any_replica_result
    def execute_command(self) -> LookupInReplicaResult:
        return self._collection.lookup_in_any_replica(self._doc_id, self._specs, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> LookupInAnyReplicaCommand:
        command = LookupInAnyReplicaCommand(**kwargs)
        command.set_options()
        return command


class LookupInAllReplicasCommand(SdkCommand):
    _STREAM_TYPE = streams_pb.Type.STREAM_LOOKUP_IN_ALL_REPLICAS

    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_lookup_in_pb.LookupInAllReplicasOptions | None = kwargs['options']
        self._specs: list[Spec] = kwargs['specs']
        self._raw_specs: Iterable[kv_lookup_in_pb.LookupInSpec] = kwargs['raw_specs']
        self._stream_config: streams_pb.Config = kwargs['stream_config']
        self._options: LookupInAllReplicasOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    @property
    def stream_type(self) -> streams_pb.Type.ValueType:
        return self._STREAM_TYPE

    @property
    def stream_config(self) -> streams_pb.Config:
        return self._stream_config

    @KvCommandResult.as_lookup_in_all_replicas_result_stream
    def execute_command(self) -> Iterable[LookupInReplicaResult]:
        return self._collection.lookup_in_all_replicas(self._doc_id, self._specs, self._options)

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'read_preference': KvCommandOptions.get_read_preference(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = LookupInAllReplicasOptions(**opt_kwargs)

    @staticmethod
    def create_command(**kwargs: Any) -> LookupInAllReplicasCommand:
        command = LookupInAllReplicasCommand(**kwargs)
        command.set_options()
        return command


class GetAndTouchCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.GetAndTouchOptions | None = kwargs['options']
        self._options: GetAndTouchOptions | None = None
        self._content_as: content_pb.ContentAs = kwargs['content_as']
        self._expiry: timedelta = kwargs['expiry']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            # 'parent_span': TODO is this supported?
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = GetAndTouchOptions(**opt_kwargs)

    @KvCommandResult.as_get_result
    def execute_command(self) -> GetResult:
        return self._collection.get_and_touch(self._doc_id, self._expiry, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetAndTouchCommand:
        command = GetAndTouchCommand(**kwargs)
        command.set_options()
        return command


class TouchCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.TouchOptions | None = kwargs['options']
        self._options: TouchOptions | None = None
        self._expiry: timedelta = kwargs['expiry']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            # 'parent_span': TODO is this supported?
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = TouchOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.touch(self._doc_id, self._expiry, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> TouchCommand:
        command = TouchCommand(**kwargs)
        command.set_options()
        return command


class GetAndLockCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.GetAndLockOptions | None = kwargs['options']
        self._options: GetAndLockOptions | None = None
        self._duration: timedelta = kwargs['duration']
        self._content_as: content_pb.ContentAs = kwargs['content_as']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
        }
        if self._raw_options is not None:
            opt_kwargs['span'] = KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)

        self._options = GetAndLockOptions(**opt_kwargs)

    @KvCommandResult.as_get_result
    def execute_command(self) -> GetResult:
        return self._collection.get_and_lock(self._doc_id, self._duration, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetAndLockCommand:
        command = GetAndLockCommand(**kwargs)
        command.set_options()
        return command


class UnlockCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.UnlockOptions | None = kwargs['options']
        self._options: UnlockOptions | None = None
        self._cas: int = kwargs['cas']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            # 'parent_span': TODO is this supported?
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = UnlockOptions(**opt_kwargs)

    @KvCommandResult.as_success
    def execute_command(self) -> None:
        return self._collection.unlock(self._doc_id, self._cas, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> UnlockCommand:
        command = UnlockCommand(**kwargs)
        command.set_options()
        return command


class ExistsCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.ExistsOptions | None = kwargs['options']
        self._options: ExistsOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = ExistsOptions(**opt_kwargs)

    @KvCommandResult.as_exists_result
    def execute_command(self) -> ExistsResult:
        return self._collection.exists(self._doc_id, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> ExistsCommand:
        command = ExistsCommand(**kwargs)
        command.set_options()
        return command


class AppendCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_binary_options_pb.AppendOptions | None = kwargs['options']
        self._options: AppendOptions | None = None
        self._bytes: bytes = kwargs['bytes']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'cas': KvCommandOptions.get_cas(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            # 'parent_span': TODO is this supported?
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = AppendOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.binary().append(self._doc_id, self._bytes, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> AppendCommand:
        command = AppendCommand(**kwargs)
        command.set_options()
        return command


class PrependCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_binary_options_pb.PrependOptions | None = kwargs['options']
        self._options: PrependOptions | None = None
        self._bytes: bytes = kwargs['bytes']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'cas': KvCommandOptions.get_cas(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            # 'parent_span': TODO is this supported?
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = PrependOptions(**opt_kwargs)

    @KvCommandResult.as_mutation_result
    def execute_command(self) -> MutationResult:
        return self._collection.binary().prepend(self._doc_id, self._bytes, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> PrependCommand:
        command = PrependCommand(**kwargs)
        command.set_options()
        return command


class DecrementCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_binary_options_pb.DecrementOptions | None = kwargs['options']
        self._options: DecrementOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'initial': KvCommandOptions.get_initial(self._raw_options),
            'delta': KvCommandOptions.get_delta(self._raw_options),
            'expiry': KvCommandOptions.get_expiry(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = DecrementOptions(**opt_kwargs)

    @KvCommandResult.as_counter_result
    def execute_command(self) -> CounterResult:
        return self._collection.binary().decrement(self._doc_id, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> DecrementCommand:
        command = DecrementCommand(**kwargs)
        command.set_options()
        return command


class IncrementCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_binary_options_pb.IncrementOptions | None = kwargs['options']
        self._options: IncrementOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'initial': KvCommandOptions.get_initial(self._raw_options),
            'delta': KvCommandOptions.get_delta(self._raw_options),
            'expiry': KvCommandOptions.get_expiry(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = IncrementOptions(**opt_kwargs)

    @KvCommandResult.as_counter_result
    def execute_command(self) -> CounterResult:
        return self._collection.binary().increment(self._doc_id, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> IncrementCommand:
        command = IncrementCommand(**kwargs)
        command.set_options()
        return command


class MutateInCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_mutate_in_pb.MutateInOptions | None = kwargs['options']
        self._specs: list[Spec] = kwargs['specs']
        self._raw_specs: Iterable[kv_mutate_in_pb.MutateInSpec] = kwargs['raw_specs']
        self._options: MutateInOptions | None = None
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'expiry': KvCommandOptions.get_expiry(self._raw_options),
            'cas': KvCommandOptions.get_cas(self._raw_options),
            'durability': KvCommandOptions.get_durability(self._raw_options),
            'store_semantics': KvCommandOptions.get_store_semantics(self._raw_options),
            'access_deleted': KvCommandOptions.get_access_deleted(self._raw_options),
            'preserve_expiry': KvCommandOptions.get_preserve_expiry(self._raw_options),
            'create_as_deleted': KvCommandOptions.get_create_as_deleted(self._raw_options),
        }
        if self._raw_options is not None:
            opt_kwargs['span'] = KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)

        self._options = MutateInOptions(**opt_kwargs)

    @KvCommandResult.as_mutate_in_result
    def execute_command(self) -> MutateInResult:
        return self._collection.mutate_in(self._doc_id, self._specs, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> MutateInCommand:
        command = MutateInCommand(**kwargs)
        command.set_options()
        return command


class GetAnyReplicaCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.GetAnyReplicaOptions | None = kwargs['options']
        self._options: GetAnyReplicaOptions | None = None
        self._content_as: content_pb.ContentAs = kwargs['content_as']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'read_preference': KvCommandOptions.get_read_preference(self._raw_options),
            'span': KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = GetAnyReplicaOptions(**opt_kwargs)

    @KvCommandResult.as_get_replica_result
    def execute_command(self) -> GetReplicaResult:
        return self._collection.get_any_replica(self._doc_id, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetAnyReplicaCommand:
        command = GetAnyReplicaCommand(**kwargs)
        command.set_options()
        return command


class GetAllReplicasCommand(SdkCommand):
    _STREAM_TYPE = streams_pb.Type.STREAM_KV_GET_ALL_REPLICAS

    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_KV_COMMAND_ARGS, **kwargs)
        self._collection: Collection = kwargs['collection']
        self._doc_id: str = kwargs['doc_id']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: kv_options_pb.GetAllReplicasOptions | None = kwargs['options']
        self._stream_config: streams_pb.Config = kwargs['stream_config']
        self._options: GetAllReplicasOptions | None = None
        self._content_as: content_pb.ContentAs = kwargs['content_as']
        self._span_owner: SpanOwner | None = kwargs['span_owner']

    @property
    def stream_type(self) -> streams_pb.Type.ValueType:
        return self._STREAM_TYPE

    @property
    def stream_config(self) -> streams_pb.Config:
        return self._stream_config

    @KvCommandResult.as_get_all_replicas_result_stream
    def execute_command(self) -> Iterable[GetReplicaResult]:
        return self._collection.get_all_replicas(self._doc_id, self._options)

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': KvCommandOptions.get_timeout(self._raw_options),
            'transcoder': KvCommandOptions.get_transcoder(self._raw_options),
            'read_preference': KvCommandOptions.get_read_preference(self._raw_options),
        }
        if self._raw_options is not None:
            opt_kwargs['span'] = KvCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)

        self._options = GetAllReplicasOptions(**opt_kwargs)

    @staticmethod
    def create_command(**kwargs: Any) -> GetAllReplicasCommand:
        command = GetAllReplicasCommand(**kwargs)
        command.set_options()
        return command
