#  Copyright 2016-2026. Couchbase, Inc.
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

from __future__ import annotations

import contextlib
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import (TYPE_CHECKING,
                    Any,
                    Literal,
                    TypedDict,
                    cast)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

from couchbase.common._observability._clock import now_ns
from couchbase.common._observability.no_op import NoOpMeter, NoOpTracer
from couchbase.common._observability.observability_types import (_ATTR_BUCKET_NAME,
                                                                 _ATTR_CLUSTER_NAME,
                                                                 _ATTR_CLUSTER_UUID,
                                                                 _ATTR_COLLECTION_NAME,
                                                                 _ATTR_DISPATCH_SPAN_NAME,
                                                                 _ATTR_DURABILITY_LEVEL,
                                                                 _ATTR_ENCODING_SPAN_NAME,
                                                                 _ATTR_ERROR_TYPE,
                                                                 _ATTR_METER_OP_DURATION,
                                                                 _ATTR_OPERATION_NAME,
                                                                 _ATTR_QUERY_STATEMENT,
                                                                 _ATTR_RESERVED_UNIT,
                                                                 _ATTR_RESERVED_UNIT_SECONDS,
                                                                 _ATTR_RETRY_COUNT,
                                                                 _ATTR_SCOPE_NAME,
                                                                 _ATTR_SERVICE,
                                                                 _ATTR_SYSTEM_NAME,
                                                                 _CPP_ATTR_CLUSTER_NAME,
                                                                 _CPP_ATTR_CLUSTER_UUID,
                                                                 _CPP_ATTR_RETRY_COUNT,
                                                                 ExceptionName,
                                                                 LegacySpanProtocol,
                                                                 LegacyTracerProtocol,
                                                                 ObservabilityInstruments,
                                                                 RequestSpanProtocol,
                                                                 RequestTracerProtocol,
                                                                 SpanProtocol,
                                                                 WrappedTracer)
from couchbase.common._operations import (Op,
                                          OpKind,
                                          ServiceType)
from couchbase.common._supportability import Supportability
from couchbase.common.durability import DurabilityLevel
from couchbase.common.exceptions import CouchbaseException, InvalidArgumentException
from couchbase.common.observability.tracing import SpanAttributeValue, SpanStatusCode

if TYPE_CHECKING:
    from couchbase.common._observability.observability_types import CoreChildSpan, CoreSpan
    from couchbase.common._observability.threshold_logging import ThresholdLoggingSpan


class KeyspaceDetails(TypedDict):
    bucket_name: str
    scope_name: str
    collection_name: str


@dataclass
class WrappedEncodingSpan:
    span: RequestSpanProtocol
    end_time: int


class OpAttributeOptions(TypedDict, total=False):
    collection_details: KeyspaceDetails | None
    statement: str | None
    query_params: dict[str, Any] | None
    parent_span: SpanProtocol | WrappedSpan | None
    bucket_name: str | None
    scope_name: str | None
    collection_name: str | None
    use_now_as_start_time: bool | None


_KV_SERVICE_VALUE = ServiceType.KeyValue.value
_NOOP_OBS_HANDLER_CTX = contextlib.nullcontext()
# A multi or datastructure operation is metered as the operations it is made of, never itself.
_METERED_BY_PART = (OpKind.KeyValueMulti, OpKind.Datastructure)


def _traced(op: Op) -> tuple[str, ServiceType]:
    if op.span_name is None or op.service is None:
        raise InvalidArgumentException(f'{op} is never traced or metered')
    return op.span_name, op.service


def get_attributes_for_kv_op(op_name: str,
                             collection_details: KeyspaceDetails) -> dict[str, str]:
    return {
        _ATTR_SYSTEM_NAME: 'couchbase',
        _ATTR_SERVICE: _KV_SERVICE_VALUE,
        _ATTR_OPERATION_NAME: op_name,
        _ATTR_BUCKET_NAME: collection_details['bucket_name'],
        _ATTR_SCOPE_NAME: collection_details['scope_name'],
        _ATTR_COLLECTION_NAME: collection_details['collection_name'],
    }


def get_attributes_for_http_op(op_name: str,
                               service_type: ServiceType,
                               **options: Unpack[OpAttributeOptions]) -> dict[str, str]:
    attrs = {
        _ATTR_SYSTEM_NAME: 'couchbase',
        _ATTR_SERVICE: service_type.value,
        _ATTR_OPERATION_NAME: op_name,
    }

    statement = options.get('statement', None)
    query_params = options.get('query_params', None)

    if (statement is not None and query_params
            and ('positional_parameters' in query_params or 'named_parameters' in query_params)):
        attrs[_ATTR_QUERY_STATEMENT] = statement

    bucket_name = options.get('bucket_name', None)
    if bucket_name:
        attrs[_ATTR_BUCKET_NAME] = bucket_name

    scope_name = options.get('scope_name', None)
    if scope_name:
        attrs[_ATTR_SCOPE_NAME] = scope_name

    collection_name = options.get('collection_name', None)
    if collection_name:
        attrs[_ATTR_COLLECTION_NAME] = collection_name

    return attrs


class ObservableRequestHandler:

    def __init__(self,
                 op: Op,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._op = op

        # Capture a single timestamp and share it between tracer and meter impls,
        # eliminating redundant now_ns() calls.
        now = now_ns()

        # cache NoOp tracer & meter impl on the ObservabilityInstruments instance to avoid
        # a fresh allocation on every KV operation.
        #   None    -> not yet checked (first call for this ObservabilityInstruments)
        #   False   -> real tracer/meter (not NoOp)
        #   <inst>  -> cached NoOp impl, ready to reuse
        _cached_tracer = observability_instruments._cached_noop_tracer_impl
        if _cached_tracer is None:
            if isinstance(observability_instruments.tracer.tracer, NoOpTracer):
                _cached_tracer = ObservableRequestHandlerNoOpTracerImpl(op, observability_instruments)
            else:
                _cached_tracer = False
            observability_instruments._cached_noop_tracer_impl = _cached_tracer

        self._tracer_impl: ObservableRequestHandlerNoOpTracerImpl | ObservableRequestHandlerTracerImpl
        if _cached_tracer is not False:
            self._tracer_impl = _cached_tracer
            self.is_noop = True
        else:
            self._tracer_impl = ObservableRequestHandlerTracerImpl(op,
                                                                   observability_instruments,
                                                                   start_time=now)
            self.is_noop = False

        _cached_meter = observability_instruments._cached_noop_meter_impl
        if _cached_meter is None:
            if isinstance(observability_instruments.meter, NoOpMeter):
                _cached_meter = ObservableRequestHandlerNoOpMeterImpl(op, observability_instruments)
            else:
                _cached_meter = False
            observability_instruments._cached_noop_meter_impl = _cached_meter

        self._meter_impl: ObservableRequestHandlerNoOpMeterImpl | ObservableRequestHandlerMeterImpl
        if _cached_meter is not False:
            self._meter_impl = _cached_meter
        else:
            self._meter_impl = ObservableRequestHandlerMeterImpl(op,
                                                                 observability_instruments,
                                                                 start_time=now)

        self._with_metrics = isinstance(self._meter_impl, ObservableRequestHandlerNoOpMeterImpl) is False

    @property
    def is_legacy_tracer(self) -> bool:
        return self._tracer_impl.is_legacy

    @property
    def legacy_request_span(self) -> LegacySpanProtocol | None:
        return self._tracer_impl.legacy_request_span

    @property
    def op(self) -> Op:
        return self._op

    @property
    def with_metrics(self) -> bool:
        return self._with_metrics

    @property
    def wrapper_span_name(self) -> str:
        return self._tracer_impl.wrapper_span_name

    @property
    def wrapped_span(self) -> WrappedSpan | None:
        return self._tracer_impl.wrapped_span

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        self._tracer_impl.add_kv_durability_attribute(durability)

    def create_kv_span(self,
                       collection_details: KeyspaceDetails,
                       parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        self._meter_impl.add_kv_attributes(collection_details)
        self._tracer_impl.create_kv_span(collection_details, parent_span=parent_span)

    def create_kv_multi_span(self,
                             collection_details: KeyspaceDetails,
                             parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        self._meter_impl.add_kv_attributes(collection_details)
        self._tracer_impl.create_kv_span(collection_details, parent_span=parent_span)

    def create_http_span(self, **options: Unpack[OpAttributeOptions]) -> None:
        self._tracer_impl.create_http_span(**options)
        self._meter_impl.add_http_attributes(**options)

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        return self._tracer_impl.maybe_add_encoding_span(encoding_fn)

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        return self._tracer_impl.maybe_create_encoding_span(encoding_fn)

    def process_core_span(self,
                          core_span: CoreSpan | None = None,
                          with_error: bool | None = False) -> None:
        self._tracer_impl.process_core_span(core_span=core_span, with_error=with_error)

    def process_meter_end(self, exc_val: BaseException | None = None) -> None:
        self._meter_impl.process_end(cluster_name=self._tracer_impl.cluster_name,
                                     cluster_uuid=self._tracer_impl.cluster_uuid,
                                     exc_val=exc_val)

    def process_multi_sub_op(self,
                             result: object,
                             exc_val: BaseException | None = None) -> None:
        if not self._with_metrics:
            return

        start_time = getattr(result, 'start_time', None)
        end_time = getattr(result, 'end_time', None)

        if start_time is None or end_time is None:
            return

        req_duration_ns: int = end_time - start_time
        self._meter_impl.process_multi_sub_op(req_duration_ns,
                                              cluster_name=self._tracer_impl.cluster_name,
                                              cluster_uuid=self._tracer_impl.cluster_uuid,
                                              exc_val=exc_val)

    def reset(self, op: Op, exc_val: BaseException | None = None) -> None:
        self._op = op
        self._tracer_impl.reset(op, with_error=(exc_val is not None))
        self._meter_impl.reset(op,
                               cluster_name=self._tracer_impl.cluster_name,
                               cluster_uuid=self._tracer_impl.cluster_uuid,
                               exc_val=exc_val)

    @staticmethod
    def create(op: Op,
               observability_instruments: ObservabilityInstruments
               ) -> ObservableRequestHandler | contextlib.nullcontext[None]:
        if observability_instruments.is_noop:
            return _NOOP_OBS_HANDLER_CTX
        return ObservableRequestHandler(op, observability_instruments)

    @staticmethod
    def create_or_none(op: Op,
                       observability_instruments: ObservabilityInstruments) -> ObservableRequestHandler | None:
        if observability_instruments.is_noop:
            return None
        handler = ObservableRequestHandler(op, observability_instruments)
        handler.__enter__()
        return handler

    @staticmethod
    def get_query_context_components(query_context: str,
                                     is_analytics: bool | None = False) -> tuple[str, str] | None:
        if is_analytics:
            components = query_context.replace('default:', '').split('.')
        else:
            components = query_context.split('.')
        if len(components) == 2:
            bucket_name = components[0].replace('`', '')
            scope_name = components[1].replace('`', '')
            return bucket_name, scope_name
        return None

    @staticmethod
    def maybe_get_parent_span(span: SpanProtocol | None = None,
                              parent_span: SpanProtocol | WrappedSpan | None = None
                              ) -> SpanProtocol | WrappedSpan | None:

        final_span = parent_span
        if not final_span and span:
            Supportability.option_deprecated('span', 'parent_span')
            final_span = span

        if final_span:
            # SpanProtocol = LegacySpanProtocol | RequestSpanProtocol
            # So, if the span is Legacy, we issue a warning, if the span is not Legacy, then it should be a
            # RequestSpanProtocol (or WrappedSpan for datastructure ops). Otherwise we have an invalid argument.
            if isinstance(final_span, LegacySpanProtocol):
                msg = ('The "parent_span" option should implement the RequestSpanProtocol '
                       '(e.g. use couchbase.observability.tracing.RequestSpan)')
                Supportability.type_deprecated('LegacySpanProtocol', 'RequestSpanProtocol', msg)
            elif not isinstance(final_span, (RequestSpanProtocol, WrappedSpan)):
                raise InvalidArgumentException('parent_span must implement SpanProtocol')

            return final_span

        return None

    def __enter__(self) -> ObservableRequestHandler:
        return self

    def __exit__(self,
                 exc_type: type[BaseException] | None,
                 exc_val: BaseException | None,
                 exc_tb: TracebackType | None) -> Literal[False]:
        self._tracer_impl.process_end(with_error=exc_type is not None)
        self._meter_impl.process_end(cluster_name=self._tracer_impl.cluster_name,
                                     cluster_uuid=self._tracer_impl.cluster_uuid,
                                     exc_val=exc_val)
        return False

    # --- Async Context Manager Protocol ---
    async def __aenter__(self) -> ObservableRequestHandler:
        return self

    async def __aexit__(self,
                        exc_type: type[BaseException] | None,
                        exc_val: BaseException | None,
                        exc_tb: TracebackType | None) -> bool | None:
        # Delegate the teardown and exception handling to the _impl
        return self.__exit__(exc_type, exc_val, exc_tb)


class ObservableRequestHandlerNoOpMeterImpl:
    def __init__(self,
                 op: Op,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._wrapped_meter = observability_instruments.meter

    def add_kv_attributes(self, collection_details: KeyspaceDetails) -> None:
        pass

    def add_http_attributes(self, **options: Unpack[OpAttributeOptions]) -> None:
        pass

    def process_end(self,
                    cluster_name: str | None = None,
                    cluster_uuid: str | None = None,
                    exc_val: BaseException | None = None) -> None:
        pass

    def process_multi_sub_op(self,
                             req_duration_ns: int,
                             cluster_name: str | None = None,
                             cluster_uuid: str | None = None,
                             exc_val: BaseException | None = None) -> None:
        pass

    def reset(self,
              op: Op,
              cluster_name: str | None = None,
              cluster_uuid: str | None = None,
              exc_val: BaseException | None = None) -> None:
        pass


class ObservableRequestHandlerNoOpTracerImpl:
    def __init__(self,
                 op: Op,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._wrapped_tracer = observability_instruments.tracer

    @property
    def cluster_name(self) -> str | None:
        return None

    @property
    def cluster_uuid(self) -> str | None:
        return None

    @property
    def is_legacy(self) -> bool:
        return self._wrapped_tracer.is_legacy

    @property
    def legacy_request_span(self) -> LegacySpanProtocol | None:
        return None

    @property
    def processed_core_span(self) -> bool:
        return False

    @property
    def wrapper_span_name(self) -> str:
        return ''

    @property
    def wrapped_span(self) -> WrappedSpan | None:
        return None

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        pass

    def create_kv_span(self,
                       collection_details: KeyspaceDetails,
                       parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        pass

    def create_kv_multi_span(self,
                             collection_details: KeyspaceDetails,
                             parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        pass

    def create_http_span(self, **options: Unpack[OpAttributeOptions]) -> None:
        pass

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        return encoding_fn()

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        return encoding_fn()

    def process_end(self, with_error: bool | None = False) -> None:
        pass

    def process_core_span(self,
                          core_span: CoreSpan | None = None,
                          with_error: bool | None = False) -> None:
        pass

    def reset(self, op: Op, with_error: bool | None = False) -> None:
        pass


class ObservableRequestHandlerMeterImpl:
    def __init__(self,
                 op: Op,
                 observability_instruments: ObservabilityInstruments,
                 start_time: int | None = None) -> None:
        self._meter = observability_instruments.meter
        self._get_cluster_labels_fn = observability_instruments.get_cluster_labels_fn
        self._start_time = start_time if start_time is not None else now_ns()
        self._set_op(op)
        self._resolved_cluster_labels: bool = False

    def _set_op(self, op: Op) -> None:
        # A multi operation's per-key records are named for the operation each key is.
        self._op_name_str, self._service_type = _traced(op.single or op)
        self._ignore_top_level_op = op.kind in _METERED_BY_PART
        self._attrs: dict[str, str] = {}

    def add_kv_attributes(self, collection_details: KeyspaceDetails) -> None:
        self._attrs = get_attributes_for_kv_op(self._op_name_str, collection_details)
        self._attrs[_ATTR_RESERVED_UNIT] = _ATTR_RESERVED_UNIT_SECONDS

    def add_http_attributes(self, **options: Unpack[OpAttributeOptions]) -> None:
        self._attrs = get_attributes_for_http_op(self._op_name_str, self._service_type, **options)
        self._attrs[_ATTR_RESERVED_UNIT] = _ATTR_RESERVED_UNIT_SECONDS

    def process_end(self,
                    cluster_name: str | None = None,
                    cluster_uuid: str | None = None,
                    exc_val: BaseException | None = None) -> None:
        end_time = now_ns()
        if self._ignore_top_level_op:
            return
        self._process_end((end_time - self._start_time),
                          attrs=self._attrs,
                          cluster_name=cluster_name,
                          cluster_uuid=cluster_uuid,
                          exc_val=exc_val)

    def process_multi_sub_op(self,
                             req_duration_ns: int,
                             cluster_name: str | None = None,
                             cluster_uuid: str | None = None,
                             exc_val: BaseException | None = None) -> None:
        self._process_end(req_duration_ns,
                          attrs=self._attrs,
                          cluster_name=cluster_name,
                          cluster_uuid=cluster_uuid,
                          exc_val=exc_val)

    def reset(self,
              op: Op,
              cluster_name: str | None = None,
              cluster_uuid: str | None = None,
              exc_val: BaseException | None = None) -> None:
        self.process_end(cluster_name=cluster_name, cluster_uuid=cluster_uuid, exc_val=exc_val)
        self._start_time = now_ns()
        self._set_op(op)
        self._resolved_cluster_labels = False

    def _handle_error(self, attrs: dict[str, str], exc_val: BaseException) -> None:
        exc_name = None
        if isinstance(exc_val, CouchbaseException):
            exc_name = ExceptionName.from_exception(exc_val)
        attrs[_ATTR_ERROR_TYPE] = exc_name.value if exc_name is not None else '_OTHER'

    def _process_end(self,
                     req_duration_ns: int,
                     attrs: Mapping[str, str] | None = None,
                     cluster_name: str | None = None,
                     cluster_uuid: str | None = None,
                     exc_val: BaseException | None = None) -> None:
        if not self._resolved_cluster_labels:
            if self._get_cluster_labels_fn and (cluster_name is None or cluster_uuid is None):
                cluster_labels = self._get_cluster_labels_fn()
                cluster_name = cluster_name or cluster_labels.get('clusterName', None)
                cluster_uuid = cluster_uuid or cluster_labels.get('clusterUUID', None)

            # if the op failed prior to the ObservableRequestHandler creating a span, we won't have attributes yet
            if not attrs:
                self._attrs = {
                    _ATTR_SYSTEM_NAME: 'couchbase',
                    _ATTR_SERVICE: self._service_type.value,
                    _ATTR_OPERATION_NAME: self._op_name_str,
                    _ATTR_RESERVED_UNIT: _ATTR_RESERVED_UNIT_SECONDS
                }

            if cluster_name:
                self._attrs[_ATTR_CLUSTER_NAME] = cluster_name
            if cluster_uuid:
                self._attrs[_ATTR_CLUSTER_UUID] = cluster_uuid
            if cluster_name and cluster_uuid:
                self._resolved_cluster_labels = True

        if exc_val is not None:
            # Copy to avoid contaminating self._attrs with the error type across batch items
            record_attrs = {**self._attrs}
            self._handle_error(record_attrs, exc_val)
            self._meter.value_recorder(_ATTR_METER_OP_DURATION,
                                       tags=record_attrs).record_value(self._to_microseconds(req_duration_ns))
        else:
            self._meter.value_recorder(_ATTR_METER_OP_DURATION,
                                       tags=self._attrs).record_value(self._to_microseconds(req_duration_ns))

    def _to_microseconds(self, duration_ns: int) -> int:
        return round(duration_ns / 1_000)


class ObservableRequestHandlerTracerImpl:

    def __init__(self,
                 op: Op,
                 observability_instruments: ObservabilityInstruments,
                 start_time: int | None = None) -> None:
        self._start_time = start_time if start_time is not None else now_ns()
        self._wrapped_tracer = observability_instruments.tracer
        self._get_cluster_labels_fn = observability_instruments.get_cluster_labels_fn
        self._processed_core_span = False
        self._set_op(op)

    def _set_op(self, op: Op) -> None:
        self._op = op
        self._span_name, self._service_type = _traced(op)
        self._is_streaming_op = op.kind is OpKind.Streaming
        self._wrapped_span: WrappedSpan | None = None
        self._end_time: int | None = None

    @property
    def cluster_name(self) -> str | None:
        return self._wrapped_span.cluster_name if self._wrapped_span else None

    @property
    def cluster_uuid(self) -> str | None:
        return self._wrapped_span.cluster_uuid if self._wrapped_span else None

    @property
    def is_legacy(self) -> bool:
        return self._wrapped_tracer.is_legacy

    @property
    def legacy_request_span(self) -> LegacySpanProtocol | None:
        if self._wrapped_span:
            return self._wrapped_span.legacy_request_span
        return None

    @property
    def processed_core_span(self) -> bool:
        return self._processed_core_span

    @property
    def wrapper_span_name(self) -> str:
        return self._wrapped_span.name if self._wrapped_span is not None else ''

    @property
    def wrapped_span(self) -> WrappedSpan | None:
        return self._wrapped_span

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        if self._wrapped_span:
            self._wrapped_span.add_kv_durability_attribute(durability)

    def create_kv_span(self,
                       collection_details: KeyspaceDetails,
                       parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        self._create_wrapped_span(collection_details=collection_details, parent_span=parent_span)

    def create_kv_multi_span(self,
                             collection_details: KeyspaceDetails,
                             parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        self._create_wrapped_span(collection_details=collection_details, parent_span=parent_span)

    def create_http_span(self, **options: Unpack[OpAttributeOptions]) -> None:
        self._create_wrapped_span(**options)

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        if not self._wrapped_span or self.is_legacy:
            return encoding_fn()

        return self._wrapped_span.maybe_add_encoding_span(encoding_fn)

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        if not self._wrapped_span or self.is_legacy:
            return encoding_fn()

        return self._wrapped_span.maybe_create_encoding_span(encoding_fn)

    def process_core_span(self,
                          core_span: CoreSpan | None = None,
                          with_error: bool | None = False) -> None:
        if not self._wrapped_span:
            return

        if core_span is None:
            # legacy tracing does not have a core_span
            if self.is_legacy:
                self.process_end()
            return

        self._wrapped_span.process_core_span(core_span)
        self._processed_core_span = True

        # we don't use a context manager for streaming (e.g. query) requests
        if self._is_streaming_op:
            self.process_end(with_error=with_error)

    def process_end(self, with_error: bool | None = False) -> None:
        self._end_time = now_ns()

        # we fall into this branch when we raise an error prior to going down into the bindings
        if (self._wrapped_span
            and not self._processed_core_span
            and not self.is_legacy
                and self._get_cluster_labels_fn):

            cluster_labels = self._get_cluster_labels_fn()
            self._wrapped_span.set_cluster_labels(cluster_name=cluster_labels.get('clusterName', None),
                                                  cluster_uuid=cluster_labels.get('clusterUUID', None))
            self._wrapped_span.set_retry_attribute(retry_count=0)

        if self._wrapped_span:
            if with_error is True and not self.is_legacy:
                self._wrapped_span.set_status(SpanStatusCode.ERROR)
            self._wrapped_span.end(self._end_time)

    def reset(self, op: Op, with_error: bool | None = False) -> None:
        self.process_end(with_error=with_error)
        self._start_time = now_ns()
        self._set_op(op)

    def _create_wrapped_span(self,
                             **options: Unpack[OpAttributeOptions]) -> None:
        use_now_as_start_time = options.get('use_now_as_start_time', False)
        if use_now_as_start_time is True:
            start_time = None
        else:
            start_time = self._start_time
        self._wrapped_span = WrappedSpan(self._service_type,
                                         self._span_name,
                                         self._wrapped_tracer,
                                         op=self._op,
                                         start_time=start_time,
                                         **options)


class WrappedSpan:

    def __init__(self,
                 service_type: ServiceType,
                 span_name: str,
                 wrapped_tracer: WrappedTracer,
                 op: Op | None = None,
                 start_time: int | None = None,
                 **options: Unpack[OpAttributeOptions]) -> None:
        """`op` is None for a span the core made, which is named by the core."""
        self._service_type = service_type
        self._wrapped_tracer = wrapped_tracer
        self._collection_details = options.get('collection_details', None)
        self._start_time = start_time or now_ns()
        # The request_span should _only_ take a SpanProtocol for the parent
        self._parent_span = options.get('parent_span', None)
        p_span: SpanProtocol | None
        if isinstance(self._parent_span, WrappedSpan):
            p_span = self._parent_span.request_span
        else:
            p_span = self._parent_span
        self._op_name_str = span_name
        self._request_span = self._create_request_span(span_name,
                                                       parent_span=p_span,
                                                       start_time=self._start_time)

        self._wrapped_parent = self._parent_span if isinstance(self._parent_span, WrappedSpan) else None
        _is_multi_op = op is not None and op.kind is OpKind.KeyValueMulti
        # PERF improvement for multi-ops: True only when BOTH conditions hold:
        #   1. The request span type supports direct dispatch recording
        #      (ThresholdLoggingSpan sets _supports_multi_op_fast_dispatch=True).
        #   2. This WrappedSpan is for a multi-op (upsert_multi, get_multi, …).
        # Single-op dispatch spans must still be created as proper child spans
        # so that threshold log records contain the correct dispatch structure.
        self._supports_multi_op_fast_dispatch = (
            _is_multi_op
            and getattr(self._request_span, '_supports_multi_op_fast_dispatch', False)
        )
        # Cache the set of attribute keys the underlying span actually processes.
        # None means the span type has no filter (e.g. OTel) — pass everything through.
        self._span_relevant_keys = getattr(self._request_span, '_processed_attribute_keys', None)
        # When OTel sampler drops a span, is_recording is False and we skip
        # all expensive attribute setting, encoding spans, and dispatch span building.
        self._is_recording = getattr(self._request_span, 'is_recording', True)
        if self._is_recording:
            self._has_multiple_encoding_spans = (_is_multi_op or op is Op.MutateIn)
            self._encoding_spans_ended = False
            self._encoding_spans: WrappedEncodingSpan | list[WrappedEncodingSpan] | None = None
            self._set_span_attrs(**options)
            self._cluster_name: str | None = None
            self._cluster_uuid: str | None = None
            self._end_time_watermark = now_ns()
        else:
            # T10: Minimal init — skip all attribute work for non-recording spans
            self._has_multiple_encoding_spans = False
            self._encoding_spans_ended = True
            self._encoding_spans = None
            self._cluster_name = None
            self._cluster_uuid = None
            self._end_time_watermark = self._start_time

    @property
    def cluster_name(self) -> str | None:
        return self._cluster_name

    @property
    def cluster_uuid(self) -> str | None:
        return self._cluster_uuid

    @property
    def is_recording(self) -> bool:
        return self._is_recording

    @property
    def request_span(self) -> SpanProtocol:
        return self._request_span

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        self._request_span.set_attribute(_ATTR_DURABILITY_LEVEL,
                                         DurabilityLevel.to_server_str(durability))

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        # legacy operations did not create an encoding span; not support now
        # we only expect certain ops to have multiple encoding spans
        if not self._is_recording or self._wrapped_tracer.is_legacy or not self._has_multiple_encoding_spans:
            return encoding_fn()

        if not isinstance(self._encoding_spans, list):
            self._encoding_spans = []
        encoding_spans = self._encoding_spans

        tracer = cast(RequestTracerProtocol, self._wrapped_tracer.tracer)
        encoding_span = tracer.request_span(_ATTR_ENCODING_SPAN_NAME, self._request_span)
        encoding_span.set_attribute(_ATTR_SYSTEM_NAME, 'couchbase')
        try:
            encoded_output = encoding_fn()
            return encoded_output
        except Exception:
            encoding_span.set_status(SpanStatusCode.ERROR)
            raise
        finally:
            # we wait to set the end time until we process the underylying
            # core span so that we can add the cluster_[name|uuid] attributes
            encoding_spans.append(WrappedEncodingSpan(encoding_span, now_ns()))

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., tuple[bytes, int]]) -> tuple[bytes, int]:
        # legacy operations did not create an encoding span; not support now
        # if the op is expected to have multiple encoding spans, maybe_add_encoding_span() should be used instead
        if not self._is_recording or self._wrapped_tracer.is_legacy or self._has_multiple_encoding_spans:
            return encoding_fn()

        tracer = cast(RequestTracerProtocol, self._wrapped_tracer.tracer)
        encoding_span = tracer.request_span(_ATTR_ENCODING_SPAN_NAME, self._request_span)
        encoding_span.set_attribute(_ATTR_SYSTEM_NAME, 'couchbase')
        try:
            encoded_output = encoding_fn()
            return encoded_output
        except Exception:
            encoding_span.set_status(SpanStatusCode.ERROR)
            raise
        finally:
            # we wait to set the end time until we process the underylying
            # core span so that we can add the cluster_[name|uuid] attributes
            self._encoding_spans = WrappedEncodingSpan(encoding_span, now_ns())

    def maybe_update_end_time_watermark(self, end_time: int) -> None:
        self._end_time_watermark = self._end_time_watermark if self._end_time_watermark > end_time else end_time

    def process_core_span(self, core_span: CoreSpan) -> None:
        # Skip all core span processing if the span is not recording
        if not self._is_recording:
            return

        # Guard cluster_[name|uuid] propagation — for multi-op batches, process_core_span
        # is called once per sub-op on the same WrappedSpan. cluster_name/uuid are
        # identical for every sub-op, so only set them on the first call.
        core_attrs = core_span.get('attributes') or {}
        if self._cluster_name is None:
            cluster_name = core_attrs.get(_CPP_ATTR_CLUSTER_NAME, None)
            if cluster_name is not None:
                self._set_attribute_on_all_spans(_ATTR_CLUSTER_NAME, cluster_name)
        if self._cluster_uuid is None:
            cluster_uuid = core_attrs.get(_CPP_ATTR_CLUSTER_UUID, None)
            if cluster_uuid is not None:
                self._set_attribute_on_all_spans(_ATTR_CLUSTER_UUID, cluster_uuid)
        retry_count = core_attrs.get(_CPP_ATTR_RETRY_COUNT, 0)
        self._set_attribute_on_all_spans(_ATTR_RETRY_COUNT, retry_count, skip_encoding_span=True)

        # now that we have the cluster_[name|uuid] attributes from the core span, we can end the encoding span(s)
        self._end_encoding_spans()
        children = core_span.get('children', None)
        if not children:
            return

        self._build_core_spans(children, parent_span=self)

    def record_multi_op_dispatch_span(self, duration: int, attributes: Mapping[str, Any] | None) -> None:
        # Bypasses ThresholdLoggingSpan child creation entirely by propagating
        # dispatch duration and attributes directly onto the request span.
        # Only called when self._supports_multi_op_fast_dispatch is True (i.e.
        # the request span is a ThresholdLoggingSpan) and the dispatch span has
        # no child spans to recurse into.

        request_span = cast('ThresholdLoggingSpan', self._request_span)
        request_span._propagate_dispatch_duration(duration)
        if attributes:
            request_span.apply_core_span_attributes(attributes)

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        # Skip setting any attribute if the span is not recording
        if not self._is_recording:
            return

        if key == _ATTR_CLUSTER_NAME:
            self._cluster_name = cast(str, value)
        elif key == _ATTR_CLUSTER_UUID:
            self._cluster_uuid = cast(str, value)
        if self._span_relevant_keys is not None and key not in self._span_relevant_keys:
            return
        self._request_span.set_attribute(key, value)

    def set_cluster_labels(self, cluster_name: str | None = None, cluster_uuid: str | None = None) -> None:
        if cluster_name:
            self._set_attribute_on_all_spans(_ATTR_CLUSTER_NAME, cluster_name)
        if cluster_uuid:
            self._set_attribute_on_all_spans(_ATTR_CLUSTER_UUID, cluster_uuid)

    def set_retry_attribute(self, retry_count: int = 0) -> None:
        self._set_attribute_on_all_spans(_ATTR_RETRY_COUNT, retry_count, skip_encoding_span=True)

    def set_status(self, status: SpanStatusCode) -> None:
        self._status = status
        self._request_span.set_status(status)

    def end(self, end_time: int) -> None:
        # For non-recording spans, still call end() for OTel context cleanup
        # but skip all the heavy processing (encoding spans, watermark, etc.)
        if not self._is_recording:
            self._request_span.end(end_time)
            return

        self._end_encoding_spans()
        if self._wrapped_tracer.is_legacy:
            cast(LegacySpanProtocol, self._request_span).finish()
            return

        self._end_time_watermark = self._end_time_watermark if self._end_time_watermark > end_time else end_time
        if self._wrapped_parent is not None:
            self._wrapped_parent.maybe_update_end_time_watermark(self._end_time_watermark)
        self._request_span.end(self._end_time_watermark)

    def _build_core_spans(self,
                          core_spans: list[CoreChildSpan],
                          parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        for span in core_spans:
            if span['name'] == _ATTR_DISPATCH_SPAN_NAME:
                self._build_dispatch_core_span(span, parent_span=parent_span)
            else:
                self._build_non_dispatch_core_span(span, parent_span=parent_span)

    def _build_dispatch_core_span(self,
                                  core_span: CoreChildSpan,
                                  parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        p_span: SpanProtocol | None
        if isinstance(parent_span, WrappedSpan):
            p_span = parent_span.request_span
        else:
            p_span = parent_span

        core_span_start_time = core_span['start']
        latest_start_time = self._start_time if self._start_time > core_span_start_time else core_span_start_time
        core_span_end_time = core_span['end']
        children = core_span.get('children', None)

        # Fast path for multi-op sub-op dispatch spans. When the parent is
        # this WrappedSpan (direct child, not a recursed nested span) and the
        # dispatch span has no children of its own, skip ThresholdLoggingSpan
        # child creation entirely. Propagates duration and attributes straight
        # onto the request span via record_multi_op_dispatch_span().
        if not children and parent_span is self and self._supports_multi_op_fast_dispatch:
            dispatch_duration = core_span_end_time - latest_start_time
            self.record_multi_op_dispatch_span(dispatch_duration, core_span.get('attributes', {}))
            end_watermark = self._end_time_watermark
            self._end_time_watermark = end_watermark if end_watermark > core_span_end_time else core_span_end_time
            return

        new_span = self._create_request_span(core_span['name'], parent_span=p_span, start_time=latest_start_time)
        if children:
            self._build_core_spans(children, parent_span=new_span)

        attrs = core_span.get('attributes', {})
        if attrs:
            if getattr(new_span, '_supports_multi_op_fast_dispatch', False):
                cast('ThresholdLoggingSpan', new_span).apply_core_span_attributes(attrs)
            else:
                for attr_name, attr_val in attrs.items():
                    new_span.set_attribute(attr_name, attr_val)

        end_watermark = self._end_time_watermark
        self._end_time_watermark = end_watermark if end_watermark > core_span_end_time else core_span_end_time
        new_span.end(core_span_end_time)

    def _build_non_dispatch_core_span(self,
                                      core_span: CoreChildSpan,
                                      parent_span: SpanProtocol | WrappedSpan | None = None) -> None:
        p_span: SpanProtocol | None
        if isinstance(parent_span, WrappedSpan):
            p_span = parent_span.request_span
        else:
            p_span = parent_span

        core_span_start_time = core_span['start']
        latest_start_time = self._start_time if self._start_time > core_span_start_time else core_span_start_time
        new_span = WrappedSpan(self._service_type,
                               core_span['name'],
                               self._wrapped_tracer,
                               collection_details=self._collection_details,
                               parent_span=p_span,
                               start_time=latest_start_time)
        children = core_span.get('children', None)
        if children:
            self._build_core_spans(children, parent_span=new_span)

        for attr_name, attr_val in (core_span.get('attributes') or {}).items():
            new_span.set_attribute(attr_name, attr_val)

        core_span_end_time = core_span['end']
        end_watermark = self._end_time_watermark
        self._end_time_watermark = end_watermark if end_watermark > core_span_end_time else core_span_end_time
        new_span.end(core_span_end_time)

    def _create_request_span(self,
                             name: str,
                             parent_span: SpanProtocol | None = None,
                             start_time: int | None = None) -> RequestSpanProtocol:
        # The tracer's is_legacy decides which protocol both the tracer and its spans follow.  A legacy
        # span is typed as a request span here and cast back where only a legacy tracer reaches it.
        if self._wrapped_tracer.is_legacy:
            legacy_tracer = cast(LegacyTracerProtocol, self._wrapped_tracer.tracer)
            legacy_span = legacy_tracer.start_span(name, parent=cast('LegacySpanProtocol | None', parent_span))
            return cast(RequestSpanProtocol, legacy_span)
        else:
            tracer = cast(RequestTracerProtocol, self._wrapped_tracer.tracer)
            return tracer.request_span(name,
                                       parent_span=cast('RequestSpanProtocol | None', parent_span),
                                       start_time=start_time)

    def _end_encoding_spans(self) -> None:
        if self._encoding_spans_ended:
            return
        self._encoding_spans_ended = True
        if isinstance(self._encoding_spans, list):
            for span in self._encoding_spans:
                span.span.end(span.end_time)
        elif self._encoding_spans is not None:
            self._encoding_spans.span.end(self._encoding_spans.end_time)

    def _set_attribute_on_all_spans(self,
                                    key: str,
                                    value: SpanAttributeValue,
                                    skip_encoding_span: bool | None = None) -> None:
        self.set_attribute(key, value)
        if self._wrapped_parent is not None:
            self._wrapped_parent.set_attribute(key, value)

        # For multi-mutation ops (insert/upsert/replace multi), process_core_span is called once per result
        # in the batch. The first call ends all encoding spans (setting _encoding_spans_ended=True) and
        # propagates cluster labels to them at that point. Subsequent calls have nothing left to update on
        # the already-ended encoding spans, so skip the iteration to avoid O(N²) attribute propagation.
        if skip_encoding_span is True or self._encoding_spans_ended:
            return

        if isinstance(self._encoding_spans, list):
            for span in self._encoding_spans:
                span.span.set_attribute(key, value)
        elif self._encoding_spans is not None:
            self._encoding_spans.span.set_attribute(key, value)

    def _set_span_attrs(self, **options: Unpack[OpAttributeOptions]) -> None:
        if self._service_type is ServiceType.KeyValue:
            attrs = get_attributes_for_kv_op(self._op_name_str,
                                             cast(KeyspaceDetails, options.get('collection_details', {})))
        else:
            attrs = get_attributes_for_http_op(self._op_name_str, self._service_type, **options)

        for k, v in attrs.items():
            self.set_attribute(k, v)

    @property
    def name(self) -> str:
        if not self._wrapped_tracer.is_legacy:
            return self._request_span.name
        return self._op_name_str

    @property
    def legacy_request_span(self) -> LegacySpanProtocol | None:
        if self._wrapped_tracer.is_legacy:
            return cast(LegacySpanProtocol, self._request_span)
        return None
