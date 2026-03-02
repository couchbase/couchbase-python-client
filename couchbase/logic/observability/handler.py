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

import sys
import time
from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    List,
                    Mapping,
                    Optional,
                    Tuple,
                    TypedDict,
                    Union)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability.no_op import NoOpTracer
from couchbase.logic.observability.observability_types import (CppOpAttributeName,
                                                               LegacySpanProtocol,
                                                               ObservabilityInstruments,
                                                               OpAttributeName,
                                                               OpName,
                                                               OpType,
                                                               RequestSpanProtocol,
                                                               ServiceType,
                                                               SpanProtocol,
                                                               WrappedTracer)
from couchbase.logic.supportability import Supportability
from couchbase.observability.tracing import SpanAttributeValue, SpanStatusCode

if TYPE_CHECKING:
    from couchbase.logic.pycbc_core.binding_cpp_types import CppWrapperSdkChildSpan, CppWrapperSdkSpan


class CollectionDetails(TypedDict):
    bucket_name: str
    scope_name: str
    collection_name: str


@dataclass
class WrappedEncodingSpan:
    span: RequestSpanProtocol
    end_time: int


class OpAttributeOptions(TypedDict, total=False):
    collection_details: CollectionDetails
    statement: Optional[str]
    query_params: Optional[Dict[str, Any]]
    parent_span: Optional[Union[SpanProtocol, WrappedSpan]]
    bucket_name: Optional[str]
    scope_name: Optional[str]
    collection_name: Optional[str]
    use_now_as_start_time: Optional[bool]


def get_attributes_for_kv_op(op_name: str,
                             collection_details: CollectionDetails) -> Mapping[str, str]:
    return {
        OpAttributeName.SystemName.value: 'couchbase',
        OpAttributeName.Service.value: ServiceType.KeyValue.value,
        OpAttributeName.OperationName.value: op_name,
        OpAttributeName.BucketName.value: collection_details['bucket_name'],
        OpAttributeName.ScopeName.value: collection_details['scope_name'],
        OpAttributeName.CollectionName.value: collection_details['collection_name'],
    }


def get_attributes_for_http_op(op_name: str,
                               service_type: ServiceType,
                               **options: Unpack[OpAttributeOptions]) -> Mapping[str, str]:
    attrs = {
        OpAttributeName.SystemName.value: 'couchbase',
        OpAttributeName.Service.value: service_type.value,
        OpAttributeName.OperationName.value: op_name,
    }

    statement = options.get('statement', None)
    query_params = options.get('query_params', None)

    if (statement is not None and query_params
            and ('positional_parameters' in query_params or 'named_parameters' in query_params)):
        attrs[OpAttributeName.QueryStatement.value] = statement

    bucket_name = options.get('bucket_name', None)
    if bucket_name:
        attrs[OpAttributeName.BucketName.value] = bucket_name

    scope_name = options.get('scope_name', None)
    if scope_name:
        attrs[OpAttributeName.ScopeName.value] = scope_name

    collection_name = options.get('collection_name', None)
    if collection_name:
        attrs[OpAttributeName.CollectionName.value] = collection_name

    return attrs


class ObservableRequestHandler:

    def __init__(self,
                 op_type: OpType,
                 observability_instruments: ObservabilityInstruments,
                 op_type_toggle: Optional[bool] = None) -> None:
        self._op_type = op_type
        if isinstance(observability_instruments.tracer.tracer, NoOpTracer):
            self._tracer_impl = ObservableRequestHandlerNoOpTracerImpl(op_type, observability_instruments)
        else:
            self._tracer_impl = ObservableRequestHandlerTracerImpl(op_type,
                                                                   observability_instruments,
                                                                   op_type_toggle=op_type_toggle)

    @property
    def is_legacy_tracer(self) -> bool:
        return self._tracer_impl.is_legacy

    @property
    def legacy_request_span(self) -> Optional[LegacySpanProtocol]:
        return self._tracer_impl.legacy_request_span

    @property
    def op_type(self) -> OpType:
        return self._op_type

    @property
    def wrapper_span_name(self) -> str:
        return self._tracer_impl.wrapper_span_name

    @property
    def wrapped_span(self) -> Optional[WrappedSpan]:
        return self._tracer_impl.wrapped_span

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        self._tracer_impl.add_kv_durability_attribute(durability)

    def create_kv_span(self,
                       collection_details: CollectionDetails,
                       parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        self._tracer_impl.create_kv_span(collection_details, parent_span=parent_span)

    def create_kv_multi_span(self,
                             collection_details: CollectionDetails,
                             parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        self._tracer_impl.create_kv_span(collection_details, parent_span=parent_span)

    def create_http_span(self, **options: Unpack[OpAttributeOptions]) -> None:
        self._tracer_impl.create_http_span(**options)

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        return self._tracer_impl.maybe_add_encoding_span(encoding_fn)

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        return self._tracer_impl.maybe_create_encoding_span(encoding_fn)

    def process_core_span(self,
                          core_span: Optional[CppWrapperSdkSpan] = None,
                          with_error: Optional[bool] = False) -> None:
        self._tracer_impl.process_core_span(core_span=core_span, with_error=with_error)

    def reset(self, op_type: OpType, with_error: Optional[bool] = False) -> None:
        self._tracer_impl.reset(op_type, with_error=with_error)

    @staticmethod
    def get_query_context_components(query_context: str,
                                     is_analytics: Optional[bool] = False) -> Optional[Tuple[str, str]]:
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
    def maybe_get_parent_span(span: Optional[SpanProtocol] = None,
                              parent_span: Optional[SpanProtocol] = None) -> Optional[SpanProtocol]:

        final_span = parent_span
        if not final_span and span:
            Supportability.option_deprecated('span', 'parent_span')
            final_span = span

        if final_span:
            # SpanProtocol = Union[LegacySpanProtocol, RequestSpanProtocol]
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

    def __exit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        self._tracer_impl.process_end(with_error=exc_type is not None)
        return False

    # --- Async Context Manager Protocol ---
    async def __aenter__(self) -> ObservableRequestHandler:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        # Delegate the teardown and exception handling to the _impl
        return self.__exit__(exc_type, exc_val, exc_tb)


class ObservableRequestHandlerNoOpTracerImpl:
    def __init__(self,
                 op_type: OpType,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._op_type = op_type
        self._wrapped_tracer = observability_instruments.tracer

    @property
    def cluster_name(self) -> Optional[str]:
        return None

    @property
    def cluster_uuid(self) -> Optional[str]:
        return None

    @property
    def is_legacy(self) -> bool:
        return self._wrapped_tracer.is_legacy

    @property
    def legacy_request_span(self) -> Optional[LegacySpanProtocol]:
        return None

    @property
    def wrapper_span_name(self) -> str:
        return ''

    @property
    def wrapped_span(self) -> Optional[WrappedSpan]:
        return None

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        pass

    def create_kv_span(self,
                       collection_details: CollectionDetails,
                       parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        pass

    def create_kv_multi_span(self,
                             collection_details: CollectionDetails,
                             parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        pass

    def create_http_span(self, **options: Unpack[OpAttributeOptions]) -> None:
        pass

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        return encoding_fn()

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        return encoding_fn()

    def process_end(self, with_error: Optional[bool] = False) -> None:
        pass

    def process_core_span(self,
                          core_span: Optional[CppWrapperSdkSpan] = None,
                          with_error: Optional[bool] = False) -> None:
        pass

    def reset(self, op_type: OpType, with_error: Optional[bool] = False) -> None:
        pass


class ObservableRequestHandlerTracerImpl:

    def __init__(self,
                 op_type: OpType,
                 observability_instruments: ObservabilityInstruments,
                 op_type_toggle: Optional[bool] = None) -> None:
        self._start_time = time.time_ns()
        self._op_type = op_type
        self._op_name = OpName.from_op_type(self._op_type, toggle=op_type_toggle)
        self._service_type = ServiceType.from_op_type(self._op_type)
        self._wrapped_tracer = observability_instruments.tracer
        self._get_cluster_labels_fn = observability_instruments.get_cluster_labels_fn
        self._wrapped_span: Optional[WrappedSpan] = None
        self._end_time: Optional[int] = None
        self._processed_core_span = False

    @property
    def cluster_name(self) -> Optional[str]:
        return self._wrapped_span.cluster_name if self._wrapped_span else None

    @property
    def cluster_uuid(self) -> Optional[str]:
        return self._wrapped_span.cluster_uuid if self._wrapped_span else None

    @property
    def is_legacy(self) -> bool:
        return self._wrapped_tracer.is_legacy

    @property
    def legacy_request_span(self) -> Optional[LegacySpanProtocol]:
        if self._wrapped_span:
            return self._wrapped_span.legacy_request_span
        return None

    @property
    def wrapper_span_name(self) -> str:
        return self._wrapped_span.name if self._wrapped_span is not None else ''

    @property
    def wrapped_span(self) -> Optional[WrappedSpan]:
        if self._wrapped_span:
            return self._wrapped_span

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        if self._wrapped_span:
            self._wrapped_span.add_kv_durability_attribute(durability)

    def create_kv_span(self,
                       collection_details: CollectionDetails,
                       parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        self._create_wrapped_span(collection_details=collection_details, parent_span=parent_span)

    def create_kv_multi_span(self,
                             collection_details: CollectionDetails,
                             parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        self._create_wrapped_span(collection_details=collection_details, parent_span=parent_span)

    def create_http_span(self, **options: Unpack[OpAttributeOptions]) -> None:
        self._create_wrapped_span(**options)

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        if not self._wrapped_span or self.is_legacy:
            return encoding_fn()

        return self._wrapped_span.maybe_add_encoding_span(encoding_fn)

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        if not self._wrapped_span or self.is_legacy:
            return encoding_fn()

        return self._wrapped_span.maybe_create_encoding_span(encoding_fn)

    def process_core_span(self,
                          core_span: Optional[CppWrapperSdkSpan] = None,
                          with_error: Optional[bool] = False) -> None:
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
        if self._op_name.is_streaming_op():
            self.process_end(with_error=with_error)

    def process_end(self, with_error: Optional[bool] = False) -> None:
        self._end_time = time.time_ns()

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

    def reset(self, op_type: OpType, with_error: Optional[bool] = False) -> None:
        self.process_end(with_error=with_error)
        self._start_time = time.time_ns()
        self._op_type = op_type
        self._op_name = OpName.from_op_type(self._op_type)
        self._service_type = ServiceType.from_op_type(self._op_type)
        self._wrapped_span: Optional[WrappedSpan] = None
        self._end_time: Optional[int] = None

    def _create_wrapped_span(self,
                             **options: Unpack[OpAttributeOptions]) -> None:
        use_now_as_start_time = options.get('use_now_as_start_time', False)
        if use_now_as_start_time is True:
            start_time = None
        else:
            start_time = self._start_time
        self._wrapped_span = WrappedSpan(self._service_type,
                                         self._op_name,
                                         self._wrapped_tracer,
                                         start_time=start_time,
                                         **options)


class WrappedSpan:

    def __init__(self,
                 service_type: ServiceType,
                 op_name: OpName,
                 wrapped_tracer: WrappedTracer,
                 start_time: Optional[int] = None,
                 **options: Unpack[OpAttributeOptions]) -> None:
        self._service_type = service_type
        self._op_name = op_name
        self._wrapped_tracer = wrapped_tracer
        self._collection_details = options.get('collection_details', None)
        # The request_span should _only_ take a SpanProtocol for the parent
        self._parent_span = options.get('parent_span', None)
        if isinstance(self._parent_span, WrappedSpan):
            p_span = self._parent_span.request_span
        else:
            p_span = self._parent_span
        self._request_span = self._create_request_span(self._op_name.value,
                                                       parent_span=p_span,
                                                       start_time=start_time)
        self._has_multiple_encoding_spans = (self._op_name.is_multi_op() or self._op_name is OpName.MutateIn)
        self._encoding_spans_ended = False
        self._encoding_spans: Optional[Union[WrappedEncodingSpan, List[WrappedEncodingSpan]]] = None
        self._set_span_attrs(**options)
        self._cluster_name: Optional[str] = None
        self._cluster_uuid: Optional[str] = None

    @property
    def cluster_name(self) -> Optional[str]:
        return self._cluster_name

    @property
    def cluster_uuid(self) -> Optional[str]:
        return self._cluster_uuid

    @property
    def request_span(self) -> SpanProtocol:
        return self._request_span

    def add_kv_durability_attribute(self, durability: DurabilityLevel) -> None:
        self._request_span.set_attribute(OpAttributeName.DurabilityLevel.value,
                                         DurabilityLevel.to_server_str(durability))

    def maybe_add_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        # legacy operations did not create an encoding span; not support now
        # we only expect certain ops to have multiple encoding spans
        if self._wrapped_tracer.is_legacy or not self._has_multiple_encoding_spans:
            return encoding_fn()

        if not self._encoding_spans:
            self._encoding_spans = []

        encoding_span = self._wrapped_tracer.tracer.request_span(OpAttributeName.EncodingSpanName.value,
                                                                 self._request_span)
        encoding_span.set_attribute(OpAttributeName.SystemName.value, 'couchbase')
        try:
            encoded_output = encoding_fn()
            return encoded_output
        except Exception:
            encoding_span.set_status(SpanStatusCode.ERROR)
            raise
        finally:
            # we wait to set the end time until we process the underylying
            # core span so that we can add the cluster_[name|uuid] attributes
            self._encoding_spans.append(WrappedEncodingSpan(encoding_span, time.time_ns()))

    def maybe_create_encoding_span(self, encoding_fn: Callable[..., Tuple[bytes, int]]) -> Tuple[bytes, int]:
        # legacy operations did not create an encoding span; not support now
        # if the op is expected to have multiple encoding spans, maybe_add_encoding_span() should be used instead
        if self._wrapped_tracer.is_legacy or self._has_multiple_encoding_spans:
            return encoding_fn()

        encoding_span = self._wrapped_tracer.tracer.request_span(OpAttributeName.EncodingSpanName.value,
                                                                 self._request_span)
        encoding_span.set_attribute(OpAttributeName.SystemName.value, 'couchbase')
        try:
            encoded_output = encoding_fn()
            return encoded_output
        except Exception:
            encoding_span.set_status(SpanStatusCode.ERROR)
            raise
        finally:
            # we wait to set the end time until we process the underylying
            # core span so that we can add the cluster_[name|uuid] attributes
            self._encoding_spans = WrappedEncodingSpan(encoding_span, time.time_ns())

    def process_core_span(self, core_span: CppWrapperSdkSpan) -> None:
        self._maybe_set_attribute_from_core_span(core_span, CppOpAttributeName.ClusterName)
        self._maybe_set_attribute_from_core_span(core_span, CppOpAttributeName.ClusterUUID)
        self._maybe_set_attribute_from_core_span(core_span, CppOpAttributeName.RetryCount, skip_encoding_span=True)
        # now that we have the cluster_[name|uuid] attributes from the core span, we can end the encoding span(s)
        self._end_encoding_spans()
        children = core_span.get('children', None)
        if not children:
            return

        self._build_core_spans(children, parent_span=self)

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        if key == OpAttributeName.ClusterName.value:
            self._cluster_name = value
        elif key == OpAttributeName.ClusterUUID.value:
            self._cluster_uuid = value
        self._request_span.set_attribute(key, value)

    def set_cluster_labels(self, cluster_name: Optional[str] = None, cluster_uuid: Optional[str] = None) -> None:
        if cluster_name:
            self._set_attribute_on_all_spans(OpAttributeName.ClusterName.value, cluster_name)
        if cluster_uuid:
            self._set_attribute_on_all_spans(OpAttributeName.ClusterUUID.value, cluster_uuid)

    def set_retry_attribute(self, retry_count: int = 0) -> None:
        self._set_attribute_on_all_spans(OpAttributeName.RetryCount.value, retry_count, skip_encoding_span=True)

    def set_status(self, status: SpanStatusCode) -> None:
        self._status = status
        self._request_span.set_status(status)

    def end(self, end_time: Optional[int]) -> None:
        self._end_encoding_spans()
        if self._wrapped_tracer.is_legacy:
            self._request_span.finish()
        else:
            self._request_span.end(end_time)

    def _build_core_spans(self,
                          core_spans: List[CppWrapperSdkChildSpan],
                          parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        for span in core_spans:
            if span['name'] == OpAttributeName.DispatchSpanName.value:
                self._build_dispatch_core_span(span, parent_span=parent_span)
            else:
                self._build_non_dispatch_core_span(span, parent_span=parent_span)

    def _build_dispatch_core_span(self,
                                  core_span: CppWrapperSdkChildSpan,
                                  parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        # TODO: handle parent span as WrappedSpan
        if isinstance(parent_span, WrappedSpan):
            p_span = parent_span.request_span
        else:
            p_span = parent_span
        new_span = self._create_request_span(core_span['name'], parent_span=p_span, start_time=core_span['start'])
        children = core_span.get('children', None)
        if children:
            self._build_core_spans(children, parent_span=new_span)

        for attr_name, attr_val in core_span.get('attributes', {}).items():
            new_span.set_attribute(attr_name, attr_val)

        new_span.end(core_span['end'])

    def _build_non_dispatch_core_span(self,
                                      core_span: CppWrapperSdkChildSpan,
                                      parent_span: Optional[Union[SpanProtocol, WrappedSpan]] = None) -> None:
        # TODO: handle parent span as WrappedSpan
        if isinstance(parent_span, WrappedSpan):
            p_span = parent_span.request_span
        else:
            p_span = parent_span

        new_span = WrappedSpan(self._service_type,
                               OpName(core_span['name']),
                               self._wrapped_tracer,
                               collection_details=self._collection_details,
                               parent_span=p_span,
                               start_time=core_span['start'])
        children = core_span.get('children', None)
        if children:
            self._build_core_spans(children, parent_span=new_span)

        for attr_name, attr_val in core_span.get('attributes', {}).items():
            new_span.set_attribute(attr_name, attr_val)

        new_span.end(core_span['end'])

    def _create_request_span(self,
                             name: str,
                             parent_span: Optional[SpanProtocol] = None,
                             start_time: Optional[int] = None) -> SpanProtocol:
        # the request span should only be a SpanProtocol
        if self._wrapped_tracer.is_legacy:
            return self._wrapped_tracer.tracer.start_span(name, parent=parent_span)
        else:
            return self._wrapped_tracer.tracer.request_span(name,
                                                            parent_span=parent_span,
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

    def _maybe_set_attribute_from_core_span(self,
                                            core_span: CppWrapperSdkSpan,
                                            attr_name: Union[CppOpAttributeName, OpAttributeName],
                                            skip_encoding_span: Optional[bool] = None) -> None:
        attr_val = None
        core_span_attr = core_span.get('attributes', {}).get(attr_name.value, None)
        if core_span_attr:
            attr_val = core_span_attr

        if attr_val is None and attr_name.value == 'retries':
            attr_val = 0

        if attr_val is not None:
            if isinstance(attr_name, CppOpAttributeName):
                filtered_attr_name = OpAttributeName[attr_name.name]
            else:
                filtered_attr_name = attr_name
            self._set_attribute_on_all_spans(filtered_attr_name.value,
                                             attr_val,
                                             skip_encoding_span=skip_encoding_span)

    def _set_attribute_on_all_spans(self,
                                    key: str,
                                    value: SpanAttributeValue,
                                    skip_encoding_span: Optional[bool] = None) -> None:
        self.set_attribute(key, value)
        if isinstance(self._parent_span, WrappedSpan):
            self._parent_span.set_attribute(key, value)

        if skip_encoding_span is True:
            return

        if isinstance(self._encoding_spans, list):
            for span in self._encoding_spans:
                span.span.set_attribute(key, value)
        elif self._encoding_spans is not None:
            self._encoding_spans.span.set_attribute(key, value)

    def _set_span_attrs(self, **options: Unpack[OpAttributeOptions]) -> None:
        if self._service_type.is_key_value_service_type():
            attrs = get_attributes_for_kv_op(self._op_name.value, options.get('collection_details', {}))
        elif self._service_type.is_http_service_type():
            attrs = get_attributes_for_http_op(self._op_name.value, self._service_type, **options)

        for k, v in attrs.items():
            self.set_attribute(k, v)

    @property
    def name(self) -> str:
        if not self._wrapped_tracer.is_legacy:
            return self._request_span.name
        return self._op_name.value

    @property
    def legacy_request_span(self) -> Optional[LegacySpanProtocol]:
        if self._wrapped_tracer.is_legacy:
            return self._request_span
        return None
