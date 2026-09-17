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

"""Assertions over what a configured meter recorded.

A metrics suite is three steps: clear the meter, run one operation, assert on the single value
recorder that operation produced.  `KeyValueMeterValidator` owns the first and third, and the
facts that do not change between operations (which collection, whether the server reports
cluster labels) are handed to it once at construction.

**Stateless between the two calls.**  `reset()` only clears; every expectation is an argument
to the assertion that reads it.  A validator that instead took the operation name on `reset()`
and read it back in a no-argument `validate_*` would put the expectation three lines above the
assertion with the operation under test in between, and nothing would fail when a test reset
for one operation and ran another.

Api-agnostic on purpose: it reads a test meter, never a cluster, so one copy serves the
generated acouchbase and txcouchbase suites as well as the sync ones.

Two validators, split the way the SDK labels operations rather than the way the suites are
filed.  `KeyValueMeterValidator` covers everything reaching the kv service: single operations,
the per-key recorders a multi operation produces, and the key-value sequence a datastructure
operation turns into.  `HttpMeterValidator` covers everything reaching a service over http,
where what varies is which service and how much of the bucket/scope/collection triple applies.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import (Any,
                    Optional,
                    Union)

from couchbase.logic.observability import (CollectionDetails,
                                           OpAttributeName,
                                           OpName)
from couchbase.logic.observability.no_op import NoOpMeter
from tests_v2.observability_ops import service_for
from tests_v2.observability_test_impls import (InertMeter,
                                               RecordingMeter,
                                               RecordingValueRecorder,
                                               TestMeterType)


def _describe(meter: TestMeterType, op_name: Optional[OpName] = None) -> str:
    """Context for an assertion message: what was asked for, and what the meter actually holds."""
    parts = []
    if op_name is not None:
        parts.append(f'op={op_name.value!r}')
    held = {name: [r.op_name for r in recorders] for name, recorders in meter.recorders.items()}
    parts.append(f'meter={held}')
    return ' | '.join(parts)


def _expect(attrs: Any, attribute: OpAttributeName, expected: Union[str, int], ctx: str) -> None:
    actual = attrs.get(attribute.value)
    assert actual == expected, (
        f'{attribute.value}: expected {expected!r}, got {actual!r}. {ctx}'
    )


class KeyValueMeterValidator:
    """Asserts on the recorder a single key-value operation produced.

    Constructed through `MetricsEnvironment.kv_meter_validator(cb_env)`, which is what knows
    where the meter and the collection details live.
    """

    def __init__(self,
                 meter: TestMeterType,
                 collection_details: CollectionDetails,
                 supports_cluster_labels: bool) -> None:
        self._meter = meter
        self._collection_details = collection_details
        self._supports_cluster_labels = supports_cluster_labels

    def reset(self) -> None:
        self._meter.clear()

    def validate_kv_op(self,
                       op_name: OpName,
                       validate_error: bool = False,
                       expect_collection_labels: bool = True) -> None:
        """Assert on the one recorder the operation under test produced.

        `expect_collection_labels=False` is for an operation the client refuses before it
        builds the request span.  The SDK populates the bucket, scope and collection
        attributes at the same point it creates that span, so an operation that never got
        there is still measured but carries none of them.  Absence is asserted rather than
        waived: those three attributes appearing on a pre-dispatch failure would mean the
        recording moved, which is a change worth failing on.
        """
        if isinstance(self._meter, NoOpMeter):
            self._validate_no_op(op_name)
            return
        self._validate_recorded(op_name, validate_error, expect_collection_labels)

    def validate_multi_kv_op(self,
                             multi_op_name: OpName,
                             sub_op_name: OpName,
                             expected_count: int,
                             validate_error: bool = False) -> None:
        """Assert on the recorders a multi-key operation produced, one per key.

        A multi operation is measured per key and never as a whole: the SDK records the
        `get`, not the `get_multi`, so `multi_op_name` is what must be *absent* while
        `sub_op_name` is what every recorder must carry.

        `expected_count=0` is the pre-dispatch case.  A multi operation the client refuses
        before it reaches the bindings never gets as far as the per-key work, so nothing is
        recorded at all, and asking for zero says that rather than passing vacuously.

        **Every recorder is checked, by position.**  The obvious alternative, a list of
        expected operations looked up by name, cannot work here: the list is `[OpName.Get] *
        len(keys)` and a lookup returns the first match, so three identical entries re-read one
        recorder.  Only a count assertion would know the other two existed, and nothing at all
        would look at their attributes.
        """
        if isinstance(self._meter, NoOpMeter):
            self._validate_no_op(multi_op_name)
            return

        assert isinstance(self._meter, RecordingMeter), (
            f'recording validation needs a RecordingMeter, got {type(self._meter).__name__!r}'
        )
        ctx = _describe(self._meter, multi_op_name)

        if expected_count == 0:
            assert not self._meter.recorders, (
                f'an operation refused before dispatch was still measured. {ctx}'
            )
            return

        names = list(self._meter.recorders)
        assert names == [OpAttributeName.MeterOperationDuration.value], (
            f'expected exactly the operation-duration metric, got {names}. {ctx}'
        )

        recorders = self._meter.recorders[OpAttributeName.MeterOperationDuration.value]
        assert len(recorders) == expected_count, (
            f'expected one recorder per key, {expected_count} in all, got {len(recorders)}. {ctx}'
        )

        for recorder in recorders:
            assert recorder.op_name != multi_op_name.value, (
                f'the multi operation was measured as a whole as well as per key. {ctx}'
            )
            assert recorder.values, f'recorder was created but nothing was recorded into it. {ctx}'
            self._validate_attributes(recorder, sub_op_name, validate_error, True, ctx)

    def validate_ds_op(self,
                       ds_op_name: OpName,
                       sub_ops: Sequence[tuple[OpName, bool]]) -> None:
        """Assert on the recorders a datastructure operation produced, in order.

        A datastructure operation is not measured under its own name: `cb_list.append(...)`
        is a `mutate_in`, or on a document that does not exist yet a `mutate_in` that fails,
        an `insert` and a second `mutate_in`.  What the SDK records is that sequence, so
        `ds_op_name` is what must be absent and `sub_ops` is what must be present, in the
        order the operation performed them, each paired with whether it was expected to fail.

        **The sequence is compared as a whole rather than walked by index.**  Reading the
        nth recorder for the nth expected operation never compares the lengths, so a
        datastructure operation that grows an extra round trip goes on passing: the recorders
        past the end of the list are simply never looked at.
        """
        if isinstance(self._meter, NoOpMeter):
            self._validate_no_op(ds_op_name)
            return

        assert isinstance(self._meter, RecordingMeter), (
            f'recording validation needs a RecordingMeter, got {type(self._meter).__name__!r}'
        )
        ctx = _describe(self._meter, ds_op_name)

        names = list(self._meter.recorders)
        assert names == [OpAttributeName.MeterOperationDuration.value], (
            f'expected exactly the operation-duration metric, got {names}. {ctx}'
        )

        recorders = self._meter.recorders[OpAttributeName.MeterOperationDuration.value]
        recorded = [r.op_name for r in recorders]
        expected = [op.value for op, _ in sub_ops]
        assert recorded == expected, (
            f'the operation performed {recorded}, expected {expected}. {ctx}'
        )

        for recorder, (sub_op, with_error) in zip(recorders, sub_ops):
            assert recorder.op_name != ds_op_name.value, (
                f'the datastructure operation was measured under its own name. {ctx}'
            )
            assert recorder.values, f'recorder was created but nothing was recorded into it. {ctx}'
            self._validate_attributes(recorder, sub_op, with_error, True, ctx)

    # ------------------------------------------------------------------
    # The two halves of the parametrization
    # ------------------------------------------------------------------

    def _validate_no_op(self, op_name: OpName) -> None:
        """A no-op meter must be handed the operation and then never asked to record it.

        `ObservableRequestHandler` caches an `ObservableRequestHandlerNoOpMeterImpl` whose
        `process_end` does nothing, so no recorder is ever requested.  `InertMeter` would have
        kept one if asked, which is what makes an empty `recorders` a claim about the SDK.
        """
        assert isinstance(self._meter, InertMeter), (
            f'no-op validation needs a meter that would record if asked, '
            f'got {type(self._meter).__name__!r}'
        )
        assert not self._meter.recorders, (
            f'a no-op meter was asked for a value recorder. {_describe(self._meter, op_name)}'
        )

    def _validate_recorded(self,
                           op_name: OpName,
                           validate_error: bool,
                           expect_collection_labels: bool) -> None:
        assert isinstance(self._meter, RecordingMeter), (
            f'recording validation needs a RecordingMeter, got {type(self._meter).__name__!r}'
        )
        ctx = _describe(self._meter, op_name)

        names = list(self._meter.recorders)
        assert names == [OpAttributeName.MeterOperationDuration.value], (
            f'expected exactly the operation-duration metric, got {names}. {ctx}'
        )

        recorder = self._meter.get_value_recorder_by_op_name(op_name)
        assert recorder is not None, f'no value recorder for the operation under test. {ctx}'

        assert recorder.values, f'recorder was created but nothing was recorded into it. {ctx}'

        self._validate_attributes(recorder, op_name, validate_error, expect_collection_labels, ctx)

    # ------------------------------------------------------------------
    # Attribute checks
    # ------------------------------------------------------------------

    def _validate_attributes(self,
                             recorder: RecordingValueRecorder,
                             op_name: OpName,
                             validate_error: bool,
                             expect_collection_labels: bool,
                             ctx: str) -> None:
        attrs = recorder.attributes
        assert isinstance(attrs, dict), f'attributes: expected a dict, got {type(attrs).__name__!r}. {ctx}'

        _expect(attrs, OpAttributeName.SystemName, 'couchbase', ctx)
        _expect(attrs, OpAttributeName.ReservedUnit, OpAttributeName.ReservedUnitSeconds.value, ctx)
        _expect(attrs, OpAttributeName.OperationName, op_name.value, ctx)
        _expect(attrs, OpAttributeName.Service, 'kv', ctx)

        if expect_collection_labels:
            _expect(attrs, OpAttributeName.BucketName, self._collection_details['bucket_name'], ctx)
            _expect(attrs, OpAttributeName.ScopeName, self._collection_details['scope_name'], ctx)
            _expect(attrs, OpAttributeName.CollectionName, self._collection_details['collection_name'], ctx)
        else:
            for label in (OpAttributeName.BucketName,
                          OpAttributeName.ScopeName,
                          OpAttributeName.CollectionName):
                assert label.value not in attrs, (
                    f'an operation that failed before dispatch still carried '
                    f'{label.value}={attrs[label.value]!r}. {ctx}'
                )

        assert recorder.name == OpAttributeName.MeterOperationDuration.value, (
            f'recorder.name: expected {OpAttributeName.MeterOperationDuration.value!r}, '
            f'got {recorder.name!r}. {ctx}'
        )

        if self._supports_cluster_labels:
            for attribute in (OpAttributeName.ClusterName, OpAttributeName.ClusterUUID):
                value = attrs.get(attribute.value)
                assert isinstance(value, str), (
                    f'{attribute.value}: expected a str, got {value!r}. {ctx}'
                )

        if validate_error:
            error_type = attrs.get(OpAttributeName.ErrorType.value)
            assert isinstance(error_type, str), (
                f'{OpAttributeName.ErrorType.value}: expected a str naming the failure, '
                f'got {error_type!r}. {ctx}'
            )
        else:
            assert OpAttributeName.ErrorType.value not in attrs, (
                f'a successful operation carried {OpAttributeName.ErrorType.value}='
                f'{attrs[OpAttributeName.ErrorType.value]!r}. {ctx}'
            )


class HttpMeterValidator:
    """Asserts on the recorder a single http operation produced.

    Constructed through `MetricsEnvironment.http_meter_validator(cb_env)`.

    Where the key-value validator always expects the same bucket, scope and collection, an http
    operation carries only as much of that triple as applies to it: a cluster-level query
    carries none of it, a scope-level query carries two, a view query carries the bucket alone.
    So the three are arguments here, and each one not given is asserted *absent* rather than
    ignored.  Attaching a scope to a cluster-level query would be a real defect and silence is
    the only way it could reach a user.
    """

    def __init__(self,
                 meter: TestMeterType,
                 supports_cluster_labels: bool) -> None:
        self._meter = meter
        self._supports_cluster_labels = supports_cluster_labels

    def reset(self) -> None:
        self._meter.clear()

    def validate_http_op(self,
                         op_name: OpName,
                         validate_error: bool = False,
                         bucket_name: Optional[str] = None,
                         scope_name: Optional[str] = None,
                         collection_name: Optional[str] = None) -> None:
        if isinstance(self._meter, NoOpMeter):
            assert isinstance(self._meter, InertMeter), (
                f'no-op validation needs a meter that would record if asked, '
                f'got {type(self._meter).__name__!r}'
            )
            assert not self._meter.recorders, (
                f'a no-op meter was asked for a value recorder. {_describe(self._meter, op_name)}'
            )
            return

        assert isinstance(self._meter, RecordingMeter), (
            f'recording validation needs a RecordingMeter, got {type(self._meter).__name__!r}'
        )
        ctx = _describe(self._meter, op_name)

        names = list(self._meter.recorders)
        assert names == [OpAttributeName.MeterOperationDuration.value], (
            f'expected exactly the operation-duration metric, got {names}. {ctx}'
        )

        recorder = self._meter.get_value_recorder_by_op_name(op_name)
        assert recorder is not None, f'no value recorder for the operation under test. {ctx}'
        assert recorder.values, f'recorder was created but nothing was recorded into it. {ctx}'

        attrs = recorder.attributes
        assert isinstance(attrs, dict), f'attributes: expected a dict, got {type(attrs).__name__!r}. {ctx}'

        _expect(attrs, OpAttributeName.SystemName, 'couchbase', ctx)
        _expect(attrs, OpAttributeName.ReservedUnit, OpAttributeName.ReservedUnitSeconds.value, ctx)
        _expect(attrs, OpAttributeName.OperationName, op_name.value, ctx)
        _expect(attrs, OpAttributeName.Service, service_for(op_name).value, ctx)

        for attribute, expected in ((OpAttributeName.BucketName, bucket_name),
                                    (OpAttributeName.ScopeName, scope_name),
                                    (OpAttributeName.CollectionName, collection_name)):
            if expected is None:
                assert attribute.value not in attrs, (
                    f'{attribute.value}: expected to be absent, got {attrs[attribute.value]!r}. {ctx}'
                )
            else:
                _expect(attrs, attribute, expected, ctx)

        if self._supports_cluster_labels:
            for attribute in (OpAttributeName.ClusterName, OpAttributeName.ClusterUUID):
                value = attrs.get(attribute.value)
                assert isinstance(value, str), (
                    f'{attribute.value}: expected a str, got {value!r}. {ctx}'
                )

        if validate_error:
            error_type = attrs.get(OpAttributeName.ErrorType.value)
            assert isinstance(error_type, str), (
                f'{OpAttributeName.ErrorType.value}: expected a str naming the failure, '
                f'got {error_type!r}. {ctx}'
            )
        else:
            assert OpAttributeName.ErrorType.value not in attrs, (
                f'a successful operation carried {OpAttributeName.ErrorType.value}='
                f'{attrs[OpAttributeName.ErrorType.value]!r}. {ctx}'
            )
