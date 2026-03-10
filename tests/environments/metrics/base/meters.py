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

from typing import (Mapping,
                    Optional,
                    Union)

from couchbase.logic.observability import (NoOpMeter,
                                           OpAttributeName,
                                           OpName)

from .value_recorders import NoOpTestValueRecorder, TestValueRecorder


class NoOpTestMeter(NoOpMeter):
    def __init__(self) -> None:
        super().__init__()
        self.recorders = {}

    def valueRecorder(self, name: str, tags: Mapping[str, str]) -> NoOpTestValueRecorder:
        if name not in self.recorders:
            self.recorders[name] = []
        recorder = NoOpTestValueRecorder(name, tags)
        self.recorders[name].append(recorder)
        return recorder

    def clear(self):
        self.recorders = {}


class TestMeter:
    def __init__(self) -> None:
        self.recorders = {}

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> TestValueRecorder:
        if name not in self.recorders:
            self.recorders[name] = []
        recorder = TestValueRecorder(name, tags)
        self.recorders[name].append(recorder)
        return recorder

    def get_value_recorder_by_op_name(self, op_name: OpName) -> Optional[TestValueRecorder]:
        name_recorders = self.recorders.get(OpAttributeName.MeterOperationDuration.value, [])
        return next((r for r in name_recorders if r.op_name == op_name.value), None)

    def clear(self) -> None:
        self.recorders = {}


TestMeterType = Union[NoOpTestMeter, TestMeter]
