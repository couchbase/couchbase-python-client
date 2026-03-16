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

from typing import Mapping, Union

from couchbase.logic.observability import NoOpValueRecorder, OpAttributeName


class NoOpTestValueRecorder(NoOpValueRecorder):

    def __init__(self, name: str, tags: Mapping[str, str]) -> None:
        self.name = name
        self.tags = tags
        self.values = []

    def record_value(self, value: Union[int, float]) -> None:
        self.values.append(value)


class TestValueRecorder:
    def __init__(self, name: str, tags: Mapping[str, str]) -> None:
        self.name = name
        self.op_name = None
        if tags and OpAttributeName.OperationName.value in tags:
            self.op_name = tags[OpAttributeName.OperationName.value]
        self.attributes = tags
        self.values = []

    def record_value(self, value: Union[int, float]) -> None:
        self.values.append(value)


TestValueRecorderType = Union[NoOpTestValueRecorder, TestValueRecorder]
