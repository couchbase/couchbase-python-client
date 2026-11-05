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

__all__ = [
    'Meter',
    'ValueRecorder',
]

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any


class ValueRecorder(ABC):
    """The interface a recorder of one metric implements.

    Implement it to send the SDK's metrics to a metrics library of your own.
    """

    @abstractmethod
    def record_value(self, value: int) -> None:
        """Record one measurement.

        Args:
            value (int): The measurement. A duration is in microseconds.
        """
        pass


class Meter(ABC):
    """The interface a source of value recorders implements.

    Implement it, with :class:`ValueRecorder`, to send the SDK's metrics to a metrics library of your own.
    :func:`~couchbase.observability.get_otel_meter` returns one that sends them to OpenTelemetry.
    """

    @abstractmethod
    def value_recorder(self, name: str, tags: Mapping[str, Any]) -> ValueRecorder:
        """Get the recorder for a metric and set of tags.

        Args:
            name (str): The metric's name.
            tags (Mapping[str, Any]): The tags each measurement carries. A ``__unit`` tag names the unit the
                metric is reported in, and is ``s`` for a duration the SDK records in microseconds.

        Returns:
            :class:`~couchbase.observability.metrics.ValueRecorder`: The recorder.
        """
        pass
