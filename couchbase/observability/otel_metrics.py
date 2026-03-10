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

import typing
from typing import (Any,
                    Dict,
                    Mapping,
                    Optional)

from couchbase._version import __version__
from couchbase.observability.metrics import Meter, ValueRecorder

if typing.TYPE_CHECKING:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.metrics import MeterProvider

try:
    from opentelemetry import metrics as otel_metrics  # noqa: F811
    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False


if HAS_OTEL:

    class OtelWrapperValueRecorder(ValueRecorder):
        """
        Wraps an OpenTelemetry Histogram to implement SDK's ValueRecorder interface.
        """

        def __init__(self, histogram: otel_metrics.Histogram, tags: Dict[str, Any], unit: str) -> None:
            self._histogram = histogram
            self._attributes = tags if tags else {}
            self._unit = unit

        def record_value(self, value: int) -> None:
            # The SDK records value in micros and will place a special __unit tag in the tags set to 's'
            # So self._unit == 's' is indication that we should convert from micros to seconds
            value = value / 1_000_000 if self._unit == 's' else value
            self._histogram.record(float(value), attributes=self._attributes)

    class OtelWrapperMeter(Meter):
        """
        Wraps an OpenTelemetry Meter to implement SDK's Meter interface.
        """

        def __init__(self, otel_meter: otel_metrics.Meter) -> None:
            self._otel_meter = otel_meter
            self._histograms: Dict[str, otel_metrics.Histogram] = {}

        def value_recorder(self, name: str, tags: Mapping[str, Any]) -> ValueRecorder:
            local_tags = dict(tags) if tags else {}
            unit = local_tags.pop('__unit', '')
            if name not in self._histograms:
                self._histograms[name] = self._otel_meter.create_histogram(name=name, unit=unit)

            histogram = self._histograms[name]
            return OtelWrapperValueRecorder(histogram, local_tags, unit=unit)


def get_otel_meter(provider: Optional[MeterProvider] = None) -> Meter:
    """
    Creates an OpenTelemetry wrapper meter.

    Args:
        provider: Optional OpenTelemetry MeterProvider. If not provided,
                  falls back to the global OTel meter provider.

    Returns:
        OtelWrapperMeter instance implementing SDK's Meter interface

    Raises:
        ImportError: If OpenTelemetry is not installed
    """
    if not HAS_OTEL:
        raise ImportError(
            "OpenTelemetry is not installed. Please install with: "
            "pip install couchbase[otel]"
        )

    pkg_name = "com.couchbase.client/python"
    pkg_version = __version__

    if provider:
        otel_meter = provider.get_meter(pkg_name, pkg_version)
    else:
        otel_meter = otel_metrics.get_meter(pkg_name, pkg_version)

    return OtelWrapperMeter(otel_meter)
