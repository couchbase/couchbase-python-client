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

__all__ = ['get_otel_meter']

import sys
import typing
from collections.abc import Mapping
from typing import Any

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from couchbase._version import __version__
from couchbase.common.observability.metrics import Meter, ValueRecorder

if typing.TYPE_CHECKING:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.metrics import MeterProvider


try:
    from opentelemetry import metrics as otel_metrics  # noqa: F811
    _HAS_OTEL = True
except ImportError:
    _HAS_OTEL = False


if _HAS_OTEL:

    __all__ += ['OtelWrapperMeter', 'OtelWrapperValueRecorder']

    class OtelWrapperValueRecorder(ValueRecorder):
        """A :class:`~couchbase.observability.metrics.ValueRecorder` over an OpenTelemetry histogram.

        A duration recorded in microseconds is reported in seconds when the unit is ``s``.

        Args:
            histogram (opentelemetry.metrics.Histogram): The histogram it records to.
            tags (dict[str, Any]): The attributes each measurement carries.
            unit (str): The unit the histogram reports in.
        """

        def __init__(self, histogram: otel_metrics.Histogram, tags: dict[str, Any], unit: str) -> None:
            self._histogram = histogram
            self._attributes = tags if tags else {}
            self._unit = unit

        @override
        def record_value(self, value: int) -> None:
            # The SDK records value in micros and will place a special __unit tag in the tags set to 's'
            # So self._unit == 's' is indication that we should convert from micros to seconds
            recorded = value / 1_000_000 if self._unit == 's' else float(value)
            self._histogram.record(recorded, attributes=self._attributes)

    class OtelWrapperMeter(Meter):
        """A :class:`~couchbase.observability.metrics.Meter` over an OpenTelemetry meter.

        It creates one histogram per metric name, in the unit the ``__unit`` tag names.

        Args:
            otel_meter (opentelemetry.metrics.Meter): The meter it creates histograms with.
        """

        def __init__(self, otel_meter: otel_metrics.Meter) -> None:
            self._otel_meter = otel_meter
            self._histograms: dict[str, otel_metrics.Histogram] = {}

        @override
        def value_recorder(self, name: str, tags: Mapping[str, Any]) -> ValueRecorder:
            local_tags = dict(tags) if tags else {}
            unit = local_tags.pop('__unit', '')
            if name not in self._histograms:
                self._histograms[name] = self._otel_meter.create_histogram(name=name, unit=unit)

            histogram = self._histograms[name]
            return OtelWrapperValueRecorder(histogram, local_tags, unit=unit)


def get_otel_meter(provider: MeterProvider | None = None) -> Meter:
    """Create a meter that sends the SDK's metrics to OpenTelemetry.

    Args:
        provider: The meter provider to get the meter from. Defaults to None, which uses the global one.

    Returns:
        The meter, an ``OtelWrapperMeter``.

    Raises:
        ImportError: If OpenTelemetry is not installed.
    """
    if not _HAS_OTEL:
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
