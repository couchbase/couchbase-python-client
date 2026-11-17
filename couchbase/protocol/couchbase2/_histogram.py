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

"""The logging meter's latency histogram, in pure Python, since couchbase2 has no core to lend one.

A value is counted in a bucket as wide as its last significant figure, so the buckets a run can fill
are bounded by the trackable range and the precision, and never by how many values were recorded.
A percentile reads as the highest value its bucket holds, as an HDR histogram's does.
"""

from __future__ import annotations

import math
import threading
from collections import Counter

from couchbase.common._observability.logging_meter import HistogramPercentiles


class PercentileHistogram:

    def __init__(self,
                 *,
                 lowest_discernible_value: int,
                 highest_trackable_value: int,
                 significant_figures: int) -> None:
        self._lowest = lowest_discernible_value
        self._highest = highest_trackable_value
        self._figures = significant_figures
        self._counts: Counter[int] = Counter()
        self._lock = threading.Lock()

    def record_value(self, value: int) -> None:
        bucket = self._bucket(min(max(value, self._lowest), self._highest))
        with self._lock:
            self._counts[bucket] += 1

    def get_percentiles_and_reset(self, percentiles: list[float]) -> HistogramPercentiles:
        with self._lock:
            counts, self._counts = self._counts, Counter()
        total = sum(counts.values())
        if total == 0:
            return {'total_count': 0, 'percentiles': [0] * len(percentiles)}
        ordered = sorted(counts.items())
        found = []
        for percentile in percentiles:
            rank = max(math.ceil(percentile / 100 * total), 1)
            seen = 0
            for bucket, count in ordered:
                seen += count
                if seen >= rank:
                    found.append(min(bucket + self._width(bucket) - 1, self._highest))
                    break
        return {'total_count': total, 'percentiles': found}

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()

    def _width(self, value: int) -> int:
        return 10 ** max(len(str(value)) - self._figures, 0)

    def _bucket(self, value: int) -> int:
        width = self._width(value)
        return value - value % width
