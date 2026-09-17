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

__all__ = ['now_ns']


def _build_now_ns():
    """Return a callable giving the current wall-clock time in nanoseconds.

    Durations here are measured against the wall clock rather than a monotonic clock
    because the timestamps are combined with the wall-clock values the core reports for
    its own spans, so both must share an epoch.

    On Windows, CPython below 3.13 backs time.time_ns() with GetSystemTimeAsFileTime,
    whose value only advances once per system clock tick (~15.6ms by default). Any
    operation that starts and finishes inside a single tick is stamped with an identical
    start and end time and reports a duration of zero. Calling
    GetSystemTimePreciseAsFileTime directly keeps the same epoch at ~1us resolution;
    3.13 and above already use it.
    """
    if sys.platform != 'win32' or sys.version_info >= (3, 13):
        return time.time_ns

    try:
        import ctypes

        # FILETIME counts 100ns intervals since 1601-01-01; the Unix epoch is
        # 11644473600 seconds later. The struct is two 32-bit halves of a
        # little-endian 64-bit integer, so a c_uint64 reads it directly.
        epoch_offset = 116444736000000000
        get_time = ctypes.windll.kernel32.GetSystemTimePreciseAsFileTime
        get_time.argtypes = [ctypes.c_void_p]
        get_time.restype = None
        buf_type = ctypes.c_uint64
        byref = ctypes.byref

        def precise_now_ns() -> int:
            buf = buf_type()
            get_time(byref(buf))
            return (buf.value - epoch_offset) * 100

        # Confirm the call works and lands in the same era as the stdlib clock before
        # handing it out; a bad value is worse than a coarse one.
        if abs(precise_now_ns() - time.time_ns()) > 1_000_000_000:
            return time.time_ns

        return precise_now_ns
    except (AttributeError, OSError, ValueError):
        return time.time_ns


now_ns = _build_now_ns()
