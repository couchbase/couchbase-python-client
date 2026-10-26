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

from typing import Any

import pytest

from couchbase.common.exceptions import _ExceptionInfo
from couchbase.exceptions import RequestCanceledException
from couchbase.protocol.classic._pycbc_core import pycbc_exception
from couchbase.protocol.classic._streaming import next_core_row
from tests.test_features import TestProtocol

# What is checked here is how classic reads a row off its binding's stream.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)


class _CoreError(pycbc_exception):
    # A subclass of the real core type, so the isinstance check in next_core_row is the one the
    # extension's own error values meet.

    def err(self) -> int:
        return 2

    def err_category(self) -> str:
        return 'couchbase.common'

    def strerror(self) -> str:
        return 'request_canceled'

    def error_context(self) -> dict[str, Any] | None:
        return None

    def error_info(self) -> _ExceptionInfo | None:
        return None


class StreamingRowUnitTests:
    # No TEST_MANIFEST: next_core_row is one function, with no cluster to generate a copy for.

    def test_returns_the_raw_row(self) -> None:
        assert next_core_row(iter([{'a': 1}]), StopIteration) == {'a': 1}

    def test_core_error_is_raised_mapped(self) -> None:
        with pytest.raises(RequestCanceledException):
            next_core_row(iter([_CoreError()]), StopIteration)

    @pytest.mark.parametrize('stop', [StopIteration, StopAsyncIteration])
    def test_none_ends_the_stream_with_the_given_stop(self, stop: type[Exception]) -> None:
        with pytest.raises(stop):
            next_core_row(iter([None]), stop)

    def test_exhausted_stream_raises_stop_iteration(self) -> None:
        with pytest.raises(StopIteration):
            next_core_row(iter([]), StopAsyncIteration)
