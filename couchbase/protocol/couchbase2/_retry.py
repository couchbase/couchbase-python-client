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

"""Why a couchbase2 request is retried, and how long it waits first.

RFC 49 marks neither reason here as always retrying, so each is put to the strategy.  A strategy
only proposes a delay: the request's deadline still caps it, so no strategy can keep a request
alive past its timeout.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Protocol


class RetryReason(Enum):
    """The member names are RFC 77's; each value is the string classic reports in `retry_reasons`."""

    SERVICE_NOT_AVAILABLE = 'service_not_available'
    KV_LOCKED = 'key_value_locked'

    @property
    def always_retry(self) -> bool:
        """Whether this reason retries with controlled backoff without asking the strategy."""
        return False

    @property
    def allows_non_idempotent_retry(self) -> bool:
        """Whether a request that is not idempotent may be retried for this reason.

        Every reason here says the request was not applied, so retrying a write cannot apply it twice.
        """
        return True


@dataclass(frozen=True)
class RetryAction:
    """A strategy's answer: wait `delay` and retry, or, when `delay` is None, give up."""

    delay: timedelta | None

    @classmethod
    def do_not_retry(cls) -> RetryAction:
        return cls(None)


class RetryRequest(Protocol):
    """What a strategy may read about the request it is deciding for."""

    @property
    def identifier(self) -> str: ...

    @property
    def idempotent(self) -> bool: ...

    @property
    def retry_attempts(self) -> int:
        """The retries made so far, not counting the one being decided."""
        ...

    @property
    def retry_reasons(self) -> Sequence[RetryReason]:
        """Each reason retried so far, once, in the order first seen."""
        ...


class RetryStrategy(Protocol):
    def retry_after(self, request: RetryRequest, reason: RetryReason) -> RetryAction: ...


_CONTROLLED_BACKOFF = tuple(timedelta(milliseconds=ms) for ms in (1, 10, 50, 100, 500))
_CONTROLLED_BACKOFF_CEILING = timedelta(milliseconds=1000)


def controlled_backoff(retry_attempts: int) -> timedelta:
    """The wait before retry number `retry_attempts + 1`.  There is no attempt cap; the deadline is one."""
    if retry_attempts < len(_CONTROLLED_BACKOFF):
        return _CONTROLLED_BACKOFF[retry_attempts]
    return _CONTROLLED_BACKOFF_CEILING


class BestEffortRetryStrategy:
    """Retry with controlled backoff whenever retrying cannot apply a write twice."""

    def retry_after(self, request: RetryRequest, reason: RetryReason) -> RetryAction:
        if request.idempotent or reason.allows_non_idempotent_retry:
            return RetryAction(controlled_backoff(request.retry_attempts))
        return RetryAction.do_not_retry()
