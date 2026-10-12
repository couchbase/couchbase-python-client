#  Copyright 2016-2024. Couchbase, Inc.
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

# A sidecar, not a standalone pytest file.  The generator merges the methods below into the
# generated class of the same name and adds their names to that class's TEST_MANIFEST;
# nothing here is collected directly, since `python_classes = ["*Tests"]` only collects
# concrete `*Tests` classes.  The class name is the merge target, and query_t.py has two
# suite classes, so naming the file is not enough to say where a test belongs.
#
# Everything here is merged verbatim: it is already asynchronous, so it must not go through
# the sync-to-async transform.  The bar for living here rather than in
# couchbase/tests/query_t.py is that the test asserts something that only exists under
# asyncio; one that merely happens to be absent from the sync suite belongs in the sync
# suite, where all three apis get it.
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from acouchbase.cluster import Cluster
from tests.environments.async_environment import AsyncTestEnvironment


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_non_blocking',
        'test_query_cancellation',
    ]

    @pytest.mark.asyncio
    async def test_non_blocking(self, cb_env: AsyncTestEnvironment) -> None:
        # PYCBC-1471: rows must be pulled from the binding's streaming result without
        # blocking the event loop.  Before the fix the queries returned in submission
        # order, because pulling rows blocked until that query was done; the assertion is
        # therefore that the completion order is *not* the submission order.
        async def run_query(cluster: Cluster, idx: int) -> dict[float, int]:
            # UNNEST rather than comma-separated FROM terms, which 6.6 cannot parse.
            slow_query = ['SELECT COUNT (1) AS c FROM ARRAY_RANGE(0,100) AS d1',
                          'UNNEST ARRAY_RANGE(0,100) AS d2',
                          'UNNEST ARRAY_RANGE(0,100) AS d3']
            result = cluster.query(' '.join(slow_query))
            rows = []
            async for r in result:
                rows.append(r)
            return {time.time(): idx}

        tasks = [asyncio.create_task(run_query(cb_env.cluster, idx)) for idx in range(10)]
        results = await asyncio.gather(*tasks)
        ordered_results = dict(sorted({k: v for r in results for k, v in r.items()}.items()))
        assert list(ordered_results.values()) != list(range(10))

    @pytest.mark.asyncio
    async def test_query_cancellation(self, cb_env: AsyncTestEnvironment) -> None:
        # PYCBC-1774: cancelling an in-flight streaming query must propagate CancelledError
        # unchanged (not swallow or convert it) and cancel the underlying C++ streamed
        # result, so the executor worker is released promptly instead of waiting for the
        # whole server-side operation to finish.
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` LIMIT 100')

        async def consume() -> list[Any]:
            rows = []
            async for row in result:
                rows.append(row)
            return rows

        task = asyncio.ensure_future(consume())
        # Let the task start streaming (submit the query) and reach the first, blocking
        # __anext__.
        while result._request._streaming_result is None and not task.done():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The cancellation must have reached through to the C++ streamed result.
        assert result._request._streaming_result is not None
        assert result._request._streaming_result.is_cancelled() is True
