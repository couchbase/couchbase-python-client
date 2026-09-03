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

# A sidecar, not a standalone pytest file; see query_additions_t.py for the mechanism.  The
# class name is the merge target, and cluster_t.py has two suite classes, so the one test
# below is named into ClusterCloseTestSuite rather than into the file.
#
# The bar for living here is that the test asserts something only the async api can be
# asked.  `couchbase.Cluster(conn_string, opts)` returns with `connected` already True,
# because the sync constructor connects; `acouchbase.Cluster(conn_string, opts)` returns
# unconnected, because connecting is what `on_connect()` is for.  So "close a cluster that
# was never connected" has no sync form to be generated from.  txcouchbase constructs the
# same lazy way, but ClusterCloseTestSuite is excluded there whole, so there is no generated
# class to merge into.
from __future__ import annotations

import pytest

from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from tests_v2.environments.async_environment import AsyncTestEnvironment


class ClusterCloseTestSuite:
    TEST_MANIFEST = [
        'test_await_on_close_cluster',
    ]

    @pytest.mark.asyncio
    async def test_await_on_close_cluster(self, cb_env: AsyncTestEnvironment) -> None:
        conn_string = cb_env.config.get_connection_string()
        username, pw = cb_env.config.get_username_and_pw()
        cluster = Cluster(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))
        assert cluster._impl.connected is False
        await cluster.close()
        assert cluster._impl.connected is False
