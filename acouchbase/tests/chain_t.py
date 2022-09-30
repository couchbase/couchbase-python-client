#  Copyright 2016-2022. Couchbase, Inc.
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

import asyncio

import pytest
import pytest_asyncio

from acouchbase.bucket import AsyncBucket
from acouchbase.cluster import (AsyncCluster,
                                Cluster,
                                get_event_loop)
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.result import GetResult


class ConnectionChainingTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest.mark.asyncio
    async def test_bucket_create_chain(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster = Cluster(conn_string, ClusterOptions(auth))
        # validate the cluster, at this point, the connect future should be pending
        assert isinstance(cluster, AsyncCluster)
        assert cluster._connect_ftr.done() is False

        bucket = cluster.bucket(couchbase_config.bucket_name)
        assert isinstance(bucket, AsyncBucket)
        await bucket.on_connect()
        # after connecting the bucket, the cluster connection should now exist b/c the connection
        # futures are chained (i.e. cluster.connect -> bucket.connect)
        assert cluster._connect_ftr is not None
        assert cluster._connect_ftr.done() is True
        assert cluster._connection is not None
        assert bucket._connect_ftr.done() is True

    @pytest.mark.asyncio
    async def test_kv_op_chain(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster = Cluster(conn_string, ClusterOptions(auth))
        bucket = cluster.bucket(couchbase_config.bucket_name)
        coll = bucket.default_collection()
        key = 'async-test-conn-key'
        doc = {'id': key, 'what': 'this is an asyncio test.'}
        await coll.upsert(key, doc)
        res = None
        for _ in range(3):
            try:
                res = await coll.get(key)
            except Exception:
                await asyncio.sleep(float(1.0))

        # after executing the KV op, the cluster connection should now exist b/c the connection
        # futures are chained (i.e. cluster.connect -> bucket.connect -> KV op)
        assert cluster._connect_ftr.done() is True
        assert bucket._connect_ftr.done() is True
        assert isinstance(res, GetResult)
        assert res.content_as[dict] == doc

    # @TODO(jc): PYCBC-1414 - once complete, validate cluster operations chaining
