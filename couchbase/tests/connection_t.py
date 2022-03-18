from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.logic.cluster import ClusterLogic
from couchbase.options import (ClusterOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions)


class ConnectionTests:

    def test_basic(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        timing_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=30), query_timeout=timedelta(seconds=60))
        tracing_opts = ClusterTracingOptions(tracing_orphaned_queue_flush_interval=timedelta(
            milliseconds=10), tracing_orphaned_queue_size=20)
        opts = ClusterOptions(auth, tracing_options=tracing_opts)
        cluster = ClusterLogic(conn_string, opts, **timing_opts)
        cluster_opts = cluster._get_connection_opts()
        print(cluster_opts)
