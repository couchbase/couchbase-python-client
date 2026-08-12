import logging
from copy import deepcopy
from datetime import timedelta
from typing import Dict
from uuid import uuid4

import grpc

from couchbase import get_transactions_protocol
from couchbase.cluster import Cluster

from .executors import RequestExecutor, WorkloadExecutorBuilder
from .generated import performer_pb2_grpc as performer_pb_grpc
from .generated.observability import top_pb2 as observability_pb
from .generated.performer import caps_pb2 as performer_caps_pb
from .generated.sdk import caps_pb2 as sdk_caps_pb
from .generated.shared import basic_pb2 as basic_pb
from .generated.shared import bounds_pb2 as bounds_pb
from .generated.shared import cluster_pb2 as cluster_pb
from .generated.shared import echo_pb2 as echo_pb
from .generated.streams import top_level_pb2 as streams_pb
from .generated.transactions import extensions_pb2 as extensions_pb
from .streaming import StreamOwner
from .telemetry.span_owner import SpanOwner
from .utils import ClusterConnectOptions, ConnectionCache
from .utils.metrics_reporter import MetricsReporter
from .workloads import Counters


class PerformerServiceServicer(performer_pb_grpc.PerformerServiceServicer):

    _VALID_EXTENSIONS = {
        "TI": extensions_pb.EXT_TRANSACTION_ID,
        "DC": extensions_pb.EXT_DEFERRED_COMMIT,
        "TO": extensions_pb.EXT_TIME_OPT_UNSTAGING,
        "MO": extensions_pb.EXT_MEMORY_OPT_UNSTAGING,
        "CM": extensions_pb.EXT_CUSTOM_METADATA_COLLECTION,
        "BM": extensions_pb.EXT_BINARY_METADATA,
        "QU": extensions_pb.EXT_QUERY,
        "SD": extensions_pb.EXT_STORE_DURABILITY,
        "BF3787": extensions_pb.BF_CBD_3787,
        "BF3794": extensions_pb.BF_CBD_3794,
        "BF3705": extensions_pb.BF_CBD_3705,
        "BF3838": extensions_pb.BF_CBD_3838,
        "RC": extensions_pb.EXT_REMOVE_COMPLETED,
        "UA": extensions_pb.EXT_UNKNOWN_ATR_STATES,
        "CO": extensions_pb.EXT_ALL_KV_COMBINATIONS,
        "BF3791": extensions_pb.BF_CBD_3791,
        "SQ": extensions_pb.EXT_SINGLE_QUERY,
        "TS": extensions_pb.EXT_THREAD_SAFE,
        "SZ": extensions_pb.EXT_SERIALIZATION,
        "SI": extensions_pb.EXT_SDK_INTEGRATION,
        "RX": extensions_pb.EXT_REPLACE_BODY_WITH_XATTR,
        "IX": extensions_pb.EXT_INSERT_EXISTING,
        "QC": extensions_pb.EXT_QUERY_CONTEXT,
        "PU": extensions_pb.EXT_PARALLEL_UNSTAGING,
        "BS": extensions_pb.EXT_BINARY_SUPPORT,
        "RP": extensions_pb.EXT_REPLICA_FROM_PREFERRED_GROUP,
        "GM": extensions_pb.EXT_GET_MULTI,
        "RPP1": extensions_pb.EXT_REPLICA_FROM_PREFERRED_GROUP_PATCH1,
    }

    def __init__(self):
        self._logger: logging.Logger = logging.getLogger()
        self._conns: Dict[str, ConnectionCache] = {}
        self._stream_owner: StreamOwner = StreamOwner()
        self._span_owner: SpanOwner = SpanOwner()
        # A single counter registry, owned for the performer's lifetime and shared into every run's
        # executor.  It starts in-process (a plain dict + threading.Lock), which is correct and fast
        # for the default multi-threading mechanism; a multi-processing run upgrades it to a
        # multiprocessing.Manager backing so the counters reach the worker processes.  Counters
        # persist across runs and are mutated by setCounter / clearAllCounters.
        self._counters: Counters = Counters()

    def performerCapsFetch(self, request, context):
        self._logger.info("performerCapsFetch called")

        response = performer_caps_pb.PerformerCapsFetchResponse(
            library_version="1.0.0",
            performer_user_agent="python",
            sdk_implementation_caps=[],
            transaction_implementations_caps=[],
            supported_apis=[basic_pb.API.DEFAULT]
        )

        try:
            transactions_protocol_version, supported_extensions = get_transactions_protocol()
        except NameError:
            transactions_protocol_version = 2.0
            # The extensions supported by the SDK before the get_transactions_protocol() function was added
            supported_extensions = ["TI", "DC", "TO", "MO", "CM", "BM", "QU", "SD", "BF3787", "BF3794",
                                    "BF3705", "BF3838", "RC", "UA", "CO", "BF3791", "SQ", "SI"]

        response.transactions_protocol_version = str(transactions_protocol_version)

        for ext in supported_extensions:
            try:
                response.transaction_implementations_caps.append(self._VALID_EXTENSIONS[ext])
            except KeyError:
                # This is a warning only as the performer doesn't have transactions support yet.
                # We can consider making this an error once we add transactions support.
                self._logger.warning(f"Library reported unexpected transactions extension '{ext}'")

        response.performer_caps.append(performer_caps_pb.KV_SUPPORT_1)
        response.performer_caps.append(performer_caps_pb.CLUSTER_CONFIG_CERT)
        response.performer_caps.append(performer_caps_pb.CLUSTER_CONFIG_INSECURE)
        # [start:4.6.0]
        response.performer_caps.append(performer_caps_pb.OBSERVABILITY_1)
        # [end:4.6.0]

        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_KV)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_QUERY_INDEX_MANAGEMENT)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SEARCH)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SCOPE_SEARCH)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SEARCH_INDEX_MANAGEMENT)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_QUERY)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_LOOKUP_IN)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_BUCKET_MANAGEMENT)
        # [start:4.1.3]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_COLLECTION_QUERY_INDEX_MANAGEMENT)
        # [end:4.1.3]
        # [start:4.1.7]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_KV_RANGE_SCAN)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_QUERY_READ_FROM_REPLICA)
        # [end:4.1.7]
        # [start:4.1.8]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_LOOKUP_IN_REPLICAS)
        # [end:4.1.8]
        # [start:4.1.9]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_MANAGEMENT_HISTORY_RETENTION)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_COLLECTION_MANAGEMENT)
        # [end:4.1.9]
        # [start:4.1.10]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_DOCUMENT_NOT_LOCKED)
        # [end:4.1.10]
        # [start:4.1.11]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_VECTOR_SEARCH)
        # [end:4.1.11]
        # [start:4.1.12]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SCOPE_SEARCH)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SCOPE_SEARCH_INDEX_MANAGEMENT)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_INDEX_MANAGEMENT_RFC_REVISION_25)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SEARCH_RFC_REVISION_11)
        # [end:4.1.12]
        # [start:4.2.2]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_VECTOR_SEARCH_BASE64)
        # [end:4.2.2]
        # [start:4.4.0]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_ZONE_AWARE_READ_FROM_REPLICA)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_APP_TELEMETRY)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_BUCKET_SETTINGS_NUM_VBUCKETS)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_PREFILTER_VECTOR_SEARCH)
        # [end:4.4.0]
        response.sdk_implementation_caps.append(sdk_caps_pb.SUPPORTS_AUTHENTICATOR)
        # [start:4.6.0]
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_SET_AUTHENTICATOR)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_JWT)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_OBSERVABILITY_RFC_REV_24)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_OBSERVABILITY_CLUSTER_LABELS)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS)
        response.sdk_implementation_caps.append(sdk_caps_pb.SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS_EMITTED_BY_DEFAULT)
        # [end:4.6.0]

        return response

    def clusterConnectionCreate(self, request, context):
        self._logger.info("clusterConnectionCreate called ")

        hostname = request.cluster_hostname
        requires_tls = request.HasField('authenticator') and (request.authenticator.HasField('certificate_auth') or
                                                              request.authenticator.HasField('jwt_auth'))

        # TODO remove when connection strings without specifying scheme are supported
        if "://" not in hostname:
            if requires_tls:
                # Certificate auth implies TLS
                hostname = "couchbases://" + hostname
            else:
                hostname = "couchbase://" + hostname
        conn_id = request.cluster_connection_id

        try:
            options = ClusterConnectOptions.get_cluster_options(request)
        except NotImplementedError as e:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
            return

        mp_options = deepcopy(options)

        conn = ConnectionCache(hostname=hostname, options=options, mp_cluster_options=mp_options)

        # [if:4.6.0]
        if (request.HasField('cluster_config')
                and request.cluster_config.HasField('observability_config')):
            obs_config = request.cluster_config.observability_config
            conn.obs_config = obs_config
            if obs_config.use_noop_tracer:
                options['enable_tracing'] = False
                mp_options['enable_tracing'] = False
                self._logger.info("Noop tracer enabled for connection (tracing disabled)")
            elif obs_config.HasField('tracing'):
                from .telemetry.otel import create_tracer_provider
                conn.tracer, conn.tracer_provider = create_tracer_provider(obs_config.tracing)
                options['tracer'] = conn.tracer
                self._logger.info("OTel tracing enabled for connection")
            if obs_config.HasField('metrics'):
                from .telemetry.otel import create_meter_provider
                conn.meter, conn.meter_provider = create_meter_provider(obs_config.metrics)
                options['meter'] = conn.meter
                self._logger.info("OTel metrics enabled for connection")
            else:
                options['enable_metrics'] = False
                mp_options['enable_metrics'] = False
                self._logger.info("Metrics disabled")
        # [end]

        self._logger.info(f"Using connection string `{hostname}`")
        connection = Cluster(hostname, options)
        connection.wait_until_ready(timedelta(seconds=5))
        conn.cluster = connection
        self._conns[conn_id] = conn
        self._logger.info("Successfully created connection")
        self._logger.info(f"There are {len(self._conns)} connections")
        response = cluster_pb.ClusterConnectionCreateResponse(cluster_connection_count=len(self._conns))
        return response

    def clusterConnectionClose(self, request, context):
        self._logger.info("clusterConnectionClose called")

        conn_id = request.cluster_connection_id
        if conn_id in self._conns:
            self._conns[conn_id].close()
            del self._conns[conn_id]
        else:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Connection ID '{request.cluster_connection_id}' is not known"
            )
            return

        return cluster_pb.ClusterConnectionCloseResponse(cluster_connection_count=len(self._conns))

    def disconnectConnections(self, request, context):
        self._logger.info("disconnectConnections called")

        for conn in self._conns.values():
            conn.close()

        self._conns = {}
        self._span_owner.clear()
        # Release the counter registry's Manager subprocess (if a multi-processing run created one);
        # counter values are preserved in-process.  No-op for the multi-threading path.
        self._counters.shutdown()

        return cluster_pb.DisconnectConnectionsResponse()

    def spanCreate(self, request, context):
        self._logger.info(f"spanCreate called: id={request.id} name={request.name}")

        conn = self._conns.get(request.cluster_connection_id)
        if conn is None:
            return grpc.Status(grpc.StatusCode.NOT_FOUND,
                               f"No connection with id '{request.cluster_connection_id}'")
        if conn.tracer is None:
            return grpc.Status(grpc.StatusCode.FAILED_PRECONDITION,
                               f"Tracer has not been set up for connection '{request.cluster_connection_id}'")

        try:
            self._span_owner.create_span(conn.tracer, request)
        except KeyError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return

        return observability_pb.SpanCreateResponse()

    def spanFinish(self, request, context):
        self._logger.info(f"spanFinish called: id={request.id}")

        try:
            self._span_owner.finish_span(request)
        except KeyError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return

        return observability_pb.SpanFinishResponse()

    def echo(self, request, context):
        self._logger.info("================ " + request.testName + " : " + request.message + " ================ ")
        return echo_pb.EchoResponse()

    def run(self, request, context):  # noqa: C901
        self._logger.info("Run called")

        run_id = str(uuid4())
        request_type = request.WhichOneof("request")
        metrics_reporter = None
        metrics_enabled = (request.HasField("config")
                           and request.config.HasField("streaming_config")
                           and request.config.streaming_config.enable_metrics)

        if metrics_enabled:
            metrics_reporter = MetricsReporter(run_id, 1)
            self._logger.info(f"Starting metrics reporter (run: {run_id})...")
            metrics_reporter.start()

        if request_type == "workloads":
            cached_conn = self._conns[request.workloads.cluster_connection_id]
            try:
                executor = WorkloadExecutorBuilder.build_executor(
                    request, run_id, cached_conn, self._stream_owner, self._counters, span_owner=self._span_owner)
                req_executor = RequestExecutor.build_request(request, executor)
                req_executor.execute_request()
                for result in req_executor.results():
                    yield result
                    if metrics_enabled and metrics_reporter is not None:
                        for report in metrics_reporter.report_queue:
                            yield report

                # drain any remaining results
                if metrics_enabled and metrics_reporter is not None:
                    for report in metrics_reporter.report_queue:
                        yield report
                self._logger.info("Sent all the results for this run")
                executor.shutdown()
                if metrics_enabled and metrics_reporter is not None:
                    metrics_reporter.stop()
                self._logger.info("Run finished")
            except ValueError as e:
                if metrics_enabled and metrics_reporter is not None:
                    metrics_reporter.stop()
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
                return None
            except NotImplementedError as e:
                if metrics_enabled and metrics_reporter is not None:
                    metrics_reporter.stop()
                context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
                return None
        else:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, f"Request type {request_type} not supported")
            return None

    def streamCancel(self, request, context):
        self._logger.info('streamCancel called')
        self._stream_owner.cancel(request)
        return streams_pb.CancelResponse()

    def streamRequestItems(self, request, context):
        self._logger.info('streamRequestItems called')
        self._stream_owner.request_items(request)
        return streams_pb.RequestItemsResponse()

    def setCounter(self, request, context):
        self._logger.info(f"setCounter called (counter_id: {request.counter_id})")
        try:
            self._counters.set(request)
        except ValueError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return
        return bounds_pb.SetCounterResponse()

    def clearAllCounters(self, request, context):
        self._logger.info("clearAllCounters called")
        self._counters.clear()
        return bounds_pb.ClearAllCountersResponse()
