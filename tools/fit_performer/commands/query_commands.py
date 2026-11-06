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

import json
import logging
from collections.abc import Callable
from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (TYPE_CHECKING,
                    Any,
                    Concatenate,
                    ParamSpec)

from google.protobuf import duration_pb2 as duration
from google.protobuf import timestamp_pb2 as timestamp

from couchbase.cluster import Cluster
from couchbase.n1ql import QueryScanConsistency, QueryStatus
from couchbase.options import QueryOptions
from couchbase.result import QueryResult
from couchbase.scope import Scope

from run import top_level_pb2 as run_pb
from sdk import query_pb2 as query_pb
from sdk import workload_pb2 as sdk_pb
from shared import basic_pb2 as basic_pb
from shared import content_pb2 as content_pb

from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

if TYPE_CHECKING:
    from couchbase.n1ql import (QueryMetaData,
                                QueryMetrics,
                                QueryWarning)

    from ..telemetry.span_owner import SpanOwner

P = ParamSpec('P')


VALID_QUERY_COMMAND_ARGS: dict[str, Callable[[Any], bool]] = {
    'cluster': lambda c: c is None or isinstance(c, Cluster),
    'scope': lambda s: s is None or isinstance(s, Scope),
    'statement': lambda i: isinstance(i, str),
    'return_result': lambda rr: isinstance(rr, bool),
    'initiated': lambda i: isinstance(i, timestamp.Timestamp),
    'options': lambda o: o is None or isinstance(o, object),
    'content_as': lambda c: isinstance(c, content_pb.ContentAs),
    'span_owner': lambda s: True,
}

logger = logging.getLogger(__name__)


class QueryCommandOptions(SdkCommandOptions):
    _VALID_SCAN_CONSISTENCY = {
        basic_pb.ScanConsistency.REQUEST_PLUS: QueryScanConsistency.REQUEST_PLUS,
        basic_pb.ScanConsistency.NOT_BOUNDED: QueryScanConsistency.NOT_BOUNDED,
    }

    @staticmethod
    def get_scan_consistency(options: query_pb.QueryOptions) -> QueryScanConsistency | None:
        if not options.HasField('scan_consistency'):
            return None

        return QueryCommandOptions._VALID_SCAN_CONSISTENCY.get(options.scan_consistency, None)

    @staticmethod
    def get_raw(options: query_pb.QueryOptions) -> dict[str, str] | None:
        if len(options.raw) == 0:
            return None

        return dict(options.raw)

    @staticmethod
    def get_adhoc(options: query_pb.QueryOptions) -> bool | None:
        return QueryCommandOptions.get_simple_option(options, 'adhoc')

    @staticmethod
    def get_profile(options: query_pb.QueryOptions) -> str | None:
        return QueryCommandOptions.get_simple_option(options, 'profile')

    @staticmethod
    def get_read_only(options: query_pb.QueryOptions) -> bool | None:
        return QueryCommandOptions.get_simple_option(options, 'readonly')

    @staticmethod
    def get_parameters_positional(options: query_pb.QueryOptions) -> list[str] | None:
        if len(options.parameters_positional) == 0:
            return None

        return [p for p in options.parameters_positional]

    @staticmethod
    def get_parameters_named(options: query_pb.QueryOptions) -> dict[str, str] | None:
        if len(options.parameters_named) == 0:
            return None

        return dict(options.parameters_named)

    @staticmethod
    def get_flex_index(options: query_pb.QueryOptions) -> bool | None:
        return QueryCommandOptions.get_simple_option(options, 'flex_index')

    @staticmethod
    def get_pipeline_cap(options: query_pb.QueryOptions) -> int | None:
        return QueryCommandOptions.get_simple_option(options, 'pipeline_cap')

    @staticmethod
    def get_pipeline_batch(options: query_pb.QueryOptions) -> int | None:
        return QueryCommandOptions.get_simple_option(options, 'pipeline_batch')

    @staticmethod
    def get_scan_cap(options: query_pb.QueryOptions) -> int | None:
        return QueryCommandOptions.get_simple_option(options, 'scan_cap')

    @staticmethod
    def get_scan_wait(options: query_pb.QueryOptions) -> timedelta | None:
        if not options.HasField('scan_wait_millis'):
            return None

        return timedelta(milliseconds=options.scan_wait_millis)

    @staticmethod
    def get_timeout(options: Any) -> timedelta | None:
        if not options.HasField("timeout_millis"):
            return None

        return timedelta(milliseconds=options.timeout_millis)

    @staticmethod
    def get_max_parallelism(options: query_pb.QueryOptions) -> int | None:
        return QueryCommandOptions.get_simple_option(options, 'max_parallelism')

    @staticmethod
    def get_metrics(options: query_pb.QueryOptions) -> bool | None:
        return QueryCommandOptions.get_simple_option(options, 'metrics')

    # @TODO
    # @staticmethod
    # def get_single_query_transaction_options(options):
    #     if not options.HasField("single_query_transaction_options"):
    #         return None

    @staticmethod
    def get_parent_span_id(options: query_pb.QueryOptions) -> str | None:
        return QueryCommandOptions.get_simple_option(options, 'parent_span_id')

    @staticmethod
    def get_use_replica(options: query_pb.QueryOptions) -> bool | None:
        return QueryCommandOptions.get_simple_option(options, 'use_replica')

    @staticmethod
    def get_client_context_id(options: query_pb.QueryOptions) -> str | None:
        return QueryCommandOptions.get_simple_option(options, 'client_context_id')


class QueryCommandResult(SdkCommandResult):
    _CB_TO_GRPC_QUERY_STATUS = {
        QueryStatus.RUNNING: query_pb.QueryStatus.RUNNING,
        QueryStatus.SUCCESS: query_pb.QueryStatus.SUCCESS,
        QueryStatus.ERRORS: query_pb.QueryStatus.ERRORS,
        QueryStatus.COMPLETED: query_pb.QueryStatus.COMPLETED,
        QueryStatus.STOPPED: query_pb.QueryStatus.STOPPED,
        QueryStatus.TIMEOUT: query_pb.QueryStatus.TIMEOUT,
        QueryStatus.CLOSED: query_pb.QueryStatus.CLOSED,
        QueryStatus.FATAL: query_pb.QueryStatus.FATAL,
        QueryStatus.ABORTED: query_pb.QueryStatus.ABORTED,
        QueryStatus.UNKNOWN: query_pb.QueryStatus.UNKNOWN,
    }

    @classmethod
    def as_query_result(cls,
                        fn: Callable[Concatenate[QueryCommand, P], QueryResult],
                        ) -> Callable[Concatenate[QueryCommand, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: QueryCommand, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                rows = [r for r in res.rows()]
                end = perf_counter_ns()
                logger.info(f'Building QueryResult, have {len(rows)} row(s).')
                if self._return_result:
                    sdk_result = sdk_pb.Result(query_result=cls.to_query_result(res, rows, self._content_as))
                else:
                    sdk_result = sdk_pb.Result(success=isinstance(res, QueryResult))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def to_query_result(cls,
                        query_result: QueryResult,
                        rows: list[Any],
                        content_as: content_pb.ContentAs,
                        ) -> query_pb.QueryResult:
        """ Converts a Couchbase QueryResult rows and metadata to the equivalent Protobuf QueryResult object
        """
        q_rows = [cls.to_content(r, content_as) for r in rows]
        meta_data = query_result.metadata()
        if meta_data is None:
            raise RuntimeError('The query result has no metadata once its rows have been read.')
        return query_pb.QueryResult(content=q_rows, meta_data=cls.to_query_meta_data(meta_data))

    @classmethod
    def to_query_meta_data(cls,
                           query_meta_data: QueryMetaData,
                           ) -> query_pb.QueryMetaData:
        """ Converts a Couchbase QueryMetaData to the equivalent Protobuf QueryMetaData object
        """
        metadata_kwargs: dict[str, Any] = {
            'request_id': query_meta_data.request_id(),
            'client_context_id': query_meta_data.client_context_id(),
            'status': cls._CB_TO_GRPC_QUERY_STATUS.get(query_meta_data.status()),
            'warnings': cls.to_query_meta_data_warnings(query_meta_data.warnings()),
        }
        if query_meta_data.signature():
            metadata_kwargs['signature'] = bytes(json.dumps(query_meta_data.signature()), encoding='utf-8')
        if query_meta_data.profile():
            metadata_kwargs['profile'] = bytes(json.dumps(query_meta_data.profile()), encoding='utf-8')
        metrics = query_meta_data.metrics()
        if metrics:
            metadata_kwargs['metrics'] = cls.to_query_meta_data_metrics(metrics)
        return query_pb.QueryMetaData(**metadata_kwargs)

    @classmethod
    def to_query_meta_data_warnings(cls,
                                    warnings: list[QueryWarning],
                                    ) -> list[query_pb.QueryWarning]:
        """ Converts a Couchbase List[QueryWarning] to the equivalent Protobuf List[QueryWarning] object
        """
        return [query_pb.QueryWarning(code=qw.code(), message=qw.message()) for qw in warnings]

    @classmethod
    def to_query_meta_data_metrics(cls,
                                   metrics: QueryMetrics,
                                   ) -> query_pb.QueryMetrics:
        """ Converts a Couchbase QueryMetrics to the equivalent Protobuf QueryMetrics object
        """
        metrics_kwargs: dict[str, Any] = {
            'sort_count': metrics.sort_count(),
            'result_count': metrics.result_count(),
            'result_size': metrics.result_size(),
            'mutation_count': metrics.mutation_count(),
            'error_count': metrics.error_count(),
            'warning_count': metrics.warning_count(),
        }
        elapsed = duration.Duration()
        elapsed.FromTimedelta(metrics.elapsed_time())
        metrics_kwargs['elapsed_time'] = elapsed
        execution = duration.Duration()
        execution.FromTimedelta(metrics.execution_time())
        metrics_kwargs['execution_time'] = execution
        return query_pb.QueryMetrics(**metrics_kwargs)


class QueryCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._scope: Scope | None = kwargs.get('scope')
        self._statement: str = kwargs['statement']
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options: query_pb.QueryOptions | None = kwargs.get('options')
        self._content_as: content_pb.ContentAs = kwargs['content_as']
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')
        self._options: QueryOptions | None = None

    @QueryCommandResult.as_query_result
    def execute_command(self) -> QueryResult:
        if self._scope is not None:
            return self._scope.query(self._statement, self._options)
        if self._cluster is None:
            raise RuntimeError('A query command needs a cluster or a scope.')
        return self._cluster.query(self._statement, self._options)

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'scan_consistency': QueryCommandOptions.get_scan_consistency(self._raw_options),
            'raw': QueryCommandOptions.get_raw(self._raw_options),
            'adhoc': QueryCommandOptions.get_adhoc(self._raw_options),
            'profile': QueryCommandOptions.get_profile(self._raw_options),
            'read_only': QueryCommandOptions.get_read_only(self._raw_options),
            'positional_parameters': QueryCommandOptions.get_parameters_positional(self._raw_options),
            'named_parameters': QueryCommandOptions.get_parameters_named(self._raw_options),
            'flex_index': QueryCommandOptions.get_flex_index(self._raw_options),
            'pipeline_cap': QueryCommandOptions.get_pipeline_cap(self._raw_options),
            'pipeline_batch': QueryCommandOptions.get_pipeline_batch(self._raw_options),
            'scan_cap': QueryCommandOptions.get_scan_cap(self._raw_options),
            'scan_wait': QueryCommandOptions.get_scan_wait(self._raw_options),
            'timeout': QueryCommandOptions.get_timeout(self._raw_options),
            'max_parallelism': QueryCommandOptions.get_max_parallelism(self._raw_options),
            'metrics': QueryCommandOptions.get_metrics(self._raw_options),
            'span': QueryCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
            'client_context_id': QueryCommandOptions.get_client_context_id(self._raw_options),
            'preserve_expiry': QueryCommandOptions.get_preserve_expiry(self._raw_options),
            'consistent_with': QueryCommandOptions.get_consistent_with(self._raw_options),
        }
        opt_kwargs['use_replica'] = QueryCommandOptions.get_use_replica(self._raw_options)
        self._options = QueryOptions(**opt_kwargs)
        logger.debug(f"Query Options = {self._options}")

    @staticmethod
    def create_command(**kwargs: Any) -> QueryCommand:
        command = QueryCommand(**kwargs)
        command.set_options()
        return command


class QueryCommandBuilder:
    @staticmethod
    def build_command(query_cmd: query_pb.Command,
                      **cmd_kwargs: Any,
                      ) -> QueryCommand:
        cmd_kwargs.update({
            'statement': query_cmd.statement,
            'content_as': query_cmd.content_as,
        })

        if query_cmd.HasField('options'):
            cmd_kwargs['options'] = query_cmd.options

        return QueryCommand.create_command(**cmd_kwargs)
