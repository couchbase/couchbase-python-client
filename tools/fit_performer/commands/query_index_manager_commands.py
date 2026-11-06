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

import logging
from collections.abc import Callable, Iterable
from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (TYPE_CHECKING,
                    Any,
                    Concatenate,
                    ParamSpec,
                    Protocol,
                    TypeVar)

from google.protobuf import timestamp_pb2 as timestamp

from couchbase.cluster import Cluster
from couchbase.collection import Collection
from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                          CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions,
                                          WatchQueryIndexOptions)

from run import top_level_pb2 as run_pb
from sdk import workload_pb2 as sdk_pb
from sdk.query import index_manager_pb2 as query_index_manager_pb

from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

if TYPE_CHECKING:
    from couchbase.management.queries import (CollectionQueryIndexManager,
                                              QueryIndex,
                                              QueryIndexManager)

    from sdk.cluster.query import index_manager_pb2 as cluster_query_index_manager_pb
    from sdk.collection.query import index_manager_pb2 as collection_query_index_manager_pb

    from ..telemetry.span_owner import SpanOwner


class _ResultCommand(Protocol):
    _initiated: timestamp.Timestamp
    _return_result: bool


C = TypeVar('C', bound=_ResultCommand)
P = ParamSpec('P')

VALID_QUERY_MGMT_COMMAND_ARGS = {
    'collection': lambda c: c is None or isinstance(c, Collection),
    'cluster': lambda c: c is None or isinstance(c, Cluster),
    'return_result': lambda rr: isinstance(rr, bool),
    'initiated': lambda i: isinstance(i, timestamp.Timestamp),
    'index_name': lambda n: n is None or isinstance(n, str),
    'index_names': lambda ns: ns is None or (isinstance(ns, list) and all(isinstance(n, str) for n in ns)),
    'fields': lambda fs: fs is None or (isinstance(fs, list) and all(isinstance(f, str) for f in fs)),
    'bucket_name': lambda bn: bn is None or isinstance(bn, str),
    'timeout_msecs': lambda t: t is None or isinstance(t, int),
    'options': lambda o: True,
    'span_owner': lambda s: True
}


class QueryIndexManagerCommandOptions(SdkCommandOptions):
    @staticmethod
    def get_collection_name(options: Any) -> str | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "collection_name")

    @staticmethod
    def get_deferred(options: Any) -> bool | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "deferred")

    @staticmethod
    def get_ignore_if_exists(options: Any) -> bool | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "ignore_if_exists")

    @staticmethod
    def get_ignore_if_not_exists(options: Any) -> bool | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "ignore_if_not_exists")

    @staticmethod
    def get_index_name(options: Any) -> str | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "index_name")

    @staticmethod
    def get_num_replicas(options: Any) -> int | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "num_replicas")

    @staticmethod
    def get_scope_name(options: Any) -> str | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "scope_name")

    @staticmethod
    def get_watch_primary(options: Any) -> bool | None:
        return QueryIndexManagerCommandOptions.get_simple_option(options, "watch_primary")


class QueryIndexManagerCommandBuilder:

    @classmethod
    def build_collection_level_command(cls,
                                       collection_query_idx_cmd: collection_query_index_manager_pb.Command,
                                       **cmd_kwargs: Any,
                                       ) -> SdkCommand:
        coll_cmd_type = collection_query_idx_cmd.WhichOneof('command')
        if coll_cmd_type == 'shared':
            return cls.build_shared_command(collection_query_idx_cmd.shared, **cmd_kwargs)
        else:
            raise NotImplementedError(
                f"Collection-level query index management command type `{coll_cmd_type}` not supported")

    @classmethod
    def build_cluster_level_command(cls,
                                    cluster_query_idx_cmd: cluster_query_index_manager_pb.Command,
                                    **cmd_kwargs: Any,
                                    ) -> SdkCommand:
        cluster_cmd_type = cluster_query_idx_cmd.WhichOneof('command')
        if cluster_cmd_type == 'shared':
            cmd_kwargs['bucket_name'] = cluster_query_idx_cmd.bucket_name
            return cls.build_shared_command(cluster_query_idx_cmd.shared, **cmd_kwargs)
        else:
            raise NotImplementedError(
                f"Cluster-level query index manager command type {cluster_cmd_type} not supported")

    @classmethod
    def build_shared_command(cls, shared_cmd: query_index_manager_pb.Command, **cmd_kwargs: Any) -> SdkCommand:
        cmd_type = shared_cmd.WhichOneof('command')
        if cmd_type == 'create_primary_index':
            create_primary_cmd = shared_cmd.create_primary_index
            cmd_kwargs['options'] = create_primary_cmd.options if create_primary_cmd.HasField('options') else None
            return CreatePrimaryQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'create_index':
            create_cmd = shared_cmd.create_index
            cmd_kwargs.update({
                'index_name': create_cmd.index_name,
                'fields': list(create_cmd.fields),
                'options': create_cmd.options if create_cmd.HasField('options') else None,
            })
            return CreateQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'get_all_indexes':
            get_all_cmd = shared_cmd.get_all_indexes
            cmd_kwargs['options'] = get_all_cmd.options if get_all_cmd.HasField('options') else None
            return GetAllQueryIndexesCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'drop_primary_index':
            drop_primary_cmd = shared_cmd.drop_primary_index
            cmd_kwargs['options'] = drop_primary_cmd.options if drop_primary_cmd.HasField('options') else None
            return DropPrimaryQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'drop_index':
            drop_cmd = shared_cmd.drop_index
            cmd_kwargs.update({
                'index_name': drop_cmd.index_name,
                'options': drop_cmd.options if drop_cmd.HasField('options') else None,
            })
            return DropQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'watch_indexes':
            watch_cmd = shared_cmd.watch_indexes
            cmd_kwargs.update({
                'index_names': list(watch_cmd.index_names),
                'timeout_msecs': watch_cmd.timeout_msecs,
                'options': watch_cmd.options if watch_cmd.HasField('options') else None,
            })
            return WatchQueryIndexesCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'build_deferred_indexes':
            build_cmd = shared_cmd.build_deferred_indexes
            cmd_kwargs['options'] = build_cmd.options if build_cmd.HasField('options') else None
            return BuildDeferredQueryIndexesCommand.create_command(**cmd_kwargs)
        else:
            raise NotImplementedError(f"Query index management command type `{cmd_type}` not supported")


class QueryIndexManagerCommandResult(SdkCommandResult):

    _QUERY_INDEX_TYPE_MAPPING = {
        'view': query_index_manager_pb.QueryIndexType.VIEW,
        'gsi': query_index_manager_pb.QueryIndexType.GSI
    }

    @classmethod
    def as_query_indexes(cls,
                         fn: Callable[Concatenate[C, P], Iterable[QueryIndex]]
                         ) -> Callable[Concatenate[C, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: C, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(query_indexes=cls.to_query_indexes(res))
                else:
                    sdk_result = sdk_pb.Result(success=True)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def to_query_indexes(cls,
                         sdk_query_indexes: Iterable[QueryIndex]
                         ) -> query_index_manager_pb.QueryIndexes:
        return query_index_manager_pb.QueryIndexes(
            indexes=[cls.to_query_index(sdk_index) for sdk_index in sdk_query_indexes]
        )

    @classmethod
    def to_query_index(cls,
                       sdk_index: QueryIndex
                       ) -> query_index_manager_pb.QueryIndex:
        res = query_index_manager_pb.QueryIndex(
            name=sdk_index.name,
            is_primary=sdk_index.is_primary,
            type=cls._QUERY_INDEX_TYPE_MAPPING[sdk_index.type],
            state=sdk_index.state,
            # Neither field has presence, so an unset name and an empty one are the same message.
            keyspace=sdk_index.keyspace or '',
            index_key=sdk_index.index_key,
            bucket_name=sdk_index.bucket_name or ''
        )

        if sdk_index.condition:
            res.condition = sdk_index.condition
        if sdk_index.partition:
            res.partition = sdk_index.partition
        if sdk_index.scope_name:
            res.scope_name = sdk_index.scope_name
        if sdk_index.collection_name:
            res.collection_name = sdk_index.collection_name

        return res


class GetAllQueryIndexesCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._initiated = kwargs['initiated']
        self._return_result: bool = kwargs['return_result']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = GetAllQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_query_indexes
    def execute_command(self) -> Iterable[QueryIndex]:
        return self._query_index_manager.get_all_indexes(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> GetAllQueryIndexesCommand:
        command = GetAllQueryIndexesCommand(**kwargs)
        command.set_options()
        return command


class CreatePrimaryQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._initiated = kwargs['initiated']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'ignore_if_exists': QueryIndexManagerCommandOptions.get_ignore_if_exists(self._raw_options),
            'num_replicas': QueryIndexManagerCommandOptions.get_num_replicas(self._raw_options),
            'deferred': QueryIndexManagerCommandOptions.get_deferred(self._raw_options),
            'index_name': QueryIndexManagerCommandOptions.get_index_name(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = CreatePrimaryQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._query_index_manager.create_primary_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> CreatePrimaryQueryIndexCommand:
        command = CreatePrimaryQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class CreateQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._command_args.append(kwargs.get('index_name'))
        self._command_args.append(kwargs.get('fields'))
        self._initiated = kwargs['initiated']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'ignore_if_exists': QueryIndexManagerCommandOptions.get_ignore_if_exists(self._raw_options),
            'num_replicas': QueryIndexManagerCommandOptions.get_num_replicas(self._raw_options),
            'deferred': QueryIndexManagerCommandOptions.get_deferred(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = CreateQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

        logger = logging.getLogger(__name__)
        logger.info(str(self._command_args))

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._query_index_manager.create_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> CreateQueryIndexCommand:
        command = CreateQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class DropPrimaryQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._initiated = kwargs['initiated']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'ignore_if_not_exists': QueryIndexManagerCommandOptions.get_ignore_if_not_exists(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = DropPrimaryQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._query_index_manager.drop_primary_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> DropPrimaryQueryIndexCommand:
        command = DropPrimaryQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class DropQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._command_args.append(kwargs.get('index_name'))
        self._initiated = kwargs['initiated']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'ignore_if_not_exists': QueryIndexManagerCommandOptions.get_ignore_if_not_exists(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = DropQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._query_index_manager.drop_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> DropQueryIndexCommand:
        command = DropQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class WatchQueryIndexesCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._command_args.append(kwargs.get('index_names'))
        self._timeout_msecs: int = kwargs['timeout_msecs']
        self._initiated = kwargs['initiated']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        opt_kwargs: dict[str, Any] = {
            'timeout': timedelta(milliseconds=self._timeout_msecs),
        }

        if self._raw_options is not None:
            opt_kwargs.update({
                'watch_primary': QueryIndexManagerCommandOptions.get_watch_primary(self._raw_options),
                'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
                'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            })

        if self._raw_options is not None and self._span_owner is not None:
            opt_kwargs['parent_span'] = QueryIndexManagerCommandOptions.resolve_parent_span(
                self._raw_options, self._span_owner)

        options = WatchQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._query_index_manager.watch_indexes(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> WatchQueryIndexesCommand:
        command = WatchQueryIndexesCommand(**kwargs)
        command.set_options()
        return command


class BuildDeferredQueryIndexesCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._collection: Collection | None = kwargs.get('collection')
        # The positional arguments of whichever manager is in use: the cluster's takes the bucket name first.
        self._command_args: list[Any] = []
        self._query_index_manager: QueryIndexManager | CollectionQueryIndexManager
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        elif self._collection is not None:
            self._query_index_manager = self._collection.query_indexes()
        else:
            raise ValueError('A query index management command needs a cluster or a collection.')
        self._initiated = kwargs['initiated']
        self._raw_options = kwargs.get('options')
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = BuildDeferredQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._query_index_manager.build_deferred_indexes(*self._command_args)

    @staticmethod
    def create_command(**kwargs: Any) -> BuildDeferredQueryIndexesCommand:
        command = BuildDeferredQueryIndexesCommand(**kwargs)
        command.set_options()
        return command
