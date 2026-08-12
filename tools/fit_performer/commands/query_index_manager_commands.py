from __future__ import annotations

import logging
from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import TYPE_CHECKING, List

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

from ..generated.run import top_level_pb2 as run_pb
from ..generated.sdk import workload_pb2 as sdk_pb
from ..generated.sdk.query import index_manager_pb2 as query_index_manager_pb
from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

if TYPE_CHECKING:
    from couchbase.management.queries import QueryIndex


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
    def get_collection_name(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "collection_name")

    @staticmethod
    def get_deferred(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "deferred")

    @staticmethod
    def get_ignore_if_exists(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "ignore_if_exists")

    @staticmethod
    def get_ignore_if_not_exists(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "ignore_if_not_exists")

    @staticmethod
    def get_index_name(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "index_name")

    @staticmethod
    def get_num_replicas(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "num_replicas")

    @staticmethod
    def get_scope_name(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "scope_name")

    @staticmethod
    def get_watch_primary(options):
        return QueryIndexManagerCommandOptions.get_simple_option(options, "watch_primary")


class QueryIndexManagerCommandBuilder:

    @classmethod
    def build_collection_level_command(cls, collection_query_idx_cmd, **cmd_kwargs):
        coll_cmd_type = collection_query_idx_cmd.WhichOneof('command')
        if coll_cmd_type == 'shared':
            return cls.build_shared_command(collection_query_idx_cmd.shared, **cmd_kwargs)
        else:
            raise NotImplementedError(
                f"Collection-level query index management command type `{coll_cmd_type}` not supported")

    @classmethod
    def build_cluster_level_command(cls, cluster_query_idx_cmd, **cmd_kwargs):
        cluster_cmd_type = cluster_query_idx_cmd.WhichOneof('command')
        if cluster_cmd_type == 'shared':
            cmd_kwargs['bucket_name'] = cluster_query_idx_cmd.bucket_name
            return cls.build_shared_command(cluster_query_idx_cmd.shared, **cmd_kwargs)
        else:
            raise NotImplementedError(
                f"Cluster-level query index manager command type {cluster_cmd_type} not supported")

    @classmethod
    def build_shared_command(cls, shared_cmd, **cmd_kwargs):
        cmd_type = shared_cmd.WhichOneof('command')
        if cmd_type == 'create_primary_index':
            cmd = shared_cmd.create_primary_index
            cmd_kwargs['options'] = cmd.options if cmd.HasField('options') else None
            return CreatePrimaryQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'create_index':
            cmd = shared_cmd.create_index
            cmd_kwargs.update({
                'index_name': cmd.index_name,
                'fields': list(cmd.fields),
                'options': cmd.options if cmd.HasField('options') else None,
            })
            return CreateQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'get_all_indexes':
            cmd = shared_cmd.get_all_indexes
            cmd_kwargs['options'] = cmd.options if cmd.HasField('options') else None
            return GetAllQueryIndexesCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'drop_primary_index':
            cmd = shared_cmd.drop_primary_index
            cmd_kwargs['options'] = cmd.options if cmd.HasField('options') else None
            return DropPrimaryQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'drop_index':
            cmd = shared_cmd.drop_index
            cmd_kwargs.update({
                'index_name': cmd.index_name,
                'options': cmd.options if cmd.HasField('options') else None,
            })
            return DropQueryIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'watch_indexes':
            cmd = shared_cmd.watch_indexes
            cmd_kwargs.update({
                'index_names': list(cmd.index_names),
                'timeout_msecs': cmd.timeout_msecs,
                'options': cmd.options if cmd.HasField('options') else None,
            })
            return WatchQueryIndexesCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'build_deferred_indexes':
            cmd = shared_cmd.build_deferred_indexes
            cmd_kwargs['options'] = cmd.options if cmd.HasField('options') else None
            return BuildDeferredQueryIndexesCommand.create_command(**cmd_kwargs)
        else:
            raise NotImplementedError(f"Query index management command type `{cmd_type}` not supported")


class QueryIndexManagerCommandResult(SdkCommandResult):

    _QUERY_INDEX_TYPE_MAPPING = {
        'view': query_index_manager_pb.QueryIndexType.VIEW,
        'gsi': query_index_manager_pb.QueryIndexType.GSI
    }

    @classmethod
    def as_query_indexes(cls, fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
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
                         sdk_query_indexes  # type: List[QueryIndex]
                         ) -> query_index_manager_pb.QueryIndexes:
        return query_index_manager_pb.QueryIndexes(
            indexes=[cls.to_query_index(sdk_index) for sdk_index in sdk_query_indexes]
        )

    @classmethod
    def to_query_index(cls,
                       sdk_index  # type: QueryIndex
                       ) -> query_index_manager_pb.QueryIndex:
        res = query_index_manager_pb.QueryIndex(
            name=sdk_index.name,
            is_primary=sdk_index.is_primary,
            type=cls._QUERY_INDEX_TYPE_MAPPING[sdk_index.type],
            state=sdk_index.state,
            keyspace=sdk_index.keyspace,
            index_key=sdk_index.index_key,
            bucket_name=sdk_index.bucket_name
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
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        opt_kwargs = {
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = GetAllQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_query_indexes
    def execute_command(self):
        return self._query_index_manager.get_all_indexes(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> GetAllQueryIndexesCommand:
        command = GetAllQueryIndexesCommand(**kwargs)
        command.set_options()
        return command


class CreatePrimaryQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._initiated = kwargs.get('initiated')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        opt_kwargs = {
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
    def execute_command(self):
        return self._query_index_manager.create_primary_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> CreatePrimaryQueryIndexCommand:
        command = CreatePrimaryQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class CreateQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._command_args.append(kwargs.get('index_name'))
        self._command_args.append(kwargs.get('fields'))
        self._initiated = kwargs.get('initiated')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        opt_kwargs = {
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
    def execute_command(self):
        return self._query_index_manager.create_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> CreateQueryIndexCommand:
        command = CreateQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class DropPrimaryQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._initiated = kwargs.get('initiated')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        opt_kwargs = {
            'ignore_if_not_exists': QueryIndexManagerCommandOptions.get_ignore_if_not_exists(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = DropPrimaryQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self):
        return self._query_index_manager.drop_primary_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> DropPrimaryQueryIndexCommand:
        command = DropPrimaryQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class DropQueryIndexCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._command_args.append(kwargs.get('index_name'))
        self._initiated = kwargs.get('initiated')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        opt_kwargs = {
            'ignore_if_not_exists': QueryIndexManagerCommandOptions.get_ignore_if_not_exists(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = DropQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self):
        return self._query_index_manager.drop_index(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> DropQueryIndexCommand:
        command = DropQueryIndexCommand(**kwargs)
        command.set_options()
        return command


class WatchQueryIndexesCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._command_args.append(kwargs.get('index_names'))
        self._timeout_msecs = kwargs.get('timeout_msecs')
        self._initiated = kwargs.get('initiated')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        opt_kwargs = {
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
    def execute_command(self):
        return self._query_index_manager.watch_indexes(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> WatchQueryIndexesCommand:
        command = WatchQueryIndexesCommand(**kwargs)
        command.set_options()
        return command


class BuildDeferredQueryIndexesCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_QUERY_MGMT_COMMAND_ARGS, **kwargs)
        self._cluster = kwargs.get('cluster')
        self._collection = kwargs.get('collection')
        self._command_args = []
        if self._cluster:
            self._query_index_manager = self._cluster.query_indexes()
            self._command_args.append(kwargs.get('bucket_name'))
        else:
            self._query_index_manager = self._collection.query_indexes()
        self._initiated = kwargs.get('initiated')
        self._raw_options = kwargs.get('options')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        opt_kwargs = {
            'timeout': QueryIndexManagerCommandOptions.get_timeout(self._raw_options),
            'scope_name': QueryIndexManagerCommandOptions.get_scope_name(self._raw_options),
            'collection_name': QueryIndexManagerCommandOptions.get_collection_name(self._raw_options),
            'parent_span': QueryIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        options = BuildDeferredQueryIndexOptions(**opt_kwargs)
        self._command_args.append(options)

    @QueryIndexManagerCommandResult.as_success
    def execute_command(self):
        return self._query_index_manager.build_deferred_indexes(*self._command_args)

    @staticmethod
    def create_command(**kwargs) -> BuildDeferredQueryIndexesCommand:
        command = BuildDeferredQueryIndexesCommand(**kwargs)
        command.set_options()
        return command
