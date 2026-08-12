from __future__ import annotations

import logging
from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (Any,
                    Dict,
                    List)

import google.protobuf.timestamp_pb2 as timestamp_pb

from couchbase.cluster import Cluster
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          CompressionMode,
                                          ConflictResolutionType,
                                          CreateBucketSettings,
                                          EvictionPolicyType,
                                          StorageBackend)
from couchbase.management.options import (CreateBucketOptions,
                                          DropBucketOptions,
                                          FlushBucketOptions,
                                          GetAllBucketOptions,
                                          GetBucketOptions,
                                          UpdateBucketOptions)

from ..generated.run import top_level_pb2 as run_pb
from ..generated.sdk import workload_pb2 as sdk_pb
from ..generated.sdk.cluster import bucket_manager_pb2 as bucket_manager_pb
from .sdk_commands import (DURABILITY_LEVEL_MAP,
                           SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

VALID_BUCKET_MANAGER_COMMAND_ARGS = {
    'return_result': lambda r: isinstance(r, bool),
    'initiated': lambda i: isinstance(i, timestamp_pb.Timestamp),
    'options': lambda o: o is not None,
    'cluster': lambda c: isinstance(c, Cluster),
    'bucket_name': lambda n: n is None or isinstance(n, str),
    'settings': lambda s: s is None or isinstance(s, BucketSettings),
    'span_owner': lambda s: True
}

BUCKET_TYPE_MAP = {
    bucket_manager_pb.BucketType.COUCHBASE: BucketType.COUCHBASE,
    bucket_manager_pb.BucketType.EPHEMERAL: BucketType.EPHEMERAL,
    bucket_manager_pb.BucketType.MEMCACHED: BucketType.MEMCACHED,
}

EVICTION_POLICY_TYPE_MAP = {
    bucket_manager_pb.EvictionPolicyType.FULL: EvictionPolicyType.FULL,
    bucket_manager_pb.EvictionPolicyType.NO_EVICTION: EvictionPolicyType.NO_EVICTION,
    bucket_manager_pb.EvictionPolicyType.NOT_RECENTLY_USED: EvictionPolicyType.NOT_RECENTLY_USED,
    bucket_manager_pb.EvictionPolicyType.VALUE_ONLY: EvictionPolicyType.VALUE_ONLY,
}

COMPRESSION_MODE_MAP = {
    bucket_manager_pb.CompressionMode.ACTIVE: CompressionMode.ACTIVE,
    bucket_manager_pb.CompressionMode.OFF: CompressionMode.OFF,
    bucket_manager_pb.CompressionMode.PASSIVE: CompressionMode.PASSIVE,
}

STORAGE_BACKEND_MAP = {
    bucket_manager_pb.StorageBackend.COUCHSTORE: StorageBackend.COUCHSTORE,
    bucket_manager_pb.StorageBackend.MAGMA: StorageBackend.MAGMA,
}

CONFLICT_RESOLUTION_TYPE_MAP = {
    bucket_manager_pb.ConflictResolutionType.TIMESTAMP: ConflictResolutionType.TIMESTAMP,
    bucket_manager_pb.ConflictResolutionType.SEQUENCE_NUMBER: ConflictResolutionType.SEQUENCE_NUMBER,
    bucket_manager_pb.ConflictResolutionType.CUSTOM: ConflictResolutionType.CUSTOM,
}

INV_BUCKET_TYPE_MAP = {v: k for k, v in BUCKET_TYPE_MAP.items()}
INV_EVICTION_POLICY_TYPE_MAP = {v: k for k, v in EVICTION_POLICY_TYPE_MAP.items()}
INV_COMPRESSION_MODE_MAP = {v: k for k, v in COMPRESSION_MODE_MAP.items()}
INV_STORAGE_BACKEND_MAP = {v: k for k, v in STORAGE_BACKEND_MAP.items()}
INV_CONFLICT_RESOLUTION_TYPE_MAP = {v: k for k, v in CONFLICT_RESOLUTION_TYPE_MAP.items()}
INV_DURABILITY_LEVEL_MAP = {v: k for k, v in DURABILITY_LEVEL_MAP.items()}

logger = logging.getLogger(__name__)


class BucketManagerCommandOptions(SdkCommandOptions):
    pass


class BucketManagerCommandResult(SdkCommandResult):
    @classmethod
    def as_bucket_settings(cls, fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(
                        bucket_manager_result=bucket_manager_pb.Result(
                            bucket_settings=cls.to_bucket_settings(res)
                        )
                    )
                else:
                    sdk_result = sdk_pb.Result(success=True)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_get_all_buckets_result(cls, fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(
                        bucket_manager_result=bucket_manager_pb.Result(
                            get_all_buckets_result=cls.to_get_all_buckets_result(res)
                        )
                    )
                else:
                    sdk_result = sdk_pb.Result(success=True)
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def to_bucket_settings(cls, sdk_settings: BucketSettings) -> bucket_manager_pb.BucketSettings:  # noqa: C901
        kwargs = {
            'name': sdk_settings.name,
            'ram_quota_MB': sdk_settings['ram_quota_mb']
        }
        if sdk_settings.flush_enabled is not None:
            kwargs['flush_enabled'] = sdk_settings.flush_enabled
        if sdk_settings.num_replicas is not None:
            kwargs['num_replicas'] = sdk_settings.num_replicas
        if sdk_settings.replica_index is not None:
            kwargs['replica_indexes'] = sdk_settings.replica_index
        if sdk_settings.bucket_type is not None:
            kwargs['bucket_type'] = INV_BUCKET_TYPE_MAP[sdk_settings.bucket_type]
        if sdk_settings.eviction_policy is not None:
            kwargs['eviction_policy'] = INV_EVICTION_POLICY_TYPE_MAP[sdk_settings.eviction_policy]
        if sdk_settings.max_expiry is not None:
            kwargs['max_expiry_seconds'] = round(sdk_settings.max_expiry.total_seconds())
        if sdk_settings.compression_mode is not None:
            kwargs['compression_mode'] = INV_COMPRESSION_MODE_MAP[sdk_settings.compression_mode]
        if 'minimum_durability_level' in sdk_settings:
            kwargs['minimum_durability_level'] = INV_DURABILITY_LEVEL_MAP[sdk_settings['minimum_durability_level']]
        if sdk_settings.storage_backend is not None and sdk_settings.storage_backend != StorageBackend.UNDEFINED:
            kwargs['storage_backend'] = INV_STORAGE_BACKEND_MAP[sdk_settings.storage_backend]
        if sdk_settings.history_retention_collection_default is not None:
            kwargs['history_retention_collection_default'] = sdk_settings.history_retention_collection_default
        if sdk_settings.history_retention_duration is not None:
            kwargs['history_retention_seconds'] = round(sdk_settings.history_retention_duration.total_seconds())
        if sdk_settings.history_retention_bytes is not None:
            kwargs['history_retention_bytes'] = sdk_settings.history_retention_bytes
        if sdk_settings.num_vbuckets is not None:
            kwargs['num_vbuckets'] = sdk_settings.num_vbuckets
        return bucket_manager_pb.BucketSettings(**kwargs)

    @classmethod
    def to_get_all_buckets_result(cls, sdk_res: List[BucketSettings]):
        res = bucket_manager_pb.GetAllBucketsResult()
        for settings in sdk_res:
            res.result[settings.name] = cls.to_bucket_settings(settings)

        return res


class GetBucketCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_BUCKET_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._bucket_manager = self._cluster.buckets()
        self._return_result = kwargs.get('return_result')
        self._bucket_name = kwargs.get('bucket_name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = GetBucketOptions(
            timeout=BucketManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=BucketManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @BucketManagerCommandResult.as_bucket_settings
    def execute_command(self) -> run_pb.Result:
        return self._bucket_manager.get_bucket(self._bucket_name, self._options)

    @staticmethod
    def create_command(**kwargs) -> GetBucketCommand:
        command = GetBucketCommand(**kwargs)
        command.set_options()
        return command


class GetAllBucketsCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_BUCKET_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._bucket_manager = self._cluster.buckets()
        self._return_result = kwargs.get('return_result')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = GetAllBucketOptions(
            timeout=BucketManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=BucketManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @BucketManagerCommandResult.as_get_all_buckets_result
    def execute_command(self) -> run_pb.Result:
        return self._bucket_manager.get_all_buckets(self._options)

    @staticmethod
    def create_command(**kwargs) -> GetAllBucketsCommand:
        command = GetAllBucketsCommand(**kwargs)
        command.set_options()
        return command


class CreateBucketCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_BUCKET_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._bucket_manager = self._cluster.buckets()
        self._settings = kwargs.get('settings')
        self._return_result = kwargs.get('return_result')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = CreateBucketOptions(
            timeout=BucketManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=BucketManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @BucketManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._bucket_manager.create_bucket(self._settings, self._options)

    @staticmethod
    def create_command(**kwargs) -> CreateBucketCommand:
        command = CreateBucketCommand(**kwargs)
        command.set_options()
        return command


class DropBucketCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_BUCKET_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._bucket_manager = self._cluster.buckets()
        self._bucket_name = kwargs.get('bucket_name')
        self._return_result = kwargs.get('return_result')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = DropBucketOptions(
            timeout=BucketManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=BucketManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @BucketManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._bucket_manager.drop_bucket(self._bucket_name, self._options)

    @staticmethod
    def create_command(**kwargs) -> DropBucketCommand:
        command = DropBucketCommand(**kwargs)
        command.set_options()
        return command


class FlushBucketCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_BUCKET_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._bucket_manager = self._cluster.buckets()
        self._bucket_name = kwargs.get('bucket_name')
        self._return_result = kwargs.get('return_result')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = FlushBucketOptions(
            timeout=BucketManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=BucketManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @BucketManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._bucket_manager.flush_bucket(self._bucket_name, self._options)

    @staticmethod
    def create_command(**kwargs) -> FlushBucketCommand:
        command = FlushBucketCommand(**kwargs)
        command.set_options()
        return command


class UpdateBucketCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_BUCKET_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._bucket_manager = self._cluster.buckets()
        self._settings = kwargs.get('settings')
        self._return_result = kwargs.get('return_result')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = UpdateBucketOptions(
            timeout=BucketManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=BucketManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @BucketManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._bucket_manager.update_bucket(self._settings, self._options)

    @staticmethod
    def create_command(**kwargs) -> UpdateBucketCommand:
        command = UpdateBucketCommand(**kwargs)
        command.set_options()
        return command


class BucketManagerCommandBuilder:
    @staticmethod
    def _get_bucket_settings_kwargs(proto_settings: bucket_manager_pb.BucketSettings) -> Dict[str, Any]:  # noqa: C901
        kwargs = {
            'name': proto_settings.name,
            'ram_quota_mb': proto_settings.ram_quota_MB,
        }

        if proto_settings.HasField('flush_enabled'):
            kwargs['flush_enabled'] = proto_settings.flush_enabled
        if proto_settings.HasField('num_replicas'):
            kwargs['num_replicas'] = proto_settings.num_replicas
        if proto_settings.HasField('replica_indexes'):
            kwargs['replica_index'] = proto_settings.replica_indexes
        if proto_settings.HasField('bucket_type'):
            kwargs['bucket_type'] = BUCKET_TYPE_MAP[proto_settings.bucket_type]
        if proto_settings.HasField('eviction_policy'):
            kwargs['eviction_policy'] = EVICTION_POLICY_TYPE_MAP[proto_settings.eviction_policy]
        if proto_settings.HasField('max_expiry_seconds'):
            kwargs['max_expiry'] = timedelta(seconds=proto_settings.max_expiry_seconds)
        if proto_settings.HasField('compression_mode'):
            kwargs['compression_mode'] = COMPRESSION_MODE_MAP[proto_settings.compression_mode]
        if proto_settings.HasField('minimum_durability_level'):
            kwargs['minimum_durability_level'] = DURABILITY_LEVEL_MAP[proto_settings.minimum_durability_level]
        if proto_settings.HasField('storage_backend'):
            kwargs['storage_backend'] = STORAGE_BACKEND_MAP[proto_settings.storage_backend]
        if proto_settings.HasField('history_retention_collection_default'):
            kwargs['history_retention_collection_default'] = proto_settings.history_retention_collection_default
        if proto_settings.HasField('history_retention_seconds'):
            kwargs['history_retention_duration'] = timedelta(seconds=proto_settings.history_retention_seconds)
        if proto_settings.HasField('history_retention_bytes'):
            kwargs['history_retention_bytes'] = proto_settings.history_retention_bytes
        if proto_settings.HasField('num_vbuckets'):
            kwargs['num_vbuckets'] = proto_settings.num_vbuckets

        return kwargs

    @classmethod
    def _get_create_bucket_settings(cls, proto_settings: bucket_manager_pb.CreateBucketSettings
                                    ) -> CreateBucketSettings:
        kwargs = cls._get_bucket_settings_kwargs(proto_settings.settings)
        if proto_settings.HasField('conflict_resolution_type'):
            kwargs['conflict_resolution_type'] = CONFLICT_RESOLUTION_TYPE_MAP[proto_settings.conflict_resolution_type]
        return CreateBucketSettings(**kwargs)

    @classmethod
    def _get_bucket_settings(cls, proto_settings: bucket_manager_pb.BucketSettings) -> BucketSettings:
        kwargs = cls._get_bucket_settings_kwargs(proto_settings)
        return BucketSettings(**kwargs)

    @classmethod
    def build_command(cls,
                      bucket_mgr_cmd: bucket_manager_pb.Command,
                      **cmd_kwargs: Dict[str, Any]
                      ) -> SdkCommand:
        cmd_type = bucket_mgr_cmd.WhichOneof('command')
        cmd = getattr(bucket_mgr_cmd, cmd_type)

        if cmd.HasField('options'):
            cmd_kwargs['options'] = cmd.options

        if cmd_type == 'get_bucket':
            cmd_kwargs['bucket_name'] = cmd.bucket_name
            return GetBucketCommand.create_command(**cmd_kwargs)
        if cmd_type == 'get_all_buckets':
            return GetAllBucketsCommand.create_command(**cmd_kwargs)
        if cmd_type == 'create_bucket':
            cmd_kwargs['settings'] = cls._get_create_bucket_settings(cmd.settings)
            return CreateBucketCommand.create_command(**cmd_kwargs)
        if cmd_type == 'drop_bucket':
            cmd_kwargs['bucket_name'] = cmd.bucket_name
            return DropBucketCommand.create_command(**cmd_kwargs)
        if cmd_type == 'flush_bucket':
            cmd_kwargs['bucket_name'] = cmd.bucket_name
            return FlushBucketCommand.create_command(**cmd_kwargs)
        if cmd_type == 'update_bucket':
            cmd_kwargs['settings'] = cls._get_bucket_settings(cmd.settings)
            return UpdateBucketCommand.create_command(**cmd_kwargs)
        raise NotImplementedError(f'Bucket management command {cmd_type} not supported by performer')
