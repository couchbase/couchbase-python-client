from __future__ import annotations

from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (Iterable,
                    Optional,
                    Union)

from couchbase.logic.n1ql import QueryScanConsistency
from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionBucketAccess,
                                           EventingFunctionBucketBinding,
                                           EventingFunctionConstantBinding,
                                           EventingFunctionDcpBoundary,
                                           EventingFunctionDeploymentStatus,
                                           EventingFunctionKeyspace,
                                           EventingFunctionLanguageCompatibility,
                                           EventingFunctionLogLevel,
                                           EventingFunctionProcessingStatus,
                                           EventingFunctionSettings,
                                           EventingFunctionsStatus,
                                           EventingFunctionState,
                                           EventingFunctionStatus,
                                           EventingFunctionUrlAuthBasic,
                                           EventingFunctionUrlAuthBearer,
                                           EventingFunctionUrlAuthDigest,
                                           EventingFunctionUrlBinding,
                                           EventingFunctionUrlNoAuth)
from couchbase.management.options import (DeployFunctionOptions,
                                          DropFunctionOptions,
                                          FunctionsStatusOptions,
                                          GetAllFunctionOptions,
                                          GetFunctionOptions,
                                          PauseFunctionOptions,
                                          ResumeFunctionOptions,
                                          UndeployFunctionOptions,
                                          UpsertFunctionOptions)

from ..generated.run import top_level_pb2 as run_pb
from ..generated.sdk import workload_pb2 as sdk_pb
from ..generated.sdk.cluster import eventing_function_manager_pb2 as eventing_pb
from ..generated.sdk.scope import eventing_function_manager_pb2 as scope_eventing_pb
from ..generated.shared import basic_pb2 as basic_pb
from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult)

BUCKET_ACCESS_MAP = {
    eventing_pb.READ_ONLY: EventingFunctionBucketAccess.ReadOnly,
    eventing_pb.READ_WRITE: EventingFunctionBucketAccess.ReadWrite,
}

DCP_BOUNDARY_MAP = {
    eventing_pb.EVERYTHING: EventingFunctionDcpBoundary.Everything,
    eventing_pb.FROM_NOW: EventingFunctionDcpBoundary.FromNow,
}

DEPLOYMENT_STATUS_MAP = {
    eventing_pb.DEPLOYMENT_STATUS_DEPLOYED: EventingFunctionDeploymentStatus.Deployed,
    eventing_pb.DEPLOYMENT_STATUS_UNDEPLOYED: EventingFunctionDeploymentStatus.Undeployed,
}

PROCESSING_STATUS_MAP = {
    eventing_pb.PROCESSING_STATUS_PAUSED: EventingFunctionProcessingStatus.Paused,
    eventing_pb.PROCESSING_STATUS_RUNNING: EventingFunctionProcessingStatus.Running,
}

LANGUAGE_COMPATIBILITY_MAP = {
    eventing_pb.VERSION_6_0_0: EventingFunctionLanguageCompatibility.Version_6_0_0,
    eventing_pb.VERSION_6_5_0: EventingFunctionLanguageCompatibility.Version_6_5_0,
    eventing_pb.VERSION_6_6_2: EventingFunctionLanguageCompatibility.Version_6_6_2,
    eventing_pb.VERSION_7_2_0: EventingFunctionLanguageCompatibility.Version_7_2_0,
}

LOG_LEVEL_MAP = {
    eventing_pb.DEBUG: EventingFunctionLogLevel.Debug,
    eventing_pb.ERROR: EventingFunctionLogLevel.Error,
    eventing_pb.INFO: EventingFunctionLogLevel.Info,
    eventing_pb.TRACE: EventingFunctionLogLevel.Trace,
    eventing_pb.WARNING: EventingFunctionLogLevel.Warning,
}

SCAN_CONSISTENCY_MAP = {
    basic_pb.REQUEST_PLUS: QueryScanConsistency.REQUEST_PLUS,
    basic_pb.NOT_BOUNDED: QueryScanConsistency.NOT_BOUNDED,
}

FUNCTION_STATE_MAP = {
    EventingFunctionState.Deployed: eventing_pb.DEPLOYED,
    EventingFunctionState.Deploying: eventing_pb.DEPLOYING,
    EventingFunctionState.Paused: eventing_pb.PAUSED,
    EventingFunctionState.Pausing: eventing_pb.PAUSING,
    EventingFunctionState.Undeployed: eventing_pb.UNDEPLOYED,
    EventingFunctionState.Undeploying: eventing_pb.UNDEPLOYING,
    EventingFunctionState.Resuming: eventing_pb.RESUMING,
    EventingFunctionState.Unknown: eventing_pb.UNKNOWN,
}


def _reverse(mapping):
    return {v: k for k, v in mapping.items()}


class EventingFunctionManagerCommandOptions(SdkCommandOptions):

    @staticmethod
    def get_timeout(options) -> Optional[timedelta]:
        # The eventing protocol uses a google.protobuf.Duration rather than the timeout_msecs used elsewhere
        if not options.HasField('timeout'):
            return None
        return options.timeout.ToTimedelta()

    @staticmethod
    def to_keyspace(proto_keyspace: eventing_pb.EventingFunctionKeyspace) -> EventingFunctionKeyspace:
        return EventingFunctionKeyspace(
            bucket=proto_keyspace.bucket,
            scope=proto_keyspace.scope if proto_keyspace.HasField('scope') else None,
            collection=proto_keyspace.collection if proto_keyspace.HasField('collection') else None,
        )

    @classmethod
    def to_bucket_binding(cls,
                          proto_binding: eventing_pb.EventingFunctionBucketBinding
                          ) -> EventingFunctionBucketBinding:
        return EventingFunctionBucketBinding(
            alias=proto_binding.alias,
            name=cls.to_keyspace(proto_binding.name),
            access=BUCKET_ACCESS_MAP[proto_binding.access],
        )

    @staticmethod
    def to_url_binding(proto_binding: eventing_pb.EventingFunctionUrlBinding) -> EventingFunctionUrlBinding:
        auth_type = proto_binding.auth.WhichOneof('auth')
        if auth_type == 'basic_auth':
            auth = EventingFunctionUrlAuthBasic(username=proto_binding.auth.basic_auth.username,
                                                password=proto_binding.auth.basic_auth.password)
        elif auth_type == 'digest_auth':
            auth = EventingFunctionUrlAuthDigest(username=proto_binding.auth.digest_auth.username,
                                                 password=proto_binding.auth.digest_auth.password)
        elif auth_type == 'bearer_auth':
            auth = EventingFunctionUrlAuthBearer(key=proto_binding.auth.bearer_auth.key)
        else:
            auth = EventingFunctionUrlNoAuth()

        return EventingFunctionUrlBinding(
            hostname=proto_binding.hostname,
            alias=proto_binding.alias,
            allow_cookies=proto_binding.allow_cookies,
            validate_ssl_certificate=proto_binding.validate_ssl_certificate,
            auth=auth,
        )

    @staticmethod
    def to_settings(proto_settings: eventing_pb.EventingFunctionSettings) -> EventingFunctionSettings:  # noqa: C901
        kwargs = {}

        simple_fields = {
            # proto field name -> SDK field name
            'app_log_dir': 'app_log_dir',
            'app_log_max_files': 'app_log_max_files',
            'app_log_max_size': 'app_log_max_size',
            'bucket_cache_size': 'bucket_cache_size',
            'cpp_worker_thread_count': 'cpp_worker_thread_count',
            'curl_max_allowed_resp_size': 'curl_max_allowed_resp_size',
            'description': 'description',
            'enable_app_log_rotation': 'enable_applog_rotation',
            'lcb_inst_capacity': 'lcb_inst_capacity',
            'lcb_retry_count': 'lcb_retry_count',
            'num_timer_partitions': 'num_timer_partitions',
            'query_prepare_all': 'query_prepare_all',
            'sock_batch_size': 'sock_batch_size',
            'timer_context_size': 'timer_context_size',
            'user_prefix': 'user_prefix',
            'worker_count': 'worker_count',
        }
        for proto_name, sdk_name in simple_fields.items():
            if proto_settings.HasField(proto_name):
                kwargs[sdk_name] = getattr(proto_settings, proto_name)

        if proto_settings.HasField('checkpoint_interval'):
            kwargs['checkpoint_interval'] = proto_settings.checkpoint_interval.ToTimedelta()
        if proto_settings.HasField('execution_timeout'):
            kwargs['execution_timeout'] = proto_settings.execution_timeout.ToTimedelta()
        if proto_settings.HasField('lcb_timeout'):
            kwargs['lcb_timeout'] = proto_settings.lcb_timeout.ToTimedelta()
        if proto_settings.HasField('tick_duration'):
            kwargs['tick_duration'] = proto_settings.tick_duration.ToTimedelta()

        # bucket_cache_age is in milliseconds
        if proto_settings.HasField('bucket_cache_age'):
            kwargs['bucket_cache_age'] = timedelta(milliseconds=proto_settings.bucket_cache_age)

        if proto_settings.HasField('dcp_stream_boundary'):
            kwargs['dcp_stream_boundary'] = DCP_BOUNDARY_MAP[proto_settings.dcp_stream_boundary]
        if proto_settings.HasField('deployment_status'):
            kwargs['deployment_status'] = DEPLOYMENT_STATUS_MAP[proto_settings.deployment_status]
        if proto_settings.HasField('processing_status'):
            kwargs['processing_status'] = PROCESSING_STATUS_MAP[proto_settings.processing_status]
        if proto_settings.HasField('language_compatibility'):
            kwargs['language_compatibility'] = LANGUAGE_COMPATIBILITY_MAP[proto_settings.language_compatibility]
        if proto_settings.HasField('log_level'):
            kwargs['log_level'] = LOG_LEVEL_MAP[proto_settings.log_level]
        if proto_settings.HasField('query_consistency'):
            kwargs['query_consistency'] = SCAN_CONSISTENCY_MAP[proto_settings.query_consistency]

        if proto_settings.handler_headers:
            kwargs['handler_headers'] = list(proto_settings.handler_headers)
        if proto_settings.handler_footers:
            kwargs['handler_footers'] = list(proto_settings.handler_footers)

        return EventingFunctionSettings(**kwargs)

    @classmethod
    def to_eventing_function(cls, proto_function: eventing_pb.EventingFunction) -> EventingFunction:
        return EventingFunction(
            name=proto_function.name,
            code=proto_function.code,
            version=proto_function.version if proto_function.HasField('version') else None,
            enforce_schema=proto_function.enforce_schema if proto_function.HasField('enforce_schema') else None,
            handler_uuid=proto_function.handler_uuid if proto_function.HasField('handler_uuid') else None,
            function_instance_id=(proto_function.function_instance_id
                                  if proto_function.HasField('function_instance_id') else None),
            metadata_keyspace=(cls.to_keyspace(proto_function.metadata_keyspace)
                               if proto_function.HasField('metadata_keyspace') else None),
            source_keyspace=(cls.to_keyspace(proto_function.source_keyspace)
                             if proto_function.HasField('source_keyspace') else None),
            bucket_bindings=[cls.to_bucket_binding(b) for b in proto_function.bucket_bindings],
            url_bindings=[cls.to_url_binding(b) for b in proto_function.url_bindings],
            constant_bindings=[EventingFunctionConstantBinding(alias=b.alias, literal=b.literal)
                               for b in proto_function.constant_bindings],
            settings=(cls.to_settings(proto_function.settings)
                      if proto_function.HasField('settings') else EventingFunctionSettings()),
        )


class EventingFunctionManagerCommandResult(SdkCommandResult):

    @staticmethod
    def to_keyspace(sdk_keyspace: EventingFunctionKeyspace) -> eventing_pb.EventingFunctionKeyspace:
        proto_keyspace = eventing_pb.EventingFunctionKeyspace(bucket=sdk_keyspace.bucket)
        if sdk_keyspace.scope is not None:
            proto_keyspace.scope = sdk_keyspace.scope
        if sdk_keyspace.collection is not None:
            proto_keyspace.collection = sdk_keyspace.collection
        return proto_keyspace

    @classmethod
    def to_bucket_binding(cls,
                          sdk_binding: EventingFunctionBucketBinding
                          ) -> eventing_pb.EventingFunctionBucketBinding:
        return eventing_pb.EventingFunctionBucketBinding(
            access=_reverse(BUCKET_ACCESS_MAP)[sdk_binding.access],
            alias=sdk_binding.alias,
            name=cls.to_keyspace(sdk_binding.name),
        )

    @staticmethod
    def to_url_binding(sdk_binding: EventingFunctionUrlBinding) -> eventing_pb.EventingFunctionUrlBinding:
        auth = eventing_pb.EventingFunctionUrlAuth()
        if isinstance(sdk_binding.auth, EventingFunctionUrlAuthBasic):
            auth.basic_auth.username = sdk_binding.auth.username or ''
            auth.basic_auth.password = sdk_binding.auth.password or ''
        elif isinstance(sdk_binding.auth, EventingFunctionUrlAuthDigest):
            auth.digest_auth.username = sdk_binding.auth.username or ''
            auth.digest_auth.password = sdk_binding.auth.password or ''
        elif isinstance(sdk_binding.auth, EventingFunctionUrlAuthBearer):
            auth.bearer_auth.key = sdk_binding.auth.key or ''
        else:
            auth.no_auth.SetInParent()

        return eventing_pb.EventingFunctionUrlBinding(
            alias=sdk_binding.alias,
            allow_cookies=bool(sdk_binding.allow_cookies),
            auth=auth,
            hostname=sdk_binding.hostname,
            validate_ssl_certificate=bool(sdk_binding.validate_ssl_certificate),
        )

    @staticmethod
    def to_settings(sdk_settings: EventingFunctionSettings) -> eventing_pb.EventingFunctionSettings:  # noqa: C901
        proto_settings = eventing_pb.EventingFunctionSettings()

        simple_fields = {
            # SDK field name -> proto field name
            'app_log_dir': 'app_log_dir',
            'app_log_max_files': 'app_log_max_files',
            'app_log_max_size': 'app_log_max_size',
            'bucket_cache_size': 'bucket_cache_size',
            'cpp_worker_thread_count': 'cpp_worker_thread_count',
            'curl_max_allowed_resp_size': 'curl_max_allowed_resp_size',
            'description': 'description',
            'enable_applog_rotation': 'enable_app_log_rotation',
            'lcb_inst_capacity': 'lcb_inst_capacity',
            'lcb_retry_count': 'lcb_retry_count',
            'num_timer_partitions': 'num_timer_partitions',
            'query_prepare_all': 'query_prepare_all',
            'sock_batch_size': 'sock_batch_size',
            'timer_context_size': 'timer_context_size',
            'user_prefix': 'user_prefix',
            'worker_count': 'worker_count',
        }
        for sdk_name, proto_name in simple_fields.items():
            value = getattr(sdk_settings, sdk_name)
            if value is not None:
                setattr(proto_settings, proto_name, value)

        for name in ['checkpoint_interval', 'execution_timeout', 'lcb_timeout', 'tick_duration']:
            value = getattr(sdk_settings, name)
            if value is not None:
                getattr(proto_settings, name).FromTimedelta(value)

        if sdk_settings.bucket_cache_age is not None:
            proto_settings.bucket_cache_age = int(sdk_settings.bucket_cache_age.total_seconds() * 1000)

        enum_fields = {
            'dcp_stream_boundary': DCP_BOUNDARY_MAP,
            'deployment_status': DEPLOYMENT_STATUS_MAP,
            'processing_status': PROCESSING_STATUS_MAP,
            'language_compatibility': LANGUAGE_COMPATIBILITY_MAP,
            'log_level': LOG_LEVEL_MAP,
            'query_consistency': SCAN_CONSISTENCY_MAP,
        }
        for name, mapping in enum_fields.items():
            value = getattr(sdk_settings, name)
            if value is not None:
                setattr(proto_settings, name, _reverse(mapping)[value])

        if sdk_settings.handler_headers:
            proto_settings.handler_headers.extend(sdk_settings.handler_headers)
        if sdk_settings.handler_footers:
            proto_settings.handler_footers.extend(sdk_settings.handler_footers)

        return proto_settings

    @classmethod
    def to_eventing_function(cls, sdk_function: EventingFunction) -> eventing_pb.EventingFunction:
        proto_function = eventing_pb.EventingFunction(
            name=sdk_function.name,
            code=sdk_function.code,
            bucket_bindings=[cls.to_bucket_binding(b) for b in sdk_function.bucket_bindings or []],
            constant_bindings=[eventing_pb.EventingFunctionConstantBinding(alias=b.alias, literal=b.literal)
                               for b in sdk_function.constant_bindings or []],
            url_bindings=[cls.to_url_binding(b) for b in sdk_function.url_bindings or []],
            settings=cls.to_settings(sdk_function.settings or EventingFunctionSettings()),
        )
        if sdk_function.enforce_schema is not None:
            proto_function.enforce_schema = sdk_function.enforce_schema
        if sdk_function.function_instance_id is not None:
            proto_function.function_instance_id = sdk_function.function_instance_id
        if sdk_function.handler_uuid is not None:
            proto_function.handler_uuid = sdk_function.handler_uuid
        if sdk_function.version is not None:
            proto_function.version = sdk_function.version
        if sdk_function.metadata_keyspace is not None:
            proto_function.metadata_keyspace.CopyFrom(cls.to_keyspace(sdk_function.metadata_keyspace))
        if sdk_function.source_keyspace is not None:
            proto_function.source_keyspace.CopyFrom(cls.to_keyspace(sdk_function.source_keyspace))
        return proto_function

    @classmethod
    def to_eventing_function_list(cls,
                                  sdk_functions: Iterable[EventingFunction]
                                  ) -> eventing_pb.EventingFunctionList:
        return eventing_pb.EventingFunctionList(functions=list(map(cls.to_eventing_function, sdk_functions)))

    @staticmethod
    def to_function_state(sdk_status: EventingFunctionStatus) -> eventing_pb.EventingFunctionState:
        proto_state = eventing_pb.EventingFunctionState(
            name=sdk_status.name,
            num_bootstrapping_nodes=sdk_status.num_bootstrapping_nodes or 0,
            num_deployed_nodes=sdk_status.num_deployed_nodes or 0,
            status=FUNCTION_STATE_MAP.get(sdk_status.state, eventing_pb.UNKNOWN),
        )
        if sdk_status.deployment_status is not None:
            proto_state.deployment_status = _reverse(DEPLOYMENT_STATUS_MAP)[sdk_status.deployment_status]
        if sdk_status.processing_status is not None:
            proto_state.processing_status = _reverse(PROCESSING_STATUS_MAP)[sdk_status.processing_status]
        return proto_state

    @classmethod
    def to_eventing_status(cls, sdk_status: EventingFunctionsStatus) -> eventing_pb.EventingStatus:
        return eventing_pb.EventingStatus(
            functions=list(map(cls.to_function_state, sdk_status.functions or [])),
            num_eventing_nodes=sdk_status.num_eventing_nodes or 0,
        )

    @classmethod
    def _as_result(cls, fn, to_proto_result):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                sdk_result = sdk_pb.Result(eventing_function_manager_result=to_proto_result(res))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_eventing_function(cls, fn):
        return cls._as_result(
            fn, lambda res: eventing_pb.Result(eventing_function=cls.to_eventing_function(res)))

    @classmethod
    def as_eventing_function_list(cls, fn):
        return cls._as_result(
            fn, lambda res: eventing_pb.Result(eventing_function_list=cls.to_eventing_function_list(res)))

    @classmethod
    def as_eventing_status(cls, fn):
        return cls._as_result(
            fn, lambda res: eventing_pb.Result(eventing_status=cls.to_eventing_status(res)))


class GetFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._name = kwargs.get('name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = GetFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_eventing_function
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.get_function(self._name, self._options)

    @staticmethod
    def create_command(**kwargs) -> GetFunctionCommand:
        command = GetFunctionCommand(**kwargs)
        command.set_options()
        return command


class GetAllFunctionsCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = GetAllFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_eventing_function_list
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.get_all_functions(self._options)

    @staticmethod
    def create_command(**kwargs) -> GetAllFunctionsCommand:
        command = GetAllFunctionsCommand(**kwargs)
        command.set_options()
        return command


class DeployFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._name = kwargs.get('name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = DeployFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.deploy_function(self._name, self._options)

    @staticmethod
    def create_command(**kwargs) -> DeployFunctionCommand:
        command = DeployFunctionCommand(**kwargs)
        command.set_options()
        return command


class DropFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._name = kwargs.get('name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = DropFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.drop_function(self._name, self._options)

    @staticmethod
    def create_command(**kwargs) -> DropFunctionCommand:
        command = DropFunctionCommand(**kwargs)
        command.set_options()
        return command


class FunctionsStatusCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = FunctionsStatusOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_eventing_status
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.functions_status(self._options)

    @staticmethod
    def create_command(**kwargs) -> FunctionsStatusCommand:
        command = FunctionsStatusCommand(**kwargs)
        command.set_options()
        return command


class PauseFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._name = kwargs.get('name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = PauseFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.pause_function(self._name, self._options)

    @staticmethod
    def create_command(**kwargs) -> PauseFunctionCommand:
        command = PauseFunctionCommand(**kwargs)
        command.set_options()
        return command


class ResumeFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._name = kwargs.get('name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = ResumeFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.resume_function(self._name, self._options)

    @staticmethod
    def create_command(**kwargs) -> ResumeFunctionCommand:
        command = ResumeFunctionCommand(**kwargs)
        command.set_options()
        return command


class UndeployFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._name = kwargs.get('name')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = UndeployFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.undeploy_function(self._name, self._options)

    @staticmethod
    def create_command(**kwargs) -> UndeployFunctionCommand:
        command = UndeployFunctionCommand(**kwargs)
        command.set_options()
        return command


class UpsertFunctionCommand(SdkCommand):
    def __init__(self, **kwargs):
        self._initiated = kwargs.get('initiated')
        self._cluster = kwargs.get('cluster')
        self._scope = kwargs.get('scope')
        if self._scope is not None:
            self._eventing_function_manager = self._scope.eventing_functions()
        else:
            self._eventing_function_manager = self._cluster.eventing_functions()
        self._function = kwargs.get('function')
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = UpsertFunctionOptions(
            timeout=EventingFunctionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=EventingFunctionManagerCommandOptions.resolve_parent_span(self._raw_options,
                                                                                  self._span_owner)
        )

    @EventingFunctionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._eventing_function_manager.upsert_function(self._function, self._options)

    @staticmethod
    def create_command(**kwargs) -> UpsertFunctionCommand:
        command = UpsertFunctionCommand(**kwargs)
        command.set_options()
        return command


class EventingFunctionManagerCommandBuilder:
    @staticmethod
    def build_command(  # noqa: C901
            eventing_cmd,  # type: Union[eventing_pb.Command | scope_eventing_pb.Command]
            **cmd_kwargs):
        cmd_type = eventing_cmd.WhichOneof('command')
        if getattr(eventing_cmd, cmd_type).HasField('options'):
            cmd_kwargs['options'] = getattr(eventing_cmd, cmd_type).options

        if cmd_type == 'get_function':
            cmd_kwargs['name'] = eventing_cmd.get_function.name
            return GetFunctionCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'get_all_functions':
            return GetAllFunctionsCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'deploy_function':
            cmd_kwargs['name'] = eventing_cmd.deploy_function.name
            return DeployFunctionCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'drop_function':
            cmd_kwargs['name'] = eventing_cmd.drop_function.name
            return DropFunctionCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'functions_status':
            return FunctionsStatusCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'pause_function':
            cmd_kwargs['name'] = eventing_cmd.pause_function.name
            return PauseFunctionCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'resume_function':
            cmd_kwargs['name'] = eventing_cmd.resume_function.name
            return ResumeFunctionCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'undeploy_function':
            cmd_kwargs['name'] = eventing_cmd.undeploy_function.name
            return UndeployFunctionCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'upsert_function':
            cmd_kwargs['function'] = EventingFunctionManagerCommandOptions.to_eventing_function(
                eventing_cmd.upsert_function.function)
            return UpsertFunctionCommand.create_command(**cmd_kwargs)
        else:
            raise NotImplementedError(f'Eventing function management command `{cmd_type}` not supported')
