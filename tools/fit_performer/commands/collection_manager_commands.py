from __future__ import annotations

import logging
from datetime import timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (Any,
                    Dict,
                    Iterable,
                    Union)

import google.protobuf.timestamp_pb2 as timestamp_pb

from couchbase.bucket import Bucket
from couchbase.management.collections import (CreateCollectionSettings,
                                              ScopeSpec,
                                              UpdateCollectionSettings)
from couchbase.management.options import (CreateCollectionOptions,
                                          CreateScopeOptions,
                                          DropCollectionOptions,
                                          DropScopeOptions,
                                          GetAllScopesOptions,
                                          UpdateCollectionOptions)

from ..generated.run import top_level_pb2 as run_pb
from ..generated.sdk import workload_pb2 as sdk_pb
from ..generated.sdk.bucket import collection_manager_pb2 as collection_manager_pb
from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

VALID_COLLECTION_MANAGER_COMMAND_ARGS = {
    'return_result': lambda r: isinstance(r, bool),
    'initiated': lambda i: isinstance(i, timestamp_pb.Timestamp),
    'options': lambda o: o is not None,
    'bucket': lambda c: isinstance(c, Bucket),
    'name': lambda n: n is None or isinstance(n, str),
    'scope_name': lambda n: n is None or isinstance(n, str),
    'collection_name': lambda n: n is None or isinstance(n, str),
    'settings': (lambda s: s is None or isinstance(s, CreateCollectionSettings)
                 or isinstance(s, UpdateCollectionSettings)),
    'span_owner': lambda s: True
}

logger = logging.getLogger(__name__)


class CollectionManagerCommandOptions(SdkCommandOptions):
    pass


class CollectionManagerCommandResult(SdkCommandResult):
    @classmethod
    def as_get_all_scopes_result(cls, fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                if self._return_result:
                    sdk_result = sdk_pb.Result(
                        collection_manager_result=collection_manager_pb.Result(
                            get_all_scopes_result=cls.to_get_all_scopes_result(res)
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
    def to_get_all_scopes_result(cls, sdk_res: Iterable[ScopeSpec]) -> collection_manager_pb.GetAllScopesResult:
        res = collection_manager_pb.GetAllScopesResult()
        for scope_spec in sdk_res:
            proto_scope_spec = collection_manager_pb.ScopeSpec(name=scope_spec.name)
            for coll_spec in scope_spec.collections:
                kwargs = {
                    'name': coll_spec.name,
                    'scope_name': coll_spec.scope_name,
                }
                if coll_spec.max_expiry is not None:
                    kwargs['expiry_secs'] = round(coll_spec.max_expiry.total_seconds())
                if coll_spec.history is not None:
                    kwargs['history'] = coll_spec.history
                proto_scope_spec.collections.append(collection_manager_pb.CollectionSpec(**kwargs))
            res.result.append(proto_scope_spec)
        return res


class GetAllScopesCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_COLLECTION_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._bucket = kwargs.get('bucket')
        self._collection_manager = self._bucket.collections()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = GetAllScopesOptions(
            timeout=CollectionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=CollectionManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @CollectionManagerCommandResult.as_get_all_scopes_result
    def execute_command(self) -> run_pb.Result:
        return self._collection_manager.get_all_scopes(self._options)

    @staticmethod
    def create_command(**kwargs) -> GetAllScopesCommand:
        command = GetAllScopesCommand(**kwargs)
        command.set_options()
        return command


class CreateScopeCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_COLLECTION_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._bucket = kwargs.get('bucket')
        self._collection_manager = self._bucket.collections()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._scope_name = kwargs.get('scope_name')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = CreateScopeOptions(
            timeout=CollectionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=CollectionManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @CollectionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._collection_manager.create_scope(self._scope_name, self._options)

    @staticmethod
    def create_command(**kwargs) -> CreateScopeCommand:
        command = CreateScopeCommand(**kwargs)
        command.set_options()
        return command


class DropScopeCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_COLLECTION_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._bucket = kwargs.get('bucket')
        self._collection_manager = self._bucket.collections()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._scope_name = kwargs.get('scope_name')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = DropScopeOptions(
            timeout=CollectionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=CollectionManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @CollectionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._collection_manager.drop_scope(self._scope_name, self._options)

    @staticmethod
    def create_command(**kwargs) -> DropScopeCommand:
        command = DropScopeCommand(**kwargs)
        command.set_options()
        return command


class CreateCollectionCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_COLLECTION_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._bucket = kwargs.get('bucket')
        self._collection_manager = self._bucket.collections()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._scope_name = kwargs.get('scope_name')
        self._collection_name = kwargs.get('collection_name')
        self._settings = kwargs.get('settings')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = CreateCollectionOptions(
            timeout=CollectionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=CollectionManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @CollectionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._collection_manager.create_collection(
            self._scope_name, self._collection_name, self._settings, self._options)

    @staticmethod
    def create_command(**kwargs) -> CreateCollectionCommand:
        command = CreateCollectionCommand(**kwargs)
        command.set_options()
        return command


class UpdateCollectionCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_COLLECTION_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._bucket = kwargs.get('bucket')
        self._collection_manager = self._bucket.collections()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._scope_name = kwargs.get('scope_name')
        self._collection_name = kwargs.get('collection_name')
        self._settings = kwargs.get('settings')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = UpdateCollectionOptions(
            timeout=CollectionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=CollectionManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @CollectionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._collection_manager.update_collection(
            self._scope_name, self._collection_name, self._settings, self._options)

    @staticmethod
    def create_command(**kwargs) -> UpdateCollectionCommand:
        command = UpdateCollectionCommand(**kwargs)
        command.set_options()
        return command


class DropCollectionCommand(SdkCommand):
    def __init__(self, **kwargs):
        validate_command(VALID_COLLECTION_MANAGER_COMMAND_ARGS, **kwargs)
        self._initiated = kwargs.get('initiated')
        self._return_result = kwargs.get('return_result')
        self._bucket = kwargs.get('bucket')
        self._collection_manager = self._bucket.collections()
        self._raw_options = kwargs.get('options')
        self._options = None
        self._scope_name = kwargs.get('scope_name')
        self._collection_name = kwargs.get('collection_name')
        self._span_owner = kwargs.get('span_owner')

    def set_options(self):
        if self._raw_options is None:
            return

        self._options = DropCollectionOptions(
            timeout=CollectionManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=CollectionManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @CollectionManagerCommandResult.as_success
    def execute_command(self) -> run_pb.Result:
        return self._collection_manager.drop_collection(
            self._scope_name, self._collection_name, self._options)

    @staticmethod
    def create_command(**kwargs) -> DropCollectionCommand:
        command = DropCollectionCommand(**kwargs)
        command.set_options()
        return command


class CollectionManagerCommandBuilder:
    @classmethod
    def _get_collection_settings(cls,
                                 proto_settings: Union[collection_manager_pb.CreateCollectionSettings,
                                                       collection_manager_pb.UpdateCollectionSettings]
                                 ) -> Union[CreateCollectionSettings, UpdateCollectionSettings]:
        if isinstance(proto_settings, collection_manager_pb.CreateCollectionSettings):
            settings_cls = CreateCollectionSettings
        else:
            settings_cls = UpdateCollectionSettings
        kwargs = {}
        if proto_settings.HasField('expiry_secs'):
            kwargs['max_expiry'] = timedelta(seconds=proto_settings.expiry_secs)
        if proto_settings.HasField('history'):
            kwargs['history'] = proto_settings.history
        return settings_cls(**kwargs)

    @classmethod
    def build_command(cls,
                      coll_mgr_cmd: collection_manager_pb.Command,
                      **cmd_kwargs: Dict[str, Any]
                      ) -> SdkCommand:
        cmd_type = coll_mgr_cmd.WhichOneof('command')
        cmd = getattr(coll_mgr_cmd, cmd_type)

        if cmd.HasField('options'):
            cmd_kwargs['options'] = cmd.options

        if cmd_type == 'get_all_scopes':
            return GetAllScopesCommand.create_command(**cmd_kwargs)
        if cmd_type == 'create_scope':
            cmd_kwargs['scope_name'] = cmd.name
            return CreateScopeCommand.create_command(**cmd_kwargs)
        if cmd_type == 'drop_scope':
            cmd_kwargs['scope_name'] = cmd.name
            return DropScopeCommand.create_command(**cmd_kwargs)
        if cmd_type == 'create_collection':
            cmd_kwargs.update({
                'scope_name': cmd.scope_name,
                'collection_name': cmd.name,
            })
            if cmd.HasField('settings'):
                cmd_kwargs['settings'] = cls._get_collection_settings(cmd.settings)
            return CreateCollectionCommand.create_command(**cmd_kwargs)
        if cmd_type == 'update_collection':
            cmd_kwargs.update({
                'scope_name': cmd.scope_name,
                'collection_name': cmd.name,
                'settings': cls._get_collection_settings(cmd.settings),
            })
            return UpdateCollectionCommand.create_command(**cmd_kwargs)
        if cmd_type == 'drop_collection':
            cmd_kwargs.update({
                'scope_name': cmd.scope_name,
                'collection_name': cmd.name,
            })
            return DropCollectionCommand.create_command(**cmd_kwargs)
        raise NotImplementedError(f'Collection management command {cmd_type} not supported by the performer')
