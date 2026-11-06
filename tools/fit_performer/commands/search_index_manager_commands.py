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
from collections.abc import Callable, Iterable
from functools import wraps
from time import perf_counter_ns
from typing import (Any,
                    Concatenate,
                    ParamSpec,
                    TypeVar)

from google.protobuf import timestamp_pb2 as timestamp_pb

from couchbase.cluster import Cluster
from couchbase.management.options import (AllowQueryingSearchIndexOptions,
                                          AnalyzeDocumentSearchIndexOptions,
                                          DisallowQueryingSearchIndexOptions,
                                          DropSearchIndexOptions,
                                          FreezePlanSearchIndexOptions,
                                          GetAllSearchIndexesOptions,
                                          GetSearchIndexedDocumentsCountOptions,
                                          GetSearchIndexOptions,
                                          PauseIngestSearchIndexOptions,
                                          ResumeIngestSearchIndexOptions,
                                          UnfreezePlanSearchIndexOptions,
                                          UpsertSearchIndexOptions)
from couchbase.management.search import (ScopeSearchIndexManager,
                                         SearchIndex,
                                         SearchIndexManager)
from couchbase.scope import Scope

from run import top_level_pb2 as run_pb
from sdk import workload_pb2 as sdk_pb
from sdk.cluster.search import index_manager_pb2 as cluster_search_index_pb
from sdk.scope.search import index_manager_pb2 as scope_search_index_pb
from sdk.search import index_manager_pb2 as search_index_pb

from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult)

P = ParamSpec('P')
C = TypeVar('C', bound=SdkCommand)

_VALID_SEARCH_MGMT_CMD_ARGS: dict[str, Callable[[Any], bool]] = {
    'scope': lambda s: s is None or isinstance(s, Scope),
    'cluster': lambda c: c is None or isinstance(c, Cluster),
    'return_result': lambda r: isinstance(r, bool),
    'initiated': lambda i: isinstance(i, timestamp_pb.Timestamp),
    'index_name': lambda n: n is None or isinstance(n, str),
    'index_definition': lambda d: d is None or isinstance(d, bytes),
    'options': lambda o: True,
    'span_owner': lambda s: True
}


def _search_index_manager(kwargs: dict[str, Any]) -> SearchIndexManager | ScopeSearchIndexManager:
    scope: Scope | None = kwargs.get('scope')
    if scope is not None:
        return scope.search_indexes()
    cluster: Cluster | None = kwargs.get('cluster')
    if cluster is None:
        raise ValueError('A search index management command needs a cluster or a scope.')
    return cluster.search_indexes()


class SearchIndexManagerCommandOptions(SdkCommandOptions):
    # Only timeout is needed which is already defined in SdkCommandOptions
    pass


class SearchIndexManagerCommandResult(SdkCommandResult):

    @classmethod
    def to_search_index(cls, sdk_index: SearchIndex) -> search_index_pb.SearchIndex:
        return search_index_pb.SearchIndex(
            uuid=sdk_index.uuid or '',
            name=sdk_index.name,
            type=sdk_index.idx_type,
            source_uuid=sdk_index.source_uuid or '',
            source_type=sdk_index.source_type,
            params=json.dumps(sdk_index.params).encode('utf-8'),
            source_params=json.dumps(sdk_index.source_params).encode('utf-8'),
            plan_params=json.dumps(sdk_index.plan_params).encode('utf-8')
        )

    @classmethod
    def to_search_indexes(cls, sdk_indexes: Iterable[SearchIndex]) -> search_index_pb.SearchIndexes:
        return search_index_pb.SearchIndexes(
            indexes=list(map(cls.to_search_index, sdk_indexes))
        )

    @classmethod
    def as_search_index(cls,
                        fn: Callable[Concatenate[C, P], SearchIndex],
                        ) -> Callable[Concatenate[C, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: C, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                sdk_result = sdk_pb.Result(search_index_manager_result=search_index_pb.Result(
                    index=cls.to_search_index(res)
                ))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_search_indexes(cls,
                          fn: Callable[Concatenate[C, P], Iterable[SearchIndex]],
                          ) -> Callable[Concatenate[C, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: C, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                sdk_result = sdk_pb.Result(search_index_manager_result=search_index_pb.Result(
                    indexes=cls.to_search_indexes(res)
                ))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_indexed_document_counts(cls,
                                   fn: Callable[Concatenate[C, P], int],
                                   ) -> Callable[Concatenate[C, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: C, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                sdk_result = sdk_pb.Result(search_index_manager_result=search_index_pb.Result(
                    indexed_document_counts=res
                ))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn

    @classmethod
    def as_analyze_document_result(cls,
                                   fn: Callable[Concatenate[C, P], dict[str, Any]],
                                   ) -> Callable[Concatenate[C, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: C, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                res = fn(self, *args, **kwargs)
                end = perf_counter_ns()
                sdk_result = sdk_pb.Result(search_index_manager_result=search_index_pb.Result(
                    analyze_document=search_index_pb.AnalyzeDocumentResult(
                        results=[json.dumps(res).encode('utf-8')]
                    )
                ))
                return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
            except Exception as e:
                sdk_result = sdk_pb.Result(exception=cls.to_exception(e))
                return run_pb.Result(sdk=sdk_result, initiated=self._initiated)

        return wrapped_fn


class GetIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: GetSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = GetSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_search_index
    def execute_command(self) -> SearchIndex:
        return self._search_index_manager.get_index(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetIndexCommand:
        command = GetIndexCommand(**kwargs)
        command.set_options()
        return command


class GetAllIndexesCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._raw_options = kwargs.get('options')
        self._options: GetAllSearchIndexesOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = GetAllSearchIndexesOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_search_indexes
    def execute_command(self) -> Iterable[SearchIndex]:
        return self._search_index_manager.get_all_indexes(self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetAllIndexesCommand:
        command = GetAllIndexesCommand(**kwargs)
        command.set_options()
        return command


class UpsertIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_definition: SearchIndex = kwargs['index_definition']
        self._raw_options = kwargs.get('options')
        self._options: UpsertSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = UpsertSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.upsert_index(self._index_definition, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> UpsertIndexCommand:
        command = UpsertIndexCommand(**kwargs)
        command.set_options()
        return command


class DropIndexCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: DropSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = DropSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.drop_index(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> DropIndexCommand:
        command = DropIndexCommand(**kwargs)
        command.set_options()
        return command


class GetIndexedDocumentsCountCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: GetSearchIndexedDocumentsCountOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = GetSearchIndexedDocumentsCountOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_indexed_document_counts
    def execute_command(self) -> int:
        return self._search_index_manager.get_indexed_documents_count(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> GetIndexedDocumentsCountCommand:
        command = GetIndexedDocumentsCountCommand(**kwargs)
        command.set_options()
        return command


class PauseIngestCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: PauseIngestSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = PauseIngestSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.pause_ingest(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> PauseIngestCommand:
        command = PauseIngestCommand(**kwargs)
        command.set_options()
        return command


class ResumeIngestCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: ResumeIngestSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = ResumeIngestSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.resume_ingest(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> ResumeIngestCommand:
        command = ResumeIngestCommand(**kwargs)
        command.set_options()
        return command


class AllowQueryingCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: AllowQueryingSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = AllowQueryingSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.allow_querying(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> AllowQueryingCommand:
        command = AllowQueryingCommand(**kwargs)
        command.set_options()
        return command


class DisallowQueryingCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: DisallowQueryingSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = DisallowQueryingSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.disallow_querying(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> DisallowQueryingCommand:
        command = DisallowQueryingCommand(**kwargs)
        command.set_options()
        return command


class FreezePlanCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: FreezePlanSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = FreezePlanSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.freeze_plan(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> FreezePlanCommand:
        command = FreezePlanCommand(**kwargs)
        command.set_options()
        return command


class UnfreezePlanCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._raw_options = kwargs.get('options')
        self._options: UnfreezePlanSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = UnfreezePlanSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_success
    def execute_command(self) -> None:
        return self._search_index_manager.unfreeze_plan(self._index_name, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> UnfreezePlanCommand:
        command = UnfreezePlanCommand(**kwargs)
        command.set_options()
        return command


class AnalyzeDocumentCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        self._initiated = kwargs['initiated']
        self._search_index_manager = _search_index_manager(kwargs)
        self._index_name: str = kwargs['index_name']
        self._document = kwargs['document']
        self._raw_options = kwargs.get('options')
        self._options: AnalyzeDocumentSearchIndexOptions | None = None
        self._span_owner = kwargs.get('span_owner')

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        self._options = AnalyzeDocumentSearchIndexOptions(
            timeout=SearchIndexManagerCommandOptions.get_timeout(self._raw_options),
            parent_span=SearchIndexManagerCommandOptions.resolve_parent_span(self._raw_options, self._span_owner)
        )

    @SearchIndexManagerCommandResult.as_analyze_document_result
    def execute_command(self) -> dict[str, Any]:
        return self._search_index_manager.analyze_document(self._index_name, self._document, self._options)

    @staticmethod
    def create_command(**kwargs: Any) -> AnalyzeDocumentCommand:
        command = AnalyzeDocumentCommand(**kwargs)
        command.set_options()
        return command


class SearchIndexManagerCommandBuilder:
    @classmethod
    def parse_index_definition(cls, raw_defn: bytes) -> SearchIndex:
        kwargs: dict[str, Any] = {}
        for camel_key, val in json.loads(raw_defn).items():
            # The keys of the JSON definiton are given in lowerCamelCase from the driver
            # Convert them to snake_case so they match the arguments of the constructor
            key = ''.join([camel_key[0]] + ['_' + ch.lower() if ch.isupper() else ch for ch in camel_key[1:]])

            # The SearchIndex constructor expects 'idx_type' instead of 'type'
            if key == 'type':
                key = 'idx_type'

            # SearchIndex does not handle id, and that appears to be in the `sample-scope-index.json` used by the driver
            if key == 'id':
                continue

            kwargs[key] = val

        return SearchIndex(**kwargs)

    @classmethod
    def build_cluster_level_command(cls,
                                    cluster_search_idx_cmd: cluster_search_index_pb.Command,
                                    **cmd_kwargs: Any
                                    ) -> SdkCommand:
        return cls.build_shared_command(cluster_search_idx_cmd.shared, **cmd_kwargs)

    @classmethod
    def build_scope_level_command(cls,
                                  scope_search_idx_cmd: scope_search_index_pb.Command,
                                  **cmd_kwargs: Any
                                  ) -> SdkCommand:
        return cls.build_shared_command(scope_search_idx_cmd.shared, **cmd_kwargs)

    @classmethod
    def build_shared_command(cls,  # noqa: C901
                             search_idx_cmd: search_index_pb.Command,
                             **cmd_kwargs: Any) -> SdkCommand:
        cmd_type = search_idx_cmd.WhichOneof('command')
        if cmd_type is None:
            raise NotImplementedError('Search index management command has no command type set')
        if getattr(search_idx_cmd, cmd_type).HasField('options'):
            cmd_kwargs['options'] = getattr(search_idx_cmd, cmd_type).options

        if cmd_type == 'get_index':
            cmd_kwargs['index_name'] = search_idx_cmd.get_index.index_name
            return GetIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'get_all_indexes':
            return GetAllIndexesCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'upsert_index':
            cmd_kwargs['index_definition'] = cls.parse_index_definition(search_idx_cmd.upsert_index.index_definition)
            return UpsertIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'drop_index':
            cmd_kwargs['index_name'] = search_idx_cmd.drop_index.index_name
            return DropIndexCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'get_indexed_documents_count':
            cmd_kwargs['index_name'] = search_idx_cmd.get_indexed_documents_count.index_name
            return GetIndexedDocumentsCountCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'pause_ingest':
            cmd_kwargs['index_name'] = search_idx_cmd.pause_ingest.index_name
            return PauseIngestCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'resume_ingest':
            cmd_kwargs['index_name'] = search_idx_cmd.resume_ingest.index_name
            return ResumeIngestCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'allow_querying':
            cmd_kwargs['index_name'] = search_idx_cmd.allow_querying.index_name
            return AllowQueryingCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'disallow_querying':
            cmd_kwargs['index_name'] = search_idx_cmd.disallow_querying.index_name
            return DisallowQueryingCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'freeze_plan':
            cmd_kwargs['index_name'] = search_idx_cmd.freeze_plan.index_name
            return FreezePlanCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'unfreeze_plan':
            cmd_kwargs['index_name'] = search_idx_cmd.unfreeze_plan.index_name
            return UnfreezePlanCommand.create_command(**cmd_kwargs)
        elif cmd_type == 'analyze_document':
            cmd_kwargs.update({
                'index_name': search_idx_cmd.analyze_document.index_name,
                'document': json.loads(search_idx_cmd.analyze_document.document),
            })
            return AnalyzeDocumentCommand.create_command(**cmd_kwargs)
        else:
            raise NotImplementedError(f'Search index management command `{cmd_type}` not supported')
