import logging

from google.protobuf import timestamp_pb2 as timestamp

from .bucket_manager_commands import BucketManagerCommandBuilder
from .collection_manager_commands import CollectionManagerCommandBuilder
from .kv_commands import KvCommandBuilder
from .query_commands import QueryCommandBuilder
from .query_index_manager_commands import QueryIndexManagerCommandBuilder
from .search_commands import SearchCommandBuilder
from .search_index_manager_commands import SearchIndexManagerCommandBuilder
from .update_authenticator_command import UpdateAuthenticatorCommand

logger = logging.getLogger(__name__)


class SdkCommandBuilder:
    @staticmethod
    def build_command(cluster, command, counters, span_owner=None):  # noqa: C901
        cmd_type = command.WhichOneof("command")
        kv_cmd_types = ['insert', 'get', 'remove', 'replace', 'upsert', 'range_scan']

        cmd_kwargs = {
            'initiated': timestamp.Timestamp(),
            'return_result': command.return_result,
            'span_owner': span_owner
        }

        cmd_kwargs['initiated'].GetCurrentTime()

        if cmd_type == 'cluster_command':
            cluster_cmd = command.cluster_command
            cluster_cmd_type = cluster_cmd.WhichOneof('command')
            cmd_kwargs['cluster'] = cluster
            if cluster_cmd_type == 'query':
                return QueryCommandBuilder.build_command(cluster_cmd.query, **cmd_kwargs)
            if cluster_cmd_type == 'query_index_manager':
                return QueryIndexManagerCommandBuilder.build_cluster_level_command(
                    cluster_cmd.query_index_manager, **cmd_kwargs)
            if cluster_cmd_type == 'search':
                return SearchCommandBuilder.build_command(cluster_cmd.search, **cmd_kwargs)
            # [start:4.1.11]
            if cluster_cmd_type == 'search_v2':
                return SearchCommandBuilder.build_command_v2(cluster_cmd.search_v2, **cmd_kwargs)
            # [end:4.1.11]
            if cluster_cmd_type == 'search_index_manager':
                return SearchIndexManagerCommandBuilder.build_cluster_level_command(
                    cluster_cmd.search_index_manager, **cmd_kwargs)
            if cluster_cmd_type == 'bucket_manager':
                return BucketManagerCommandBuilder.build_command(
                    cluster_cmd.bucket_manager, **cmd_kwargs)
            # [start:4.6.0]
            if cluster_cmd_type == 'authenticator':
                return UpdateAuthenticatorCommand.create_command(
                    auth_message=cluster_cmd.authenticator, **cmd_kwargs)
            # [end:4.6.0]
            raise NotImplementedError(f'Cluster-level command `{cluster_cmd_type}` not implemented in performer')

        elif cmd_type == 'bucket_command':
            bucket_cmd = command.bucket_command
            bucket_cmd_type = bucket_cmd.WhichOneof('command')
            cmd_kwargs['bucket'] = cluster.bucket(bucket_cmd.bucket_name)
            if bucket_cmd_type == 'collection_manager':
                return CollectionManagerCommandBuilder.build_command(
                    bucket_cmd.collection_manager, **cmd_kwargs)
            raise NotImplementedError(f'Bucket-level command `{bucket_cmd_type}` not implemeneted in performer')

        elif cmd_type == 'scope_command':
            scope_cmd = command.scope_command
            scope_cmd_type = scope_cmd.WhichOneof('command')
            scope_descr = scope_cmd.scope
            cmd_kwargs['scope'] = cluster.bucket(scope_descr.bucket_name).scope(scope_descr.scope_name)
            if scope_cmd_type == 'query':
                return QueryCommandBuilder.build_command(scope_cmd.query, **cmd_kwargs)
            if scope_cmd_type == 'search':
                return SearchCommandBuilder.build_command(scope_cmd.search, **cmd_kwargs)
            # [start:4.1.12]
            if scope_cmd_type == 'search_index_manager':
                return SearchIndexManagerCommandBuilder.build_scope_level_command(
                    scope_cmd.search_index_manager, **cmd_kwargs)
            if scope_cmd_type == 'search_v2':
                return SearchCommandBuilder.build_command_v2(scope_cmd.search_v2, **cmd_kwargs)
            # [end:4.1.12]
            raise NotImplementedError(f'Scope-level command `{scope_cmd_type}` not implemented in performer')

        elif cmd_type == 'collection_command':
            collection_cmd = command.collection_command

            if collection_cmd.HasField('collection'):
                coll_descr = collection_cmd.collection
                cmd_kwargs['collection'] = (cluster
                                            .bucket(coll_descr.bucket_name)
                                            .scope(coll_descr.scope_name)
                                            .collection(coll_descr.collection_name))

            collection_cmd_type = collection_cmd.WhichOneof('command')
            if collection_cmd_type == 'query_index_manager':
                return QueryIndexManagerCommandBuilder.build_collection_level_command(
                    collection_cmd.query_index_manager, **cmd_kwargs)
            if collection_cmd_type in ['lookup_in', 'lookup_in_any_replica', 'lookup_in_all_replicas', 'binary',
                                       'get_and_touch', 'touch', 'get_and_lock', 'unlock', 'get_any_replica',
                                       'get_all_replicas', 'exists', 'mutate_in']:
                return KvCommandBuilder.build_command(
                    cluster, getattr(collection_cmd, collection_cmd_type),
                    collection_cmd_type, counters, **cmd_kwargs)

            raise NotImplementedError(
                f'Collection-level command `{collection_cmd_type}` not implemented in performer')

        elif cmd_type in kv_cmd_types:
            return KvCommandBuilder.build_command(
                cluster, getattr(command, cmd_type), cmd_type, counters, **cmd_kwargs)

        raise NotImplementedError(f'Command type `{cmd_type}` not supported')
