#  Copyright 2016-2023. Couchbase, Inc.
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

from dataclasses import dataclass, field
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional)

from couchbase.exceptions import QueryIndexAlreadyExistsException, QueryIndexNotFoundException
from couchbase.management.options import GetAllQueryIndexOptions
from couchbase.options import forward_args
from couchbase.pycbc_core import (management_operation,
                                  mgmt_operations,
                                  query_index_mgmt_operations)

if TYPE_CHECKING:
    from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                              CreatePrimaryQueryIndexOptions,
                                              CreateQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions,
                                              DropQueryIndexOptions)


class QueryIndexManagerLogic:

    _ERROR_MAPPING = {r'.*[iI]ndex.*already exists.*': QueryIndexAlreadyExistsException,
                      r'.*[iI]ndex.*[nN]ot [fF]ound.*': QueryIndexNotFoundException}

    def __init__(self, connection):
        self._connection = connection

    def _create_index(self,
                      bucket_name,  # type: str
                      fields,  # type: Iterable[str]
                      index_name=None,  # type: Optional[str]
                      **kwargs  # type: Dict[str, Any]
                      ) -> None:

        op_args = self._parse_create_index_args(bucket_name,
                                                fields,
                                                index_name=index_name,
                                                **kwargs)

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.QUERY_INDEX.value,
            'op_type': query_index_mgmt_operations.CREATE_INDEX.value,
            'op_args': op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if kwargs.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = kwargs.get('timeout')

        return management_operation(**mgmt_kwargs)

    def _parse_create_index_args(self,   # noqa: C901
                                 bucket_name,  # type: str
                                 fields,  # type: Iterable[str]
                                 index_name=None,  # type: Optional[str]
                                 **kwargs  # type: Dict[str, Any]
                                 ) -> Dict[str, Any]:

        # @TODO: validate scope/collection?

        primary = kwargs.get('primary', False)
        condition = kwargs.get('condition', None)

        if primary and fields:
            raise TypeError('Cannot create primary index with explicit fields')
        elif not primary and not fields:
            raise ValueError('Fields required for non-primary index')

        if condition and primary:
            raise ValueError('cannot specify condition for primary index')

        op_args = {
            'bucket_name': bucket_name,
        }
        if index_name:
            op_args['index_name'] = index_name
        if primary is True:
            op_args['is_primary'] = primary
        if condition:
            op_args['condition'] = condition
        if fields and len(fields) > 0:
            if isinstance(fields, list):
                op_args['fields'] = fields
            else:
                op_args['fields'] = list(fields)
        if kwargs.get('ignore_if_exists', False) is True:
            op_args['ignore_if_exists'] = kwargs.get('ignore_if_exists')
        if kwargs.get('scope_name', None) is not None:
            op_args['scope_name'] = kwargs.get('scope_name')
        if kwargs.get('collection_name', None) is not None:
            op_args['collection_name'] = kwargs.get('collection_name')
        if kwargs.get('deferred', None) is not None:
            op_args['deferred'] = kwargs.get('deferred')
        if kwargs.get('num_replicas', None) is not None:
            op_args['num_replicas'] = kwargs.get('num_replicas')

        return op_args

    def _drop_index(self,
                    bucket_name,  # type: str
                    index_name=None,  # type: Optional[str]
                    **kwargs  # type: Dict[str, Any]
                    ) -> None:

        # @TODO: validate scope/collection?

        ignore_if_not_exists = kwargs.pop('ignore_if_not_exists', False)
        # previous ignore_missing was a variable kwarg - should only have ignore_if_not_exists
        ignore_missing = kwargs.pop('ignore_missing', False)

        op_args = {
            'bucket_name': bucket_name,
        }
        if index_name:
            op_args['index_name'] = index_name

        if kwargs.get('scope_name', None) is not None:
            op_args['scope_name'] = kwargs.get('scope_name')

        if kwargs.get('collection_name', None) is not None:
            op_args['collection_name'] = kwargs.get('collection_name')

        if ignore_if_not_exists is True or ignore_missing is True:
            op_args['ignore_if_does_not_exist'] = True

        if kwargs.get('primary', False) is True:
            op_args['is_primary'] = kwargs.get('primary')

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.QUERY_INDEX.value,
            'op_type': query_index_mgmt_operations.DROP_INDEX.value,
            'op_args': op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if kwargs.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = kwargs.get('timeout')

        return management_operation(**mgmt_kwargs)

    def create_index(self,
                     bucket_name,   # type: str
                     index_name,    # type: str
                     fields,        # type: Iterable[str]
                     *options,      # type: CreateQueryIndexOptions
                     **kwargs       # type: Dict[str,Any]
                     ) -> None:

        final_args = forward_args(kwargs, *options)
        return self._create_index(bucket_name, fields, index_name, **final_args)

    def create_primary_index(self,
                             bucket_name,  # type: str
                             *options,     # type: CreatePrimaryQueryIndexOptions
                             **kwargs      # type: Dict[str,Any]
                             ) -> None:
        """
        Creates a new primary index.

        :param str bucket_name:  name of the bucket.
        :param str index_name: name of the index.
        :param CreatePrimaryQueryIndexOptions options: Options to use when creating primary index
        :param Any kwargs: Override corresponding values in options.
        :raises: QueryIndexAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # CREATE INDEX index_name ON bucket_name WITH { "num_replica": 2 }
        #         https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/createindex.html
        #

        kwargs['primary'] = True
        final_args = forward_args(kwargs, *options)
        index_name = final_args.pop('index_name', None)
        return self._create_index(bucket_name, [], index_name, **final_args)

    def drop_index(self,
                   bucket_name,     # type: str
                   index_name,      # type: str
                   *options,        # type: DropQueryIndexOptions
                   **kwargs         # type: Dict[str,Any]
                   ) -> None:
        """
        Drops an index.

        :param str bucket_name: name of the bucket.
        :param str index_name: name of the index.
        :param DropQueryIndexOptions options: Options for dropping index.
        :param Any kwargs: Override corresponding value in options.
        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        final_args = forward_args(kwargs, *options)
        return self._drop_index(bucket_name, index_name, **final_args)

    def drop_primary_index(self,
                           bucket_name,     # type: str
                           *options,        # type: DropPrimaryQueryIndexOptions
                           **kwargs         # type: Dict[str,Any]
                           ) -> None:
        """
        Drops a primary index.

        :param bucket_name: name of the bucket.
        :param index_name:  name of the index.
        :param ignore_if_not_exists: Don't error/throw if the index does not exist.
        :param timeout:  the time allowed for the operation to be terminated. This is controlled by the client.

        :raises: QueryIndexNotFoundException
        :raises: InvalidArgumentsException
        """
        kwargs['primary'] = True
        final_args = forward_args(kwargs, *options)
        index_name = final_args.pop('index_name', None)
        return self._drop_index(bucket_name, index_name, **final_args)

    def get_all_indexes(self,
                        bucket_name,    # type: str
                        *options,       # type: GetAllQueryIndexOptions
                        **kwargs        # type: Dict[str,Any]
                        ) -> Optional[Iterable[QueryIndex]]:

        op_args = {
            'bucket_name': bucket_name,
        }
        final_args = forward_args(kwargs, *options)

        if final_args.get('scope_name', None) is not None:
            op_args['scope_name'] = final_args.get('scope_name')

        if final_args.get('collection_name', None) is not None:
            op_args['collection_name'] = final_args.get('collection_name')

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.QUERY_INDEX.value,
            'op_type': query_index_mgmt_operations.GET_ALL_INDEXES.value,
            'op_args': op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def build_deferred_indexes(self,
                               bucket_name,     # type: str
                               *options,        # type: BuildDeferredQueryIndexOptions
                               **kwargs
                               ) -> None:
        """
        Build Deferred builds all indexes which are currently in deferred state.

        :param str bucket_name: name of the bucket.
        :param BuildDeferredQueryIndexOptions options: Options for building deferred indexes.
        :param Any kwargs: Override corresponding value in options.
        :raise: InvalidArgumentsException

        """
        op_args = {
            'bucket_name': bucket_name,
        }
        final_args = forward_args(kwargs, *options)

        if final_args.get('scope_name', None) is not None:
            op_args['scope_name'] = final_args.get('scope_name')

        if final_args.get('collection_name', None) is not None:
            op_args['collection_name'] = final_args.get('collection_name')

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.QUERY_INDEX.value,
            'op_type': query_index_mgmt_operations.BUILD_DEFERRED_INDEXES.value,
            'op_args': op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)


@dataclass
class QueryIndex:
    name: str
    is_primary: bool
    type: str
    state: str
    namespace: str
    datastore_id: str
    keyspace: str
    index_key: list = field(default_factory=list)
    condition: str = None
    bucket_name: str = None
    scope_name: str = None
    collection_name: str = None
    partition: str = None

    @classmethod
    def from_server(cls,
                    json_data  # type: Dict[str, Any]
                    ):

        bucket_name = json_data.get('bucket_name', None)
        if bucket_name is None:
            bucket_name = json_data.get('keyspace_id', None)
        return cls(json_data.get('name'),
                   bool(json_data.get('is_primary')),
                   json_data.get('type'),
                   json_data.get('state'),
                   json_data.get('namespace_id'),
                   json_data.get('datastore_id'),
                   json_data.get('keyspace_id'),
                   json_data.get('index_key', []),
                   json_data.get('condition', None),
                   bucket_name,
                   json_data.get('scope_name', None),
                   json_data.get('collection_name', None),
                   json_data.get('partition', None)
                   )
