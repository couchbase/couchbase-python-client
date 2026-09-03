#  Copyright 2016-2024. Couchbase, Inc.
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

"""Index cleanup for the acouchbase query-index-management suites.

The async counterpart of query_index_mgmt_environment.py.  Same two classes, same reason
for there being two: the cluster-level manager addresses indexes by (bucket, scope,
collection), the collection-level one is bound to a single collection and takes no
namespace arguments.

There is a Twisted counterpart too; see twisted_query_index_mgmt_environment.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from couchbase.management.options import (DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions)
from tests_v2.environments.async_environment import AsyncTestEnvironment
from tests_v2.environments.managers import (AsyncTestEnvironmentCollectionQueryIndexManager,
                                            AsyncTestEnvironmentQueryIndexManager)
from tests_v2.environments.test_environment import CollectionStrategy

# Kept in step with the sync module rather than re-tuned: a different poll budget here
# would make an async-only flake look like a real difference in server behaviour.
DROP_POLL_ATTEMPTS = 10
DROP_POLL_SECONDS = 2

DEFAULT_PRIMARY_INDEX_NAME = '#primary'

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.queries import QueryIndex


def _xfail_not_dropped(namespace: str) -> None:
    pytest.xfail(f'Indexes in {namespace} were not dropped after {DROP_POLL_ATTEMPTS} '
                 f'waits of {DROP_POLL_SECONDS} seconds each.')


class AsyncQueryIndexManagementEnvironment:
    """Index cleanup through the cluster-level manager, cluster.query_indexes()."""

    @staticmethod
    async def clear_all_indexes(cb_env: AsyncTestEnvironment,
                                collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
                                ignore_fail: bool = False) -> None:
        """Drop every index in every namespace this suite could have written to.

        `_default`.`_default` is always cleared, because tests reach it whenever they omit
        scope_name/collection_name, which several deliberately do.
        """
        if not cb_env.has_manager(AsyncTestEnvironmentQueryIndexManager):
            return

        namespaces = [('_default', '_default')]
        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            handle = cb_env.collection_pool.current
            namespaces.append((handle.scope_name, handle.collection_name))

        qixm = cb_env.get_manager(AsyncTestEnvironmentQueryIndexManager)
        for scope_name, collection_name in namespaces:
            dropped = await AsyncQueryIndexManagementEnvironment._drop_until_empty(
                cb_env, qixm, scope_name, collection_name
            )
            if not dropped and not ignore_fail:
                _xfail_not_dropped(f'`{scope_name}`.`{collection_name}`')

    @staticmethod
    async def _drop_until_empty(cb_env: AsyncTestEnvironment,
                                qixm: AsyncTestEnvironmentQueryIndexManager,
                                scope_name: str,
                                collection_name: str) -> bool:
        for index in await AsyncQueryIndexManagementEnvironment._get_all_indexes(
            cb_env, qixm, scope_name, collection_name
        ):
            await AsyncQueryIndexManagementEnvironment._drop_index(
                cb_env, qixm, index, scope_name, collection_name
            )
        for _ in range(DROP_POLL_ATTEMPTS):
            remaining = await AsyncQueryIndexManagementEnvironment._get_all_indexes(
                cb_env, qixm, scope_name, collection_name
            )
            if not remaining:
                return True
            await AsyncTestEnvironment.sleep(DROP_POLL_SECONDS)
        return False

    @staticmethod
    async def _get_all_indexes(cb_env: AsyncTestEnvironment,
                               qixm: AsyncTestEnvironmentQueryIndexManager,
                               scope_name: str,
                               collection_name: str) -> Iterable[QueryIndex]:
        return await qixm.get_all_indexes(cb_env.bucket.name,
                                          GetAllQueryIndexOptions(scope_name=scope_name,
                                                                  collection_name=collection_name))

    @staticmethod
    async def _drop_index(cb_env: AsyncTestEnvironment,
                          qixm: AsyncTestEnvironmentQueryIndexManager,
                          index: QueryIndex,
                          scope_name: str,
                          collection_name: str) -> None:
        if index.is_primary:
            opts = DropPrimaryQueryIndexOptions(scope_name=scope_name,
                                                collection_name=collection_name)
            if index.name != DEFAULT_PRIMARY_INDEX_NAME:
                opts['index_name'] = index.name
            await qixm.drop_primary_index(cb_env.bucket.name, opts)
        else:
            await qixm.drop_index(cb_env.bucket.name,
                                  index.name,
                                  DropQueryIndexOptions(scope_name=scope_name,
                                                        collection_name=collection_name))


class AsyncCollectionQueryIndexManagementEnvironment:
    """Index cleanup through the collection-level manager, collection.query_indexes().

    No namespace arguments: the manager follows cb_env's current collection and can
    neither see nor drop indexes anywhere else.
    """

    @staticmethod
    async def clear_all_indexes(cb_env: AsyncTestEnvironment, ignore_fail: bool = False) -> None:
        if not cb_env.has_manager(AsyncTestEnvironmentCollectionQueryIndexManager):
            return

        qixm = cb_env.get_manager(AsyncTestEnvironmentCollectionQueryIndexManager)
        for index in await qixm.get_all_indexes():
            await AsyncCollectionQueryIndexManagementEnvironment._drop_index(qixm, index)
        for _ in range(DROP_POLL_ATTEMPTS):
            if not await qixm.get_all_indexes():
                return
            await AsyncTestEnvironment.sleep(DROP_POLL_SECONDS)
        if not ignore_fail:
            _xfail_not_dropped(cb_env.fqdn)

    @staticmethod
    async def _drop_index(qixm: AsyncTestEnvironmentCollectionQueryIndexManager,
                          index: QueryIndex) -> None:
        if index.is_primary:
            opts = DropPrimaryQueryIndexOptions()
            if index.name != DEFAULT_PRIMARY_INDEX_NAME:
                opts['index_name'] = index.name
            await qixm.drop_primary_index(opts)
        else:
            # No options, matching the sync module exactly.  The collection-level manager
            # is already bound to one collection, so there is no namespace to pass.
            await qixm.drop_index(index.name)
