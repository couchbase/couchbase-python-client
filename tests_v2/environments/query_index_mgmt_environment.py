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

"""Index cleanup for the query-index-management suites.

One module per api style, the same split query_environment.py describes: the async and
twisted variants have their own modules beside this one.

Two classes rather than one class with a flag.  The cluster-level manager addresses
indexes by (bucket, scope, collection); the collection-level manager is bound to a single
collection and takes no namespace arguments at all.  Folding both into one boolean means
deriving that boolean from something, and the only thing available is the concrete test
class's name, which makes renaming a test class silently change which cleanup path runs.
Each suite names the helper it wants instead, and the helper's own name says which manager
it drives.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from couchbase.management.options import (DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions)
from tests_v2.environments.managers import TestEnvironmentCollectionQueryIndexManager, TestEnvironmentQueryIndexManager
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SyncTestEnvironment,
                                                    TestEnvironment)

# How long to let the server take to actually remove the indexes: the drop calls are
# issued once, then the namespace is polled until it reports empty.
DROP_POLL_ATTEMPTS = 10
DROP_POLL_SECONDS = 2

# The name the server gives an unnamed primary index.  Dropping a primary index that
# carries an explicit name requires passing that name back; passing '#primary' does not
# work, because it is not a name the client ever supplied.
DEFAULT_PRIMARY_INDEX_NAME = '#primary'

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.queries import QueryIndex


def _xfail_not_dropped(namespace: str) -> None:
    pytest.xfail(f'Indexes in {namespace} were not dropped after {DROP_POLL_ATTEMPTS} '
                 f'waits of {DROP_POLL_SECONDS} seconds each.')


class QueryIndexManagementEnvironment:
    """Index cleanup through the cluster-level manager, cluster.query_indexes()."""

    @staticmethod
    def clear_all_indexes(cb_env: SyncTestEnvironment,
                          collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
                          ignore_fail: bool = False) -> None:
        """Drop every index in every namespace this suite could have written to.

        `_default`.`_default` is always cleared, because tests reach it whenever they
        omit scope_name/collection_name, which several deliberately do.
        """
        if not cb_env.has_manager(TestEnvironmentQueryIndexManager):
            return

        namespaces = [('_default', '_default')]
        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            handle = cb_env.collection_pool.current
            namespaces.append((handle.scope_name, handle.collection_name))

        qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)
        for scope_name, collection_name in namespaces:
            dropped = QueryIndexManagementEnvironment._drop_until_empty(
                cb_env, qixm, scope_name, collection_name
            )
            if not dropped and not ignore_fail:
                _xfail_not_dropped(f'`{scope_name}`.`{collection_name}`')

    @staticmethod
    def _drop_until_empty(cb_env: SyncTestEnvironment,
                          qixm: TestEnvironmentQueryIndexManager,
                          scope_name: str,
                          collection_name: str) -> bool:
        for index in QueryIndexManagementEnvironment._get_all_indexes(
            cb_env, qixm, scope_name, collection_name
        ):
            QueryIndexManagementEnvironment._drop_index(
                cb_env, qixm, index, scope_name, collection_name
            )
        for _ in range(DROP_POLL_ATTEMPTS):
            remaining = QueryIndexManagementEnvironment._get_all_indexes(
                cb_env, qixm, scope_name, collection_name
            )
            if not remaining:
                return True
            TestEnvironment.sleep(DROP_POLL_SECONDS)
        return False

    @staticmethod
    def _get_all_indexes(cb_env: SyncTestEnvironment,
                         qixm: TestEnvironmentQueryIndexManager,
                         scope_name: str,
                         collection_name: str) -> Iterable[QueryIndex]:
        return qixm.get_all_indexes(cb_env.bucket.name,
                                    GetAllQueryIndexOptions(scope_name=scope_name,
                                                            collection_name=collection_name))

    @staticmethod
    def _drop_index(cb_env: SyncTestEnvironment,
                    qixm: TestEnvironmentQueryIndexManager,
                    index: QueryIndex,
                    scope_name: str,
                    collection_name: str) -> None:
        if index.is_primary:
            opts = DropPrimaryQueryIndexOptions(scope_name=scope_name,
                                                collection_name=collection_name)
            if index.name != DEFAULT_PRIMARY_INDEX_NAME:
                opts['index_name'] = index.name
            qixm.drop_primary_index(cb_env.bucket.name, opts)
        else:
            qixm.drop_index(cb_env.bucket.name,
                            index.name,
                            DropQueryIndexOptions(scope_name=scope_name,
                                                  collection_name=collection_name))


class CollectionQueryIndexManagementEnvironment:
    """Index cleanup through the collection-level manager, collection.query_indexes().

    No namespace arguments: the manager follows cb_env's current collection and can
    neither see nor drop indexes anywhere else.  Looping over `_default`.`_default` plus the
    named scope/collection here, as the cluster-level class does, would run both passes
    against whichever collection is current and empty it twice.
    """

    @staticmethod
    def clear_all_indexes(cb_env: SyncTestEnvironment, ignore_fail: bool = False) -> None:
        if not cb_env.has_manager(TestEnvironmentCollectionQueryIndexManager):
            return

        qixm = cb_env.get_manager(TestEnvironmentCollectionQueryIndexManager)
        for index in qixm.get_all_indexes():
            CollectionQueryIndexManagementEnvironment._drop_index(qixm, index)
        for _ in range(DROP_POLL_ATTEMPTS):
            if not qixm.get_all_indexes():
                return
            TestEnvironment.sleep(DROP_POLL_SECONDS)
        if not ignore_fail:
            _xfail_not_dropped(cb_env.fqdn)

    @staticmethod
    def _drop_index(qixm: TestEnvironmentCollectionQueryIndexManager, index: QueryIndex) -> None:
        if index.is_primary:
            opts = DropPrimaryQueryIndexOptions()
            if index.name != DEFAULT_PRIMARY_INDEX_NAME:
                opts['index_name'] = index.name
            qixm.drop_primary_index(opts)
        else:
            qixm.drop_index(index.name)
