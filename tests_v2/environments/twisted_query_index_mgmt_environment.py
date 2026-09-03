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

"""Index cleanup for the txcouchbase query-index-management suites.

The Twisted counterpart of query_index_mgmt_environment.py, completing the trio.

**One class, where the sync and async modules have two.** They also carry a
CollectionQueryIndexManagementEnvironment, for the manager reached through
`collection.query_indexes()`.  txcouchbase has no such manager: `txcouchbase/collection.py`
defines no `query_indexes()`, and its only one is the cluster-level manager on Cluster.  The
suite class that drives it carries a `testgen_exclude` for this api, so a Twisted counterpart
here would have no caller; see the note at the end of managers/query_index.py.

Unlike twisted_query_environment.py, nothing here goes through run_in_reactor_thread.  That
module issues raw N1QL for servers predating the manager API, so it holds a `cluster.query()`
call and has to bridge it; this one only ever calls the manager wrappers, and
TwistedTestEnvironmentQueryIndexManager bridges every call itself.  So the bodies read as
straight-line synchronous code and are, in fact, the sync module with the manager classes
renamed.  That is the honest description, and it is worth stating rather than dressing the
file up as more Twisted-specific than it is.

`txcouchbase.n1ql.N1QLRequest` defines no `execute()`, so a suite driving DDL through
`.execute()` cannot run here at all.  Nothing needs to: `.execute()` only drives a statement
to completion, and cb_env.drain_rows() does that in all three api styles.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from couchbase.management.options import (DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions)
from tests_v2.environments.managers import TwistedTestEnvironmentQueryIndexManager
from tests_v2.environments.test_environment import CollectionStrategy
from tests_v2.environments.twisted_environment import TwistedTestEnvironment

# Kept in step with the sync and async modules rather than re-tuned: a different poll budget
# here would make a Twisted-only flake look like a real difference in server behaviour.
DROP_POLL_ATTEMPTS = 10
DROP_POLL_SECONDS = 2

DEFAULT_PRIMARY_INDEX_NAME = '#primary'

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.queries import QueryIndex


def _xfail_not_dropped(namespace: str) -> None:
    pytest.xfail(f'Indexes in {namespace} were not dropped after {DROP_POLL_ATTEMPTS} '
                 f'waits of {DROP_POLL_SECONDS} seconds each.')


class TwistedQueryIndexManagementEnvironment:
    """Index cleanup through the cluster-level manager, cluster.query_indexes()."""

    @staticmethod
    def clear_all_indexes(cb_env: TwistedTestEnvironment,
                          collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
                          ignore_fail: bool = False) -> None:
        """Drop every index in every namespace this suite could have written to.

        `_default`.`_default` is always cleared, because tests reach it whenever they omit
        scope_name/collection_name, which several deliberately do.
        """
        if not cb_env.has_manager(TwistedTestEnvironmentQueryIndexManager):
            return

        namespaces = [('_default', '_default')]
        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            handle = cb_env.collection_pool.current
            namespaces.append((handle.scope_name, handle.collection_name))

        qixm = cb_env.get_manager(TwistedTestEnvironmentQueryIndexManager)
        for scope_name, collection_name in namespaces:
            dropped = TwistedQueryIndexManagementEnvironment._drop_until_empty(
                cb_env, qixm, scope_name, collection_name
            )
            if not dropped and not ignore_fail:
                _xfail_not_dropped(f'`{scope_name}`.`{collection_name}`')

    @staticmethod
    def _drop_until_empty(cb_env: TwistedTestEnvironment,
                          qixm: TwistedTestEnvironmentQueryIndexManager,
                          scope_name: str,
                          collection_name: str) -> bool:
        for index in TwistedQueryIndexManagementEnvironment._get_all_indexes(
            cb_env, qixm, scope_name, collection_name
        ):
            TwistedQueryIndexManagementEnvironment._drop_index(
                cb_env, qixm, index, scope_name, collection_name
            )
        for _ in range(DROP_POLL_ATTEMPTS):
            remaining = TwistedQueryIndexManagementEnvironment._get_all_indexes(
                cb_env, qixm, scope_name, collection_name
            )
            if not remaining:
                return True
            TwistedTestEnvironment.sleep(DROP_POLL_SECONDS)
        return False

    @staticmethod
    def _get_all_indexes(cb_env: TwistedTestEnvironment,
                         qixm: TwistedTestEnvironmentQueryIndexManager,
                         scope_name: str,
                         collection_name: str) -> Iterable[QueryIndex]:
        return qixm.get_all_indexes(cb_env.bucket.name,
                                    GetAllQueryIndexOptions(scope_name=scope_name,
                                                            collection_name=collection_name))

    @staticmethod
    def _drop_index(cb_env: TwistedTestEnvironment,
                    qixm: TwistedTestEnvironmentQueryIndexManager,
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
