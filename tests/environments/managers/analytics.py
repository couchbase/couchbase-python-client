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

"""Analytics index test-infrastructure managers, one per api style.

Every wrapper mirrors the SDK's `options, **kwargs` tail as `*options: Any, **kwargs: Any`.
The analytics manager takes its options block as an ordinary trailing positional
(`options=None`), and analyticsmgmt_t passes it that way in seventeen places, so a wrapper
accepting only `**kwargs` makes those calls unwritable.  It
does not merely fail to type them: mypy reports `Too many arguments`, which reads as a defect
in the test rather than a gap in the wrapper.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests.environments.managers.base import TestEnvironmentManager
from tests.test_features import ServerFeatures

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.analytics import (AnalyticsDataset,
                                                AnalyticsDataType,
                                                AnalyticsIndex,
                                                AnalyticsLink)
    from tests.environments.test_environment import TestEnvironment


class TestEnvironmentAnalyticsIndexManager(TestEnvironmentManager):
    """Wraps couchbase.cluster.Cluster.analytics_indexes()."""
    required_feature = ServerFeatures.Analytics

    def setup(self, env: TestEnvironment) -> None:
        self._aixm = env.cluster.analytics_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_pending_mutations(self, *options: Any, **kwargs: Any) -> dict[str, int]:
        return self._aixm.get_pending_mutations(*options, **kwargs)

    def create_dataverse(self, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        self._aixm.create_dataverse(dataverse_name, *options, **kwargs)

    def drop_dataverse(self, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        self._aixm.drop_dataverse(dataverse_name, *options, **kwargs)

    def create_dataset(self, dataset_name: str, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._aixm.create_dataset(dataset_name, bucket_name, *options, **kwargs)

    def drop_dataset(self, dataset_name: str, *options: Any, **kwargs: Any) -> None:
        self._aixm.drop_dataset(dataset_name, *options, **kwargs)

    def get_all_datasets(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsDataset]:
        return self._aixm.get_all_datasets(*options, **kwargs)

    def create_index(self,
                     index_name: str,
                     dataset_name: str,
                     fields: dict[str, AnalyticsDataType],
                     *options: Any,
                     **kwargs: Any) -> None:
        self._aixm.create_index(index_name, dataset_name, fields, *options, **kwargs)

    def drop_index(self, index_name: str, dataset_name: str, *options: Any, **kwargs: Any) -> None:
        self._aixm.drop_index(index_name, dataset_name, *options, **kwargs)

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsIndex]:
        return self._aixm.get_all_indexes(*options, **kwargs)

    def connect_link(self, *options: Any, **kwargs: Any) -> None:
        self._aixm.connect_link(*options, **kwargs)

    def disconnect_link(self, *options: Any, **kwargs: Any) -> None:
        self._aixm.disconnect_link(*options, **kwargs)

    def get_links(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsLink]:
        return self._aixm.get_links(*options, **kwargs)

    def create_link(self, link: AnalyticsLink, *options: Any, **kwargs: Any) -> None:
        self._aixm.create_link(link, *options, **kwargs)

    def replace_link(self, link: AnalyticsLink, *options: Any, **kwargs: Any) -> None:
        self._aixm.replace_link(link, *options, **kwargs)

    def drop_link(self, link_name: str, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        self._aixm.drop_link(link_name, dataverse_name, *options, **kwargs)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentAnalyticsIndexManager(TestEnvironmentManager):
    """Wraps acouchbase.cluster.AsyncCluster.analytics_indexes()."""
    required_feature = ServerFeatures.Analytics

    def setup(self, env: TestEnvironment) -> None:
        self._aixm = env.cluster.analytics_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def get_pending_mutations(self, *options: Any, **kwargs: Any) -> dict[str, int]:
        return await self._aixm.get_pending_mutations(*options, **kwargs)

    async def create_dataverse(self, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        await self._aixm.create_dataverse(dataverse_name, *options, **kwargs)

    async def drop_dataverse(self, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        await self._aixm.drop_dataverse(dataverse_name, *options, **kwargs)

    async def create_dataset(self, dataset_name: str, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        await self._aixm.create_dataset(dataset_name, bucket_name, *options, **kwargs)

    async def drop_dataset(self, dataset_name: str, *options: Any, **kwargs: Any) -> None:
        await self._aixm.drop_dataset(dataset_name, *options, **kwargs)

    async def get_all_datasets(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsDataset]:
        return await self._aixm.get_all_datasets(*options, **kwargs)

    async def create_index(self,
                           index_name: str,
                           dataset_name: str,
                           fields: dict[str, AnalyticsDataType],
                           *options: Any,
                           **kwargs: Any) -> None:
        await self._aixm.create_index(index_name, dataset_name, fields, *options, **kwargs)

    async def drop_index(self, index_name: str, dataset_name: str, *options: Any, **kwargs: Any) -> None:
        await self._aixm.drop_index(index_name, dataset_name, *options, **kwargs)

    async def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsIndex]:
        return await self._aixm.get_all_indexes(*options, **kwargs)

    async def connect_link(self, *options: Any, **kwargs: Any) -> None:
        await self._aixm.connect_link(*options, **kwargs)

    async def disconnect_link(self, *options: Any, **kwargs: Any) -> None:
        await self._aixm.disconnect_link(*options, **kwargs)

    async def get_links(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsLink]:
        return await self._aixm.get_links(*options, **kwargs)

    async def create_link(self, link: AnalyticsLink, *options: Any, **kwargs: Any) -> None:
        await self._aixm.create_link(link, *options, **kwargs)

    async def replace_link(self, link: AnalyticsLink, *options: Any, **kwargs: Any) -> None:
        await self._aixm.replace_link(link, *options, **kwargs)

    async def drop_link(self, link_name: str, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        await self._aixm.drop_link(link_name, dataverse_name, *options, **kwargs)


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentAnalyticsIndexManager(TestEnvironmentManager):
    """Wraps txcouchbase.cluster.Cluster.analytics_indexes(), dispatched via the reactor."""
    required_feature = ServerFeatures.Analytics

    def setup(self, env: TestEnvironment) -> None:
        from tests.environments.twisted_environment import _get_run_in_reactor_thread
        self._aixm = env.cluster.analytics_indexes()
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_pending_mutations(self, *options: Any, **kwargs: Any) -> dict[str, int]:
        return self._rit(self._aixm.get_pending_mutations, *options, **kwargs)

    def create_dataverse(self, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.create_dataverse, dataverse_name, *options, **kwargs)

    def drop_dataverse(self, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.drop_dataverse, dataverse_name, *options, **kwargs)

    def create_dataset(self, dataset_name: str, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.create_dataset, dataset_name, bucket_name, *options, **kwargs)

    def drop_dataset(self, dataset_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.drop_dataset, dataset_name, *options, **kwargs)

    def get_all_datasets(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsDataset]:
        return self._rit(self._aixm.get_all_datasets, *options, **kwargs)

    def create_index(self,
                     index_name: str,
                     dataset_name: str,
                     fields: dict[str, AnalyticsDataType],
                     *options: Any,
                     **kwargs: Any) -> None:
        self._rit(self._aixm.create_index, index_name, dataset_name, fields, *options, **kwargs)

    def drop_index(self, index_name: str, dataset_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.drop_index, index_name, dataset_name, *options, **kwargs)

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsIndex]:
        return self._rit(self._aixm.get_all_indexes, *options, **kwargs)

    def connect_link(self, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.connect_link, *options, **kwargs)

    def disconnect_link(self, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.disconnect_link, *options, **kwargs)

    def get_links(self, *options: Any, **kwargs: Any) -> Iterable[AnalyticsLink]:
        return self._rit(self._aixm.get_links, *options, **kwargs)

    def create_link(self, link: AnalyticsLink, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.create_link, link, *options, **kwargs)

    def replace_link(self, link: AnalyticsLink, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.replace_link, link, *options, **kwargs)

    def drop_link(self, link_name: str, dataverse_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._aixm.drop_link, link_name, dataverse_name, *options, **kwargs)
