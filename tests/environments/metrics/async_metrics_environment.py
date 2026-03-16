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

import random
from enum import IntEnum
from typing import (Any,
                    Dict,
                    Optional)

from couchbase.logic.observability import CollectionDetails
from tests.environments.metrics.base import (HttpMeterValidator,
                                             KeyValueMeterValidator,
                                             NoOpTestMeter,
                                             TestMeter,
                                             TestMeterType)
from tests.environments.test_environment import AsyncTestEnvironment


class MeterType(IntEnum):
    Basic = 1
    NoOp = 2


class AsyncMetricsEnvironment(AsyncTestEnvironment):

    def __init__(self, **kwargs: Any) -> None:
        self._meter: TestMeterType = kwargs.pop('meter')
        super().__init__(**kwargs)
        self._collection_details = {
            'bucket': self.bucket.name,
            'scope': self.scope.name,
            'collection_name': self.collection.name,
        }
        self._kv_meter_validator: Optional[KeyValueMeterValidator] = None
        self._http_meter_validator: Optional[HttpMeterValidator] = None

    @property
    def meter(self) -> TestMeterType:
        return self._meter

    @property
    def collection_details(self) -> CollectionDetails:
        return self._collection_details

    @property
    def http_meter_validator(self) -> HttpMeterValidator:
        if not self._http_meter_validator:
            self._http_meter_validator = HttpMeterValidator(self.meter, self.supports_cluster_labels)
        return self._http_meter_validator

    @property
    def kv_meter_validator(self) -> KeyValueMeterValidator:
        if not self._kv_meter_validator:
            self._kv_meter_validator = KeyValueMeterValidator(
                self.meter, self.collection_details, self.supports_cluster_labels)
        return self._kv_meter_validator

    def get_docs(self, num_docs: int) -> Dict[str, Any]:
        """Get existing documents from the loaded set.

        Args:
            num_docs: Number of documents to retrieve

        Returns:
            Dictionary mapping keys to document values
        """
        filtered_keys = set(self._loaded_docs.keys())
        available_keys = filtered_keys.difference(self._used_docs)
        keys = random.choices(list(available_keys), k=num_docs)
        self._used_docs.update(keys)
        return {k: self._loaded_docs[k] for k in keys}

    def get_new_docs(self, num_docs: int) -> Dict[str, Any]:
        """Generate new documents not yet in the loaded set.

        Args:
            num_docs: Number of new documents to generate

        Returns:
            Dictionary mapping keys to document values
        """
        docs = {}
        for v in self.data_provider.get_simple_docs(num_docs):
            key = f'{v["id"]}'
            self._used_extras.add(key)
            docs[key] = v

        return docs

    @classmethod
    async def from_environment(cls, env: AsyncTestEnvironment, **kwargs: Any) -> AsyncMetricsEnvironment:
        base_env_args = {
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        meter_type: MeterType = kwargs.pop('meter_type', None)
        meter = kwargs.pop('meter', None)
        if meter_type == MeterType.Basic:
            meter = TestMeter()
        elif meter_type == MeterType.NoOp:
            meter = NoOpTestMeter()

        if not meter:
            raise ValueError('Meter type or meter instance must be provided to create metrics environment.')

        # we need to pass in the trace to the base environment so the cluster is created w/ the tracer
        base_env_args['meter'] = meter

        # we have to create a new environment b/c we need a new cluster in order to set the tracer
        cb_env = await AsyncTestEnvironment.get_environment(**base_env_args)
        env_args = {
            'bucket': cb_env.bucket,
            'cluster': cb_env.cluster,
            'default_collection': cb_env.default_collection,
            'couchbase_config': cb_env.config,
            'data_provider': cb_env.data_provider,
            'meter': meter
        }

        return cls(**env_args)
