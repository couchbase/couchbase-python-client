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

import random
from typing import (Any,
                    Dict,
                    Optional)

from couchbase.logic.observability import CollectionDetails
from tests.environments.test_environment import AsyncTestEnvironment
from tests.environments.tracing.base import (HttpSpanValidator,
                                             KeyValueSpanValidator,
                                             LegacyTestTracer,
                                             NoOpTestTracer,
                                             TestThresholdLoggingTracer,
                                             TestTracer,
                                             TestTracerType)
from tests.environments.tracing.base_tracing_environment import TracingType


class AsyncBaseTracingEnvironment(AsyncTestEnvironment):
    """Async base environment for all tracing tests.

    Provides common tracing infrastructure including:
    - Tracer instances (both legacy and new)
    - Collection details for span validation
    - Span validators (KV and HTTP)
    - Helper methods for document management
    """

    def __init__(self, **kwargs: Any) -> None:
        self._tracer: TestTracerType = kwargs.pop('tracer')
        super().__init__(**kwargs)
        self._collection_details = {
            'bucket': self.bucket.name,
            'scope': self.scope.name,
            'collection_name': self.collection.name,
        }
        self._kv_span_validator: Optional[KeyValueSpanValidator] = None
        self._http_span_validator: Optional[HttpSpanValidator] = None

    @property
    def tracer(self) -> Optional[TestTracerType]:
        return self._tracer

    @property
    def collection_details(self) -> CollectionDetails:
        return self._collection_details

    @property
    def http_span_validator(self) -> HttpSpanValidator:
        if not self._http_span_validator:
            self._http_span_validator = HttpSpanValidator(self.tracer, self.supports_cluster_labels)
        return self._http_span_validator

    @property
    def kv_span_validator(self) -> KeyValueSpanValidator:
        if not self._kv_span_validator:
            self._kv_span_validator = KeyValueSpanValidator(
                self.tracer, self.collection_details, self.supports_cluster_labels)
        return self._kv_span_validator

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
    async def from_environment(cls, env: AsyncTestEnvironment, **kwargs: Any) -> AsyncBaseTracingEnvironment:
        """Create a tracing environment from an existing async test environment.

        Args:
            env: The base async test environment
            **kwargs: Additional arguments:
                - create_tracer: If present, creates a new RequestTracer
                - create_legacy_tracer: If present, creates a new CouchbaseTracer

        Returns:
            New async tracing environment with tracer instances configured
        """
        base_env_args = {
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        # let this fail if not provided, since we need to know which tracer to create
        tracing_type: TracingType = kwargs.pop('tracing_type')
        if tracing_type == TracingType.Modern:
            tracer = TestTracer()
        elif tracing_type == TracingType.Legacy:
            tracer = LegacyTestTracer()
        elif tracing_type == TracingType.NoOp:
            tracer = NoOpTestTracer()
        elif tracing_type == TracingType.ThresholdLogging:
            tracer = TestThresholdLoggingTracer()
        else:
            raise ValueError(f"Unsupported tracing type: {tracing_type}")

        # we need to pass in the trace to the base environment so the cluster is created w/ the tracer
        base_env_args['tracer'] = tracer

        # we have to create a new environment b/c we need a new cluster in order to set the tracer
        cb_env = await AsyncTestEnvironment.get_environment(**base_env_args)
        env_args = {
            'bucket': cb_env.bucket,
            'cluster': cb_env.cluster,
            'default_collection': cb_env.default_collection,
            'couchbase_config': cb_env.config,
            'data_provider': cb_env.data_provider,
            'tracer': tracer
        }

        return cls(**env_args)
