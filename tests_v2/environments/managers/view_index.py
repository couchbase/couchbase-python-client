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

"""View index test-infrastructure managers, one per api style.

All three wrap the same five methods, and all three carry use_bucket(), which no other manager
in the package needs.  The reason is the views *query* suite: it creates a bucket of its own,
because a view is a property of a bucket and the shared test bucket cannot grow a design
document without every other suite in the run seeing it.  The design document therefore has to
go on a bucket that does not exist when the registry is built, so the manager is re-pointed once
it does.

Re-pointing rather than a second registry entry, and the registry is why.  It is keyed by
manager *class*, so a second view index manager on a second bucket would need a second class
whose only difference is which bucket setup() reads, and the suite would then have to name the
right one of two nearly identical classes at every call site.  One manager that can be told
which bucket it manages says the same thing once.

Every wrapped method forwards *options positionally as well as **kwargs, because the SDK
methods are spelled `(..., *options, **kwargs)` and the management suite passes real
GetDesignDocumentOptions, GetAllDesignDocumentsOptions and PublishDesignDocumentOptions
objects.  A wrapper taking only **kwargs would still reach the server, since each option
class has a keyword equivalent, but it would leave those three public classes with no test
constructing them at all.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests_v2.environments.managers.base import TestEnvironmentManager
from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.views import DesignDocument, DesignDocumentNamespace
    from tests_v2.environments.test_environment import TestEnvironment


class TestEnvironmentViewIndexManager(TestEnvironmentManager):
    """Wraps couchbase.bucket.Bucket.view_indexes()."""
    required_feature = ServerFeatures.ViewIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._vixm = env.bucket.view_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def use_bucket(self, bucket: Any) -> None:
        """Manage design documents on `bucket` instead of the environment's shared one.

        Called by the views environment after it creates the bucket the suite queries; see the
        module docstring.  Untyped by the same rule the base class states for the wrapped
        manager itself: this takes whichever Bucket class its api style has, and pinning it to
        one would be a lie for the other two.
        """
        self._vixm = bucket.view_indexes()

    def get_design_document(self,
                            name: str,
                            namespace: DesignDocumentNamespace,
                            *options: Any, **kwargs: Any) -> DesignDocument:
        return self._vixm.get_design_document(name, namespace, *options, **kwargs)

    def get_all_design_documents(self,
                                 namespace: DesignDocumentNamespace,
                                 *options: Any, **kwargs: Any) -> Iterable[DesignDocument]:
        return self._vixm.get_all_design_documents(namespace, *options, **kwargs)

    def upsert_design_document(self,
                               design_doc: DesignDocument,
                               namespace: DesignDocumentNamespace,
                               *options: Any, **kwargs: Any) -> None:
        self._vixm.upsert_design_document(design_doc, namespace, *options, **kwargs)

    def drop_design_document(self,
                             name: str,
                             namespace: DesignDocumentNamespace,
                             *options: Any, **kwargs: Any) -> None:
        self._vixm.drop_design_document(name, namespace, *options, **kwargs)

    def publish_design_document(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._vixm.publish_design_document(name, *options, **kwargs)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentViewIndexManager(TestEnvironmentManager):
    """Wraps acouchbase.bucket.AsyncBucket.view_indexes().

    setup(), teardown() and use_bucket() stay synchronous, matching the bucket and collection
    managers: TestEnvironment._setup_managers() calls setup() through an unawaited loop for
    every api style, and bucket.view_indexes() itself performs no I/O.

    Every wrapped method returns the delegate's awaitable rather than swallowing it.  The sync
    wrapper can drop upsert_design_document's return value because there is not one; the async
    delegate returns a coroutine, and a wrapper that discards it leaves the design document
    uncreated while setup proceeds as though the view exists.
    """
    required_feature = ServerFeatures.ViewIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._vixm = env.bucket.view_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def use_bucket(self, bucket: Any) -> None:
        self._vixm = bucket.view_indexes()

    async def get_design_document(self,
                                  name: str,
                                  namespace: DesignDocumentNamespace,
                                  *options: Any, **kwargs: Any) -> DesignDocument:
        return await self._vixm.get_design_document(name, namespace, *options, **kwargs)

    async def get_all_design_documents(self,
                                       namespace: DesignDocumentNamespace,
                                       *options: Any, **kwargs: Any) -> Iterable[DesignDocument]:
        return await self._vixm.get_all_design_documents(namespace, *options, **kwargs)

    async def upsert_design_document(self,
                                     design_doc: DesignDocument,
                                     namespace: DesignDocumentNamespace,
                                     *options: Any, **kwargs: Any) -> None:
        await self._vixm.upsert_design_document(design_doc, namespace, *options, **kwargs)

    async def drop_design_document(self,
                                   name: str,
                                   namespace: DesignDocumentNamespace,
                                   *options: Any, **kwargs: Any) -> None:
        await self._vixm.drop_design_document(name, namespace, *options, **kwargs)

    async def publish_design_document(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._vixm.publish_design_document(name, *options, **kwargs)


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentViewIndexManager(TestEnvironmentManager):
    """Wraps txcouchbase.bucket.TxBucket.view_indexes(), dispatching via the reactor thread.

    The txcouchbase manager methods return Deferreds, so each call goes through
    run_in_reactor_thread(), which blocks the calling (pytest) thread until the Deferred
    resolves.  That is what lets a synchronous test body drive them, and it is why these methods
    keep the sync signatures while the async ones above do not.
    """
    required_feature = ServerFeatures.ViewIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        from tests_v2.environments.twisted_environment import _get_run_in_reactor_thread
        self._vixm = env.bucket.view_indexes()
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def use_bucket(self, bucket: Any) -> None:
        self._vixm = bucket.view_indexes()

    def get_design_document(self,
                            name: str,
                            namespace: DesignDocumentNamespace,
                            *options: Any, **kwargs: Any) -> DesignDocument:
        return self._rit(self._vixm.get_design_document, name, namespace, *options, **kwargs)

    def get_all_design_documents(self,
                                 namespace: DesignDocumentNamespace,
                                 *options: Any, **kwargs: Any) -> Iterable[DesignDocument]:
        return self._rit(self._vixm.get_all_design_documents, namespace, *options, **kwargs)

    def upsert_design_document(self,
                               design_doc: DesignDocument,
                               namespace: DesignDocumentNamespace,
                               *options: Any, **kwargs: Any) -> None:
        self._rit(self._vixm.upsert_design_document, design_doc, namespace, *options, **kwargs)

    def drop_design_document(self,
                             name: str,
                             namespace: DesignDocumentNamespace,
                             *options: Any, **kwargs: Any) -> None:
        self._rit(self._vixm.drop_design_document, name, namespace, *options, **kwargs)

    def publish_design_document(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._vixm.publish_design_document, name, *options, **kwargs)
