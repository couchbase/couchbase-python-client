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

"""Base class for all TestEnvironmentManager subclasses."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from tests_v2.environments.test_environment import TestEnvironment


class TestEnvironmentManager:
    """Base for all test-framework manager wrappers.

    Subclasses MUST set required_feature as a ClassVar.
    Set required_feature = None when the manager is always available
    (e.g. BasicBucketManagement which only needs a cluster connection).

    Naming convention: TestEnvironment{Feature}Manager

    TestEnvironment maintains a _manager_registry keyed by manager class.
    Calling cb_env.get_manager(TestEnvironmentQueryIndexManager) returns a
    fully typed instance; the TypeVar M resolves to the concrete subclass.

    Usage in a non-management test suite:
        cb_env.get_manager(TestEnvironmentQueryIndexManager).create_primary_index(
            cb_env.bucket.name, ignore_if_exists=True
        )

    Usage in a management test suite (testing the SDK manager itself): the same call.
    A management suite goes through a wrapper like everything else, and this is not a
    style preference.  A local fixture handing back a raw `cb_env.bucket.collections()`
    cannot work: the acouchbase and txcouchbase suites are generated from the sync source,
    and the generator decides what to await from a class model holding these wrappers and no
    SDK manager class, so a raw manager in a fixture generates unawaited calls.  What a
    management suite does differently is the wrapper it gets: one mirroring the SDK
    surface, variadic where the SDK is overloaded.  See managers/collection_mgmt.py.

    Annotating a subclass: the wrapped SDK manager is deliberately left untyped.
    setup() reaches it through env.cluster / env.bucket / env.scope, which on the
    unparameterized TestEnvironment are Any, so the held manager is Any and the
    delegation itself is unchecked.  That is the correct trade today: these wrappers
    are shared by all three API styles, and cluster.buckets() yields BucketManager
    under couchbase but AsyncBucketManager under acouchbase.  Pinning the attribute
    to either one would be a lie for the other two.

    What subclasses DO annotate is their own signature, using the sync SDK's
    parameter and return types.  Those are what the sync suites see, and they are
    what makes a wrapper method more useful than a bare getattr.  That is also why each
    module carries a concrete class per api style rather than one shared class: an async
    delegate returns a coroutine rather than the value annotated here.  Where a module stops
    short of three, that is a statement about the SDK and not about the suites: eventing.py
    stops at two api styles, four classes over a cluster-level and a scope-level manager,
    because txcouchbase has no eventing surface to wrap.
    """
    required_feature: ClassVar[ServerFeatures | None] = None

    def setup(self, env: TestEnvironment) -> None:  # noqa: B027
        """Called by TestEnvironment._setup_managers() after feature check passes."""
        pass

    def teardown(self, env: TestEnvironment) -> None:  # noqa: B027
        """Called by TestEnvironment._teardown_managers()."""
        pass
