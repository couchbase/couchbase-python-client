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

"""tests.environments.managers package.

Re-exports every public manager class so callers can use the short form::

    from tests.environments.managers import TestEnvironmentCollectionManager

Package layout is one module per management surface, named for the surface it manages, each
holding the base class and whichever api-style variants that surface supports.  `base.py` is
the exception: it holds the class the rest derive from.
"""

from __future__ import annotations

from tests.environments.managers.analytics import (AsyncTestEnvironmentAnalyticsIndexManager,
                                                   TestEnvironmentAnalyticsIndexManager,
                                                   TwistedTestEnvironmentAnalyticsIndexManager)
from tests.environments.managers.base import TestEnvironmentManager
from tests.environments.managers.bucket import (AsyncTestEnvironmentBucketManager,
                                                TestEnvironmentBucketManager,
                                                TwistedTestEnvironmentBucketManager)
from tests.environments.managers.collection import (AsyncTestEnvironmentCollectionManager,
                                                    TestEnvironmentCollectionManager,
                                                    TwistedTestEnvironmentCollectionManager)
from tests.environments.managers.collection_mgmt import (AsyncTestEnvironmentCollectionMgmtManager,
                                                         TestEnvironmentCollectionMgmtManager,
                                                         TwistedTestEnvironmentCollectionMgmtManager)
from tests.environments.managers.eventing import (AsyncTestEnvironmentEventingFunctionManager,
                                                  AsyncTestEnvironmentScopeEventingFunctionManager,
                                                  TestEnvironmentEventingFunctionManager,
                                                  TestEnvironmentScopeEventingFunctionManager)
from tests.environments.managers.query_index import (AsyncTestEnvironmentCollectionQueryIndexManager,
                                                     AsyncTestEnvironmentQueryIndexManager,
                                                     TestEnvironmentCollectionQueryIndexManager,
                                                     TestEnvironmentQueryIndexManager,
                                                     TwistedTestEnvironmentQueryIndexManager)
from tests.environments.managers.search_index import (AsyncTestEnvironmentScopeSearchIndexManager,
                                                      AsyncTestEnvironmentSearchIndexManager,
                                                      TestEnvironmentScopeSearchIndexManager,
                                                      TestEnvironmentSearchIndexManager,
                                                      TwistedTestEnvironmentScopeSearchIndexManager,
                                                      TwistedTestEnvironmentSearchIndexManager)
from tests.environments.managers.user import (AsyncTestEnvironmentUserManager,
                                              TestEnvironmentUserManager,
                                              TwistedTestEnvironmentUserManager)
from tests.environments.managers.view_index import (AsyncTestEnvironmentViewIndexManager,
                                                    TestEnvironmentViewIndexManager,
                                                    TwistedTestEnvironmentViewIndexManager)

__all__ = [
    'TestEnvironmentManager',
    'TestEnvironmentAnalyticsIndexManager',
    'AsyncTestEnvironmentAnalyticsIndexManager',
    'TwistedTestEnvironmentAnalyticsIndexManager',
    'TestEnvironmentBucketManager',
    'AsyncTestEnvironmentBucketManager',
    'TwistedTestEnvironmentBucketManager',
    'TestEnvironmentCollectionManager',
    'AsyncTestEnvironmentCollectionManager',
    'TwistedTestEnvironmentCollectionManager',
    'TestEnvironmentCollectionMgmtManager',
    'AsyncTestEnvironmentCollectionMgmtManager',
    'TwistedTestEnvironmentCollectionMgmtManager',
    'TestEnvironmentEventingFunctionManager',
    'TestEnvironmentScopeEventingFunctionManager',
    'AsyncTestEnvironmentEventingFunctionManager',
    'AsyncTestEnvironmentScopeEventingFunctionManager',
    'TestEnvironmentCollectionQueryIndexManager',
    'TestEnvironmentQueryIndexManager',
    'AsyncTestEnvironmentCollectionQueryIndexManager',
    'AsyncTestEnvironmentQueryIndexManager',
    'TwistedTestEnvironmentQueryIndexManager',
    'TestEnvironmentScopeSearchIndexManager',
    'TestEnvironmentSearchIndexManager',
    'AsyncTestEnvironmentScopeSearchIndexManager',
    'AsyncTestEnvironmentSearchIndexManager',
    'TwistedTestEnvironmentScopeSearchIndexManager',
    'TwistedTestEnvironmentSearchIndexManager',
    'TestEnvironmentUserManager',
    'AsyncTestEnvironmentUserManager',
    'TwistedTestEnvironmentUserManager',
    'TestEnvironmentViewIndexManager',
    'AsyncTestEnvironmentViewIndexManager',
    'TwistedTestEnvironmentViewIndexManager',
]
