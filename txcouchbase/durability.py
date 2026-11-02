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

"""The `txcouchbase.durability` import path.

The same objects as `couchbase.durability`, re-exported so Twisted code imports them from its own
package.
"""

__all__ = [
    'ClientDurability',
    'Durability',
    'DurabilityLevel',
    'DurabilityType',
    'PersistTo',
    'PersistToExtended',
    'ReplicateTo',
    'ServerDurability',
]

from couchbase.common.durability import ClientDurability  # noqa: F401
from couchbase.common.durability import Durability  # noqa: F401
from couchbase.common.durability import DurabilityLevel  # noqa: F401
from couchbase.common.durability import DurabilityType  # noqa: F401
from couchbase.common.durability import PersistTo  # noqa: F401
from couchbase.common.durability import PersistToExtended  # noqa: F401
from couchbase.common.durability import ReplicateTo  # noqa: F401
from couchbase.common.durability import ServerDurability  # noqa: F401
