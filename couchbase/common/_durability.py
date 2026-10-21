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

from couchbase.common.durability import (ClientDurability,
                                         DurabilityType,
                                         ServerDurability)


class DurabilityParser:
    @staticmethod
    def parse_durability(durability: DurabilityType) -> int | dict[str, int] | None:
        if isinstance(durability, ClientDurability):
            return {
                'replicate_to': durability.replicate_to.value,
                'persist_to': durability.persist_to.value
            }

        if isinstance(durability, ServerDurability):
            return durability.level.value

        return None
