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

"""The read preference for replica reads."""

__all__ = [
    'ReadPreference',
]

from enum import IntEnum


class ReadPreference(IntEnum):
    """Which replicas a replica read may read from.

    NO_PREFERENCE reads from any replica. SELECTED_SERVER_GROUP reads only from nodes in the server group the
    ``preferred_server_group`` cluster option names.
    """

    NO_PREFERENCE = 0
    SELECTED_SERVER_GROUP = 1
