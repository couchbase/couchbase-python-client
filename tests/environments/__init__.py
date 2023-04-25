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

from collections import namedtuple
from enum import IntEnum


class CollectionType(IntEnum):
    DEFAULT = 1
    NAMED = 2


class CouchbaseTestEnvironmentException(Exception):
    """Raised when something with the test environment is incorrect."""

    def __init__(self, message=None, **kwargs):
        if message and 'message' not in kwargs:
            kwargs['message'] = message
        super().__init__(**kwargs)

    def __repr__(self):
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self):
        return self.__repr__()


KVPair = namedtuple("KVPair", "key value")
