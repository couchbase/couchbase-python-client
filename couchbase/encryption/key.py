#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import Union


class Key:
    def __init__(self,
                 id,  # type: str
                 bytes_,  # type: Union[bytes, bytearray]
                 ):
        self._id = id
        self._bytes = bytes_ if isinstance(bytes_, bytes) else bytes(bytes_)

    @property
    def id(self) -> str:
        return self._id

    @property
    def bytes(self) -> bytes:
        return self._bytes
