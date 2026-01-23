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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (Any,
                    Callable,
                    Dict,
                    Optional)


class MgmtBaseRequest(ABC):
    @abstractmethod
    def req_to_dict(self,
                    conn: Any,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:

        raise NotImplementedError

    @property
    @abstractmethod
    def op_name(self) -> str:
        raise NotImplementedError


@dataclass
class MgmtRequest(MgmtBaseRequest):
    error_map: Dict[str, Exception]
