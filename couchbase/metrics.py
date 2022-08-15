# Copyright 2021, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from abc import ABC, abstractmethod
from typing import (Any,
                    Dict,
                    Optional)


class CouchbaseValueRecorder(ABC):
    """
    This is an abstract base class intended to wrap an external recorder implementation, to use withing the
    python client.  You will want to create a class deriving from this, which implements the :meth:`record_value`
    method.
    """

    def __init__(self,
                 recorder=None  # type: Optional[Any]
                 ):
        self._recorder = recorder
        super().__init__()

    @property
    def recorder(self) -> Optional[Any]:
        """
            Optional[Any]: The underlying recorder object this class wrapped.
        """
        return self._recorder

    @abstractmethod
    def record_value(self,
                     value,      # type: int
                     ) -> None:
        """
        This method will need to be implemented in derived classes.

        Args:
            value (int): The value to record.

        """
        pass


class CouchbaseMeter(ABC):
    """
    This is an abstract base class intended to wrap an external meter implementation, to use withing the
    python client.  You will want to create a class deriving from this, which implements the :meth:`value_recorder`
    method.
    """

    def __init__(self,
                 external_meter=None  # type: Optional[Any]
                 ):
        self._external_meter = external_meter
        super().__init__()

    @abstractmethod
    def value_recorder(self,
                       name,      # type: str
                       tags       # type: Dict[str, str]
                       ) -> CouchbaseValueRecorder:
        """
        This method will need to be implemented in derived classes.

        Args:
            name (int): The name of the recorder.
            tags (Dict[str, str]): The tags associated with the recorder.

        Returns:
            :class:`~couchbase.metrics.CouchbaseValueRecorder`:
        """
        pass
