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

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    List,
                    Optional,
                    Union)

from couchbase.exceptions import DocumentNotFoundException, InvalidIndexException
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from acouchbase.collection import Collection as AsyncCollection
    from couchbase.collection import Collection
    from couchbase.pycbc_core import transaction_get_multi_result


class TransactionGetMultiMode(Enum):
    PRIORITISE_LATENCY = 'prioritise_latency'
    DISABLE_READ_SKEW_DETECTION = 'disable_read_skew_detection'
    PRIORITISE_READ_SKEW_DETECTION = 'prioritise_read_skew_detection'


class TransactionGetMultiReplicasFromPreferredServerGroupMode(Enum):
    PRIORITISE_LATENCY = 'prioritise_latency'
    DISABLE_READ_SKEW_DETECTION = 'disable_read_skew_detection'
    PRIORITISE_READ_SKEW_DETECTION = 'prioritise_read_skew_detection'


log = logging.getLogger(__name__)


@dataclass
class TransactionGetMultiSpec:
    collection: Union[Collection, AsyncCollection]
    id: str
    transcoder: Optional[Transcoder] = None

    def _astuple(self):
        """
          **INTERNAL**
        """
        return (self.collection._scope.bucket_name,
                self.collection._scope.name,
                self.collection.name,
                self.id)


@dataclass
class TransactionGetMultiReplicasFromPreferredServerGroupSpec:
    collection: Union[Collection, AsyncCollection]
    id: str
    transcoder: Optional[Transcoder] = None

    def _astuple(self):
        """
          **INTERNAL**
        """
        return (self.collection._scope.bucket_name,
                self.collection._scope.name,
                self.collection.name,
                self.id)


class TransactionMultiContentProxy:
    """
    Used to provide access to TransactionGetMultiResult content via TransactionGetMultiResult.content_as[type](index)
    """

    def __init__(self,
                 content_as_index  # type: Callable[[int], Any]
                 ) -> None:
        self._content_as_index = content_as_index

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Callable[[int], Any]:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return lambda index: type_(self._content_as_index(index))


class TransactionGetMultiResult:
    def __init__(self,
                 res,                # type: transaction_get_multi_result
                 transcoders,        # type: List[Transcoder]
                 default_transcoder  # type: Transcoder
                 ):
        self._res = res
        self._transcoders = transcoders
        self._default_transcoder = default_transcoder
        self._decoded_values = {}

    def exists(self,
               index  # type: int
               ) -> bool:
        if index > len(self._res.content) - 1 or index < 0:
            raise InvalidIndexException(f'Provided index ({index}) is invalid.')

        return self._res.content[index] is not None

    @property
    def content_as(self) -> TransactionMultiContentProxy:

        def content_at_index(index: int) -> Any:
            if not self.exists(index):
                raise DocumentNotFoundException(f'Document does not exist at index={index}.')

            if index not in self._decoded_values:
                val, flags = self._res.content[index]
                tc = self._transcoders[index] or self._default_transcoder
                decoded_value = tc.decode_value(val, flags)
                self._decoded_values[index] = decoded_value

            return self._decoded_values[index]

        return TransactionMultiContentProxy(content_at_index)

    def __repr__(self):
        content = []
        for idx in range(len(self._res.content)):
            if not self.exists(idx):
                content.append(None)
            else:
                content.append(self.content_as[str](idx))
        return f'TransactionGetMultiResult(content_list={content})'

    def __str__(self):
        return self.__repr__()


class TransactionGetMultiReplicasFromPreferredServerGroupResult:
    def __init__(self,
                 res,                # type: transaction_get_multi_result
                 transcoders,        # type: List[Transcoder]
                 default_transcoder  # type: Transcoder
                 ):
        self._res = res
        self._transcoders = transcoders
        self._default_transcoder = default_transcoder
        self._decoded_values = {}

    def exists(self,
               index  # type: int
               ) -> bool:
        if index > len(self._res.content) - 1 or index < 0:
            raise InvalidIndexException(f'Provided index ({index}) is invalid.')

        return self._res.content[index] is not None

    @property
    def content_as(self) -> TransactionMultiContentProxy:

        def content_at_index(index: int) -> Any:
            if not self.exists(index):
                raise DocumentNotFoundException(f'Document does not exist at index={index}.')

            if index not in self._decoded_values:
                val, flags = self._res.content[index]
                tc = self._transcoders[index] or self._default_transcoder
                decoded_value = tc.decode_value(val, flags)
                self._decoded_values[index] = decoded_value

            return self._decoded_values[index]

        return TransactionMultiContentProxy(content_at_index)

    def __repr__(self):
        content = []
        for idx in range(len(self._res.content)):
            if not self.exists(idx):
                content.append(None)
            else:
                content.append(self.content_as[str](idx))
        return f'TransactionGetMultiReplicasFromPreferredServerGroupResult(content_list={content})'

    def __str__(self):
        return self.__repr__()
