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

import logging
from typing import TYPE_CHECKING

from couchbase.result import ContentProxy
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.pycbc_core import transaction_get_result

log = logging.getLogger(__name__)


class TransactionGetResult:
    def __init__(self,
                 res,    # type: transaction_get_result
                 serializer  # type: Serializer
                 ):
        self._res = res
        self._serializer = serializer
        self._decoded_value = None

    @property
    def id(self):
        return self._res.get("id")

    @property
    def cas(self):
        return self._res.get("cas")

    @property
    def content_as(self):
        if not self._decoded_value:
            val = self._res.get('value')
            if val:
                self._decoded_value = self._serializer.deserialize(self._res.get("value"))
                log.debug(f'Result has decoded value {self._decoded_value}')
                return ContentProxy(self._decoded_value)

        log.debug('Result is missing decoded value ')
        return ContentProxy('')

    def __str__(self):
        return f'TransactionGetResult{{id={self.id}, cas={self.cas}, value={self.content_as[str]} }}'
