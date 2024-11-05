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
from typing import TYPE_CHECKING, Optional

from couchbase.pycbc_core import (transaction_op,
                                  transaction_operations,
                                  transaction_query_op)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase._utils import PyCapsuleType
    from couchbase.transcoder import Transcoder

log = logging.getLogger(__name__)


class AttemptContextLogic:
    def __init__(self,
                 ctx,    # type: PyCapsuleType
                 loop,    # type: Optional[AbstractEventLoop]
                 transcoder,  # type: Transcoder
                 ):
        log.debug('creating new attempt context with context=%s, loop=%s, and transcoder=%s', ctx, loop, transcoder)
        self._ctx = ctx
        self._loop = loop
        self._transcoder = transcoder

    def get(self, coll, key, **kwargs):
        # make sure we don't pass the transcoder along
        kwargs.pop('transcoder', None)
        kwargs.update(coll._get_connection_args())
        kwargs.pop("conn")
        kwargs["key"] = key
        kwargs["ctx"] = self._ctx
        kwargs["op"] = transaction_operations.GET.value
        log.debug('get calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def insert(self, coll, key, value, **kwargs):
        transcoder = kwargs.pop('transcoder', self._transcoder)
        kwargs.update(coll._get_connection_args())
        kwargs.pop("conn")
        kwargs.update({
            'key': key,
            'ctx': self._ctx,
            'op': transaction_operations.INSERT.value,
            'value': transcoder.encode_value(value)
        })
        log.debug('insert calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def replace(self, txn_get_result, value, **kwargs):
        transcoder = kwargs.pop('transcoder', self._transcoder)
        kwargs.update({
            'ctx': self._ctx,
            'op': transaction_operations.REPLACE.value,
            'value': transcoder.encode_value(value),
            'txn_get_result': txn_get_result._res
        })
        log.debug('replace calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def remove(self, txn_get_result, **kwargs):
        kwargs.update({"ctx": self._ctx,
                       "op": transaction_operations.REMOVE.value,
                       "txn_get_result": txn_get_result._res})
        log.debug('remove calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def query(self, query, options, **kwargs):
        kwargs.update({"ctx": self._ctx, "statement": query, "options": options._base})
        log.debug('query calling transaction_op with %s', kwargs)
        return transaction_query_op(**kwargs)
