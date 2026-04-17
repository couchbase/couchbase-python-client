
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

from typing import (TYPE_CHECKING,
                    Any,
                    Union)

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import KeyValueOperationType
from couchbase.result import CounterResult, MutationResult

if TYPE_CHECKING:
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   IncrementOptions,
                                   PrependOptions)
    from txcouchbase.logic.collection_impl import TxCollectionImpl


class BinaryCollection:

    def __init__(self, collection_impl: TxCollectionImpl) -> None:
        self._impl = collection_impl

    def increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> Deferred[CounterResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_increment_request(key, None, *opts, **kwargs)
            d = self._impl.increment_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Increment, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_increment_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.increment_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> Deferred[CounterResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_decrement_request(key, None, *opts, **kwargs)
            d = self._impl.decrement_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Decrement, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_decrement_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.decrement_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_append_request(key, value, None, *opts, **kwargs)
            d = self._impl.append_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Append, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_append_request(key, value, obs_handler, *opts, **kwargs)
            d = self._impl.append_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_prepend_request(key, value, None, *opts, **kwargs)
            d = self._impl.prepend_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Prepend, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_prepend_request(key, value, obs_handler, *opts, **kwargs)
            d = self._impl.prepend_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
