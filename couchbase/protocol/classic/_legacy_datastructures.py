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

"""The deprecated collection-level datastructure methods, which only the classic protocol provides.

The classic `CollectionImpl` holds one `LegacyDatastructures` and exposes it as `legacy_datastructures`;
`Collection.list_append` and its siblings reach it through `require()` and `Capability.LEGACY_DATASTRUCTURES`.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.exceptions import (DocumentExistsException,
                                         DocumentNotFoundException,
                                         PathExistsException,
                                         PathNotFoundException,
                                         QueueEmpty)
from couchbase.common.result import GetResult, OperationResult
from couchbase.common.subdocument import (array_addunique,
                                          array_append,
                                          array_prepend,
                                          count)
from couchbase.common.subdocument import get as subdoc_get
from couchbase.common.subdocument import remove as subdoc_remove
from couchbase.common.subdocument import replace
from couchbase.common.subdocument import upsert as subdoc_upsert

if TYPE_CHECKING:
    from couchbase.common._observability import WrappedSpan
    from couchbase.common._observability.observability_types import SpanProtocol
    from couchbase.common._utils import JSONType
    from couchbase.protocol.classic._collection_impl import CollectionImpl
    from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
    from couchbase.serializer import Serializer
    from couchbase.transcoder import Transcoder


def _map_path(mapkey: str) -> str:
    # A map key is one field of the document, so it is quoted as a single path element: a dot or a
    # bracket in it would otherwise address a nested field.  A backtick is escaped by doubling it.
    return '`' + mapkey.replace('`', '``') + '`'


class LegacyDatastructures:
    """Performs each operation through the classic `CollectionImpl` it belongs to."""

    def __init__(self, impl: CollectionImpl) -> None:
        self._impl = impl

    def _start_ds_span(self, ds_obs_handler: ObservableRequestHandler, kwargs: dict[str, object]) -> None:
        """Opens the datastructure span under the caller's parent, and makes it the parent of the sub-operations."""
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=cast('SpanProtocol | None', kwargs.pop('span', None)),
            parent_span=cast('SpanProtocol | WrappedSpan | None', kwargs.pop('parent_span', None)))
        ds_obs_handler.create_kv_span(self._impl.keyspace, parent_span=parent_span)
        kwargs['parent_span'] = ds_obs_handler.wrapped_span

    def _execute_legacy_ds_op(self,
                              fn: Callable[..., Any],
                              req: PycbcCoreKeyValueRequest,
                              obs_handler: ObservableRequestHandler,
                              parent_span: WrappedSpan | None,
                              create: bool | None = False,
                              create_type: type[dict[str, Any]] | type[list[Any]] | None = None,
                              rebuild: Callable[[], PycbcCoreKeyValueRequest] | None = None,
                              wrap_missing_path: bool | None = True,
                              path_value: Any | None = None,
                              codec: Transcoder | Serializer | None = None) -> Any:
        try:
            fn_args: list[Any] = [req]
            if codec:
                fn_args.append(codec)
            fn_args.append(obs_handler)
            return fn(*fn_args)
        except DocumentNotFoundException as ex:
            if create and create_type is not None and rebuild is not None:
                orig_op = obs_handler.op
                obs_handler.reset(Op.Insert, exc_val=ex)
                try:
                    ins_req = self._impl.request_builder.build_insert_request(req.key,
                                                                              create_type(),
                                                                              obs_handler,
                                                                              parent_span=parent_span)
                    self._impl._insert(ins_req, obs_handler)
                except DocumentExistsException:
                    pass
                obs_handler.reset(orig_op)
                fn_args = [rebuild()]
                if codec:
                    fn_args.append(codec)
                fn_args.append(obs_handler)
                return fn(*fn_args)
            else:
                raise
        except PathNotFoundException:
            if wrap_missing_path and path_value is not None:
                raise IndexError(path_value)
            raise

    def _legacy_get(self, key: str, **kwargs: object) -> GetResult:
        with ObservableRequestHandler(Op.Get, self._impl.observability_instruments) as obs_handler:
            req, transcoder = self._impl.request_builder.build_get_request(key, obs_handler, None, **kwargs)
            return self._impl._get(req, transcoder, obs_handler)

    def list_append(self,
                    key: str,
                    value: JSONType,
                    create: bool | None = False,
                    **kwargs: object) -> OperationResult:
        with ObservableRequestHandler(Op.ListAppend, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                op = array_append('', value)
                build = partial(self._impl.request_builder.build_mutate_in_request,
                                key, (op,), obs_handler, None, **kwargs)
                req, serializer = build()
                sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    create=create,
                                                    rebuild=lambda: build()[0],
                                                    create_type=list,
                                                    path_value=value,
                                                    codec=serializer)
                return OperationResult(sd_res.cas, sd_res.mutation_token())

    def list_prepend(self,
                     key: str,
                     value: JSONType,
                     create: bool | None = False,
                     **kwargs: object) -> OperationResult:
        with ObservableRequestHandler(Op.ListPrepend, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                op = array_prepend('', value)
                build = partial(self._impl.request_builder.build_mutate_in_request,
                                key, (op,), obs_handler, None, **kwargs)
                req, serializer = build()
                sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    create=create,
                                                    rebuild=lambda: build()[0],
                                                    create_type=list,
                                                    path_value=value,
                                                    codec=serializer)
                return OperationResult(sd_res.cas, sd_res.mutation_token())

    def list_set(self, key: str, index: int, value: JSONType, **kwargs: object) -> OperationResult:
        with ObservableRequestHandler(Op.ListSetAt, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                op = replace(f'[{index}]', value)
                req, serializer = self._impl.request_builder.build_mutate_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    path_value=index,
                                                    codec=serializer)
                return OperationResult(sd_res.cas, sd_res.mutation_token())

    def list_get(self, key: str, index: int, **kwargs: object) -> Any:
        with ObservableRequestHandler(Op.ListGetAt, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.LookupIn, self._impl.observability_instruments) as obs_handler:
                op = subdoc_get(f'[{index}]')
                req, serializer = self._impl.request_builder.build_lookup_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._lookup_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    path_value=index,
                                                    codec=serializer)
                return sd_res.value[0].get('value', None)

    def list_remove(self, key: str, index: int, **kwargs: object) -> OperationResult:
        with ObservableRequestHandler(Op.ListRemoveAt, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                op = subdoc_remove(f'[{index}]')
                req, serializer = self._impl.request_builder.build_mutate_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    path_value=index,
                                                    codec=serializer)
                return OperationResult(sd_res.cas, sd_res.mutation_token())

    def list_size(self, key: str, **kwargs: object) -> int:
        with ObservableRequestHandler(Op.ListSize, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.LookupIn, self._impl.observability_instruments) as obs_handler:
                op = count('')
                req, serializer = self._impl.request_builder.build_lookup_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._lookup_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    codec=serializer)
                return sd_res.value[0].get('value', None)

    def map_add(self,
                key: str,
                mapkey: str,
                value: Any,
                create: bool | None = False,
                **kwargs: object) -> OperationResult:
        with ObservableRequestHandler(Op.MapAdd, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                op = subdoc_upsert(_map_path(mapkey), value)
                build = partial(self._impl.request_builder.build_mutate_in_request,
                                key, (op,), obs_handler, None, **kwargs)
                req, serializer = build()
                sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    create=create,
                                                    rebuild=lambda: build()[0],
                                                    create_type=dict,
                                                    path_value=mapkey,
                                                    codec=serializer)
                return OperationResult(sd_res.cas, sd_res.mutation_token())

    def map_get(self, key: str, mapkey: str, **kwargs: object) -> Any:
        with ObservableRequestHandler(Op.MapGet, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.LookupIn, self._impl.observability_instruments) as obs_handler:
                op = subdoc_get(_map_path(mapkey))
                req, serializer = self._impl.request_builder.build_lookup_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._lookup_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    path_value=mapkey,
                                                    codec=serializer)
                return sd_res.value[0].get('value', None)

    def map_remove(self, key: str, mapkey: str, **kwargs: object) -> OperationResult:
        with ObservableRequestHandler(Op.MapRemove, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                op = subdoc_remove(_map_path(mapkey))
                req, serializer = self._impl.request_builder.build_mutate_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    path_value=mapkey,
                                                    codec=serializer)
                return OperationResult(sd_res.cas, sd_res.mutation_token())

    def map_size(self, key: str, **kwargs: object) -> int:
        with ObservableRequestHandler(Op.MapSize, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            with ObservableRequestHandler(Op.LookupIn, self._impl.observability_instruments) as obs_handler:
                op = count('')
                req, serializer = self._impl.request_builder.build_lookup_in_request(key, (op,), obs_handler, None,
                                                                                     **kwargs)
                sd_res = self._execute_legacy_ds_op(self._impl._lookup_in,
                                                    req,
                                                    obs_handler,
                                                    ds_obs_handler.wrapped_span,
                                                    codec=serializer)
                return sd_res.value[0].get('value', None)

    def set_add(self,
                key: str,
                value: Any,
                create: bool | None = False,
                **kwargs: object) -> OperationResult | None:
        with ObservableRequestHandler(Op.SetAdd, self._impl.observability_instruments) as ds_obs_handler:
            self._start_ds_span(ds_obs_handler, kwargs)
            # The sub-operation's handler has to see the failure, so it is caught outside it.
            try:
                with ObservableRequestHandler(Op.MutateIn, self._impl.observability_instruments) as obs_handler:
                    op = array_addunique('', value)
                    build = partial(self._impl.request_builder.build_mutate_in_request,
                                    key, (op,), obs_handler, None, **kwargs)
                    req, serializer = build()
                    sd_res = self._execute_legacy_ds_op(self._impl._mutate_in,
                                                        req,
                                                        obs_handler,
                                                        ds_obs_handler.wrapped_span,
                                                        create=create,
                                                        rebuild=lambda: build()[0],
                                                        create_type=list,
                                                        path_value=value,
                                                        codec=serializer)
                    return OperationResult(sd_res.cas, sd_res.mutation_token())
            except PathExistsException:
                return None

    def set_remove(self, key: str, value: Any, **kwargs: object) -> OperationResult | None:
        while True:
            rv = self._legacy_get(key, **kwargs)
            try:
                ix = cast(list[Any], rv.value).index(value)
                kwargs['cas'] = rv.cas
                return self.list_remove(key, ix, **kwargs)
            except DocumentExistsException:
                pass
            except ValueError:
                return None

    def set_size(self, key: str, **kwargs: object) -> int:
        return self.list_size(key, **kwargs)

    def set_contains(self, key: str, value: Any, **kwargs: object) -> bool:
        rv = self._legacy_get(key, **kwargs)
        return value in cast(list[Any], rv.value)

    def queue_push(self,
                   key: str,
                   value: Any,
                   create: bool | None = False,
                   **kwargs: object) -> OperationResult:
        return self.list_prepend(key, value, create=create, **kwargs)

    def queue_pop(self, key: str, **kwargs: object) -> Any:
        while True:
            try:
                itm = self.list_get(key, -1, **kwargs)
            except IndexError:
                raise QueueEmpty

            kwargs.update({k: v for k, v in getattr(itm, '__dict__', {}).items() if k in {'cas'}})
            try:
                self.list_remove(key, -1, **kwargs)
                return itm
            except DocumentExistsException:
                pass
            except IndexError:
                raise QueueEmpty

    def queue_size(self, key: str) -> int:
        return self.list_size(key)
