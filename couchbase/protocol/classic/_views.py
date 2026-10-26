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

import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import SpanProtocol
from couchbase.common._transforms import to_milliseconds
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.views import DesignDocumentNamespace
from couchbase.common.options import ViewOptions
from couchbase.common.views import (ViewErrorMode,
                                    ViewOrdering,
                                    ViewScanConsistency)
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.common._utils import JSONType


class ViewQuery:

    # empty transform will skip updating the attribute when creating an
    # N1QLQuery object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "skip": {"skip": lambda x: x},
        "limit": {"limit": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "startkey": {"startkey": lambda x: x},
        "endkey": {"endkey": lambda x: x},
        "startkey_docid": {"startkey_docid": lambda x: x},
        "endkey_docid": {"endkey_docid": lambda x: x},
        "inclusive_end": {"inclusive_end": lambda x: x},
        "group": {"group": lambda x: x},
        "group_level": {"group_level": lambda x: x},
        "key": {"key": lambda x: x},
        "keys": {"keys": lambda x: x},
        "reduce": {"reduce": lambda x: x},
        "order": {"order": lambda x: x},
        "on_error": {"on_error": lambda x: x},
        "namespace": {"namespace": lambda x: x},
        "debug": {"debug": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "raw": {"raw": lambda x: x},
        "query_string": {"query_string": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "span": {"span": lambda x: x},
        "parent_span": {"parent_span": lambda x: x},
        "full_set": {"full_set": lambda x: x}
    }

    def __init__(self, bucket_name: str, design_doc_name: str, view_name: str, *args: Any, **kwargs: Any) -> None:
        self._params: dict[str, Any] = {
            'bucket_name': bucket_name,
            'document_name': design_doc_name,
            'view_name': view_name
        }

    def set_option(self, name: str, value: Any) -> None:
        """
        Set a raw option in the query. This option is encoded
        as part of the query parameters without any client-side
        verification. Use this for settings not directly exposed
        by the Python client.

        Args:
            name: The name of the option
            value: The value of the option
        """
        self._params[name] = value

    # @TODO:  I imagine some things might need to be jsonified...

    def as_encodable(self) -> dict[str, Any]:
        return self._params

    @property
    def timeout(self) -> float | None:
        total_ms = self._params.get('timeout', None)
        if not total_ms:
            return None
        return total_ms / 1000

    @timeout.setter
    def timeout(self, value: timedelta | float | int) -> None:
        if not value:
            self._params.pop('timeout', 0)
        else:
            total_ms = to_milliseconds(value)
            self.set_option('timeout', total_ms)

    @property
    def limit(self) -> int | None:
        return self._params.get('limit', None)

    @limit.setter
    def limit(self, value: int) -> None:
        self.set_option('limit', value)

    @property
    def skip(self) -> int | None:
        return self._params.get('skip', None)

    @skip.setter
    def skip(self, value: int) -> None:
        self.set_option('skip', value)

    @property
    def consistency(self) -> ViewScanConsistency:
        value = self._params.get(
            'consistency', None
        )
        if value is None:
            return ViewScanConsistency.NOT_BOUNDED
        if isinstance(value, str):
            return ViewScanConsistency.from_str(value)
        raise InvalidArgumentException(
            message=(f"{value} is not a valid ViewScanConsistency option. "
                     "Expected str representation of type ViewScanConsistency."))

    @consistency.setter
    def consistency(self, value: ViewScanConsistency | str) -> None:
        if isinstance(value, ViewScanConsistency):
            self.set_option('consistency', value.to_str())
        elif isinstance(value, str) and value in [sc.value for sc in ViewScanConsistency]:
            self.set_option('consistency', ViewScanConsistency(value).to_str())
        else:
            raise InvalidArgumentException(message=("Expected consistency to be either of type "
                                                    "ViewScanConsistency or str representation "
                                                    "of ViewScanConsistency"))

    @property
    def startkey(self) -> str | None:
        return self._params.get('start_key', None)

    @startkey.setter
    def startkey(self, value: JSONType) -> None:
        self.set_option('start_key', json.dumps(value))

    @property
    def endkey(self) -> str | None:
        return self._params.get('end_key', None)

    @endkey.setter
    def endkey(self, value: JSONType) -> None:
        self.set_option('end_key', json.dumps(value))

    @property
    def startkey_docid(self) -> str | None:
        return self._params.get('start_key_doc_id', None)

    @startkey_docid.setter
    def startkey_docid(self, value: str) -> None:
        self.set_option('start_key_doc_id', value)

    @property
    def endkey_docid(self) -> str | None:
        return self._params.get('end_key_doc_id', None)

    @endkey_docid.setter
    def endkey_docid(self, value: str) -> None:
        self.set_option('end_key_doc_id', value)

    @property
    def inclusive_end(self) -> bool | None:
        return self._params.get('inclusive_end', None)

    @inclusive_end.setter
    def inclusive_end(self, value: bool) -> None:
        self.set_option('inclusive_end', value)

    @property
    def group(self) -> bool | None:
        return self._params.get('group', None)

    @group.setter
    def group(self, value: bool) -> None:
        self.set_option('group', value)

    @property
    def group_level(self) -> int | None:
        return self._params.get('group_level', None)

    @group_level.setter
    def group_level(self, value: int) -> None:
        self.set_option('group_level', value)

    @property
    def key(self) -> str | None:
        return self._params.get('key', None)

    @key.setter
    def key(self, value: JSONType) -> None:
        self.set_option('key', json.dumps(value))

    @property
    def keys(self) -> list[str] | None:
        return self._params.get('keys', None)

    @keys.setter
    def keys(self, value: JSONType) -> None:
        if not isinstance(value, list):
            raise InvalidArgumentException('keys must be a list.')
        # couchbase++ wants a list of JSONified strings
        self.set_option('keys', list(map(lambda k: json.dumps(k), value)))

    @property
    def reduce(self) -> bool | None:
        return self._params.get('reduce', None)

    @reduce.setter
    def reduce(self, value: bool) -> None:
        self.set_option('reduce', value)

    @property
    def order(self) -> ViewOrdering:
        value = self._params.get(
            'order', None
        )
        if value is None:
            return ViewOrdering.DESCENDING
        if isinstance(value, str):
            return ViewOrdering.from_str(value)
        raise InvalidArgumentException(
            message=(f"{value} is not a valid ViewOrdering option. "
                     "Expected str representation of type ViewOrdering."))

    @order.setter
    def order(self, value: ViewOrdering | str) -> None:
        if isinstance(value, ViewOrdering):
            self.set_option('order', value.to_str())
        elif isinstance(value, str) and value in [sc.value for sc in ViewOrdering]:
            self.set_option('order', ViewOrdering(value).to_str())
        else:
            raise InvalidArgumentException(message=("Expected order to be either of type "
                                                    "ViewOrdering or str representation "
                                                    "of ViewOrdering"))

    @property
    def on_error(self) -> ViewErrorMode:
        value = self._params.get(
            'on_error', None
        )
        if value is None:
            return ViewErrorMode.STOP
        if isinstance(value, str):
            return ViewErrorMode.from_str(value)
        raise InvalidArgumentException(
            message=(f"{value} is not a valid ViewErrorMode option. "
                     "Expected str representation of type ViewErrorMode."))

    @on_error.setter
    def on_error(self, value: ViewErrorMode | str) -> None:
        if isinstance(value, ViewErrorMode):
            self.set_option('on_error', value.to_str())
        elif isinstance(value, str) and value in [sc.value for sc in ViewErrorMode]:
            self.set_option('on_error', ViewErrorMode(value).to_str())
        else:
            raise InvalidArgumentException(message=("Expected on_error to be either of type "
                                                    "ViewErrorMode or str representation "
                                                    "of ViewErrorMode"))

    @property
    def namespace(self) -> DesignDocumentNamespace:
        value = self._params.get(
            'ns', None
        )
        if value is None:
            return DesignDocumentNamespace.DEVELOPMENT

        if isinstance(value, str):
            return DesignDocumentNamespace.from_str(value)
        if isinstance(value, bool):
            if not value:
                return DesignDocumentNamespace.PRODUCTION

        return DesignDocumentNamespace.DEVELOPMENT

    @namespace.setter
    def namespace(self, value: DesignDocumentNamespace | str) -> None:
        if isinstance(value, DesignDocumentNamespace):
            self.set_option('ns', value.to_str())
        elif isinstance(value, str) and value in [sc.value for sc in DesignDocumentNamespace]:
            self.set_option('ns', DesignDocumentNamespace(value).to_str())
        else:
            raise InvalidArgumentException(message=("Expected namespace to be either of type "
                                                    "DesignDocumentNamespace or str representation "
                                                    "of DesignDocumentNamespace"))

    @property
    def debug(self) -> bool | None:
        return self._params.get('debug', None)

    @debug.setter
    def debug(self, value: bool) -> None:
        self.set_option('debug', value)

    @property
    def raw(self) -> dict[str, str] | None:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value: dict[str, str]) -> None:
        self.set_option('raw', value)

    @property
    def query_string(self) -> list[str] | None:
        return self._params.get('query_string', None)

    @query_string.setter
    def query_string(self, value: str) -> None:
        if not isinstance(value, list):
            raise InvalidArgumentException('query_string must be a list.')
        self.set_option('query_string', value)

    @property
    def client_context_id(self) -> str | None:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value: str) -> None:
        self.set_option('client_context_id', value)

    @property
    def serializer(self) -> Serializer | None:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value: Serializer) -> None:
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
        self.set_option('serializer', value)

    @property
    def span(self) -> SpanProtocol | None:
        return self._params.get('span', None)

    @span.setter
    def span(self, value: SpanProtocol) -> None:
        self.set_option('span', value)

    @property
    def parent_span(self) -> SpanProtocol | None:
        return self._params.get('parent_span', None)

    @parent_span.setter
    def parent_span(self, value: SpanProtocol) -> None:
        self.set_option('parent_span', value)

    @property
    def full_set(self) -> bool | None:
        return self._params.get('full_set', None)

    @full_set.setter
    def full_set(self, value: bool) -> None:
        self.set_option('full_set', value)

    @classmethod
    def create_view_query_object(cls,
                                 bucket_name: str,
                                 design_doc_name: str,
                                 view_name: str,
                                 *options: ViewOptions | None,
                                 **kwargs: Any) -> ViewQuery:

        # lets make a copy of the options, and update with kwargs...
        opt = ViewOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, ViewOptions):
                opt = o
                opts.remove(o)
        args = opt.copy()
        args.update(kwargs)

        query = cls(bucket_name, design_doc_name, view_name)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query
