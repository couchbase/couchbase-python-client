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

import json
from dataclasses import dataclass, fields
from enum import Enum
from typing import (Any,
                    Callable,
                    Dict,
                    Optional)

from couchbase.exceptions import DesignDocumentNotFoundException, RateLimitedException
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import MgmtOperationType, ViewIndexMgmtOperationType
from couchbase.management.logic.mgmt_req import MgmtRequest


class DesignDocumentNamespace(Enum):
    PRODUCTION = False
    DEVELOPMENT = True

    # def prefix(self, ddocname):
    #     if ddocname.startswith('dev_') or not self.value:
    #         return ddocname
    #     return 'dev_' + ddocname

    def to_str(self) -> str:
        return 'development' if self.value else 'production'

    @classmethod
    def unprefix(cls, name: str) -> str:
        for prefix in ('_design/', 'dev_'):
            name = name[name.startswith(prefix) and len(prefix):]
        return name

    @classmethod
    def from_str(cls, value: str) -> DesignDocumentNamespace:
        if value == 'production':
            return cls.PRODUCTION
        else:
            return cls.DEVELOPMENT


class View:
    def __init__(self, map: str, reduce: Optional[str] = None, name: Optional[str] = None) -> None:
        self._map = map
        self._reduce = reduce

    @property
    def map(self) -> str:
        return self._map

    @property
    def reduce(self) -> Optional[str]:
        return self._reduce

    def as_dict(self, name: Optional[str] = None) -> Dict[str, Any]:
        output = {'map': self._map}
        if self._reduce:
            output['reduce'] = self._reduce
        if name:
            output['name'] = name
        return output

    def to_json(self) -> str:
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_view: Dict[str, Any]) -> View:
        return cls(json.loads(json_view))


class DesignDocument(object):
    def __init__(self,
                 name: str,
                 views: Dict[str, View],
                 namespace: Optional[DesignDocumentNamespace] = None,
                 rev: Optional[str] = None,
                 ) -> None:
        self._name = DesignDocumentNamespace.unprefix(name)
        self._views = views
        self._rev = rev
        self._namespace = namespace

    @property
    def name(self) -> str:
        return self._name

    @property
    def views(self) -> Dict[str, View]:
        return self._views

    @property
    def rev(self) -> Optional[str]:
        return self._rev

    @property
    def namespace(self) -> Optional[DesignDocumentNamespace]:
        return self._namespace

    def as_dict(self, namespace: DesignDocumentNamespace) -> Dict[str, Any]:
        output = {
            'name': self._name
        }
        if namespace is not None:
            output['ns'] = namespace.to_str()
        output['views'] = dict({key: value.as_dict(name=key) for key, value in self.views.items()})

        if self.rev:
            output['rev'] = self.rev

        return output

    def add_view(self, name: str, view: View) -> DesignDocument:
        self.views[name] = view
        return self

    def get_view(self, name: str) -> View:
        return self._views.get(name, None)

    @classmethod
    def from_json(cls, raw_json: Dict[str, Any]) -> DesignDocument:
        name = raw_json.get('name')
        rev = raw_json.get('rev', None)
        ns = DesignDocumentNamespace.from_str(raw_json.get('namespace', None))
        views = raw_json.get('views', dict())
        views = dict({key: View(**value) for key, value in views.items()})
        return cls(name, views, namespace=ns, rev=rev)

    def __repr__(self) -> str:
        output = self.as_dict(self.namespace)
        return f'DesignDocument({output})'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: Dict[type, list] = {}


@dataclass
class ViewIndexMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: Optional[ObservableRequestHandler] = None,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        cls = type(self)
        cached_fields = _FIELDS_CACHE.get(cls)
        if cached_fields is None:
            cached_fields = [f for f in fields(cls) if f.name not in _OPARG_SKIP_SET]
            _FIELDS_CACHE[cls] = cached_fields

        mgmt_kwargs = {
            f.name: getattr(self, f.name)
            for f in cached_fields
            if getattr(self, f.name) is not None
        }

        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if obs_handler.is_legacy_tracer:
                legacy_request_span = obs_handler.legacy_request_span
                if legacy_request_span:
                    mgmt_kwargs['parent_span'] = legacy_request_span
            else:
                mgmt_kwargs['wrapper_span_name'] = obs_handler.wrapper_span_name

        return mgmt_kwargs


@dataclass
class DropDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    ns: str
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexDrop.value


@dataclass
class GetAllDesignDocumentsRequest(ViewIndexMgmtRequest):
    bucket_name: str
    ns: str
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexGetAll.value


@dataclass
class GetDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    ns: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexGet.value


@dataclass
class UpsertDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document: Dict[str, Any]
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexUpsert.value


@dataclass
class PublishDesignDocumentRequest:
    """Request for publish_design_document.

    This is a Python-only composite operation (get + upsert) that doesn't
    have a corresponding C++ core request. This class holds the necessary
    info for the operation to keep the API consistent with other operations.
    """
    bucket_name: str
    design_doc_name: str

    @property
    def op_name(self) -> str:
        return MgmtOperationType.ViewIndexPublish.value


VIEW_INDEX_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'not_found': DesignDocumentNotFoundException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
