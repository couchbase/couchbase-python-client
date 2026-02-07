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
from couchbase.logic.operation_types import ViewIndexMgmtOperationType
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
    def __init__(self, map: str, reduce: Optional[str] = None) -> None:
        self._map = map
        self._reduce = reduce

    @property
    def map(self) -> str:
        return self._map

    @property
    def reduce(self) -> Optional[str]:
        return self._reduce

    def as_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in {"map": self._map,
                                  "reduce": self._reduce}.items() if v}

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
            output['namespace'] = namespace.to_str()
        output['views'] = dict({key: value.as_dict() for key, value in self.views.items()})

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
OPARG_SKIP_LIST = ['mgmt_op', 'op_type', 'timeout', 'error_map']


@dataclass
class ViewIndexMgmtRequest(MgmtRequest):
    mgmt_op: str
    op_type: str
    # TODO: maybe timeout isn't optional, but defaults to default timeout?
    #       otherwise that makes inheritance tricky w/ child classes having required params

    def req_to_dict(self,
                    conn: Any,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        mgmt_kwargs = {
            'conn': conn,
            'mgmt_op': self.mgmt_op,
            'op_type': self.op_type,
        }

        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if self.timeout is not None:
            mgmt_kwargs['timeout'] = self.timeout

        mgmt_kwargs['op_args'] = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        }

        return mgmt_kwargs


@dataclass
class DropDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    namespace: str
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.DropDesignDocument.value


@dataclass
class GetAllDesignDocumentsRequest(ViewIndexMgmtRequest):
    bucket_name: str
    namespace: str
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.GetAllDesignDocuments.value


@dataclass
class GetDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    namespace: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.GetDesignDocument.value


@dataclass
class UpsertDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    design_document: Dict[str, Any]
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ViewIndexMgmtOperationType.UpsertDesignDocument.value


VIEW_INDEX_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'not_found': DesignDocumentNotFoundException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
