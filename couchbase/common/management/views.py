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

__all__ = [
    'DesignDocument',
    'DesignDocumentNamespace',
    'View',
]

import json
from enum import Enum
from typing import Any


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
    def from_str(cls, value: str | None) -> DesignDocumentNamespace:
        if value == 'production':
            return cls.PRODUCTION
        else:
            return cls.DEVELOPMENT


class View:
    def __init__(self, map: str, reduce: str | None = None, name: str | None = None) -> None:
        # name is accepted but not stored: the core puts it in every view dict it builds, and
        # DesignDocument.from_json splats the whole value in here, so dropping the parameter
        # breaks every read.  A view's name is its key in the design document.
        self._map = map
        self._reduce = reduce

    @property
    def map(self) -> str:
        return self._map

    @property
    def reduce(self) -> str | None:
        return self._reduce

    def as_dict(self, name: str | None = None) -> dict[str, Any]:
        output = {'map': self._map}
        if self._reduce:
            output['reduce'] = self._reduce
        if name:
            output['name'] = name
        return output

    def to_json(self) -> str:
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_view: dict[str, Any]) -> View:
        return cls(**json_view)


class DesignDocument(object):
    def __init__(self,
                 name: str,
                 views: dict[str, View],
                 namespace: DesignDocumentNamespace | None = None,
                 rev: str | None = None,
                 ) -> None:
        self._name = DesignDocumentNamespace.unprefix(name)
        self._views = views
        self._rev = rev
        self._namespace = namespace

    @property
    def name(self) -> str:
        return self._name

    @property
    def views(self) -> dict[str, View]:
        return self._views

    @property
    def rev(self) -> str | None:
        return self._rev

    @property
    def namespace(self) -> DesignDocumentNamespace | None:
        return self._namespace

    def as_dict(self, namespace: DesignDocumentNamespace | None) -> dict[str, Any]:
        output: dict[str, Any] = {
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

    def get_view(self, name: str) -> View | None:
        return self._views.get(name, None)

    @classmethod
    def from_json(cls, raw_json: dict[str, Any]) -> DesignDocument:
        name = raw_json['name']
        rev = raw_json.get('rev', None)
        ns = DesignDocumentNamespace.from_str(raw_json.get('namespace', None))
        raw_views = raw_json.get('views', dict())
        views = dict({key: View(**value) for key, value in raw_views.items()})
        return cls(name, views, namespace=ns, rev=rev)

    def __repr__(self) -> str:
        output = self.as_dict(self.namespace)
        return f'DesignDocument({output})'
