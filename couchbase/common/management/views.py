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

from couchbase.common._utils import fields_repr


class DesignDocumentNamespace(Enum):
    """Whether a design document is in production or in development.

    A development design document indexes only part of the bucket, so its views can be tried out before they are
    published to production.
    """

    PRODUCTION = False
    DEVELOPMENT = True

    # def prefix(self, ddocname):
    #     if ddocname.startswith('dev_') or not self.value:
    #         return ddocname
    #     return 'dev_' + ddocname

    def to_str(self) -> str:
        """**INTERNAL** The core's name of the namespace."""
        return 'development' if self.value else 'production'

    @classmethod
    def unprefix(cls, name: str) -> str:
        """**INTERNAL** A design document's name with its ``_design/`` and ``dev_`` prefixes removed."""
        for prefix in ('_design/', 'dev_'):
            name = name[name.startswith(prefix) and len(prefix):]
        return name

    @classmethod
    def from_str(cls, value: str | None) -> DesignDocumentNamespace:
        """**INTERNAL** The member for the core's name of a namespace."""
        if value == 'production':
            return cls.PRODUCTION
        else:
            return cls.DEVELOPMENT


class View:
    """A view in a design document: the map function that indexes documents, and optionally a reduce function.

    Args:
        map (str): The JavaScript source of the map function.
        reduce (str, optional): The JavaScript source of the reduce function, or the name of a built-in one such as
        ``_count``.
        name (str, optional): Accepted and not kept: a view's name is its key in :attr:`.DesignDocument.views`.
    """

    def __init__(self, map: str, reduce: str | None = None, name: str | None = None) -> None:
        # name is accepted but not stored: the core puts it in every view dict it builds, and
        # DesignDocument.from_json splats the whole value in here, so dropping the parameter
        # breaks every read.  A view's name is its key in the design document.
        self._map = map
        self._reduce = reduce

    @property
    def map(self) -> str:
        """The JavaScript source of the map function."""
        return self._map

    @property
    def reduce(self) -> str | None:
        """The JavaScript source of the reduce function, the name of a built-in one, or ``None``."""
        return self._reduce

    def as_dict(self, name: str | None = None) -> dict[str, Any]:
        """**INTERNAL** The view as the dict the management request is built from."""
        output = {'map': self._map}
        if self._reduce:
            output['reduce'] = self._reduce
        if name:
            output['name'] = name
        return output

    def to_json(self) -> str:
        """Get the view as JSON.

        Returns:
            str: The view as a JSON object holding ``map`` and, if it is set, ``reduce``.
        """
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_view: dict[str, Any]) -> View:
        """Create a view from a dict holding its ``map`` and, optionally, its ``reduce``.

        Args:
            json_view (dict[str, Any]): The view's fields.

        Returns:
            :class:`.View`: The view.
        """
        return cls(**json_view)

    def __repr__(self) -> str:
        return fields_repr(self, map=self._map, reduce=self._reduce)


class DesignDocument(object):
    """A design document: a named set of views.

    Args:
        name (str): The name of the design document. A ``_design/`` or ``dev_`` prefix is removed.
        views (dict[str, :class:`.View`]): The views, keyed by name.
        namespace (:class:`.DesignDocumentNamespace`, optional): Whether the design document is in production or in
        development.
        rev (str, optional): The revision of the design document.
    """

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
        """The name of the design document, without a ``_design/`` or ``dev_`` prefix."""
        return self._name

    @property
    def views(self) -> dict[str, View]:
        """The views, keyed by name."""
        return self._views

    @property
    def rev(self) -> str | None:
        """The revision of the design document, if it was read from the server."""
        return self._rev

    @property
    def namespace(self) -> DesignDocumentNamespace | None:
        """Whether the design document is in production or in development."""
        return self._namespace

    def as_dict(self, namespace: DesignDocumentNamespace | None) -> dict[str, Any]:
        """**INTERNAL** The design document as the dict the management request is built from."""
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
        """Add a view, replacing any view of the same name.

        Args:
            name (str): The name of the view.
            view (:class:`.View`): The view.

        Returns:
            :class:`.DesignDocument`: This design document, so calls can be chained.
        """
        self.views[name] = view
        return self

    def get_view(self, name: str) -> View | None:
        """Get a view by name.

        Args:
            name (str): The name of the view.

        Returns:
            :class:`.View` | None: The view, or ``None`` if the design document has no view of that name.
        """
        return self._views.get(name, None)

    @classmethod
    def from_json(cls, raw_json: dict[str, Any]) -> DesignDocument:
        """Create a design document from a dict describing it.

        Args:
            raw_json (dict[str, Any]): The design document's ``name``, and optionally its ``rev``, its ``namespace`` and
                its ``views``.

        Returns:
            :class:`.DesignDocument`: The design document.
        """
        name = raw_json['name']
        rev = raw_json.get('rev', None)
        ns = DesignDocumentNamespace.from_str(raw_json.get('namespace', None))
        raw_views = raw_json.get('views', dict())
        views = dict({key: View(**value) for key, value in raw_views.items()})
        return cls(name, views, namespace=ns, rev=rev)

    def __repr__(self) -> str:
        return fields_repr(self, name=self._name, views=self._views, namespace=self._namespace, rev=self._rev)
