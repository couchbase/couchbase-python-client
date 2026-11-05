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
    'CollectionSpec',
    'CreateCollectionSettings',
    'ScopeSpec',
    'UpdateCollectionSettings',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING

from couchbase.common._supportability import Supportability
from couchbase.common._utils import fields_repr

if TYPE_CHECKING:
    from datetime import timedelta


class CreateCollectionSettings:
    """Specifies settings for a collection that will be created."""

    def __init__(self, max_expiry: timedelta | None = None, history: bool | None = None) -> None:
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> timedelta | None:
        """The maximum expiry for documents in the collection."""
        return self._max_expiry

    @property
    def history(self) -> bool | None:
        """Whether the collection keeps a change history.

        ``None`` leaves it to the bucket's ``history_retention_collection_default``.
        """
        return self._history

    def __repr__(self) -> str:
        return fields_repr(self, max_expiry=self._max_expiry, history=self._history)


class UpdateCollectionSettings:
    """Specifies settings for a collection that will be updated."""

    def __init__(self, max_expiry: timedelta | None = None, history: bool | None = None) -> None:
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> timedelta | None:
        """The maximum expiry for documents in the collection."""
        return self._max_expiry

    @property
    def history(self) -> bool | None:
        """Whether the collection keeps a change history.

        ``None`` leaves it to the bucket's ``history_retention_collection_default``.
        """
        return self._history

    def __repr__(self) -> str:
        return fields_repr(self, max_expiry=self._max_expiry, history=self._history)


class CollectionSpec:
    """A collection, as :meth:`.CollectionManager.get_all_scopes` describes it.

    Args:
        collection_name (str): The name of the collection.
        scope_name (str, optional): The name of the collection's scope. Defaults to ``_default``.
        max_expiry (timedelta, optional): The longest expiry a document in the collection can have.
        max_ttl (timedelta, optional): **DEPRECATED** Use ``max_expiry``.
        history (bool, optional): Whether the collection keeps a change history.
    """

    def __init__(self,
                 collection_name: str,
                 scope_name: str = '_default',
                 max_expiry: timedelta | None = None,
                 max_ttl: timedelta | None = None,
                 history: bool | None = None
                 ) -> None:
        self._name, self._scope_name = collection_name, scope_name
        # in the event users utilize kwargs, set max_expiry if not set and max_ttl is set
        if max_ttl is not None:
            Supportability.class_property_deprecated('max_ttl', 'max_expiry')
            if max_expiry is None:
                max_expiry = max_ttl
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> timedelta | None:
        """The longest expiry a document in the collection can have."""
        return self._max_expiry

    @property
    def max_ttl(self) -> timedelta | None:
        """The longest expiry a document in the collection can have.

        .. deprecated:: 4.3.5
            Use :attr:`max_expiry` instead.
        """
        Supportability.class_property_deprecated('max_ttl', 'max_expiry')
        return self._max_expiry

    @property
    def history(self) -> bool | None:
        """Whether the collection keeps a change history.

        ``None`` means the collection follows the bucket's setting.
        """
        return self._history

    @property
    def name(self) -> str:
        """The name of the collection."""
        return self._name

    @property
    def scope_name(self) -> str:
        """The name of the collection's scope."""
        return self._scope_name

    def __repr__(self) -> str:
        return fields_repr(self, collection_name=self._name, scope_name=self._scope_name, max_expiry=self._max_expiry,
                           history=self._history)


class ScopeSpec:
    """A scope and its collections, as :meth:`.CollectionManager.get_all_scopes` describes them.

    Args:
        name (str): The name of the scope.
        collections (Iterable[:class:`.CollectionSpec`]): The scope's collections.
    """

    def __init__(self, name: str, collections: Iterable[CollectionSpec]) -> None:
        self._name = name
        self._collections = collections

    @property
    def name(self) -> str:
        """The name of the scope."""
        return self._name

    @property
    def collections(self) -> Iterable[CollectionSpec]:
        """The scope's collections."""
        return self._collections

    def __repr__(self) -> str:
        return fields_repr(self, name=self._name, collections=self._collections)
