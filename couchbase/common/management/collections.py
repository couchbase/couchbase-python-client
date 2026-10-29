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

if TYPE_CHECKING:
    from datetime import timedelta


class CreateCollectionSettings:
    """
        Specifies settings for a collection that will be created.
    """

    def __init__(self, max_expiry: timedelta | None = None, history: bool | None = None) -> None:
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> timedelta | None:
        """
            timedelta | None: The maximum expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def history(self) -> bool | None:
        """
            bool | None: Whether history retention override should be enabled in the collection. If not set, it will
            default to the bucket-level setting
        """
        return self._history


class UpdateCollectionSettings:
    """
        Specifies settings for a collection that will be updated.
    """

    def __init__(self, max_expiry: timedelta | None = None, history: bool | None = None) -> None:
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> timedelta | None:
        """
            timedelta | None: The maximum expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def history(self) -> bool | None:
        """
            bool | None: Whether history retention override should be enabled in the collection. If not set, it will
            default to the bucket-level setting
        """
        return self._history


class CollectionSpec:
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
        """
            timedelta | None: The expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def max_ttl(self) -> timedelta | None:
        """timedelta | None: The expiry for documents in the collection.

        .. deprecated:: 4.3.5
            Use :attr:`max_expiry` instead.
        """
        Supportability.class_property_deprecated('max_ttl', 'max_expiry')
        return self._max_expiry

    @property
    def history(self) -> bool | None:
        """
            bool | None: Whether history retention is enabled in the collection. None represents the bucket-level
            setting.
        """
        return self._history

    @property
    def name(self) -> str:
        """
            str: The name of the collection
        """
        return self._name

    @property
    def scope_name(self) -> str:
        """
            str: The name of the collection's scope
        """
        return self._scope_name


class ScopeSpec:
    def __init__(self, name: str, collections: Iterable[CollectionSpec]) -> None:
        self._name = name
        self._collections = collections

    @property
    def name(self) -> str:
        """
            str: The name of the scope
        """
        return self._name

    @property
    def collections(self) -> Iterable[CollectionSpec]:
        """
            list[:class:`.CollectionSpec`]: A list of the scope's collections.
        """
        return self._collections
