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
    'CollectionManager',
    'CollectionSpec',
    'CreateCollectionSettings',
    'ScopeSpec',
    'UpdateCollectionSettings',
]

import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING, overload

if sys.version_info >= (3, 13):
    from warnings import deprecated
else:
    from typing_extensions import deprecated

from couchbase.common.management.collections import (CollectionSpec,
                                                     CreateCollectionSettings,
                                                     ScopeSpec,
                                                     UpdateCollectionSettings)

# @TODO:  lets deprecate import of options from couchbase.management.collections
from couchbase.management.options import (CreateCollectionOptions,
                                          CreateScopeOptions,
                                          DropCollectionOptions,
                                          DropScopeOptions,
                                          GetAllScopesOptions,
                                          UpdateCollectionOptions)

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._collection_mgmt import CollectionMgmtInterface
    from couchbase.common.management.options import (CreateCollectionOptionsKwargs,
                                                     CreateScopeOptionsKwargs,
                                                     DropCollectionOptionsKwargs,
                                                     DropScopeOptionsKwargs,
                                                     GetAllScopesOptionsKwargs,
                                                     UpdateCollectionOptionsKwargs)


class CollectionManager:

    def __init__(self, impl: CollectionMgmtInterface) -> None:
        self._impl = impl

    def create_scope(self,
                     scope_name: str,
                     options: CreateScopeOptions | None = None,
                     **kwargs: Unpack[CreateScopeOptionsKwargs]) -> None:
        """Creates a new scope.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.CreateScopeOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.ScopeAlreadyExistsException`: If the scope already exists.
        """
        self._impl.create_scope(scope_name, options, **kwargs)

    def drop_scope(self,
                   scope_name: str,
                   options: DropScopeOptions | None = None,
                   **kwargs: Unpack[DropScopeOptionsKwargs]) -> None:
        """Drops an existing scope.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.DropScopeOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        self._impl.drop_scope(scope_name, options, **kwargs)

    def get_all_scopes(self,
                       options: GetAllScopesOptions | None = None,
                       **kwargs: Unpack[GetAllScopesOptionsKwargs]) -> Iterable[ScopeSpec]:
        """Returns all configured scopes along with their collections.

        Args:
            options (:class:`~couchbase.management.options.GetAllScopesOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.ScopeSpec`]: A list of all configured scopes.
        """
        return self._impl.get_all_scopes(options, **kwargs)

    @overload
    @deprecated("Use ``create_collection(scope_name, collection_name, settings=None, options=None, **kwargs)`` "
                "instead.")
    def create_collection(self,
                          collection: CollectionSpec,
                          options: CreateCollectionOptions | None = None,
                          **kwargs: Unpack[CreateCollectionOptionsKwargs]
                          ) -> None:
        ...

    @overload
    def create_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: CreateCollectionSettings | None = None,
                          options: CreateCollectionOptions | None = None,
                          **kwargs: Unpack[CreateCollectionOptionsKwargs]
                          ) -> None:
        ...

    def create_collection(self, *args: object, **kwargs: object) -> None:
        """Creates a new collection in a specified scope.

        .. deprecated:: 4.1.9
            The overload that takes a :class:`~couchbase.management.collections.CollectionSpec` is deprecated; pass
            ``scope_name`` and ``collection_name`` instead.

        Args:
            scope_name (str): The name of the scope the collection will be created in.
            collection_name (str): The name of the collection to be created
            settings (:class:`~.CreateCollectionSettings`, optional): Settings to apply for the collection
            options (:class:`~couchbase.management.options.CreateCollectionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.CollectionAlreadyExistsException`: If the collection already exists.
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        self._impl.create_collection(*args, **kwargs)

    @overload
    @deprecated("Use ``drop_collection(scope_name, collection_name, options=None, **kwargs)`` instead.")
    def drop_collection(self,
                        collection: CollectionSpec,
                        options: DropCollectionOptions | None = None,
                        **kwargs: Unpack[DropCollectionOptionsKwargs]
                        ) -> None:
        ...

    @overload
    def drop_collection(self,
                        scope_name: str,
                        collection_name: str,
                        options: DropCollectionOptions | None = None,
                        **kwargs: Unpack[DropCollectionOptionsKwargs]) -> None:
        ...

    def drop_collection(self, *args: object, **kwargs: object) -> None:
        """Drops a collection from the specified scope.

        .. deprecated:: 4.1.9
            The overload that takes a :class:`~couchbase.management.collections.CollectionSpec` is deprecated; pass
            ``scope_name`` and ``collection_name`` instead.

        Args:
            scope_name (str): The name of the scope the collection is in.
            collection_name (str): The name of the collection to be dropped
            options (:class:`~couchbase.management.options.DropCollectionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
        """
        self._impl.drop_collection(*args, **kwargs)

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          options: UpdateCollectionOptions | None = None,
                          **kwargs: Unpack[UpdateCollectionOptionsKwargs]) -> None:
        """Updates a collection in a specified scope.

        Args:
            scope_name (str): The name of the scope the collection is in.
            collection_name (str): The name of the collection that will be updated
            settings (:class:`~.UpdateCollectionSettings`): Settings to apply for the collection
            options (:class:`~couchbase.management.options.UpdateCollectionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        self._impl.update_collection(scope_name, collection_name, settings, options, **kwargs)
