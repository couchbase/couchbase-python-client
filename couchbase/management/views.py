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

__all__ = [
    'DesignDocument',
    'DesignDocumentNamespace',
    'View',
    'ViewIndexManager',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING

from couchbase.common.management.views import View  # noqa: F401
from couchbase.common.management.views import DesignDocument, DesignDocumentNamespace

# @TODO:  lets deprecate import of options from couchbase.management.views
from couchbase.management.options import (DropDesignDocumentOptions,
                                          GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions,
                                          UpsertDesignDocumentOptions)

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._view_index_mgmt import ViewIndexMgmtInterface
    from couchbase.common.management.options import (DropDesignDocumentOptionsKwargs,
                                                     GetAllDesignDocumentsOptionsKwargs,
                                                     GetDesignDocumentOptionsKwargs,
                                                     PublishDesignDocumentOptionsKwargs,
                                                     UpsertDesignDocumentOptionsKwargs)


class ViewIndexManager:
    """Performs management operations on a bucket's view design documents."""

    def __init__(self, impl: ViewIndexMgmtInterface) -> None:
        self._impl = impl

    def get_design_document(self,
                            design_doc_name: str,
                            namespace: DesignDocumentNamespace,
                            options: GetDesignDocumentOptions | None = None,
                            **kwargs: Unpack[GetDesignDocumentOptionsKwargs]) -> DesignDocument:
        """Returns a design document.

        Args:
            design_doc_name (str): The name of the design document.
            namespace (:class:`.DesignDocumentNamespace`): Whether the design document is in development or production.
            options (:class:`~couchbase.management.options.GetDesignDocumentOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            :class:`.DesignDocument`: The design document.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the design_doc_name or namespace is not given.
            :class:`~couchbase.exceptions.DesignDocumentNotFoundException`: If the design document does not exist.
        """
        return self._impl.get_design_document(design_doc_name, namespace, options, **kwargs)

    def get_all_design_documents(self,
                                 namespace: DesignDocumentNamespace,
                                 options: GetAllDesignDocumentsOptions | None = None,
                                 **kwargs: Unpack[GetAllDesignDocumentsOptionsKwargs]) -> Iterable[DesignDocument]:
        """Returns every design document in a namespace.

        Args:
            namespace (:class:`.DesignDocumentNamespace`): Whether the design document is in development or production.
            options (:class:`~couchbase.management.options.GetAllDesignDocumentsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.DesignDocument`]: The design documents.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the namespace is not given.
        """
        return self._impl.get_all_design_documents(namespace, options, **kwargs)

    def upsert_design_document(self,
                               design_doc_data: DesignDocument,
                               namespace: DesignDocumentNamespace,
                               options: UpsertDesignDocumentOptions | None = None,
                               **kwargs: Unpack[UpsertDesignDocumentOptionsKwargs]) -> None:
        """Creates a design document, or replaces one of the same name.

        Args:
            design_doc_data (:class:`.DesignDocument`): The design document to create or replace.
            namespace (:class:`.DesignDocumentNamespace`): Whether the design document is in development or production.
            options (:class:`~couchbase.management.options.UpsertDesignDocumentOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the design_doc_data or namespace is not given.
        """
        self._impl.upsert_design_document(design_doc_data, namespace, options, **kwargs)

    def drop_design_document(self,
                             design_doc_name: str,
                             namespace: DesignDocumentNamespace,
                             options: DropDesignDocumentOptions | None = None,
                             **kwargs: Unpack[DropDesignDocumentOptionsKwargs]) -> None:
        """Drops an existing design document.

        Args:
            design_doc_name (str): The name of the design document.
            namespace (:class:`.DesignDocumentNamespace`): Whether the design document is in development or production.
            options (:class:`~couchbase.management.options.DropDesignDocumentOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the design_doc_name or namespace is not given.
            :class:`~couchbase.exceptions.DesignDocumentNotFoundException`: If the design document does not exist.
        """
        self._impl.drop_design_document(design_doc_name, namespace, options, **kwargs)

    def publish_design_document(self,
                                design_doc_name: str,
                                options: PublishDesignDocumentOptions | None = None,
                                **kwargs: Unpack[PublishDesignDocumentOptionsKwargs]) -> None:
        """Copies a design document from the development namespace to production.

        The copy is a get followed by an upsert, and the timeout covers both.

        Args:
            design_doc_name (str): The name of the development design document to publish.
            options (:class:`~couchbase.management.options.PublishDesignDocumentOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the design_doc_name is not given.
            :class:`~couchbase.exceptions.DesignDocumentNotFoundException`: If the design document does not exist in the
                development namespace.
        """
        self._impl.publish_design_document(design_doc_name, options, **kwargs)
