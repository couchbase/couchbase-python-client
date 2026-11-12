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

# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

__all__ = [
    'Collection',
    'TxCollection',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING

from twisted.internet.defer import Deferred

from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)
from txcouchbase.binary_collection import BinaryCollection

if TYPE_CHECKING:
    from datetime import timedelta

    from typing_extensions import Unpack

    from couchbase.common._interfaces._collection import TxCollectionInterface
    from couchbase.common._utils import DocumentValue
    from couchbase.common.options import (ExistsOptionsKwargs,
                                          GetAllReplicasOptionsKwargs,
                                          GetAndLockOptionsKwargs,
                                          GetAndTouchOptionsKwargs,
                                          GetAnyReplicaOptionsKwargs,
                                          GetOptionsKwargs,
                                          GetReplicaOptionsKwargs,
                                          InsertOptionsKwargs,
                                          LookupInAllReplicasOptionsKwargs,
                                          LookupInAnyReplicaOptionsKwargs,
                                          LookupInOptionsKwargs,
                                          MutateInOptionsKwargs,
                                          RemoveOptionsKwargs,
                                          ReplaceOptionsKwargs,
                                          TouchOptionsKwargs,
                                          UnlockOptionsKwargs,
                                          UpsertOptionsKwargs)
    from couchbase.options import (ExistsOptions,
                                   GetAllReplicasOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetAnyReplicaOptions,
                                   GetOptions,
                                   GetReplicaOptions,
                                   InsertOptions,
                                   LookupInAllReplicasOptions,
                                   LookupInAnyReplicaOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.replica_reads import GetReplicaStrategy
    from couchbase.subdocument import Spec
    from txcouchbase.scope import TxScope


class Collection:
    """A collection in a scope, and the key-value operations on its documents.

    Get one from :meth:`~txcouchbase.scope.Scope.collection`.

    Args:
        scope (:class:`~txcouchbase.scope.Scope`): The scope the collection belongs to.
        name (str): The name of the collection.
    """

    def __init__(self, scope: TxScope, name: str) -> None:
        self._impl: TxCollectionInterface = scope._impl.collection(name)

    @property
    def name(self) -> str:
        """The name of this collection."""
        return self._impl.name

    def get(self,
            key: str,
            opts: GetOptions | None = None,
            **kwargs: Unpack[GetOptionsKwargs]) -> Deferred[GetResult]:
        """Retrieves the value of a document from the collection.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetOptions`

        Returns:
            Deferred[:class:`~couchbase.result.GetResult`]: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.get_deferred(key, opts, **kwargs)

    def get_any_replica(self,
                        key: str,
                        opts: GetAnyReplicaOptions | None = None,
                        **kwargs: Unpack[GetAnyReplicaOptionsKwargs]) -> Deferred[GetReplicaResult]:
        """Retrieves a document from whichever of the active copy and the replicas answers first.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAnyReplicaOptions`

        Returns:
            Deferred[:class:`~couchbase.result.GetReplicaResult`]: An instance of
            :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentUnretrievableException`: If the key provided does not exist
                on the server.
        """
        return self._impl.get_any_replica_deferred(key, opts, **kwargs)

    def get_all_replicas(self,
                         key: str,
                         opts: GetAllReplicasOptions | None = None,
                         **kwargs: Unpack[GetAllReplicasOptionsKwargs]) -> Deferred[Iterable[GetReplicaResult]]:
        """Retrieves the value of a document from the collection returning both active and all available replicas.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAllReplicasOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAllReplicasOptions`

        Returns:
            Deferred[Iterable[:class:`~couchbase.result.GetReplicaResult`]]: A stream of
            :class:`~couchbase.result.GetReplicaResult` representing both active and replicas of the document retrieved.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.get_all_replicas_deferred(key, opts, **kwargs)

    def get_replica(self,
                    key: str,
                    strategy: GetReplicaStrategy,
                    opts: GetReplicaOptions | None = None,
                    **kwargs: Unpack[GetReplicaOptionsKwargs]) -> Deferred[GetReplicaResult]:
        """Retrieves a document from the replica the strategy selects.

        Args:
            key (str): The key for the document to retrieve.
            strategy (:class:`~couchbase.replica_reads.GetReplicaStrategy`): Selects the replica to read, created by
                :meth:`~couchbase.replica_reads.GetReplicaStrategy.from_index`.
            opts (:class:`~couchbase.options.GetReplicaOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetReplicaOptions`

        Returns:
            Deferred[:class:`~couchbase.result.GetReplicaResult`]: An instance of
            :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundOnReplicaException`: If the replica does not hold the
                document.
            :class:`~couchbase.exceptions.ReplicaIndexOutOfBoundsException`: If the bucket has no replica at the
                requested index, and the strategy does not wrap.
            :class:`~couchbase.exceptions.ReplicaIndexCurrentlyUnavailableException`: If the requested replica cannot
                be read now, and the strategy does not wrap or no other replica can be read either.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If ``strategy`` was not created by
                :meth:`~couchbase.replica_reads.GetReplicaStrategy.from_index`.
        """
        return self._impl.get_replica_deferred(key, strategy, opts, **kwargs)

    def exists(self,
               key: str,
               opts: ExistsOptions | None = None,
               **kwargs: Unpack[ExistsOptionsKwargs]) -> Deferred[ExistsResult]:
        """Checks whether a specific document exists or not.

        Args:
            key (str): The key for the document to check existence.
            opts (:class:`~couchbase.options.ExistsOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ExistsOptions`

        Returns:
            Deferred[:class:`~couchbase.result.ExistsResult`]: An instance of :class:`~couchbase.result.ExistsResult`.
        """
        return self._impl.exists_deferred(key, opts, **kwargs)

    def insert(self,
               key: str,
               value: DocumentValue,
               opts: InsertOptions | None = None,
               **kwargs: Unpack[InsertOptionsKwargs]) -> Deferred[MutationResult]:
        """Inserts a new document to the collection, failing if the document already exists.

        Args:
            key (str): Document key to insert.
            value (DocumentValue): The value of the document to insert.
            opts (:class:`~couchbase.options.InsertOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.InsertOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentExistsException`: If the document already exists on the
                server.
        """
        return self._impl.insert_deferred(key, value, opts, **kwargs)

    def upsert(self,
               key: str,
               value: DocumentValue,
               opts: UpsertOptions | None = None,
               **kwargs: Unpack[UpsertOptionsKwargs]) -> Deferred[MutationResult]:
        """Upserts a document to the collection. This operation succeeds whether or not the document already exists.

        Args:
            key (str): Document key to upsert.
            value (DocumentValue): The value of the document to upsert.
            opts (:class:`~couchbase.options.UpsertOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UpsertOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.
        """
        return self._impl.upsert_deferred(key, value, opts, **kwargs)

    def replace(self,
                key: str,
                value: DocumentValue,
                opts: ReplaceOptions | None = None,
                **kwargs: Unpack[ReplaceOptionsKwargs]) -> Deferred[MutationResult]:
        """Replaces the value of an existing document. Failing if the document does not exist.

        Args:
            key (str): Document key to replace.
            value (DocumentValue): The value of the document to replace.
            opts (:class:`~couchbase.options.ReplaceOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ReplaceOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.
        """
        return self._impl.replace_deferred(key, value, opts, **kwargs)

    def remove(self,
               key: str,
               opts: RemoveOptions | None = None,
               **kwargs: Unpack[RemoveOptionsKwargs]) -> Deferred[MutationResult]:
        """Removes an existing document. Failing if the document does not exist.

        Args:
            key (str): Key for the document to remove.
            opts (:class:`~couchbase.options.RemoveOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.RemoveOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.
        """
        return self._impl.remove_deferred(key, opts, **kwargs)

    def touch(self,
              key: str,
              expiry: timedelta,
              opts: TouchOptions | None = None,
              **kwargs: Unpack[TouchOptionsKwargs]) -> Deferred[MutationResult]:
        """Updates the expiry on an existing document.

        Args:
            key (str): Key for the document to touch.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TouchOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.
        """
        return self._impl.touch_deferred(key, expiry, opts, **kwargs)

    def get_and_touch(self,
                      key: str,
                      expiry: timedelta,
                      opts: GetAndTouchOptions | None = None,
                      **kwargs: Unpack[GetAndTouchOptionsKwargs]) -> Deferred[GetResult]:
        """Retrieves the value of the document and simultanously updates the expiry time for the same document.

        Args:
            key (str): The key for the document retrieve and set expiry time.
            expiry (timedelta):  The new expiry to apply to the document.
            opts (:class:`~couchbase.options.GetAndTouchOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndTouchOptions`

        Returns:
            Deferred[:class:`~couchbase.result.GetResult`]: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.get_and_touch_deferred(key, expiry, opts, **kwargs)

    def get_and_lock(self,
                     key: str,
                     lock_time: timedelta,
                     opts: GetAndLockOptions | None = None,
                     **kwargs: Unpack[GetAndLockOptionsKwargs]) -> Deferred[GetResult]:
        """Locks a document and retrieves the value of that document at the time it is locked.

        Args:
            key (str): The key for the document to lock and retrieve.
            lock_time (timedelta):  The amount of time to lock the document.
            opts (:class:`~couchbase.options.GetAndLockOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndLockOptions`

        Returns:
            Deferred[:class:`~couchbase.result.GetResult`]: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.get_and_lock_deferred(key, lock_time, opts, **kwargs)

    def unlock(self,
               key: str,
               cas: int,
               opts: UnlockOptions | None = None,
               **kwargs: Unpack[UnlockOptionsKwargs]) -> Deferred[None]:
        """Unlocks a previously locked document.

        Args:
            key (str): The key for the document to unlock.
            cas (int): The CAS of the document, used to validate lock ownership.
            opts (:class:`~couchbase.options.UnlockOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UnlockOptions`

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

            :class:`~couchbase.exceptions.DocumentLockedException`: If the provided cas is invalid.
        """
        return self._impl.unlock_deferred(key, cas, opts, **kwargs)

    def lookup_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: LookupInOptions | None = None,
                  **kwargs: Unpack[LookupInOptionsKwargs]) -> Deferred[LookupInResult]:
        """Fetches individual fields of a document, or information about them.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInOptions`

        Returns:
            Deferred[:class:`~couchbase.result.LookupInResult`]: An instance of
            :class:`~couchbase.result.LookupInResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.lookup_in_deferred(key, spec, opts, **kwargs)

    def lookup_in_any_replica(self,
                              key: str,
                              spec: Iterable[Spec],
                              opts: LookupInAnyReplicaOptions | None = None,
                              **kwargs: Unpack[LookupInAnyReplicaOptionsKwargs]) -> Deferred[LookupInReplicaResult]:
        """Fetches fields of a document from whichever of the active copy and the replicas answers first.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInAnyReplicaOptions`

        Returns:
            Deferred[:class:`~couchbase.result.LookupInReplicaResult`]: An instance of
            :class:`~couchbase.result.LookupInReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentUnretrievableException`: If the key provided does not exist
                on the server.
        """
        return self._impl.lookup_in_any_replica_deferred(key, spec, opts, **kwargs)

    def lookup_in_all_replicas(self,
                               key: str,
                               spec: Iterable[Spec],
                               opts: LookupInAllReplicasOptions | None = None,
                               **kwargs: Unpack[LookupInAllReplicasOptionsKwargs]
                               ) -> Deferred[Iterable[LookupInReplicaResult]]:
        """Fetches fields of a document from the active copy and every available replica.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInAllReplicasOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInAllReplicasOptions`

        Returns:
            Deferred[Iterable[:class:`~couchbase.result.LookupInReplicaResult`]]: A stream of
            :class:`~couchbase.result.LookupInReplicaResult` representing both active and replicas of the sub-document
            retrieved.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.lookup_in_all_replicas_deferred(key, spec, opts, **kwargs)

    def mutate_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: MutateInOptions | None = None,
                  **kwargs: Unpack[MutateInOptionsKwargs]) -> Deferred[MutateInResult]:
        """Atomically modifies individual fields of a document.

        The fields may include the document's extended attributes (xattrs).

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the operations to
                perform on the document.
            opts (:class:`~couchbase.options.MutateInOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.MutateInOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutateInResult`]: An instance of
            :class:`~couchbase.result.MutateInResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.mutate_in_deferred(key, spec, opts, **kwargs)

    def binary(self) -> BinaryCollection:
        """Get the binary operations on this collection.

        .. seealso::
            :class:`~txcouchbase.binary_collection.BinaryCollection`

        Returns:
            :class:`~txcouchbase.binary_collection.BinaryCollection`: A BinaryCollection instance.
        """
        return BinaryCollection(self._impl.binary())

    @staticmethod
    def default_name() -> str:
        """The name of a bucket's default collection."""
        return "_default"


TxCollection = Collection
