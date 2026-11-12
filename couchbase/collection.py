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

from __future__ import annotations

__all__ = [
    'Collection',
]

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from couchbase.binary_collection import BinaryCollection
from couchbase.common import options
from couchbase.common._capabilities import Capability, require
from couchbase.common._supportability import Supportability
from couchbase.datastructures import (CouchbaseList,
                                      CouchbaseMap,
                                      CouchbaseQueue,
                                      CouchbaseSet)
from couchbase.exceptions import CouchbaseException
from couchbase.management.queries import CollectionQueryIndexManager
from couchbase.options import (ExistsMultiOptions,
                               GetAllReplicasMultiOptions,
                               GetAndLockMultiOptions,
                               GetAnyReplicaMultiOptions,
                               GetMultiOptions,
                               InsertMultiOptions,
                               LockMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               ScanOptions,
                               TouchMultiOptions,
                               UnlockMultiOptions,
                               UpsertMultiOptions)
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MultiExistsResult,
                              MultiGetReplicaResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutateInResult,
                              MutationResult,
                              OperationResult,
                              ScanResultIterable)

if TYPE_CHECKING:
    from datetime import timedelta

    from typing_extensions import Unpack

    from couchbase.common._interfaces._collection import CollectionInterface
    from couchbase.common._utils import DocumentValue, JSONType
    from couchbase.kv_range_scan import ScanType
    from couchbase.options import (LookupInAllReplicasOptions,
                                   LookupInAnyReplicaOptions,
                                   MutateInOptions)
    from couchbase.replica_reads import GetReplicaStrategy
    from couchbase.result import MultiResultType
    from couchbase.scope import Scope
    from couchbase.subdocument import Spec


class Collection:
    """A collection in a scope, and the key-value operations on its documents.

    Get one from :meth:`~couchbase.scope.Scope.collection`.

    Args:
        scope (:class:`~couchbase.scope.Scope`): The scope the collection belongs to.
        name (str): The name of the collection.
    """

    def __init__(self, scope: Scope, name: str) -> None:
        self._impl: CollectionInterface = scope._impl.collection(name)

    @property
    def name(self) -> str:
        """The name of this collection."""
        return self._impl.name

    def get(self,
            key: str,
            opts: options.GetOptions | None = None,
            **kwargs: Unpack[options.GetOptionsKwargs]) -> GetResult:
        """Retrieves the value of a document from the collection.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetOptions`

        Returns:
            :class:`~couchbase.result.GetResult`: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple get operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.get('airline_10')
                print(f'Document value: {res.content_as[dict]}')


            Simple get operation with options::

                from datetime import timedelta
                from couchbase.options import GetOptions

                # ... other code ...

                res = collection.get('airline_10', GetOptions(timeout=timedelta(seconds=2)))
                print(f'Document value: {res.content_as[dict]}')

        """
        return self._impl.get(key, opts, **kwargs)

    def get_any_replica(self,
                        key: str,
                        opts: options.GetAnyReplicaOptions | None = None,
                        **kwargs: Unpack[options.GetAnyReplicaOptionsKwargs]) -> GetReplicaResult:
        """Retrieves a document from whichever of the active copy and the replicas answers first.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAnyReplicaOptions`

        Returns:
            :class:`~couchbase.result.GetReplicaResult`: An instance of :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentUnretrievableException`: If the key provided does not exist
                on the server.

        Examples:
            Simple get_any_replica operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.get_any_replica('airline_10')
                print(f'Document is replica: {res.is_replica}')
                print(f'Document value: {res.content_as[dict]}')


            Simple get_any_replica operation with options::

                from datetime import timedelta
                from couchbase.options import GetAnyReplicaOptions

                # ... other code ...

                res = collection.get_any_replica('airline_10', GetAnyReplicaOptions(timeout=timedelta(seconds=5)))
                print(f'Document is replica: {res.is_replica}')
                print(f'Document value: {res.content_as[dict]}')

        """
        return self._impl.get_any_replica(key, opts, **kwargs)

    def get_all_replicas(self,
                         key: str,
                         opts: options.GetAllReplicasOptions | None = None,
                         **kwargs: Unpack[options.GetAllReplicasOptionsKwargs]) -> Iterable[GetReplicaResult]:
        """Retrieves the value of a document from the collection returning both active and all available replicas.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAllReplicasOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAllReplicasOptions`

        Returns:
            Iterable[:class:`~couchbase.result.GetReplicaResult`]: A stream of
            :class:`~couchbase.result.GetReplicaResult` representing both active and replicas of the document retrieved.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple get_all_replicas operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                result = collection.get_all_replicas('airline_10')
                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Document value: {res.content_as[dict]}')


            Simple get_all_replicas operation with options::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                result = collection.get_all_replicas('airline_10', GetAllReplicasOptions(timeout=timedelta(seconds=10)))
                for res in result:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Document value: {res.content_as[dict]}')

            Stream get_all_replicas results::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                result = collection.get_all_replicas('airline_10', GetAllReplicasOptions(timeout=timedelta(seconds=10)))
                while True:
                    try:
                        res = next(result)
                        print(f'Document is replica: {res.is_replica}')
                        print(f'Document value: {res.content_as[dict]}')
                    except StopIteration:
                        print('Done streaming replicas.')
                        break

        """
        return self._impl.get_all_replicas(key, opts, **kwargs)

    def get_replica(self,
                    key: str,
                    strategy: GetReplicaStrategy,
                    opts: options.GetReplicaOptions | None = None,
                    **kwargs: Unpack[options.GetReplicaOptionsKwargs]) -> GetReplicaResult:
        """Retrieves a document from the replica the strategy selects.

        Args:
            key (str): The key for the document to retrieve.
            strategy (:class:`~couchbase.replica_reads.GetReplicaStrategy`): Selects the replica to read, created by
                :meth:`~couchbase.replica_reads.GetReplicaStrategy.from_index`.
            opts (:class:`~couchbase.options.GetReplicaOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetReplicaOptions`

        Returns:
            :class:`~couchbase.result.GetReplicaResult`: An instance of :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundOnReplicaException`: If the replica does not hold the
                document.
            :class:`~couchbase.exceptions.ReplicaIndexOutOfBoundsException`: If the bucket has no replica at the
                requested index, and the strategy does not wrap.
            :class:`~couchbase.exceptions.ReplicaIndexCurrentlyUnavailableException`: If the requested replica cannot
                be read now, and the strategy does not wrap or no other replica can be read either.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If ``strategy`` was not created by
                :meth:`~couchbase.replica_reads.GetReplicaStrategy.from_index`.

        Examples:
            Read the first replica::

                from couchbase.replica_reads import GetReplicaStrategy, ReplicaIndex

                # ... other code ...

                res = collection.get_replica('airline_10', GetReplicaStrategy.from_index(ReplicaIndex.FIRST))
                print(f'Document value: {res.content_as[dict]}')

            Read the second replica, or the next one that can be read::

                res = collection.get_replica('airline_10',
                                             GetReplicaStrategy.from_index(ReplicaIndex.SECOND, wrap=True))

        """
        return self._impl.get_replica(key, strategy, opts, **kwargs)

    def exists(self,
               key: str,
               opts: options.ExistsOptions | None = None,
               **kwargs: Unpack[options.ExistsOptionsKwargs]) -> ExistsResult:
        """Checks whether a specific document exists or not.

        Args:
            key (str): The key for the document to check existence.
            opts (:class:`~couchbase.options.ExistsOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ExistsOptions`

        Returns:
            :class:`~couchbase.result.ExistsResult`: An instance of :class:`~couchbase.result.ExistsResult`.

        Examples:
            Simple exists operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.exists(key)
                print(f'Document w/ key - {key} {"exists" if res.exists else "does not exist"}')


            Simple exists operation with options::

                from datetime import timedelta
                from couchbase.options import ExistsOptions

                # ... other code ...

                key = 'airline_10'
                res = collection.exists(key, ExistsOptions(timeout=timedelta(seconds=2)))
                print(f'Document w/ key - {key} {"exists" if res.exists else "does not exist"}')

        """
        return self._impl.exists(key, opts, **kwargs)

    def insert(self,
               key: str,
               value: DocumentValue,
               opts: options.InsertOptions | None = None,
               **kwargs: Unpack[options.InsertOptionsKwargs]) -> MutationResult:
        """Inserts a new document to the collection, failing if the document already exists.

        Args:
            key (str): Document key to insert.
            value (DocumentValue): The value of the document to insert.
            opts (:class:`~couchbase.options.InsertOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.InsertOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentExistsException`: If the document already exists on the
                server.

        Examples:
            Simple insert operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                res = collection.insert(key, doc)


            Simple insert operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import InsertOptions

                # ... other code ...

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
                res = collection.insert(key, doc, InsertOptions(durability=durability))

        """
        return self._impl.insert(key, value, opts, **kwargs)

    def upsert(self,
               key: str,
               value: DocumentValue,
               opts: options.UpsertOptions | None = None,
               **kwargs: Unpack[options.UpsertOptionsKwargs]) -> MutationResult:
        """Upserts a document to the collection. This operation succeeds whether or not the document already exists.

        Args:
            key (str): Document key to upsert.
            value (DocumentValue): The value of the document to upsert.
            opts (:class:`~couchbase.options.UpsertOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UpsertOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Examples:
            Simple upsert operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                res = collection.upsert(key, doc)


            Simple upsert operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import UpsertOptions

                # ... other code ...

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = collection.upsert(key, doc, InsertOptions(durability=durability))

        """
        return self._impl.upsert(key, value, opts, **kwargs)

    def replace(self,
                key: str,
                value: DocumentValue,
                opts: options.ReplaceOptions | None = None,
                **kwargs: Unpack[options.ReplaceOptionsKwargs]) -> MutationResult:
        """Replaces the value of an existing document. Failing if the document does not exist.

        Args:
            key (str): Document key to replace.
            value (DocumentValue): The value of the document to replace.
            opts (:class:`~couchbase.options.ReplaceOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ReplaceOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:
            Simple replace operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                res = collection.get(key)
                content = res.content_as[dict]
                airline["name"] = "Couchbase Airways!!"
                res = collection.replace(key, doc)


            Simple replace operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import ReplaceOptions

                # ... other code ...

                key = 'airline_8091'
                res = collection.get(key)
                content = res.content_as[dict]
                airline["name"] = "Couchbase Airways!!"
                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = collection.replace(key, doc, InsertOptions(durability=durability))

        """
        return self._impl.replace(key, value, opts, **kwargs)

    def remove(self,
               key: str,
               opts: options.RemoveOptions | None = None,
               **kwargs: Unpack[options.RemoveOptionsKwargs]) -> MutationResult:
        """Removes an existing document. Failing if the document does not exist.

        Args:
            key (str): Key for the document to remove.
            opts (:class:`~couchbase.options.RemoveOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.RemoveOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:
            Simple remove operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.remove('airline_10')


            Simple remove operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import RemoveOptions

                # ... other code ...

                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = collection.remove('airline_10', RemoveOptions(durability=durability))

        """
        return self._impl.remove(key, opts, **kwargs)

    def touch(self,
              key: str,
              expiry: timedelta,
              opts: options.TouchOptions | None = None,
              **kwargs: Unpack[options.TouchOptionsKwargs]) -> MutationResult:
        """Updates the expiry on an existing document.

        Args:
            key (str): Key for the document to touch.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TouchOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:
            Simple touch operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.touch('airline_10', timedelta(seconds=300))


            Simple touch operation with options::

                from datetime import timedelta

                from couchbase.options import TouchOptions

                # ... other code ...

                res = collection.touch('airline_10',
                                        timedelta(seconds=300),
                                        TouchOptions(timeout=timedelta(seconds=2)))

        """
        return self._impl.touch(key, expiry, opts, **kwargs)

    def get_and_touch(self,
                      key: str,
                      expiry: timedelta,
                      opts: options.GetAndTouchOptions | None = None,
                      **kwargs: Unpack[options.GetAndTouchOptionsKwargs]) -> GetResult:
        """Retrieves the value of the document and simultanously updates the expiry time for the same document.

        Args:
            key (str): The key for the document retrieve and set expiry time.
            expiry (timedelta):  The new expiry to apply to the document.
            opts (:class:`~couchbase.options.GetAndTouchOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndTouchOptions`

        Returns:
            :class:`~couchbase.result.GetResult`: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple get and touch operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.get_and_touch(key, timedelta(seconds=20))
                print(f'Document w/ updated expiry: {res.content_as[dict]}')


            Simple get and touch operation with options::

                from datetime import timedelta
                from couchbase.options import GetAndTouchOptions

                # ... other code ...

                key = 'airline_10'
                res = collection.get_and_touch(key,
                                            timedelta(seconds=20),
                                            GetAndTouchOptions(timeout=timedelta(seconds=2)))
                print(f'Document w/ updated expiry: {res.content_as[dict]}')

        """
        return self._impl.get_and_touch(key, expiry, opts, **kwargs)

    def get_and_lock(self,
                     key: str,
                     lock_time: timedelta,
                     opts: options.GetAndLockOptions | None = None,
                     **kwargs: Unpack[options.GetAndLockOptionsKwargs]) -> GetResult:
        """Locks a document and retrieves the value of that document at the time it is locked.

        Args:
            key (str): The key for the document to lock and retrieve.
            lock_time (timedelta):  The amount of time to lock the document.
            opts (:class:`~couchbase.options.GetAndLockOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndLockOptions`

        Returns:
            :class:`~couchbase.result.GetResult`: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple get and lock operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.get_and_lock(key, timedelta(seconds=20))
                print(f'Locked document: {res.content_as[dict]}')


            Simple get and lock operation with options::

                from datetime import timedelta
                from couchbase.options import GetAndLockOptions

                # ... other code ...

                key = 'airline_10'
                res = collection.get_and_lock(key,
                                            timedelta(seconds=20),
                                            GetAndLockOptions(timeout=timedelta(seconds=2)))
                print(f'Locked document: {res.content_as[dict]}')

        """
        return self._impl.get_and_lock(key, lock_time, opts, **kwargs)

    def unlock(self,
               key: str,
               cas: int,
               opts: options.UnlockOptions | None = None,
               **kwargs: Unpack[options.UnlockOptionsKwargs]) -> None:
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

        Examples:
            Simple unlock operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.get_and_lock(key, timedelta(seconds=5))
                collection.unlock(key, res.cas)
                # this should be okay once document is unlocked
                collection.upsert(key, res.content_as[dict])

        """
        self._impl.unlock(key, cas, opts, **kwargs)

    def lookup_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: options.LookupInOptions | None = None,
                  **kwargs: Unpack[options.LookupInOptionsKwargs]) -> LookupInResult:
        """Fetches individual fields of a document, or information about them.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInOptions`

        Returns:
            :class:`~couchbase.result.LookupInResult`: An instance of :class:`~couchbase.result.LookupInResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple look-up in operation::

                import couchbase.subdocument as SD

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('hotel')

                key = 'hotel_10025'
                res = collection.lookup_in(key, (SD.get("geo"),))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple look-up in operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import LookupInOptions

                # ... other code ...

                key = 'hotel_10025'
                res = collection.lookup_in(key,
                                            (SD.get("geo"),),
                                            LookupInOptions(timeout=timedelta(seconds=2)))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

        """
        return self._impl.lookup_in(key, spec, opts, **kwargs)

    def lookup_in_any_replica(self,
                              key: str,
                              spec: Iterable[Spec],
                              opts: LookupInAnyReplicaOptions | None = None,
                              **kwargs: Unpack[options.LookupInAnyReplicaOptionsKwargs]) -> LookupInReplicaResult:
        """Fetches fields of a document from whichever of the active copy and the replicas answers first.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInAnyReplicaOptions`

        Returns:
            :class:`~couchbase.result.LookupInReplicaResult`: An instance of
            :class:`~couchbase.result.LookupInReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentUnretrievableException`: If the key provided does not exist
                on the server.

        Examples:
            Simple lookup_in_any_replica operation::

                import couchbase.subdocument as SD

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('hotel')

                key = 'hotel_10025'
                res = collection.lookup_in_any_replica(key, (SD.get("geo"),))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple lookup_in_any_replica operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import LookupInAnyReplicaOptions

                # ... other code ...

                key = 'hotel_10025'
                res = collection.lookup_in_any_replica(key,
                                                       (SD.get("geo"),),
                                                       LookupInAnyReplicaOptions(timeout=timedelta(seconds=2)))
                print(f'Document is replica: {res.is_replica}')
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

        """
        return self._impl.lookup_in_any_replica(key, spec, opts, **kwargs)

    def lookup_in_all_replicas(self,
                               key: str,
                               spec: Iterable[Spec],
                               opts: LookupInAllReplicasOptions | None = None,
                               **kwargs: Unpack[options.LookupInAllReplicasOptionsKwargs]
                               ) -> Iterable[LookupInReplicaResult]:
        """Fetches fields of a document from the active copy and every available replica.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInAllReplicasOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInAllReplicasOptions`

        Returns:
            Iterable[:class:`~couchbase.result.LookupInReplicaResult`]: A stream of
            :class:`~couchbase.result.LookupInReplicaResult` representing both active and replicas of the sub-document
            retrieved.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple lookup_in_all_replicas operation::

                import couchbase.subdocument as SD

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('hotel')

                key = 'hotel_10025'
                results = collection.lookup_in_all_replicas(key, (SD.get("geo"),))
                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple lookup_in_all_replicas operation with options::

                import couchbase.subdocument as SD
                from datetime import timedelta
                from couchbase.options import LookupInAllReplicasOptions

                # ... other code ...

                key = 'hotel_10025'
                opts = LookupInAllReplicasOptions(timeout=timedelta(seconds=2))
                results = collection.lookup_in_all_replicas(key, (SD.get("geo"),), opts)

                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

            Stream lookup_in_all_replicas results::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                key = 'hotel_10025'
                opts = LookupInAllReplicasOptions(timeout=timedelta(seconds=2))
                results = collection.lookup_in_all_replicas(key, (SD.get("geo"),), opts)
                while True:
                    try:
                        res = next(results)
                        print(f'Document is replica: {res.is_replica}')
                        print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')
                    except StopIteration:
                        print('Done streaming replicas.')
                        break

        """
        return self._impl.lookup_in_all_replicas(key, spec, opts, **kwargs)

    def mutate_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: MutateInOptions | None = None,
                  **kwargs: Unpack[options.MutateInOptionsKwargs]) -> MutateInResult:
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
            :class:`~couchbase.result.MutateInResult`: An instance of :class:`~couchbase.result.MutateInResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple mutate-in operation::

                import couchbase.subdocument as SD

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('hotel')

                key = 'hotel_10025'
                res = collection.mutate_in(key, (SD.replace("city", "New City"),))


            Simple mutate-in operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import MutateInOptions

                # ... other code ...

                key = 'hotel_10025'
                res = collection.mutate_in(key,
                                            (SD.replace("city", "New City"),),
                                            MutateInOptions(timeout=timedelta(seconds=2)))

        """
        return self._impl.mutate_in(key, spec, opts, **kwargs)

    def scan(self,
             scan_type: ScanType,
             opts: ScanOptions | None = None,
             **kwargs: Unpack[options.ScanOptionsKwargs]) -> ScanResultIterable:
        """Execute a key-value range scan operation from the collection.

        .. note::
            Use this API for low concurrency batch queries where latency is not a critical as the system may have to
            scan a lot of documents to find the matching documents. For low latency range queries, it is recommended
            that you use SQL++ with the necessary indexes.

        Args:
            scan_type (:class:`~couchbase.kv_range_scan.ScanType`): Either a
                :class:`~couchbase.kv_range_scan.RangeScan`,
                :class:`~couchbase.kv_range_scan.PrefixScan` or
                :class:`~couchbase.kv_range_scan.SamplingScan` instance.
            opts (:class:`~couchbase.options.ScanOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ScanOptions`

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If scan_type is not either a RangeScan or
                SamplingScan instance.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If sort option is provided and is incorrect type.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If consistent_with option is provided and is not a

        Returns:
            :class:`~couchbase.result.ScanResultIterable`: An instance of :class:`~couchbase.result.ScanResultIterable`.

        Examples:
            Simple range scan operation::

                from couchbase.kv_range_scan import RangeScan
                from couchbase.options import ScanOptions

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                scan_type = RangeScan(ScanTerm('airline-00'), ScanTerm('airline-99'))
                scan_iter = collection.scan(scan_type, ScanOptions(ids_only=True))

                for res in scan_iter:
                    print(res)


        """
        return self._impl.scan(scan_type, opts, **kwargs)

    def binary(self) -> BinaryCollection:
        """Get the binary operations on this collection.

        .. seealso::
            :class:`~couchbase.binary_collection.BinaryCollection`

        Returns:
            :class:`~couchbase.binary_collection.BinaryCollection`: A BinaryCollection instance.

        """
        return BinaryCollection(self._impl.binary())

    def couchbase_list(self, key: str) -> CouchbaseList:
        """Returns a CouchbaseList permitting simple list storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseList`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseList`: A CouchbaseList instance.

        """
        return CouchbaseList(key, self)

    def list_append(self,
                    key: str,
                    value: JSONType,
                    create: bool | None = False,
                    **kwargs: object) -> OperationResult:
        """Add an item to the end of a list.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseList.append` instead.

        Args:
            key (str): The key for the list document.
            value (JSONType): The value to append to the list.
            create (bool, optional): Whether the list should be created if it does not exist.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('list_append', 'CouchbaseList.append')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.list_append(key, value, create, **kwargs)

    def list_prepend(self,
                     key: str,
                     value: JSONType,
                     create: bool | None = False,
                     **kwargs: object
                     ) -> OperationResult:
        """Add an item to the beginning of a list.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseList.prepend` instead.

        Args:
            key (str): The key for the list document.
            value (JSONType): The value to prepend to the list.
            create (bool, optional): Whether the list should be created if it does not exist.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('list_prepend', 'CouchbaseList.prepend')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.list_prepend(key, value, create, **kwargs)

    def list_set(self,
                 key: str,
                 index: int,
                 value: JSONType,
                 **kwargs: object
                 ) -> OperationResult:
        """Sets an item within a list at a given position.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseList.set_at` instead.

        Args:
            key (str): The key for the list document.
            index (int): The position to replace.
            value (JSONType): The value to prepend to the list.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            IndexError: If the index is out of bounds.

        """
        Supportability.method_deprecated('list_set', 'CouchbaseList.set_at')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.list_set(key, index, value, **kwargs)

    def list_get(self, key: str, index: int, **kwargs: object) -> Any:
        """Get a specific element within a list.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseList.get_at` instead.

        Args:
            key (str): The key for the list document.
            index (int): The position to retrieve.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Any: The value of the element at the specified index.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            IndexError: If the index is out of bounds.

        """
        Supportability.method_deprecated('list_get', 'CouchbaseList.get_at')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.list_get(key, index, **kwargs)

    def list_remove(self, key: str, index: int, **kwargs: object) -> OperationResult:
        """Remove the element at a specific index from a list.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseList.remove_at` instead.

        Args:
            key (str): The key for the list document.
            index (int): The position to remove.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            IndexError: If the index is out of bounds.

        """
        Supportability.method_deprecated('list_remove', 'CouchbaseList.remove_at')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.list_remove(key, index, **kwargs)

    def list_size(self, key: str, **kwargs: object) -> int:
        """Returns the number of items in the list.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseList.size` instead.

        Args:
            key (str): The key for the list document.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            int: The number of items in the list.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('list_size', 'CouchbaseList.size')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.list_size(key, **kwargs)

    def couchbase_map(self, key: str) -> CouchbaseMap:
        """Returns a CouchbaseMap permitting simple map storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseMap`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseMap`: A CouchbaseMap instance.

        """
        return CouchbaseMap(key, self)

    def map_add(self,
                key: str,
                mapkey: str,
                value: Any,
                create: bool | None = False,
                **kwargs: object
                ) -> OperationResult:
        """Set a value for a key in a map.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseMap.add` instead.

        Args:
            key (str): The key for the map document.
            mapkey (str): The key in the map to set.
            value (Any): The value to use.
            create (bool, optional): Whether the map should be created if it does not exist.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('map_add', 'CouchbaseMap.add')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.map_add(key, mapkey, value, create, **kwargs)

    def map_get(self, key: str, mapkey: str, **kwargs: object) -> Any:
        """Retrieve a value from a map.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseMap.get` instead.

        Args:
            key (str): The key for the map document.
            mapkey (str): The key in the map to set.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Any: The value of the specified key.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('map_get', 'CouchbaseMap.get')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.map_get(key, mapkey, **kwargs)

    def map_remove(self, key: str, mapkey: str, **kwargs: object) -> OperationResult:
        """Remove an item from a map.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseMap.remove` instead.

        Args:
            key (str): The key for the map document.
            mapkey (str): The key in the map to set.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('map_remove', 'CouchbaseMap.remove')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.map_remove(key, mapkey, **kwargs)

    def map_size(self, key: str, **kwargs: object) -> int:
        """Get the number of items in the map.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseMap.size` instead.

        Args:
            key (str): The key for the map document.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            int: The number of items in the map.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        Supportability.method_deprecated('map_size', 'CouchbaseMap.size')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.map_size(key, **kwargs)

    def couchbase_set(self, key: str) -> CouchbaseSet:
        """Returns a CouchbaseSet permitting simple map storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseSet`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseSet`: A CouchbaseSet instance.

        """
        return CouchbaseSet(key, self)

    def set_add(self,
                key: str,
                value: Any,
                create: bool | None = False,
                **kwargs: object
                ) -> OperationResult | None:
        """Add an item to a set if the item does not yet exist.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseSet.add` instead.

        Args:
            key (str): The key for the set document.
            value (Any): The value to add to the set.
            create (bool, optional): Whether the set should be created if it does not exist.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        Supportability.method_deprecated('set_add', 'CouchbaseSet.add')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.set_add(key, value, create, **kwargs)

    def set_remove(self, key: str, value: Any, **kwargs: object) -> OperationResult | None:
        """Remove an item from a set.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseSet.remove` instead.

        Args:
            key (str): The key for the set document.
            value (Any): The value to remove from the set.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult` | None: An instance of
            :class:`~couchbase.result.OperationResult`, or ``None`` if the set does not hold the value.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            :class:`~couchbase.exceptions.CasMismatchException`: If the set changed between reading it and
                removing the value.  :meth:`.CouchbaseSet.remove` retries instead.
        """
        Supportability.method_deprecated('set_remove', 'CouchbaseSet.remove')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.set_remove(key, value, **kwargs)

    def set_size(self, key: str, **kwargs: object) -> int:
        """Get the length of a set.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseSet.size` instead.

        Args:
            key (str): The key for the set document.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            int: The length of a set.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        Supportability.method_deprecated('set_size', 'CouchbaseSet.size')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.set_size(key, **kwargs)

    def set_contains(self, key: str, value: Any, **kwargs: object) -> bool:
        """Determine if an item exists in a set.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseSet.contains` instead.

        Args:
            key (str): The key for the set document.
            value (Any):  The value to check for.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            bool: True if the set contains the specified value.  False otherwise.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        Supportability.method_deprecated('set_contains', 'CouchbaseSet.contains')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.set_contains(key, value, **kwargs)

    def couchbase_queue(self, key: str) -> CouchbaseQueue:
        """Returns a CouchbaseQueue permitting simple map storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseQueue`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseQueue`: A CouchbaseQueue instance.

        """
        return CouchbaseQueue(key, self)

    def queue_push(self,
                   key: str,
                   value: Any,
                   create: bool | None = False,
                   **kwargs: object
                   ) -> OperationResult:
        """Add an item to the end of a queue.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseQueue.push` instead.

        Args:
            key (str): The key for the queue document.
            value (Any):  The value to add.
            create (bool, optional): Whether the queue should be created if it does not exist.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        Supportability.method_deprecated('queue_push', 'CouchbaseQueue.push')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.queue_push(key, value, create, **kwargs)

    def queue_pop(self, key: str, **kwargs: object) -> Any:
        """Remove and return the oldest item in a queue.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseQueue.pop` instead.

        .. warning::
            This method is not safe with more than one consumer.  Two concurrent calls can return the same
            item, and the queue then loses a different one.  :meth:`.CouchbaseQueue.pop` does not have this
            problem.

        Args:
            key (str): The key for the queue document.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Any: The item removed from the queue.

        Raises:
            :class:`~couchbase.exceptions.QueueEmpty`: If the queue is empty.
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        Supportability.method_deprecated('queue_pop', 'CouchbaseQueue.pop')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.queue_pop(key, **kwargs)

    def queue_size(self, key: str) -> int:
        """Get the length of a queue.

        .. deprecated:: 4.0.0
            Use :meth:`.CouchbaseQueue.size` instead.

        Args:
            key (str): The key for the queue document.

        Returns:
            int: The length of the queue.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        Supportability.method_deprecated('queue_size', 'CouchbaseQueue.size')
        legacy = require(self._impl, Capability.LEGACY_DATASTRUCTURES).legacy_datastructures
        return legacy.queue_size(key)

    def get_multi(self,
                  keys: list[str],
                  opts: GetMultiOptions | None = None,
                  **kwargs: Unpack[options.GetMultiOptionsKwargs]) -> MultiGetResult:
        """For each key in the provided list, retrieve the document associated with the key.

        Args:
            keys (list[str]): The keys to use for the multiple get operations.
            opts (:class:`~couchbase.options.GetMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetResult`: An instance of
            :class:`~couchbase.result.MultiGetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple get-multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_multi(keys)
                for k, v in res.results.items():
                    print(f'Doc {k} has value: {v.content_as[dict]}')

            Simple get-multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import GetMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_multi(keys,
                                            GetMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Doc {k} has value: {v.content_as[dict]}')

            Simple get-multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import GetMultiOptions, GetOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                per_key_opts = {'doc1': GetOptions(timeout=timedelta(seconds=10))}
                res = collection.get_multi(keys,
                                            GetMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Doc {k} has value: {v.content_as[dict]}')


        """
        return self._impl.get_multi(keys, opts, **kwargs)

    def get_any_replica_multi(self,
                              keys: list[str],
                              opts: GetAnyReplicaMultiOptions | None = None,
                              **kwargs: Unpack[options.GetAnyReplicaMultiOptionsKwargs]
                              ) -> MultiGetReplicaResult[GetReplicaResult]:
        """Retrieves each document from whichever of the active copy and the replicas answers first.

        Args:
            keys (list[str]): The keys to use for the multiple get operations.
            opts (:class:`~couchbase.options.GetAnyReplicaMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAnyReplicaMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetReplicaResult`: An instance of
            :class:`~couchbase.result.MultiGetReplicaResult`, mapping each key to a single
            :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentUnretrievableException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple get_any_replica_multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_any_replica_multi(keys)
                for k, v in res.results.items():
                    if v.is_replica:
                        print(f'Replica doc {k} has value: {v.content_as[dict]}')
                    else:
                        print(f'Active doc {k} has value: {v.content_as[dict]}')

            Simple get_any_replica_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import GetAnyReplicaMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_any_replica_multi(keys,
                                            GetAnyReplicaMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    if v.is_replica:
                        print(f'Replica doc {k} has value: {v.content_as[dict]}')
                    else:
                        print(f'Active doc {k} has value: {v.content_as[dict]}')

            Simple get_any_replica_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import GetAnyReplicaMultiOptions, GetAnyReplicaOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                per_key_opts = {'doc1': GetAnyReplicaOptions(timeout=timedelta(seconds=10))}
                res = collection.get_any_replica_multi(keys,
                                            GetAnyReplicaMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    if v.is_replica:
                        print(f'Replica doc {k} has value: {v.content_as[dict]}')
                    else:
                        print(f'Active doc {k} has value: {v.content_as[dict]}')

        """
        return self._impl.get_any_replica_multi(keys, opts, **kwargs)

    def get_all_replicas_multi(self,
                               keys: list[str],
                               opts: GetAllReplicasMultiOptions | None = None,
                               **kwargs: Unpack[options.GetAllReplicasMultiOptionsKwargs]
                               ) -> MultiGetReplicaResult[list[GetReplicaResult]]:
        """Retrieves each document from the active copy and every available replica.

        Args:
            keys (list[str]): The keys to use for the multiple get operations.
            opts (:class:`~couchbase.options.GetAllReplicasMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAllReplicasMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetReplicaResult`: An instance of
            :class:`~couchbase.result.MultiGetReplicaResult`, mapping each key to a list of
            :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple get_all_replicas_multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_all_replicas_multi(keys)
                for k, docs in res.results.items():
                    for doc in docs:
                        if doc.is_replica:
                            print(f'Replica doc {k} has value: {doc.content_as[dict]}')
                        else:
                            print(f'Active doc {k} has value: {doc.content_as[dict]}')

            Simple get_all_replicas_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import GetAllReplicasMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_all_replicas_multi(keys,
                                            GetAllReplicasMultiOptions(return_exceptions=False))
                for k, docs in res.results.items():
                    for doc in docs:
                        if doc.is_replica:
                            print(f'Replica doc {k} has value: {doc.content_as[dict]}')
                        else:
                            print(f'Active doc {k} has value: {doc.content_as[dict]}')

            Simple get_all_replicas_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import GetAllReplicasMultiOptions, GetAllReplicasOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                per_key_opts = {'doc1': GetAllReplicasOptions(timeout=timedelta(seconds=10))}
                res = collection.get_all_replicas_multi(keys,
                                            GetAllReplicasMultiOptions(per_key_options=per_key_opts))
                for k, docs in res.results.items():
                    for doc in docs:
                        if doc.is_replica:
                            print(f'Replica doc {k} has value: {doc.content_as[dict]}')
                        else:
                            print(f'Active doc {k} has value: {doc.content_as[dict]}')

        """
        return self._impl.get_all_replicas_multi(keys, opts, **kwargs)

    def lock_multi(self,
                   keys: list[str],
                   lock_time: timedelta,
                   opts: LockMultiOptions | None = None,
                   **kwargs: Unpack[options.GetAndLockMultiOptionsKwargs]) -> MultiGetResult:
        """For each key in the provided list, lock the document associated with the key.

        .. deprecated:: 4.2.0
            Use :meth:`~couchbase.collection.Collection.get_and_lock_multi` instead.
        """
        Supportability.method_deprecated('lock_multi', 'get_and_lock_multi')
        return self.get_and_lock_multi(keys, lock_time, opts, **kwargs)

    def get_and_lock_multi(self,
                           keys: list[str],
                           lock_time: timedelta,
                           opts: GetAndLockMultiOptions | None = None,
                           **kwargs: Unpack[options.GetAndLockMultiOptionsKwargs]) -> MultiGetResult:
        """For each key in the provided list, lock the document associated with the key.

        Args:
            keys (list[str]): The keys to use for the multiple lock operations.
            lock_time (timedelta):  The amount of time to lock the documents.
            opts (:class:`~couchbase.options.GetAndLockMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndLockMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetResult`: An instance of
            :class:`~couchbase.result.MultiGetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple get_and_lock_multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_and_lock_multi(keys, timedelta(seconds=20))
                for k, v in res.results.items():
                    print(f'Locked document: key={k}, content={v.content_as[str]}')

            Simple get_and_lock_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import GetAndLockMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_and_lock_multi(keys,
                                                     timedelta(seconds=20),
                                                     GetAndLockMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Locked document: key={k}, content={v.content_as[str]}')

            Simple get_and_lock_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import GetAndLockMultiOptions, GetAndLockOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                per_key_opts = {'doc1': GetAndLockOptions(timeout=timedelta(seconds=10))}
                res = collection.get_and_lock_multi(keys,
                                                     timedelta(seconds=20),
                                                     GetAndLockMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Locked document: key={k}, content={v.content_as[str]}')

        """
        return self._impl.get_and_lock_multi(keys, lock_time, opts, **kwargs)

    def exists_multi(self,
                     keys: list[str],
                     opts: ExistsMultiOptions | None = None,
                     **kwargs: Unpack[options.ExistsMultiOptionsKwargs]) -> MultiExistsResult:
        """For each key in the provided list, check if the document associated with the key exists.

        Args:
            keys (list[str]): The keys to use for the multiple exists operations.
            opts (:class:`~couchbase.options.ExistsMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ExistsMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiExistsResult`: An instance of :class:`~couchbase.result.MultiExistsResult`.

        Examples:
            Simple exists_multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.exists_multi(keys)
                for k, v in res.results.items():
                    print(f'Doc with key={k} {"exists" if v.exists else "does not exist"}')

            Simple exists_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import ExistsMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.exists_multi(keys,
                                              ExistsMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Doc with key={k} {"exists" if v.exists else "does not exist"}')

            Simple exists_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import ExistsMultiOptions, ExistsOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                per_key_opts = {'doc1': ExistsOptions(timeout=timedelta(seconds=10))}
                res = collection.exists_multi(keys,
                                              ExistsMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Doc with key={k} {"exists" if v.exists else "does not exist"}')
        """
        return self._impl.exists_multi(keys, opts, **kwargs)

    def insert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: InsertMultiOptions | None = None,
                     **kwargs: Unpack[options.InsertMultiOptionsKwargs]) -> MultiMutationResult:
        """Inserts a new document for each key-value pair, failing for a key whose document already exists.

        Args:
            keys_and_docs (Mapping[str, DocumentValue]): The keys and values/docs to use for the multiple insert
                operations.
            opts (:class:`~couchbase.options.InsertMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.InsertMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentExistsException`: If the key provided already exists on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple insert_multi operation::

                collection = bucket.default_collection()
                keys_and_docs = {
                    'doc1': {'foo': 'bar', 'id': 'doc1'},
                    'doc2': {'bar': 'baz', 'id': 'doc2'},
                    'doc3': {'baz': 'qux', 'id': 'doc3'},
                    'doc4': {'qux': 'quux', 'id': 'doc4'}
                }
                res = collection.insert_multi(keys_and_docs)
                for k, v in res.results.items():
                    print(f'Doc inserted: key={k}, cas={v.cas}')

            Simple insert_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import InsertMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys_and_docs = {
                    'doc1': {'foo': 'bar', 'id': 'doc1'},
                    'doc2': {'bar': 'baz', 'id': 'doc2'},
                    'doc3': {'baz': 'qux', 'id': 'doc3'},
                    'doc4': {'qux': 'quux', 'id': 'doc4'}
                }
                res = collection.insert_multi(keys_and_docs,
                                              InsertMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Doc inserted: key={k}, cas={v.cas}')

            Simple insert_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import InsertMultiOptions, InsertOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys_and_docs = {
                    'doc1': {'foo': 'bar', 'id': 'doc1'},
                    'doc2': {'bar': 'baz', 'id': 'doc2'},
                    'doc3': {'baz': 'qux', 'id': 'doc3'},
                    'doc4': {'qux': 'quux', 'id': 'doc4'}
                }
                per_key_opts = {'doc1': InsertOptions(timeout=timedelta(seconds=10))}
                res = collection.insert_multi(keys_and_docs,
                                              InsertMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Doc inserted: key={k}, cas={v.cas}')

        """
        return self._impl.insert_multi(keys_and_docs, opts, **kwargs)

    def upsert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: UpsertMultiOptions | None = None,
                     **kwargs: Unpack[options.UpsertMultiOptionsKwargs]) -> MultiMutationResult:
        """Upserts a document for each key-value pair, whether or not the document already exists.

        Args:
            keys_and_docs (Mapping[str, DocumentValue]): The keys and values/docs to use for the multiple upsert
                operations.
            opts (:class:`~couchbase.options.UpsertMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UpsertMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Examples:
            Simple upsert_multi operation::

                collection = bucket.default_collection()
                keys_and_docs = {
                    'doc1': {'foo': 'bar', 'id': 'doc1'},
                    'doc2': {'bar': 'baz', 'id': 'doc2'},
                    'doc3': {'baz': 'qux', 'id': 'doc3'},
                    'doc4': {'qux': 'quux', 'id': 'doc4'}
                }
                res = collection.upsert_multi(keys_and_docs)
                for k, v in res.results.items():
                    print(f'Doc upserted: key={k}, cas={v.cas}')

            Simple upsert_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import UpsertMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys_and_docs = {
                    'doc1': {'foo': 'bar', 'id': 'doc1'},
                    'doc2': {'bar': 'baz', 'id': 'doc2'},
                    'doc3': {'baz': 'qux', 'id': 'doc3'},
                    'doc4': {'qux': 'quux', 'id': 'doc4'}
                }
                res = collection.upsert_multi(keys_and_docs,
                                              UpsertMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Doc upserted: key={k}, cas={v.cas}')

            Simple upsert_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import UpsertMultiOptions, UpsertOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys_and_docs = {
                    'doc1': {'foo': 'bar', 'id': 'doc1'},
                    'doc2': {'bar': 'baz', 'id': 'doc2'},
                    'doc3': {'baz': 'qux', 'id': 'doc3'},
                    'doc4': {'qux': 'quux', 'id': 'doc4'}
                }
                per_key_opts = {'doc1': UpsertOptions(timeout=timedelta(seconds=10))}
                res = collection.upsert_multi(keys_and_docs,
                                              UpsertMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Doc upserted: key={k}, cas={v.cas}')

        """
        return self._impl.upsert_multi(keys_and_docs, opts, **kwargs)

    def replace_multi(self,
                      keys_and_docs: Mapping[str, DocumentValue],
                      opts: ReplaceMultiOptions | None = None,
                      **kwargs: Unpack[options.ReplaceMultiOptionsKwargs]) -> MultiMutationResult:
        """Replaces the document for each key-value pair, failing for a key whose document does not exist.

        Args:
            keys_and_docs (Mapping[str, DocumentValue]): The keys and values/docs to use for the multiple replace
                operations.
            opts (:class:`~couchbase.options.ReplaceMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ReplaceMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple replace_multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_multi(keys)
                keys_and_docs = {}
                for k, v in res.results.items():
                    # assuming document is JSON
                    content = v.content_as[dict]
                    content['foo'] = 'bar'
                    keys_and_docs[k] = content
                res = collection.replace_multi(keys_and_docs)
                for k, v in res.results.items():
                    print(f'Doc replaced: key={k}, cas={v.cas}')

            Simple replace_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import ReplaceMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_multi(keys)
                keys_and_docs = {}
                for k, v in res.results.items():
                    # assuming document is JSON
                    content = v.content_as[dict]
                    content['foo'] = 'bar'
                    keys_and_docs[k] = content
                res = collection.replace_multi(keys_and_docs,
                                               ReplaceMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Doc replaced: key={k}, cas={v.cas}')

            Simple replace_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import ReplaceMultiOptions, ReplaceOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.get_multi(keys)
                keys_and_docs = {}
                for k, v in res.results.items():
                    # assuming document is JSON
                    content = v.content_as[dict]
                    content['foo'] = 'bar'
                    keys_and_docs[k] = content
                per_key_opts = {'doc1': ReplaceOptions(timeout=timedelta(seconds=10))}
                res = collection.replace_multi(keys_and_docs,
                                               ReplaceMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Doc replaced: key={k}, cas={v.cas}')

        """
        return self._impl.replace_multi(keys_and_docs, opts, **kwargs)

    def remove_multi(self,
                     keys: list[str],
                     opts: RemoveMultiOptions | None = None,
                     **kwargs: Unpack[options.RemoveMultiOptionsKwargs]) -> MultiMutationResult:
        """Removes the document for each key, failing for a key whose document does not exist.

        Args:
            keys (list[str]): The keys to use for the multiple remove operations.
            opts (:class:`~couchbase.options.RemoveMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.RemoveMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:
            Simple remove_multi operation::

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.remove_multi(keys)

            Simple remove_multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import RemoveMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                res = collection.remove_multi(keys,
                                              RemoveMultiOptions(return_exceptions=False))

            Simple remove_multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import RemoveMultiOptions, RemoveOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['doc1', 'doc2', 'doc3']
                per_key_opts = {'doc1': RemoveOptions(timeout=timedelta(seconds=10))}
                res = collection.remove_multi(keys,
                                              RemoveMultiOptions(per_key_options=per_key_opts))

        """
        return self._impl.remove_multi(keys, opts, **kwargs)

    def touch_multi(self,
                    keys: list[str],
                    expiry: timedelta,
                    opts: TouchMultiOptions | None = None,
                    **kwargs: Unpack[options.TouchMultiOptionsKwargs]) -> MultiMutationResult:
        """Updates the expiry of the document for each key, failing for a key whose document does not exist.

        Args:
            keys (list[str]): The keys to use for the multiple touch operations.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TouchMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        """
        return self._impl.touch_multi(keys, expiry, opts, **kwargs)

    def unlock_multi(self,
                     keys: MultiResultType | dict[str, int],
                     opts: UnlockMultiOptions | None = None,
                     **kwargs: Unpack[options.UnlockMultiOptionsKwargs]) -> dict[str, CouchbaseException | None]:
        """Unlocks the document behind each result, failing for one whose document does not exist.

        Args:
            keys (MultiResultType | dict[str, int]): The result from a previous multi operation.
            opts (:class:`~couchbase.options.UnlockMultiOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UnlockMultiOptions`

        Returns:
            dict[str, CouchbaseException | None]: Either None if operation successful or an Exception
            if the operation was unsuccessful

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

            :class:`~couchbase.exceptions.DocumentLockedException`: If the provided cas is invalid and the
                return_exceptions options is False.  Otherwise the exception is returned as a match to the key,
                but is not raised.

        """
        return self._impl.unlock_multi(keys, opts, **kwargs)

    def query_indexes(self) -> CollectionQueryIndexManager:
        """Get a manager for the query indexes of this collection.

        Returns:
            :class:`~couchbase.management.queries.CollectionQueryIndexManager`: A
            :class:`~couchbase.management.queries.CollectionQueryIndexManager` instance.
        """
        return CollectionQueryIndexManager(self._impl.query_index_mgmt(), self._impl.bucket_name)

    @staticmethod
    def default_name() -> str:
        """The name of a bucket's default collection."""
        return "_default"


if not TYPE_CHECKING:
    __getattr__ = Supportability.deprecated_paths(__name__)
