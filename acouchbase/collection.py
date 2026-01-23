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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from acouchbase.binary_collection import BinaryCollection
from acouchbase.datastructures import (CouchbaseList,
                                       CouchbaseMap,
                                       CouchbaseQueue,
                                       CouchbaseSet)
from acouchbase.logic.collection_impl import AsyncCollectionImpl
from acouchbase.management.queries import CollectionQueryIndexManager
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult,
                              ScanResultIterable)

if TYPE_CHECKING:
    from datetime import timedelta

    from acouchbase.scope import AsyncScope
    from couchbase._utils import JSONType
    from couchbase.kv_range_scan import ScanType
    from couchbase.options import (ExistsOptions,
                                   GetAllReplicasOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetAnyReplicaOptions,
                                   GetOptions,
                                   InsertOptions,
                                   LookupInAllReplicasOptions,
                                   LookupInAnyReplicaOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   ScanOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.subdocument import Spec


class AsyncCollection:

    def __init__(self, scope: AsyncScope, name: str) -> None:
        self._impl = AsyncCollectionImpl(name, scope)

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Collection` instance.
        """
        return self._impl.name

    async def get(self,
                  key,  # type: str
                  *opts,  # type: GetOptions
                  **kwargs,  # type: Any
                  ) -> GetResult:
        """Retrieves the value of a document from the collection.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.GetResult`]: A future that contains an instance
            of :class:`~couchbase.result.GetResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple get operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = await collection.get('airline_10')
                print(f'Document value: {res.content_as[dict]}')


            Simple get operation with options::

                from datetime import timedelta
                from couchbase.options import GetOptions

                # ... other code ...

                res = await collection.get('airline_10', GetOptions(timeout=timedelta(seconds=2)))
                print(f'Document value: {res.content_as[dict]}')

        """
        req = self._impl.request_builder.build_get_request(key, *opts, **kwargs)
        return await self._impl.get(req)

    async def get_any_replica(self,
                              key,  # type: str
                              *opts,  # type: GetAnyReplicaOptions
                              **kwargs,  # type: Dict[str, Any]
                              ) -> GetReplicaResult:
        """Retrieves the value of a document from the collection leveraging both active and all available replicas returning
        the first available.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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

                res = await collection.get_any_replica('airline_10')
                print(f'Document is replica: {res.is_replica}')
                print(f'Document value: {res.content_as[dict]}')


            Simple get_any_replica operation with options::

                from datetime import timedelta
                from couchbase.options import GetAnyReplicaOptions

                # ... other code ...

                res = await collection.get_any_replica('airline_10',
                                                        GetAnyReplicaOptions(timeout=timedelta(seconds=5)))
                print(f'Document is replica: {res.is_replica}')
                print(f'Document value: {res.content_as[dict]}')

        """  # noqa: E501
        req = self._impl.request_builder.build_get_any_replica_request(key, *opts, **kwargs)
        return await self._impl.get_any_replica(req)

    async def get_all_replicas(self,
                               key,  # type: str
                               *opts,  # type: GetAllReplicasOptions
                               **kwargs,  # type: Dict[str, Any]
                               ) -> Iterable[GetReplicaResult]:
        """Retrieves the value of a document from the collection returning both active and all available replicas.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAllReplicasOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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

                result = await collection.get_all_replicas('airline_10')
                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Document value: {res.content_as[dict]}')


            Simple get_all_replicas operation with options::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                result = await collection.get_all_replicas('airline_10',
                                                            GetAllReplicasOptions(timeout=timedelta(seconds=10)))
                for res in result:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Document value: {res.content_as[dict]}')

            Stream get_all_replicas results::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                result = await collection.get_all_replicas('airline_10',
                                                            GetAllReplicasOptions(timeout=timedelta(seconds=10)))
                while True:
                    try:
                        res = next(result)
                        print(f'Document is replica: {res.is_replica}')
                        print(f'Document value: {res.content_as[dict]}')
                    except StopIteration:
                        print('Done streaming replicas.')
                        break

        """
        req = self._impl.request_builder.build_get_all_replicas_request(key, *opts, **kwargs)
        return await self._impl.get_all_replicas(req)

    async def exists(self,
                     key,  # type: str
                     *opts,  # type: ExistsOptions
                     **kwargs,  # type: Any
                     ) -> ExistsResult:
        """Checks whether a specific document exists or not.

        Args:
            key (str): The key for the document to check existence.
            opts (:class:`~couchbase.options.ExistsOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ExistsOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.ExistsResult`]: A future that contains an instance
            of :class:`~couchbase.result.ExistsResult` if successful.

        Examples:

            Simple exists operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = await collection.exists(key)
                print(f'Document w/ key - {key} {"exists" if res.exists else "does not exist"}')


            Simple exists operation with options::

                from datetime import timedelta
                from couchbase.options import ExistsOptions

                # ... other code ...

                key = 'airline_10'
                res = await collection.exists(key, ExistsOptions(timeout=timedelta(seconds=2)))
                print(f'Document w/ key - {key} {"exists" if res.exists else "does not exist"}')

        """
        req = self._impl.request_builder.build_exists_request(key, *opts, **kwargs)
        return await self._impl.exists(req)

    async def insert(self,
                     key,  # type: str
                     value,  # type: JSONType
                     *opts,  # type: InsertOptions
                     **kwargs,  # type: Any
                     ) -> MutationResult:
        """Inserts a new document to the collection, failing if the document already exists.

        Args:
            key (str): Document key to insert.
            value (JSONType): The value of the document to insert.
            opts (:class:`~couchbase.options.InsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.InsertOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

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
                res = await collection.insert(key, doc)


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
                res = await collection.insert(key, doc, InsertOptions(durability=durability))

        """
        req = self._impl.request_builder.build_insert_request(key, value, *opts, **kwargs)
        return await self._impl.insert(req)

    async def upsert(self,
                     key,  # type: str
                     value,  # type: JSONType
                     *opts,  # type: UpsertOptions
                     **kwargs,  # type: Any
                     ) -> MutationResult:
        """Upserts a document to the collection. This operation succeeds whether or not the document already exists.

        Args:
            key (str): Document key to upsert.
            value (JSONType): The value of the document to upsert.
            opts (:class:`~couchbase.options.UpsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UpsertOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

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
                res = await collection.upsert(key, doc)


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
                res = await collection.upsert(key, doc, InsertOptions(durability=durability))

        """
        req = self._impl.request_builder.build_upsert_request(key, value, *opts, **kwargs)
        return await self._impl.upsert(req)

    async def replace(self,
                      key,  # type: str
                      value,  # type: JSONType
                      *opts,  # type: ReplaceOptions
                      **kwargs,  # type: Any
                      ) -> MutationResult:
        """Replaces the value of an existing document. Failing if the document does not exist.

        Args:
            key (str): Document key to replace.
            value (JSONType): The value of the document to replace.
            opts (:class:`~couchbase.options.ReplaceOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ReplaceOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:

            Simple replace operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                res = await collection.get(key)
                content = res.content_as[dict]
                airline["name"] = "Couchbase Airways!!"
                res = await collection.replace(key, doc)


            Simple replace operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import ReplaceOptions

                # ... other code ...

                key = 'airline_8091'
                res = await collection.get(key)
                content = res.content_as[dict]
                airline["name"] = "Couchbase Airways!!"
                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = await collection.replace(key, doc, InsertOptions(durability=durability))

        """
        req = self._impl.request_builder.build_replace_request(key, value, *opts, **kwargs)
        return await self._impl.replace(req)

    async def remove(self,
                     key,  # type: str
                     *opts,  # type: RemoveOptions
                     **kwargs,  # type: Any
                     ) -> MutationResult:
        """Removes an existing document. Failing if the document does not exist.

        Args:
            key (str): Key for the document to remove.
            opts (:class:`~couchbase.options.RemoveOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.RemoveOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

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
        req = self._impl.request_builder.build_remove_request(key, *opts, **kwargs)
        return await self._impl.remove(req)

    async def touch(self,
                    key,  # type: str
                    expiry,  # type: timedelta
                    *opts,  # type: TouchOptions
                    **kwargs,  # type: Any
                    ) -> MutationResult:
        """Updates the expiry on an existing document.

        Args:
            key (str): Key for the document to touch.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TouchOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:

            Simple touch operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = await collection.touch('airline_10', timedelta(seconds=300))


            Simple touch operation with options::

                from datetime import timedelta

                from couchbase.options import TouchOptions

                # ... other code ...

                res = await collection.touch('airline_10',
                                        timedelta(seconds=300),
                                        TouchOptions(timeout=timedelta(seconds=2)))

        """
        req = self._impl.request_builder.build_touch_request(key, expiry, *opts, **kwargs)
        return await self._impl.touch(req)

    async def get_and_touch(self,
                            key,  # type: str
                            expiry,  # type: timedelta
                            *opts,  # type: GetAndTouchOptions
                            **kwargs,  # type: Any
                            ) -> GetResult:
        """Retrieves the value of the document and simultanously updates the expiry time for the same document.

        Args:
            key (str): The key for the document retrieve and set expiry time.
            expiry (timedelta):  The new expiry to apply to the document.
            opts (:class:`~couchbase.options.GetAndTouchOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndTouchOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.GetResult`]: A future that contains an instance
            of :class:`~couchbase.result.GetResult` if successful.

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
                res = await collection.get_and_touch(key, timedelta(seconds=20))
                print(f'Document w/ updated expiry: {res.content_as[dict]}')


            Simple get and touch operation with options::

                from datetime import timedelta
                from couchbase.options import GetAndTouchOptions

                # ... other code ...

                key = 'airline_10'
                res = await collection.get_and_touch(key,
                                            timedelta(seconds=20),
                                            GetAndTouchOptions(timeout=timedelta(seconds=2)))
                print(f'Document w/ updated expiry: {res.content_as[dict]}')

        """
        req = self._impl.request_builder.build_get_and_touch_request(key, expiry, *opts, **kwargs)
        return await self._impl.get_and_touch(req)

    async def get_and_lock(self,
                           key,  # type: str
                           lock_time,  # type: timedelta
                           *opts,  # type: GetAndLockOptions
                           **kwargs,  # type: Any
                           ) -> GetResult:
        """Locks a document and retrieves the value of that document at the time it is locked.

        Args:
            key (str): The key for the document to lock and retrieve.
            lock_time (timedelta):  The amount of time to lock the document.
            opts (:class:`~couchbase.options.GetAndLockOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndLockOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.GetResult`]: A future that contains an instance
            of :class:`~couchbase.result.GetResult` if successful.

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
                res = await collection.get_and_lock(key, timedelta(seconds=20))
                print(f'Locked document: {res.content_as[dict]}')


            Simple get and lock operation with options::

                from datetime import timedelta
                from couchbase.options import GetAndLockOptions

                # ... other code ...

                key = 'airline_10'
                res = await collection.get_and_lock(key,
                                            timedelta(seconds=20),
                                            GetAndLockOptions(timeout=timedelta(seconds=2)))
                print(f'Locked document: {res.content_as[dict]}')

        """
        req = self._impl.request_builder.build_get_and_lock_request(key, lock_time, *opts, **kwargs)
        return await self._impl.get_and_lock(req)

    async def unlock(self,
                     key,  # type: str
                     cas,  # type: int
                     *opts,  # type: UnlockOptions
                     **kwargs,  # type: Any
                     ) -> None:
        """Unlocks a previously locked document.

        Args:
            key (str): The key for the document to unlock.
            cas (int): The CAS of the document, used to validate lock ownership.
            opts (:class:`couchbaseoptions.UnlockOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UnlockOptions`

        Returns:
            Awaitable[None]: A future that contains an empty result if successful.

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
                res = await collection.get_and_lock(key, timedelta(seconds=5))
                await collection.unlock(key, res.cas)
                # this should be okay once document is unlocked
                await collection.upsert(key, res.content_as[dict])

        """
        req = self._impl.request_builder.build_unlock_request(key, cas, *opts, **kwargs)
        await self._impl.unlock(req)

    async def lookup_in(self,
                        key,  # type: str
                        spec,  # type: Iterable[Spec]
                        *opts,  # type: LookupInOptions
                        **kwargs,  # type: Any
                        ) -> LookupInResult:
        """Performs a lookup-in operation against a document, fetching individual fields or information
        about specific fields inside the document value.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.LookupInResult`]: A future that contains an instance
            of :class:`~couchbase.result.LookupInResult` if successful.

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
                res = await collection.lookup_in(key, (SD.get("geo"),))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple look-up in operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import LookupInOptions

                # ... other code ...

                key = 'hotel_10025'
                res = await collection.lookup_in(key,
                                            (SD.get("geo"),),
                                            LookupInOptions(timeout=timedelta(seconds=2)))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

        """
        req = self._impl.request_builder.build_lookup_in_request(key, spec, *opts, **kwargs)
        return await self._impl.lookup_in(req)

    async def lookup_in_any_replica(self,
                                    key,  # type: str
                                    spec,  # type: Iterable[Spec]
                                    *opts,  # type: LookupInAnyReplicaOptions
                                    **kwargs,  # type: Any
                                    ) -> LookupInReplicaResult:
        """Performs a lookup-in operation against a document, fetching individual fields or information
        about specific fields inside the document value. It leverages both active and all available replicas
        returning the first available

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInAnyReplicaOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.LookupInReplicaResult`]: A future that contains an instance
            of :class:`~couchbase.result.LookupInReplicaResult` if successful.

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
                res = await collection.lookup_in_any_replica(key, (SD.get("geo"),))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple lookup_in_any_replica operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import LookupInAnyReplicaOptions

                # ... other code ...

                key = 'hotel_10025'
                res = await collection.lookup_in_any_replica(key,
                                                             (SD.get("geo"),),
                                                             LookupInAnyReplicaOptions(timeout=timedelta(seconds=2)))
                print(f'Document is replica: {res.is_replica}')
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

        """
        req = self._impl.request_builder.build_lookup_in_any_replica_request(key, spec, *opts, **kwargs)
        return await self._impl.lookup_in_any_replica(req)

    async def lookup_in_all_replicas(self,
                                     key,  # type: str
                                     spec,  # type: Iterable[Spec]
                                     *opts,  # type: LookupInAllReplicasOptions
                                     **kwargs,  # type: Any
                                     ) -> Iterable[LookupInReplicaResult]:
        """Performs a lookup-in operation against a document, fetching individual fields or information
        about specific fields inside the document value, returning results from both active and all available replicas

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInAllReplicasOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
                results = await collection.lookup_in_all_replicas(key, (SD.get("geo"),))
                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple lookup_in_all_replicas operation with options::

                import couchbase.subdocument as SD
                from datetime import timedelta
                from couchbase.options import LookupInAllReplicasOptions

                # ... other code ...

                key = 'hotel_10025'
                results = await collection.lookup_in_all_replicas(key,
                                                                  (SD.get("geo"),),
                                                                  LookupInAllReplicasOptions(timeout=timedelta(seconds=2)))

                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

            Stream lookup_in_all_replicas results::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                key = 'hotel_10025'
                results = await collection.lookup_in_all_replicas(key,
                                                                  (SD.get("geo"),),
                                                                  LookupInAllReplicasOptions(timeout=timedelta(seconds=2)))
                while True:
                    try:
                        res = next(results)
                        print(f'Document is replica: {res.is_replica}')
                        print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')
                    except StopIteration:
                        print('Done streaming replicas.')
                        break

        """  # noqa: E501
        req = self._impl.request_builder.build_lookup_in_all_replicas_request(key, spec, *opts, **kwargs)
        return await self._impl.lookup_in_all_replicas(req)

    async def mutate_in(self,
                        key,  # type: str
                        spec,  # type: Iterable[Spec]
                        *opts,  # type: MutateInOptions
                        **kwargs,  # type: Any
                        ) -> MutateInResult:
        """Performs a mutate-in operation against a document. Allowing atomic modification of specific fields
        within a document. Also enables access to document extended-attributes (i.e. xattrs).

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the operations to
                perform on the document.
            opts (:class:`~couchbase.options.MutateInOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.MutateInOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutateInResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutateInResult` if successful.

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
                res = await collection.mutate_in(key, (SD.replace("city", "New City"),))


            Simple mutate-in operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import MutateInOptions

                # ... other code ...

                key = 'hotel_10025'
                res = await collection.mutate_in(key,
                                            (SD.replace("city", "New City"),),
                                            MutateInOptions(timeout=timedelta(seconds=2)))

        """
        req = self._impl.request_builder.build_mutate_in_request(key, spec, *opts, **kwargs)
        return await self._impl.mutate_in(req)

    def scan(self,
             scan_type,  # type: ScanType
             *opts,  # type: ScanOptions
             **kwargs,  # type: Dict[str, Any]
             ) -> ScanResultIterable:
        """Execute a key-value range scan operation from the collection.

        .. note::
            Use this API for low concurrency batch queries where latency is not a critical as the system may have to scan a lot of documents to find the matching documents.
            For low latency range queries, it is recommended that you use SQL++ with the necessary indexes.

        Args:
            scan_type (:class:`~couchbase.kv_range_scan.ScanType`): Either a :class:`~couchbase.kv_range_scan.RangeScan`,
                :class:`~couchbase.kv_range_scan.PrefixScan` or
                :class:`~couchbase.kv_range_scan.SamplingScan` instance.
            opts (:class:`~couchbase.options.ScanOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ScanOptions`

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If scan_type is not either a RangeScan or SamplingScan instance.
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

                async for res in scan_iter:
                    print(res)


        """  # noqa: E501
        req = self._impl.request_builder.build_range_scan_async_request(scan_type, *opts, **kwargs)
        return self._impl.range_scan(req)

    def binary(self) -> BinaryCollection:
        """Creates a BinaryCollection instance, allowing access to various binary operations
        possible against a collection.

        .. seealso::
            :class:`~acouchbase.binary_collection.BinaryCollection`

        Returns:
            :class:`~acouchbase.binary_collection.BinaryCollection`: A BinaryCollection instance.

        """
        return BinaryCollection(self._impl)

    def couchbase_list(self, key: str) -> CouchbaseList:
        """Returns a CouchbaseList permitting simple list storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseList`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseList`: A CouchbaseList instance.

        """
        return CouchbaseList(key, self._impl)

    def couchbase_map(self, key: str) -> CouchbaseMap:
        """Returns a CouchbaseMap permitting simple map storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseMap`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseMap`: A CouchbaseMap instance.

        """
        return CouchbaseMap(key, self._impl)

    def couchbase_set(self, key: str) -> CouchbaseSet:
        """Returns a CouchbaseSet permitting simple map storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseSet`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseSet`: A CouchbaseSet instance.

        """
        return CouchbaseSet(key, self._impl)

    def couchbase_queue(self, key: str) -> CouchbaseQueue:
        """Returns a CouchbaseQueue permitting simple map storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseQueue`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseQueue`: A CouchbaseQueue instance.

        """
        return CouchbaseQueue(key, self._impl)

    def query_indexes(self) -> CollectionQueryIndexManager:
        """
        Get a :class:`~acouchbase.management.queries.CollectionQueryIndexManager` which can be used to manage the query
        indexes of this cluster.

        Returns:
            :class:`~acouchbase.management.queries.CollectionQueryIndexManager`: A :class:`~acouchbase.management.queries.CollectionQueryIndexManager` instance.
        """  # noqa: E501
        return CollectionQueryIndexManager(self._impl.connection,
                                           self._impl.loop,
                                           self._impl.bucket_name,
                                           self._impl.scope.name,
                                           self.name)

    @staticmethod
    def default_name() -> str:
        return "_default"


Collection = AsyncCollection
