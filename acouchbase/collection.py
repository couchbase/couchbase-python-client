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
                    Awaitable,
                    Dict,
                    Iterable,
                    Union)

from acouchbase.binary_collection import BinaryCollection
from acouchbase.datastructures import (CouchbaseList,
                                       CouchbaseMap,
                                       CouchbaseQueue,
                                       CouchbaseSet)
from acouchbase.kv_range_scan import AsyncRangeScanRequest
from acouchbase.logic import AsyncWrapper
from acouchbase.management.queries import CollectionQueryIndexManager
from couchbase.logic.collection import CollectionLogic
from couchbase.options import forward_args
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult,
                              ScanResultIterable)

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType
    from couchbase.kv_range_scan import ScanType
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   ExistsOptions,
                                   GetAllReplicasOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetAnyReplicaOptions,
                                   GetOptions,
                                   IncrementOptions,
                                   InsertOptions,
                                   LookupInAllReplicasOptions,
                                   LookupInAnyReplicaOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   PrependOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   ScanOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.subdocument import Spec


class AsyncCollection(CollectionLogic):

    def __init__(self, scope, name):
        super().__init__(scope, name)
        self._loop = scope.loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Any
            ) -> Awaitable[GetResult]:
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
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_internal(key, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Awaitable[GetResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.get` instead.
        """
        super().get(key, **kwargs)

    def get_any_replica(self,
                        key,  # type: str
                        *opts,  # type: GetAnyReplicaOptions
                        **kwargs,  # type: Dict[str, Any]
                        ) -> Awaitable[GetReplicaResult]:
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

        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_any_replica_internal(key, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(GetReplicaResult)
    def _get_any_replica_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Awaitable[GetReplicaResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.get_any_replica` instead.
        """
        super().get_any_replica(key, **kwargs)

    def get_all_replicas(self,
                         key,  # type: str
                         *opts,  # type: GetAllReplicasOptions
                         **kwargs,  # type: Dict[str, Any]
                         ) -> Awaitable[Iterable[GetReplicaResult]]:
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

        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_all_replicas_internal(key, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(GetReplicaResult)
    def _get_all_replicas_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Awaitable[Iterable[GetReplicaResult]]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.get_all_replicas` instead.
        """
        # return super().get_all_replicas(key, **kwargs)
        super().get_all_replicas(key, **kwargs)

    @AsyncWrapper.inject_callbacks(ExistsResult)
    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Any
    ) -> Awaitable[ExistsResult]:
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
        super().exists(key, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def insert(
        self,  # type: "Collection"
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
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
        super().insert(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
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
        super().upsert(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Any
                ) -> Awaitable[MutationResult]:
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
        super().replace(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Any
               ) -> Awaitable[MutationResult]:
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
        super().remove(key, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Any
              ) -> Awaitable[MutationResult]:
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
        super().touch(key, expiry, *opts, **kwargs)

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Any
                      ) -> Awaitable[GetResult]:
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
        # add to kwargs for conversion to int
        kwargs["expiry"] = expiry
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_touch_internal(key, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(GetResult)
    def _get_and_touch_internal(self,
                                key,  # type: str
                                **kwargs,  # type: Any
                                ) -> Awaitable[GetResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.get_and_touch` instead.
        """
        super().get_and_touch(key, **kwargs)

    def get_and_lock(
        self,
        key,  # type: str
        lock_time,  # type: timedelta
        *opts,  # type: GetAndLockOptions
        **kwargs,  # type: Any
    ) -> Awaitable[GetResult]:
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
        # add to kwargs for conversion to int
        kwargs["lock_time"] = lock_time
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_lock_internal(key, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(GetResult)
    def _get_and_lock_internal(self,
                               key,  # type: str
                               **kwargs,  # type: Any
                               ) -> Awaitable[GetResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.get_and_lock` instead.
        """
        super().get_and_lock(key, **kwargs)

    @AsyncWrapper.inject_callbacks(None)
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Any
               ) -> Awaitable[None]:
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
        super().unlock(key, cas, *opts, **kwargs)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Any
    ) -> Awaitable[LookupInResult]:
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
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_internal(key, spec, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(LookupInResult)
    def _lookup_in_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Any
    ) -> Awaitable[LookupInResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.lookup_in` instead.

        """
        super().lookup_in(key, spec, **kwargs)

    def lookup_in_any_replica(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAnyReplicaOptions
        **kwargs,  # type: Any
    ) -> Awaitable[LookupInReplicaResult]:
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
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_any_replica_internal(key, spec, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(LookupInReplicaResult)
    def _lookup_in_any_replica_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Any
    ) -> Awaitable[LookupInReplicaResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.lookup_in` instead.

        """
        super().lookup_in_any_replica(key, spec, **kwargs)

    def lookup_in_all_replicas(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAllReplicasOptions
        **kwargs,  # type: Any
    ) -> Awaitable[Iterable[LookupInReplicaResult]]:
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
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_all_replicas_internal(key, spec, **final_args)

    @AsyncWrapper.inject_callbacks_and_decode(LookupInReplicaResult)
    def _lookup_in_all_replicas_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Any
    ) -> Awaitable[Iterable[LookupInReplicaResult]]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`AsyncCollection.lookup_in_all_replicas` instead.

        """
        super().lookup_in_all_replicas(key, spec, **kwargs)

    @AsyncWrapper.inject_callbacks(MutateInResult)
    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutateInResult]:
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
        super().mutate_in(key, spec, *opts, **kwargs)

    def scan(self, scan_type,  # type: ScanType
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
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            final_args['transcoder'] = self.default_transcoder
        scan_args = super().build_scan_args(scan_type, **final_args)
        range_scan_request = AsyncRangeScanRequest(self.loop, **scan_args)
        return ScanResultIterable(range_scan_request)

    def binary(self) -> BinaryCollection:
        """Creates a BinaryCollection instance, allowing access to various binary operations
        possible against a collection.

        .. seealso::
            :class:`~acouchbase.binary_collection.BinaryCollection`

        Returns:
            :class:`~acouchbase.binary_collection.BinaryCollection`: A BinaryCollection instance.

        """
        return BinaryCollection(self)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def _append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`acouchbase.BinaryCollection.append` instead.

        """
        super().append(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(MutationResult)
    def _prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`acouchbase.BinaryCollection.prepend` instead.

        """
        super().prepend(key, value, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(CounterResult)
    def _increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> Awaitable[CounterResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`acouchbase.BinaryCollection.increment` instead.

        """
        super().increment(key, *opts, **kwargs)

    @AsyncWrapper.inject_callbacks(CounterResult)
    def _decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> Awaitable[CounterResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`acouchbase.BinaryCollection.decrement` instead.

        """
        super().decrement(key, *opts, **kwargs)

    def couchbase_list(self, key  # type: str
                       ) -> CouchbaseList:
        """Returns a CouchbaseList permitting simple list storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseList`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseList`: A CouchbaseList instance.

        """
        return CouchbaseList(key, self)

    def couchbase_map(self, key  # type: str
                      ) -> CouchbaseMap:
        """Returns a CouchbaseMap permitting simple map storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseMap`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseMap`: A CouchbaseMap instance.

        """
        return CouchbaseMap(key, self)

    def couchbase_set(self, key  # type: str
                      ) -> CouchbaseSet:
        """Returns a CouchbaseSet permitting simple map storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseSet`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseSet`: A CouchbaseSet instance.

        """
        return CouchbaseSet(key, self)

    def couchbase_queue(self, key  # type: str
                        ) -> CouchbaseQueue:
        """Returns a CouchbaseQueue permitting simple map storage in a document.

        .. seealso::
            :class:`~acouchbase.datastructures.CouchbaseQueue`

        Returns:
            :class:`~acouchbase.datastructures.CouchbaseQueue`: A CouchbaseQueue instance.

        """
        return CouchbaseQueue(key, self)

    def query_indexes(self) -> CollectionQueryIndexManager:
        """
        Get a :class:`~acouchbase.management.queries.CollectionQueryIndexManager` which can be used to manage the query
        indexes of this cluster.

        Returns:
            :class:`~acouchbase.management.queries.CollectionQueryIndexManager`: A :class:`~acouchbase.management.queries.CollectionQueryIndexManager` instance.
        """  # noqa: E501
        return CollectionQueryIndexManager(self.connection,
                                           self.loop,
                                           self._scope.bucket_name,
                                           self._scope.name,
                                           self.name)

    @staticmethod
    def default_name():
        return "_default"


Collection = AsyncCollection
