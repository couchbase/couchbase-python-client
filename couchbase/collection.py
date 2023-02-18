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

from copy import copy
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.binary_collection import BinaryCollection
from couchbase.datastructures import (CouchbaseList,
                                      CouchbaseMap,
                                      CouchbaseQueue,
                                      CouchbaseSet)
from couchbase.exceptions import (DocumentExistsException,
                                  ErrorMapper,
                                  InvalidArgumentException,
                                  PathExistsException,
                                  QueueEmpty)
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic import (BlockingWrapper,
                             decode_replicas,
                             decode_value)
from couchbase.logic.collection import CollectionLogic
from couchbase.logic.supportability import Supportability
from couchbase.management.queries import CollectionQueryIndexManager
from couchbase.options import (AppendMultiOptions,
                               DecrementMultiOptions,
                               ExistsMultiOptions,
                               GetAllReplicasMultiOptions,
                               GetAnyReplicaMultiOptions,
                               GetMultiOptions,
                               IncrementMultiOptions,
                               InsertMultiOptions,
                               LockMultiOptions,
                               PrependMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UnlockMultiOptions,
                               UpsertMultiOptions,
                               forward_args,
                               get_valid_multi_args)
from couchbase.pycbc_core import (binary_multi_operation,
                                  kv_multi_operation,
                                  operations)
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInResult,
                              MultiCounterResult,
                              MultiExistsResult,
                              MultiGetReplicaResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutateInResult,
                              MutationResult,
                              OperationResult)
from couchbase.subdocument import (array_addunique,
                                   array_append,
                                   array_prepend,
                                   count)
from couchbase.subdocument import get as subdoc_get
from couchbase.subdocument import remove as subdoc_remove
from couchbase.subdocument import replace
from couchbase.subdocument import upsert as subdoc_upsert
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   ExistsOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetAnyReplicaOptions,
                                   GetOptions,
                                   IncrementOptions,
                                   InsertOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   MutationMultiOptions,
                                   NoValueMultiOptions,
                                   PrependOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.result import MultiResultType
    from couchbase.subdocument import Spec


class Collection(CollectionLogic):

    def __init__(self, scope, name):
        super().__init__(scope, name)

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        """Retrieves the value of a document from the collection.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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

        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> GetResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get` instead.
        """
        return super().get(key, **kwargs)

    def get_any_replica(self,
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

        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_any_replica_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetReplicaResult)
    def _get_any_replica_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> GetReplicaResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get_any_replica` instead.
        """
        return super().get_any_replica(key, **kwargs)

    def get_all_replicas(self,
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

        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_all_replicas_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetReplicaResult)
    def _get_all_replicas_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Iterable[GetReplicaResult]:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get_all_replicas` instead.
        """
        return super().get_all_replicas(key, **kwargs)

    @BlockingWrapper.block(ExistsResult)
    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> ExistsResult:
        """Checks whether a specific document exists or not.

        Args:
            key (str): The key for the document to check existence.
            opts (:class:`~couchbase.options.ExistsOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().exists(key, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def insert(
        self,  # type: "Collection"
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """Inserts a new document to the collection, failing if the document already exists.

        Args:
            key (str): Document key to insert.
            value (JSONType): The value of the document to insert.
            opts (:class:`~couchbase.options.InsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().insert(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """Upserts a document to the collection. This operation succeeds whether or not the document already exists.

        Args:
            key (str): Document key to upsert.
            value (JSONType): The value of the document to upsert.
            opts (:class:`~couchbase.options.UpsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().upsert(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> MutationResult:
        """Replaces the value of an existing document. Failing if the document does not exist.

        Args:
            key (str): Document key to replace.
            value (JSONType): The value of the document to replace.
            opts (:class:`~couchbase.options.ReplaceOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().replace(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        """Removes an existing document. Failing if the document does not exist.

        Args:
            key (str): Key for the document to remove.
            opts (:class:`~couchbase.options.RemoveOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().remove(key, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Dict[str, Any]
              ) -> MutationResult:
        """Updates the expiry on an existing document.

        Args:
            key (str): Key for the document to touch.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().touch(key, expiry, *opts, **kwargs)

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> GetResult:
        """Retrieves the value of the document and simultanously updates the expiry time for the same document.

        Args:
            key (str): The key for the document retrieve and set expiry time.
            expiry (timedelta):  The new expiry to apply to the document.
            opts (:class:`~couchbase.options.GetAndTouchOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        # add to kwargs for conversion to int
        kwargs["expiry"] = expiry
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_touch_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_and_touch_internal(self,
                                key,  # type: str
                                **kwargs,  # type: Dict[str, Any]
                                ) -> GetResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get_and_touch` instead.

        """
        return super().get_and_touch(key, **kwargs)

    def get_and_lock(
        self,
        key,  # type: str
        lock_time,  # type: timedelta
        *opts,  # type: GetAndLockOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> GetResult:
        """Locks a document and retrieves the value of that document at the time it is locked.

        Args:
            key (str): The key for the document to lock and retrieve.
            lock_time (timedelta):  The amount of time to lock the document.
            opts (:class:`~couchbase.options.GetAndLockOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        # add to kwargs for conversion to int
        kwargs["lock_time"] = lock_time
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_lock_internal(key, **final_args)

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_and_lock_internal(self,
                               key,  # type: str
                               **kwargs,  # type: Dict[str, Any]
                               ) -> GetResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get_and_lock` instead.

        """
        return super().get_and_lock(key, **kwargs)

    @BlockingWrapper.block(None)
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> None:
        """Unlocks a previously locked document.

        Args:
            key (str): The key for the document to unlock.
            cas (int): The CAS of the document, used to validate lock ownership.
            opts (:class:`couchbaseoptions.UnlockOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        return super().unlock(key, cas, *opts, **kwargs)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Dict[str, Any]
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
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_internal(key, spec, **final_args)

    @BlockingWrapper.block_and_decode(LookupInResult)
    def _lookup_in_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.lookup_in` instead.

        """
        return super().lookup_in(key, spec, **kwargs)

    @BlockingWrapper.block(MutateInResult)
    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Dict[str, Any]
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
        return super().mutate_in(key, spec, *opts, **kwargs)

    def binary(self) -> BinaryCollection:
        """Creates a BinaryCollection instance, allowing access to various binary operations
        possible against a collection.

        .. seealso::
            :class:`~couchbase.binary_collection.BinaryCollection`

        Returns:
            :class:`~couchbase.binary_collection.BinaryCollection`: A BinaryCollection instance.

        """
        return BinaryCollection(self)

    @BlockingWrapper.block(MutationResult)
    def _append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`.BinaryCollection.append` instead.

        """
        return super().append(key, value, *opts, **kwargs)

    @BlockingWrapper.block(MutationResult)
    def _prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`.BinaryCollection.prepend` instead.

        """
        return super().prepend(key, value, *opts, **kwargs)

    @BlockingWrapper.block(CounterResult)
    def _increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> CounterResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`.BinaryCollection.increment` instead.

        """
        return super().increment(key, *opts, **kwargs)

    @BlockingWrapper.block(CounterResult)
    def _decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> CounterResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`.BinaryCollection.decrement` instead.

        """
        return super().decrement(key, *opts, **kwargs)

    def couchbase_list(self, key  # type: str
                       ) -> CouchbaseList:
        """Returns a CouchbaseList permitting simple list storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseList`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseList`: A CouchbaseList instance.

        """
        return CouchbaseList(key, self)

    @BlockingWrapper._dsop(create_type='list')
    def list_append(self, key,  # type: str
                    value,  # type: JSONType
                    create=False,  # type: Optional[bool]
                    **kwargs,  # type: Dict[str, Any]
                    ) -> OperationResult:
        """Add an item to the end of a list.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseList.append`
            instead.

        Args:
            key (str): The key for the list document.
            value (JSONType): The value to append to the list.
            create (bool, optional): Whether the list should be created if it does not exist.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`couchbase~.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        op = array_append('', value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop(create_type='list')
    def list_prepend(self, key,  # type: str
                     value,  # type: JSONType
                     create=False,  # type: Optional[bool]
                     **kwargs,  # type: Dict[str, Any]
                     ) -> OperationResult:
        """ Add an item to the beginning of a list.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseList.prepend`
            instead.

        Args:
            key (str): The key for the list document.
            value (JSONType): The value to prepend to the list.
            create (bool, optional): Whether the list should be created if it does not exist.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        op = array_prepend('', value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def list_set(self, key,  # type: str
                 index,  # type: int
                 value,  # type: JSONType
                 **kwargs  # type: Dict[str, Any]
                 ) -> OperationResult:
        """Sets an item within a list at a given position.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseList.set_at`
            instead.

        Args:
            key (str): The key for the list document.
            index (int): The position to replace.
            value (JSONType): The value to prepend to the list.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            IndexError: If the index is out of bounds.

        """

        op = replace(f'[{index}]', value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def list_get(self, key,  # type: str
                 index,  # type: int
                 **kwargs  # type: Dict[str, Any]
                 ) -> Any:
        """Get a specific element within a list.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseList.get_at`
            instead.

        Args:
            key (str): The key for the list document.
            index (int): The position to retrieve.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Any: The value of the element at the specified index.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            IndexError: If the index is out of bounds.

        """
        op = subdoc_get(f'[{index}]')
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    @BlockingWrapper._dsop()
    def list_remove(self, key,  # type: str
                    index,  # type: int
                    **kwargs  # type: Dict[str, Any]
                    ) -> OperationResult:
        """Remove the element at a specific index from a list.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseList.remove_at`
            instead.

        Args:
            key (str): The key for the list document.
            index (int): The position to remove.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
            IndexError: If the index is out of bounds.

        """

        op = subdoc_remove(f'[{index}]')
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def list_size(self, key,  # type: str
                  **kwargs  # type: Dict[str, Any]
                  ) -> int:
        """Returns the number of items in the list.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseList.size`
            instead.

        Args:
            key (str): The key for the list document.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            int: The number of items in the list.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """

        op = count('')
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    def couchbase_map(self, key  # type: str
                      ) -> CouchbaseMap:
        """Returns a CouchbaseMap permitting simple map storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseMap`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseMap`: A CouchbaseMap instance.

        """
        return CouchbaseMap(key, self)

    @BlockingWrapper._dsop(create_type='dict')
    def map_add(self,
                key,  # type: str
                mapkey,  # type: str
                value,  # type: Any
                create=False,  # type: Optional[bool]
                **kwargs  # type: Dict[str, Any]
                ) -> OperationResult:
        """Set a value for a key in a map.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseMap.add`
            instead.

        Args:
            key (str): The key for the map document.
            mapkey (str): The key in the map to set.
            value (Any): The value to use.
            create (bool, optional): Whether the map should be created if it does not exist.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        op = subdoc_upsert(mapkey, value)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def map_get(self,
                key,  # type: str
                mapkey,  # type: str
                **kwargs  # type: Dict[str, Any]
                ) -> Any:
        """Retrieve a value from a map.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseMap.get`
            instead.

        Args:
            key (str): The key for the map document.
            mapkey (str): The key in the map to set.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Any: The value of the specified key.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        op = subdoc_get(mapkey)
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    @BlockingWrapper._dsop()
    def map_remove(self,
                   key,  # type: str
                   mapkey,  # type: str
                   **kwargs  # type: Dict[str, Any]
                   ) -> OperationResult:
        """Remove an item from a map.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseMap.remove`
            instead.

        Args:
            key (str): The key for the map document.
            mapkey (str): The key in the map to set.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        op = subdoc_remove(mapkey)
        sd_res = self.mutate_in(key, (op,), **kwargs)
        return OperationResult(sd_res.cas, sd_res.mutation_token())

    @BlockingWrapper._dsop()
    def map_size(self,
                 key,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> int:
        """Get the number of items in the map.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseMap.remove`
            instead.

        Args:
            key (str): The key for the map document.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            int: The number of items in the map.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        """
        op = count('')
        sd_res = self.lookup_in(key, (op,), **kwargs)
        return sd_res.value[0].get("value", None)

    def couchbase_set(self, key  # type: str
                      ) -> CouchbaseSet:
        """Returns a CouchbaseSet permitting simple map storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseSet`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseSet`: A CouchbaseSet instance.

        """
        return CouchbaseSet(key, self)

    @BlockingWrapper._dsop(create_type='list')
    def set_add(self,
                key,            # type: str
                value,          # type: Any
                create=False,   # type: Optional[bool]
                **kwargs        # type: Dict[str, Any]
                ) -> Optional[OperationResult]:
        """Add an item to a set if the item does not yet exist.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseSet.add`
            instead.

        Args:
            key (str): The key for the set document.
            value (Any): The value to add to the set.
            create (bool, optional): Whether the set should be created if it does not exist.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        op = array_addunique('', value)
        try:
            sd_res = self.mutate_in(key, (op,), **kwargs)
            return OperationResult(sd_res.cas, sd_res.mutation_token())
        except PathExistsException:
            pass

    @BlockingWrapper._dsop()
    def set_remove(self,
                   key,        # type: str
                   value,      # type: Any
                   **kwargs    # type: Dict[str, Any]
                   ) -> Optional[OperationResult]:
        """Remove an item from a set.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseSet.remove`
            instead.

        Args:
            key (str): The key for the set document.
            value (Any): The value to remove from the set.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        while True:
            rv = self.get(key, **kwargs)
            try:
                ix = rv.value.index(value)
                kwargs['cas'] = rv.cas
                return self.list_remove(key, ix, **kwargs)
            except DocumentExistsException:
                pass
            except ValueError:
                return

    def set_size(self,
                 key,        # type: str
                 **kwargs    # type: Dict[str, Any]
                 ) -> int:
        """Get the length of a set.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseSet.size`
            instead.

        Args:
            key (str): The key for the set document.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            int: The length of a set.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self.list_size(key, **kwargs)

    def set_contains(self,
                     key,        # type: str
                     value,      # type: Any
                     **kwargs    # type: Dict[str, Any]
                     ) -> bool:
        """Determine if an item exists in a set

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseSet.contains`
            instead.

        Args:
            key (str): The key for the set document.
            value (Any):  The value to check for.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            bool: True if the set contains the specified value.  False othwerwise.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        rv = self.get(key, **kwargs)
        return value in rv.value

    def couchbase_queue(self, key  # type: str
                        ) -> CouchbaseQueue:
        """Returns a CouchbaseQueue permitting simple map storage in a document.

        .. seealso::
            :class:`~couchbase.datastructures.CouchbaseQueue`

        Returns:
            :class:`~couchbase.datastructures.CouchbaseQueue`: A CouchbaseQueue instance.

        """
        return CouchbaseQueue(key, self)

    @BlockingWrapper._dsop(create_type='list')
    def queue_push(self,
                   key,            # type: str
                   value,          # type: Any
                   create=False,   # type: Optional[bool]
                   **kwargs        # type: Dict[str, Any]
                   ) -> OperationResult:
        """Add an item to the end of a queue.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseQueue.push`
            instead.

        Args:
            key (str): The key for the queue document.
            value (Any):  The value to add.
            create (bool, optional): Whether the queue should be created if it does not exist.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self.list_prepend(key, value, **kwargs)

    @BlockingWrapper._dsop()
    def queue_pop(self,
                  key,        # type: str
                  **kwargs    # type: Dict[str, Any]
                  ) -> OperationResult:
        """Remove and return the first item queue.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseQueue.pop`
            instead.

        Args:
            key (str): The key for the queue document.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.result.OperationResult`: An instance of :class:`~couchbase.result.OperationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        while True:
            try:
                itm = self.list_get(key, -1, **kwargs)
            except IndexError:
                raise QueueEmpty

            kwargs.update({k: v for k, v in getattr(
                itm, '__dict__', {}).items() if k in {'cas'}})
            try:
                self.list_remove(key, -1, **kwargs)
                return itm
            except DocumentExistsException:
                pass
            except IndexError:
                raise QueueEmpty

    @BlockingWrapper._dsop()
    def queue_size(self,
                   key     # type: str
                   ) -> int:
        """Get the length of a queue.

        .. warning::
            This method is deprecated and will be removed in a future version.  Use :meth:`.CouchbaseQueue.size`
            instead.

        Args:
            key (str): The key for the queue document.

        Returns:
            int: The length of the queue.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self.list_size(key)

    def _get_multi_mutation_transcoded_op_args(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: MutationMultiOptions
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool]:

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        per_key_args = final_args.pop('per_key_options', None)
        op_transcoder = final_args.pop('transcoder', self.default_transcoder)
        op_args = {}
        for key, value in keys_and_docs.items():
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                key_transcoder = per_key_args.pop('transcoder', op_transcoder)
                op_args[key].update(per_key_args[key])
                transcoded_value = key_transcoder.encode_value(value)
            else:
                transcoded_value = op_transcoder.encode_value(value)
            op_args[key]['value'] = transcoded_value

        if isinstance(opts_type, ReplaceMultiOptions):
            for k, v in op_args.items():
                expiry = v.get('expiry', None)
                preserve_expiry = v.get('preserve_expiry', False)
                if expiry and preserve_expiry is True:
                    raise InvalidArgumentException(
                        message=("The expiry and preserve_expiry options cannot "
                                 f"both be set for replace operations.  Multi-op key: {k}.")
                    )

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions

    def _get_multi_op_args(
        self,
        keys,  # type: List[str]
        *opts,  # type: NoValueMultiOptions
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool, Dict[str, Transcoder]]:
        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        op_transcoder = final_args.pop('transcoder', self.default_transcoder)
        per_key_args = final_args.pop('per_key_options', None)
        op_args = {}
        key_transcoders = {}
        for key in keys:
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                key_transcoder = per_key_args.pop('transcoder', op_transcoder)
                key_transcoders[key] = key_transcoder
                op_args[key].update(per_key_args[key])
            else:
                key_transcoders[key] = op_transcoder

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions, key_transcoders

    def get_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: GetMultiOptions
        **kwargs,  # type: Any
    ) -> MultiGetResult:
        """For each key in the provided list, retrieve the document associated with the key.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple get operations.
            opts (:class:`~couchbase.options.GetMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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
        op_args, return_exceptions, transcoders = self._get_multi_op_args(keys,
                                                                          *opts,
                                                                          opts_type=GetMultiOptions,
                                                                          **kwargs)
        op_type = operations.GET.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                continue
            value = v.raw_result.get('value', None)
            flags = v.raw_result.get('flags', None)
            tc = transcoders[k]
            v.raw_result['value'] = decode_value(tc, value, flags)

        return MultiGetResult(res, return_exceptions)

    def get_any_replica_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: GetAnyReplicaMultiOptions
        **kwargs,  # type: Any
    ) -> MultiGetReplicaResult:
        """For each key in the provided list, retrieve the document associated with the key from the collection
        leveraging both active and all available replicas returning the first available.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple get operations.
            opts (:class:`~couchbase.options.GetAnyReplicaMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAnyReplicaMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetReplicaResult`: An instance of
            :class:`~couchbase.result.MultiGetReplicaResult`.

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
        op_args, return_exceptions, transcoders = self._get_multi_op_args(keys,
                                                                          *opts,
                                                                          opts_type=GetAnyReplicaMultiOptions,
                                                                          **kwargs)
        op_type = operations.GET_ANY_REPLICA.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                continue
            value = v.raw_result.get('value', None)
            flags = v.raw_result.get('flags', None)
            tc = transcoders[k]
            v.raw_result['value'] = decode_value(tc, value, flags)

        return MultiGetReplicaResult(res, return_exceptions)

    def get_all_replicas_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: GetAllReplicasMultiOptions
        **kwargs,  # type: Any
    ) -> MultiGetReplicaResult:
        """For each key in the provided list, retrieve the document from the collection returning both
        active and all available replicas.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple get operations.
            opts (:class:`~couchbase.options.GetAllReplicasMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAllReplicasMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetReplicaResult`: An instance of
            :class:`~couchbase.result.MultiGetReplicaResult`.

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
        op_args, return_exceptions, transcoders = self._get_multi_op_args(keys,
                                                                          *opts,
                                                                          opts_type=GetAllReplicasMultiOptions,
                                                                          **kwargs)
        op_type = operations.GET_ALL_REPLICAS.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )

        # all the successful results will be streamed_results, so lets
        # pop those off the main result dict and re-add the key back
        # transformed into a List[GetReplicaResult]
        result_keys = []
        for k, v in res.raw_result.items():
            if k == 'all_okay' or isinstance(v, CouchbaseBaseException):
                continue
            result_keys.append(k)

        for k in result_keys:
            value = res.raw_result.pop(k)
            tc = transcoders[k]
            res.raw_result[k] = list(r for r in decode_replicas(tc, value, GetReplicaResult))

        return MultiGetReplicaResult(res, return_exceptions)

    def lock_multi(
        self,
        keys,  # type: List[str]
        lock_time,  # type: timedelta
        *opts,  # type: LockMultiOptions
        **kwargs,  # type: Any
    ) -> MultiGetResult:
        """For each key in the provided list, lock the document associated with the key.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple lock operations.
            lock_time (timedelta):  The amount of time to lock the documents.
            opts (:class:`~couchbase.options.LockMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LockMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiGetResult`: An instance of
            :class:`~couchbase.result.MultiGetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        """
        kwargs["lock_time"] = lock_time
        op_args, return_exceptions, transcoders = self._get_multi_op_args(keys,
                                                                          *opts,
                                                                          opts_type=LockMultiOptions,
                                                                          **kwargs)
        op_type = operations.GET_AND_LOCK.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                continue
            value = v.raw_result.get('value', None)
            flags = v.raw_result.get('flags', None)
            tc = transcoders[k]
            v.raw_result['value'] = decode_value(tc, value, flags)

        return MultiGetResult(res, return_exceptions)

    def exists_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: ExistsMultiOptions
        **kwargs,  # type: Any
    ) -> MultiExistsResult:
        """For each key in the provided list, check if the document associated with the key exists.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple exists operations.
            lock_time (timedelta):  The amount of time to lock the documents.
            opts (:class:`~couchbase.options.ExistsMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ExistsMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiExistsResult`: An instance of
            :class:`~couchbase.result.MultiExistsResult`.

        """
        op_args, return_exceptions, _ = self._get_multi_op_args(keys,
                                                                *opts,
                                                                opts_type=ExistsMultiOptions,
                                                                **kwargs)
        op_type = operations.EXISTS.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiExistsResult(res, return_exceptions)

    def insert_multi(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: InsertMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        """For each key, value pair in the provided dict, inserts a new document to the collection,
        failing if the document already exists.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys_and_docs (Dict[str, JSONType]): The keys and values/docs to use for the multiple insert operations.
            opts (:class:`~couchbase.options.InsertMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.InsertMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentExistsException`: If the key provided already exists on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        """
        op_args, return_exceptions = self._get_multi_mutation_transcoded_op_args(keys_and_docs,
                                                                                 *opts,
                                                                                 opts_type=InsertMultiOptions,
                                                                                 **kwargs)
        op_type = operations.INSERT.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def upsert_multi(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: UpsertMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        """For each key, value pair in the provided dict, upserts a document to the collection. This operation
        succeeds whether or not the document already exists.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys_and_docs (Dict[str, JSONType]): The keys and values/docs to use for the multiple upsert operations.
            opts (:class:`~couchbase.options.UpsertMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UpsertMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        """
        op_args, return_exceptions = self._get_multi_mutation_transcoded_op_args(keys_and_docs,
                                                                                 *opts,
                                                                                 opts_type=UpsertMultiOptions,
                                                                                 **kwargs)
        op_type = operations.UPSERT.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def replace_multi(
        self,
        keys_and_docs,  # type: Dict[str, JSONType]
        *opts,  # type: ReplaceMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        """For each key, value pair in the provided dict, replaces the value of a document in the collection.
        This operation fails if the document does not exist.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys_and_docs (Dict[str, JSONType]): The keys and values/docs to use for the multiple replace operations.
            opts (:class:`~couchbase.options.ReplaceMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ReplaceMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        """
        op_args, return_exceptions = self._get_multi_mutation_transcoded_op_args(keys_and_docs,
                                                                                 *opts,
                                                                                 opts_type=ReplaceMultiOptions,
                                                                                 **kwargs)
        op_type = operations.REPLACE.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def remove_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: RemoveMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        """For each key in the provided list, remove the existing document.  This operation fails
        if the document does not exist.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple remove operations.
            opts (:class:`~couchbase.options.RemoveMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.RemoveMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        """
        op_args, return_exceptions, _ = self._get_multi_op_args(keys,
                                                                *opts,
                                                                opts_type=RemoveMultiOptions,
                                                                **kwargs)
        op_type = operations.REMOVE.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def touch_multi(
        self,
        keys,  # type: List[str]
        expiry,  # type: timedelta
        *opts,  # type: TouchMultiOptions
        **kwargs,  # type: Any
    ) -> MultiMutationResult:
        """For each key in the provided list, update the expiry on an existing document. This operation fails
        if the document does not exist.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (List[str]): The keys to use for the multiple touch operations.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TouchMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        """
        kwargs['expiry'] = expiry
        op_args, return_exceptions, _ = self._get_multi_op_args(keys,
                                                                *opts,
                                                                opts_type=TouchMultiOptions,
                                                                **kwargs)
        op_type = operations.TOUCH.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def unlock_multi(  # noqa: C901
        self,
        keys,  # type: Union[MultiResultType, Dict[str, int]]
        *opts,  # type: UnlockMultiOptions
        **kwargs,  # type: Any
    ) -> Dict[str, Union[None, CouchbaseBaseException]]:
        """For each result in the provided :class:`~couchbase.result.MultiResultType` in the provided list,
        unlocks a previously locked document. This operation fails if the document does not exist.

        .. note::
            This method is part of an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Args:
            keys (Union[MultiResultType, Dict[str, int]]): The result from a previous multi operation.
            opts (:class:`~couchbase.options.UnlockMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UnlockMultiOptions`

        Returns:
            Dict[str, Union[None, CouchbaseBaseException]]: Either None if operation successful or an Exception
            if the operation was unsuccessful

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

            :class:`~couchbase.exceptions.DocumentLockedException`: If the provided cas is invalid and the
                return_exceptions options is False.  Otherwise the exception is returned as a match to the key,
                but is not raised.

        """
        op_keys_cas = {}
        if isinstance(keys, dict):
            if not all(map(lambda k: isinstance(k, str), keys.keys())):
                raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
            if not all(map(lambda v: isinstance(v, int), keys.values())):
                raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
            op_keys_cas = copy(keys)
        elif isinstance(keys, (MultiGetResult, MultiMutationResult)):
            for k, v in keys.results.items():
                op_keys_cas[k] = v.cas
        else:
            raise InvalidArgumentException(
                'keys type must be Union[MultiGetResult, MultiMutationResult, Dict[str, int].')

        op_args, return_exceptions, _ = self._get_multi_op_args(list(op_keys_cas.keys()),
                                                                *opts,
                                                                opts_type=UnlockMultiOptions,
                                                                **kwargs)

        for k, v in op_args.items():
            v['cas'] = op_keys_cas[k]

        op_type = operations.UNLOCK.value
        res = kv_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        output = {}
        for k, v in res.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, CouchbaseBaseException):
                if not return_exceptions:
                    raise ErrorMapper.build_exception(v)
                else:
                    output[k] = ErrorMapper.build_exception(v)
            else:
                output[k] = None

        return output

    def _get_multi_counter_op_args(
        self,
        keys,  # type: List[str]
        *opts,  # type: Union[IncrementMultiOptions, DecrementMultiOptions]
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool]:
        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)

        global_delta, global_initial = self._get_and_validate_delta_initial(final_args)
        final_args['delta'] = int(global_delta)
        final_args['initial'] = int(global_initial)

        per_key_args = final_args.pop('per_key_options', None)
        op_args = {}
        for key in keys:
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                # need to validate delta/initial if provided per key
                delta = per_key_args[key].get('delta', None)
                initial = per_key_args[key].get('initial', None)
                self._validate_delta_initial(delta=delta, initial=initial)
                if delta:
                    per_key_args[key]['delta'] = int(delta)
                if initial:
                    per_key_args[key]['initial'] = int(initial)
                op_args[key].update(per_key_args[key])

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions

    def _get_multi_binary_mutation_op_args(
        self,
        keys_and_docs,  # type: Dict[str, Union[str, bytes, bytearray]]
        *opts,  # type: Union[AppendMultiOptions, PrependMultiOptions]
        **kwargs,  # type: Any
    ) -> Tuple[Dict[str, Any], bool]:

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        opts_type = kwargs.pop('opts_type', None)
        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        parsed_keys_and_docs = {}
        for k, v in keys_and_docs.items():
            if isinstance(v, str):
                value = v.encode("utf-8")
            elif isinstance(v, bytearray):
                value = bytes(v)
            else:
                value = v

            if not isinstance(value, bytes):
                raise ValueError(
                    "The value provided must of type str, bytes or bytearray.")

            parsed_keys_and_docs[k] = value

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        per_key_args = final_args.pop('per_key_options', None)
        op_args = {}
        for key, value in parsed_keys_and_docs.items():
            op_args[key] = copy(final_args)
            # per key args override global args
            if per_key_args and key in per_key_args:
                op_args[key].update(per_key_args[key])
            op_args[key]['value'] = value

        return_exceptions = final_args.pop('return_exceptions', True)
        return op_args, return_exceptions

    def _append_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: AppendMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_binary_mutation_op_args(keys_and_values,
                                                                             *opts,
                                                                             opts_type=AppendMultiOptions,
                                                                             **kwargs)
        op_type = operations.APPEND.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def _prepend_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: PrependMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        op_args, return_exceptions = self._get_multi_binary_mutation_op_args(keys_and_values,
                                                                             *opts,
                                                                             opts_type=PrependMultiOptions,
                                                                             **kwargs)
        op_type = operations.PREPEND.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiMutationResult(res, return_exceptions)

    def _increment_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: IncrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        op_args, return_exceptions = self._get_multi_counter_op_args(keys,
                                                                     *opts,
                                                                     opts_type=IncrementMultiOptions,
                                                                     **kwargs)
        op_type = operations.INCREMENT.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiCounterResult(res, return_exceptions)

    def _decrement_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: DecrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        op_args, return_exceptions = self._get_multi_counter_op_args(keys,
                                                                     *opts,
                                                                     opts_type=DecrementMultiOptions,
                                                                     **kwargs)
        op_type = operations.DECREMENT.value
        res = binary_multi_operation(
            **self._get_connection_args(),
            op_type=op_type,
            op_args=op_args
        )
        return MultiCounterResult(res, return_exceptions)

    def query_indexes(self) -> CollectionQueryIndexManager:
        """
        Get a :class:`~couchbase.management.queries.CollectionQueryIndexManager` which can be used to manage the query
        indexes of this cluster.

        Returns:
            :class:`~couchbase.management.queries.CollectionQueryIndexManager`: A :class:`~couchbase.management.queries.CollectionQueryIndexManager` instance.
        """  # noqa: E501
        return CollectionQueryIndexManager(self.connection, self._scope.bucket_name, self._scope.name, self.name)

    @staticmethod
    def default_name():
        return "_default"


"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.
    * All constrained int classes should be imported from `couchbase.options`.
    * Scope object should be imported from `couchbase.scope`.
    * Do not use the `CBCollection` class, use Collection instead.

"""

from couchbase.logic.options import AppendOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import DecrementOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import DeltaValueBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import DurabilityOptionBlockBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import ExistsOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import GetAllReplicasOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import GetAndLockOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import GetAndTouchOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import GetAnyReplicaOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import GetOptionsBase    # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import IncrementOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import InsertOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import LookupInOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import OptionsTimeoutBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import PrependOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import RemoveOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import ReplaceOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import TouchOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import UnlockOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.options import UpsertOptionsBase  # nopep8 # isort:skip # noqa: E402
from couchbase.logic.scope import ScopeLogic  # nopep8 # isort:skip # noqa: E402

from couchbase.options import ConstrainedInt  # nopep8 # isort:skip # noqa: E402, F401
from couchbase.options import SignedInt64  # nopep8 # isort:skip # noqa: E402, F401


@Supportability.import_deprecated('couchbase.collection', 'couchbase.scope')
class Scope(ScopeLogic):
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')   # noqa: F811
class AppendOptions(AppendOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')   # noqa: F811
class DecrementOptions(DecrementOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')   # noqa: F811
class DeltaValue(DeltaValueBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class DurabilityOptionBlock(DurabilityOptionBlockBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class ExistsOptions(ExistsOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class GetAllReplicasOptions(GetAllReplicasOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class GetAndTouchOptions(GetAndTouchOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class GetAndLockOptions(GetAndLockOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class GetAnyReplicaOptions(GetAnyReplicaOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class GetOptions(GetOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class IncrementOptions(IncrementOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class InsertOptions(InsertOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class LookupInOptions(LookupInOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class OptionsTimeout(OptionsTimeoutBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class PrependOptions(PrependOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class RemoveOptions(RemoveOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class ReplaceOptions(ReplaceOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class TouchOptions(TouchOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class UnlockOptions(UnlockOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.collection', 'couchbase.options')  # noqa: F811
class UpsertOptions(UpsertOptionsBase):  # noqa: F811
    pass


@Supportability.class_deprecated('couchbase.collection.Collection')
class CBCollection(Collection):
    pass
