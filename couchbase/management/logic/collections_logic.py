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

from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional)

from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  QuotaLimitedException,
                                  RateLimitedException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.logic.supportability import Supportability
from couchbase.options import forward_args
from couchbase.pycbc_core import (collection_mgmt_operations,
                                  management_operation,
                                  mgmt_operations)

if TYPE_CHECKING:
    from couchbase.management.options import (CreateCollectionOptions,
                                              CreateScopeOptions,
                                              DropCollectionOptions,
                                              DropScopeOptions,
                                              GetAllScopesOptions)


class CollectionManagerLogic:

    _ERROR_MAPPING = {r'.*Scope with.*name.*already exists': ScopeAlreadyExistsException,
                      r'.*Scope with.*name.*not found': ScopeNotFoundException,
                      r'.*Collection with.*name.*not found': CollectionNotFoundException,
                      r'.*Collection with.*name.*already exists': CollectionAlreadyExistsException,
                      r'.*collection_not_found.*': CollectionNotFoundException,
                      r'.*scope_not_found.*': ScopeNotFoundException,
                      r'.*Maximum number of collections has been reached for scope.*': QuotaLimitedException,
                      r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException}

    def __init__(self, connection, bucket_name):
        self._connection = connection
        self._bucket_name = bucket_name

    def create_scope(self,
                     scope_name,      # type: str
                     *options,        # type: CreateScopeOptions
                     **kwargs         # type: Dict[str, Any]
                     ) -> None:

        op_args = {
            "bucket_name": self._bucket_name,
            "scope_name": scope_name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.COLLECTION.value,
            "op_type": collection_mgmt_operations.CREATE_SCOPE.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_scope(self,
                   scope_name,      # type: str
                   *options,        # type: DropScopeOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> None:
        op_args = {
            "bucket_name": self._bucket_name,
            "scope_name": scope_name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.COLLECTION.value,
            "op_type": collection_mgmt_operations.DROP_SCOPE.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_scopes(self,
                       *options,        # type: GetAllScopesOptions
                       **kwargs         # type: Dict[str, Any]
                       ) -> List[ScopeSpec]:
        op_args = {
            "bucket_name": self._bucket_name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.COLLECTION.value,
            "op_type": collection_mgmt_operations.GET_ALL_SCOPES.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def create_collection(self,
                          collection,     # type: CollectionSpec
                          *options,       # type: CreateCollectionOptions
                          **kwargs        # type: Dict[str, Any]
                          ) -> None:
        op_args = {
            "bucket_name": self._bucket_name,
            "scope_name": collection.scope_name,
            "collection_name": collection.name
        }

        if collection.max_ttl is not None:
            op_args["max_expiry"] = int(collection.max_ttl.total_seconds())

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.COLLECTION.value,
            "op_type": collection_mgmt_operations.CREATE_COLLECTION.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_collection(self,
                        collection,     # type: CollectionSpec
                        *options,       # type: DropCollectionOptions
                        **kwargs        # type: Dict[str, Any]
                        ) -> None:
        op_args = {
            "bucket_name": self._bucket_name,
            "scope_name": collection.scope_name,
            "collection_name": collection.name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.COLLECTION.value,
            "op_type": collection_mgmt_operations.DROP_COLLECTION.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)


class CollectionSpec(object):
    def __init__(self,
                 collection_name,           # type: str
                 scope_name='_default',     # type: Optional[str]
                 max_expiry=None,           # type: Optional[timedelta]
                 max_ttl=None,              # type: Optional[timedelta]
                 ):
        self._name, self._scope_name = collection_name, scope_name
        # in the event users utilize kwargs, set max_expiry if not set and max_ttl is set
        if max_ttl is not None:
            Supportability.class_property_deprecated('max_ttl', 'max_expiry')
            if max_expiry is None:
                max_expiry = max_ttl
        self._max_expiry = max_expiry

    @property
    def max_expiry(self) -> Optional[timedelta]:
        """
            Optional[timedelta]: The expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def max_ttl(self) -> Optional[timedelta]:
        """
            **DEPRECATED**
            Optional[timedelta]: The expiry for documents in the collection.
        """
        Supportability.class_property_deprecated('max_ttl', 'max_expiry')
        return self._max_expiry

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


class ScopeSpec(object):
    def __init__(self,
                 name,  # type : str
                 collections,  # type: Iterable[CollectionSpec]
                 ):
        self._name, self._collections = name, collections

    @property
    def name(self) -> str:
        """
            str: The name of the scope
        """
        return self._name

    @property
    def collections(self) -> Iterable[CollectionSpec]:
        """
            List[:class:`.CollectionSpec`]: A list of the scope's collections.
        """
        return self._collections
