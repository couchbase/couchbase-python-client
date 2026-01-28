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

from dataclasses import dataclass, fields
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Iterable,
                    Optional)

from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  QuotaLimitedException,
                                  RateLimitedException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.logic.operation_types import CollectionMgmtOperationType
from couchbase.logic.supportability import Supportability
from couchbase.management.logic.mgmt_req import MgmtRequest

if TYPE_CHECKING:
    from datetime import timedelta


class CreateCollectionSettings:
    """
        Specifies settings for a collection that will be created.
    """

    def __init__(self, max_expiry: Optional[timedelta] = None, history: Optional[bool] = None) -> None:
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> Optional[timedelta]:
        """
            Optional[timedelta]: The maximum expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def history(self) -> Optional[bool]:
        """
            Optional[bool]: Whether history retention override should be enabled in the collection. If not set, it will
            default to the bucket-level setting
        """
        return self._history


class UpdateCollectionSettings:
    """
        Specifies settings for a collection that will be updated.
    """

    def __init__(self, max_expiry: Optional[timedelta] = None, history: Optional[bool] = None) -> None:
        self._max_expiry = max_expiry
        self._history = history

    @property
    def max_expiry(self) -> Optional[timedelta]:
        """
            Optional[timedelta]: The maximum expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def history(self) -> Optional[bool]:
        """
            Optional[bool]: Whether history retention override should be enabled in the collection. If not set, it will
            default to the bucket-level setting
        """
        return self._history


class CollectionSpec:
    def __init__(self,
                 collection_name: str,
                 scope_name: Optional[str] = '_default',
                 max_expiry: Optional[timedelta] = None,
                 max_ttl: Optional[timedelta] = None,
                 history: Optional[bool] = None
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
    def max_expiry(self) -> Optional[timedelta]:
        """
            Optional[timedelta]: The expiry for documents in the collection.
        """
        return self._max_expiry

    @property
    def max_ttl(self) -> Optional[timedelta]:
        """
            Optional[timedelta]: The expiry for documents in the collection.

            **DEPRECATED** Use :attr:`max_expiry` instead.
        """
        Supportability.class_property_deprecated('max_ttl', 'max_expiry')
        return self._max_expiry

    @property
    def history(self) -> Optional[bool]:
        """
            Optional[bool]: Whether history retention is enabled in the collection. None represents the bucket-level
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
            List[:class:`.CollectionSpec`]: A list of the scope's collections.
        """
        return self._collections


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['mgmt_op', 'op_type', 'timeout', 'error_map']


@dataclass
class CollectionMgmtRequest(MgmtRequest):
    mgmt_op: str
    op_type: str
    # TODO: maybe timeout isn't optional, but defaults to default timeout?
    #       otherwise that makes inheritance tricky w/ child classes having required params

    def req_to_dict(self,
                    conn: Any,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        mgmt_kwargs = {
            'conn': conn,
            'mgmt_op': self.mgmt_op,
            'op_type': self.op_type,
        }
        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if self.timeout is not None:
            mgmt_kwargs['timeout'] = self.timeout

        mgmt_kwargs['op_args'] = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        }

        return mgmt_kwargs


@dataclass
class CreateCollectionRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    collection_name: str
    max_expiry: Optional[int] = None
    history: Optional[bool] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return CollectionMgmtOperationType.CreateCollection.value


@dataclass
class CreateScopeRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return CollectionMgmtOperationType.CreateScope.value


@dataclass
class DropCollectionRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    collection_name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return CollectionMgmtOperationType.DropCollection.value


@dataclass
class DropScopeRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return CollectionMgmtOperationType.DropScope.value


@dataclass
class GetAllScopesRequest(CollectionMgmtRequest):
    bucket_name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return CollectionMgmtOperationType.GetAllScopes.value


@dataclass
class UpdateCollectionRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    collection_name: str
    max_expiry: Optional[int] = None
    history: Optional[bool] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return CollectionMgmtOperationType.UpdateCollection.value


COLLECTION_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'.*Scope with.*name.*already exists': ScopeAlreadyExistsException,
    r'.*Scope with.*name.*not found': ScopeNotFoundException,
    r'.*Collection with.*name.*not found': CollectionNotFoundException,
    r'.*Collection with.*name.*already exists': CollectionAlreadyExistsException,
    r'.*collection_not_found.*': CollectionNotFoundException,
    r'.*scope_not_found.*': ScopeNotFoundException,
    r'.*Maximum number of collections has been reached for scope.*': QuotaLimitedException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
