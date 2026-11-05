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
    'TransactionKeyspace',
]

from typing import TYPE_CHECKING

from couchbase.common._supportability import Supportability
from couchbase.common._utils import fields_repr
from couchbase.common.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from acouchbase.collection import Collection as AsyncCollection
    from couchbase.collection import Collection


class TransactionKeyspace:
    """The bucket, scope and collection that transaction metadata is written to.

    Args:
        bucket (str): The name of the bucket.
        scope (str): The name of the scope.
        collection (str): The name of the collection.
        coll (:class:`~couchbase.collection.Collection`, optional): When given, the three names are read from it
            and the other arguments are ignored.

            .. deprecated:: 4.7.0
                Use :meth:`from_collection` instead.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If a name is missing and no
            collection was given.
    """

    def __init__(self,
                 bucket: str | None = None,
                 scope: str | None = None,
                 collection: str | None = None,
                 coll: Collection | AsyncCollection | None = None
                 ) -> None:
        if coll is not None:
            Supportability.option_deprecated('coll',
                                             use_instead='TransactionKeyspace.from_collection')
            bucket, scope, collection = self._names_of(coll)
        if bucket is None or scope is None or collection is None:
            missing = [name for name, value in (('bucket', bucket),
                                                ('scope', scope),
                                                ('collection', collection)) if value is None]
            raise InvalidArgumentException('A TransactionKeyspace needs {}.'
                                           .format(', '.join(missing)))
        self._bucket = bucket
        self._scope = scope
        self._collection = collection

    @classmethod
    def from_collection(cls, coll: Collection | AsyncCollection) -> TransactionKeyspace:
        """Name the bucket, scope and collection that a collection belongs to.

        Args:
            coll (:class:`~couchbase.collection.Collection`): The collection to name.

        Returns:
            :class:`~couchbase.transactions.TransactionKeyspace`
        """
        return cls(*cls._names_of(coll))

    @staticmethod
    def _names_of(coll: Collection | AsyncCollection) -> tuple[str, str, str]:
        return coll._impl.bucket_name, coll._impl.scope_name, coll.name

    @property
    def bucket(self) -> str:
        """The name of the bucket."""
        return self._bucket

    @property
    def scope(self) -> str:
        """The name of the scope."""
        return self._scope

    @property
    def collection(self) -> str:
        """The name of the collection."""
        return self._collection

    def __repr__(self) -> str:
        return fields_repr(self, bucket=self._bucket, scope=self._scope, collection=self._collection)
