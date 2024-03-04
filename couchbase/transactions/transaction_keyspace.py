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

from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from couchbase.logic.collection import CollectionLogic


class TransactionKeyspace:
    @overload
    def __init__(self,
                 coll=None,  # type: CollectionLogic
                 bucket=None,  # type: str
                 scope=None,  # type: str
                 collection=None  # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        self._bucket = None
        self._scope = None
        self._collection = None
        if 'coll' in kwargs:
            self._bucket = kwargs['coll']._scope._bucket.name
            self._scope = kwargs['coll']._scope.name
            self._collection = kwargs['coll'].name
        else:
            self._bucket = kwargs['bucket']
            self._scope = kwargs['scope']
            self._collection = kwargs['collection']

    @property
    def bucket(self):
        return self._bucket

    @property
    def scope(self):
        return self._scope

    @property
    def collection(self):
        return self._collection
