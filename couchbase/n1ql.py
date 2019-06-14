#
# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
try:
    from abc import abstractmethod
except:
    import abstractmethod

try:
    StopAsyncIteration = StopAsyncIteration
except:
    StopAsyncIteration = StopIteration

from couchbase_core.n1ql import N1QLRequest


class IQueryResult(object):

    @abstractmethod
    def request_id(self):
        # type: (...) ->UUID
        pass

    @abstractmethod
    def client_context_id(self):
        # type: (...)->str
        pass

    @abstractmethod
    def signature(self):
        # type: (...)->Any
        pass

    @abstractmethod
    def rows(self):
        # type: (...)->List[T]
        pass

    @abstractmethod
    def warnings(self):
        # type: (...)->List[Warning]
        pass

    @abstractmethod
    def metrics(self):
        # type: (...)->QueryMetrics
        pass


class QueryResult(IQueryResult):
    def __init__(self,
                 parent  # type: N1QLRequest
                 ):
        # type (...)->None
        self.parent = parent
        self.buffered_rows=[]
        self.done = False

    def metadata(self):
        return self.parent.meta

    def rows(self):
        return list(x for x in self)

    def __iter__(self):
        for row in self.buffered_rows:
            yield row
        while not self.done:
            parent_iter=iter(self.parent)
            try:
                next_item = next(parent_iter)
                self.buffered_rows.append(next_item)
                yield next_item
            except (StopAsyncIteration, StopIteration):
                self.done=True
                break

    def metrics(self):  # type: (...)->QueryMetrics
        return self.parent.metrics

    def request_id(self):
        raise NotImplementedError("To be implemented")

    def client_context_id(self):
        raise NotImplementedError("To be implemented")

    def signature(self):
        raise NotImplementedError("To be implemented")

    def warnings(self):
        raise NotImplementedError("To be implemented")


