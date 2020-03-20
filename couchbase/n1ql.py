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


from couchbase_core.n1ql import N1QLRequest
from couchbase_core import iterable_wrapper
from typing import *


class QueryResult(iterable_wrapper(N1QLRequest)):
    def __init__(self,
                 *args, **kwargs
                 ):
        # type (...)->None
        super(QueryResult,self).__init__(*args, **kwargs)

    def metrics(self):  # type: (...) -> QueryMetrics
        return super(QueryResult, self).metrics

    def profile(self):
        return super(QueryResult, self).profile

    def request_id(self):
        raise NotImplementedError("To be implemented")

    def client_context_id(self):
        raise NotImplementedError("To be implemented")

    def signature(self):
        raise NotImplementedError("To be implemented")

    def warnings(self):
        raise NotImplementedError("To be implemented")


