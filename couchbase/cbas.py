#
# Copyright 2017, Couchbase, Inc.
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

import couchbase.n1ql as N


class AnalyticsQuery(N.N1QLQuery):
    def __init__(self, querystr):
        querystr = querystr.rstrip()
        if not querystr.endswith(';'):
            querystr += ';'
        super(AnalyticsQuery, self).__init__(querystr)


class AnalyticsRequest(N.N1QLRequest):
    def __init__(self, params, host, parent):
        self._host = host
        super(AnalyticsRequest, self).__init__(params, parent)

    def _submit_query(self):
        return self._parent._cbas_query(self._params.encoded,
                                        self._host)