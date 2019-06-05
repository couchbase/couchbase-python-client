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

from couchbase_tests.base import CouchbaseTestCase
from couchbase_core.connstr import ConnectionString


class ConnStrTest(CouchbaseTestCase):

    def test_pathless_connstr(self):
        connstr = ConnectionString.parse('couchbase://localhost?opt1=val1&opt2=val2')
        self.assertTrue('opt1' in connstr.options)
        self.assertTrue('opt2' in connstr.options)

    def test_does_not_encode_slashes(self):
        connstr = ConnectionString.parse('couchbases://10.112.170.101?certpath=/var/rootcert.pem')
        self.assertTrue('certpath' in connstr.options)
        self.assertEqual('/var/rootcert.pem', connstr.options.get('certpath')[0])

        encoded = connstr.encode()
        self.assertEqual('couchbases://10.112.170.101?certpath=/var/rootcert.pem', encoded)
