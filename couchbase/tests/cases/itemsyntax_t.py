#
# Copyright 2013, Couchbase, Inc.
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

from time import sleep

from couchbase.exceptions import (ValueFormatError,
                                  ArgumentError, NotFoundError)
from couchbase.tests.base import ConnectionTestCase


class ConnectionItemSyntaxTest(ConnectionTestCase):

    def test_simple_accessors(self):
        cb = self.cb
        cb.quiet = True
        k = self.gen_key('__getitem__')

        del cb[k]
        cb[k] = "bar"
        self.assertEqual(cb[k].value, 'bar')

        del cb['blah']


if __name__ == '__main__':
    unittest.main()
