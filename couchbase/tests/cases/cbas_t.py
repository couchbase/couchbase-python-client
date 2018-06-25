#
# Copyright 2018, Couchbase, Inc.
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

from __future__ import print_function

from couchbase.tests.base import MockTestCase


class CBASTest(MockTestCase):
    def test_importworks(self):
        try:
            #this is probably not even valid CBAS syntax, just enough to test that ImportError is gone
            row = self.cb._analytics_query('SELECT mockrow',self.cluster_info.host).get_single_result()
        except Exception as e:
            self.assertEquals(e.rc,0x13)
        except ImportError as e:
            raise e

