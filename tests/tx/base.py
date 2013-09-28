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

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from txcouchbase.connection import Connection
from tests.base import ConnectionTestCase

def gen_base(basecls):
    class _TxTestCase(basecls, TestCase):
        def register_cleanup(self, obj):
            d = defer.Deferred()
            obj.registerDeferred('_dtor', d)
            self.addCleanup(lambda x: d, None)

        def make_connection(self, **kwargs):
            kwargs['conncls'] = Connection
            ret = super(_TxTestCase, self).make_connection(**kwargs)
            self.register_cleanup(ret)
            return ret

        def checkCbRefcount(self):
            pass

        def setUp(self):
            super(_TxTestCase, self).setUp()
            self.register_cleanup(self.cb)
            self.cb = None

        def tearDown(self):
            super(_TxTestCase, self).tearDown()

    return _TxTestCase
