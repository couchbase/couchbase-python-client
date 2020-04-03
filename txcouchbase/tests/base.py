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
import twisted.internet.base
from couchbase.cluster import PasswordAuthenticator
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from couchbase_core.client import Client
from couchbase_tests.base import ConnectionTestCase

from typing import *

from txcouchbase.cluster import TxCluster
from couchbase.cluster import ClassicAuthenticator
from couchbase_core.connstr import ConnectionString
import twisted.python.util
import sys
import os


T = TypeVar('T', bound=ConnectionTestCase)
Factory = Callable[[Any], Client]
twisted.internet.base.DelayedCall.debug = True


if os.getenv("PYCBC_DEBUG_SPEWER"):
    # enable very detailed call logging
    sys.settrace(twisted.python.util.spewer)


def gen_base(basecls,  # type: Type[T]
             timeout=5,
             factory=None  # type: Factory
             ):
    # type: (...) -> Union[Type[_TxTestCase],Type[T]]
    class _TxTestCase(basecls, TestCase):
        def register_cleanup(self, obj):
            d = defer.Deferred()
            try:
                obj.registerDeferred('_dtor', d)
            except Exception as e:
                raise
            def cleanup(*args, **kwargs):
                return d, None
            self.addCleanup(cleanup)

            # Add another callback (invoked _outside_ of C) to ensure
            # the instance's destroy function is properly triggered
            if hasattr(obj, '_async_shutdown'):
                self.addCleanup(obj._async_shutdown)

        def make_connection(self,  # type: _TxTestCase
                            **kwargs):
            # type: (...) -> Factory
            ret = super(_TxTestCase, self).make_connection(**kwargs)
            self.register_cleanup(ret)
            return ret

        def checkCbRefcount(self):
            pass

        def gen_cluster(self,  # type: _TxTestCase
                        *args,
                        **kwargs):
            # type: (...) -> TxCluster
            args = list(args)
            connstr_nobucket, bucket = self._get_connstr_and_bucket_name(args, kwargs)
            return self._instantiate_txcluster(connstr_nobucket, **kwargs)

        def _instantiate_txcluster(self, connstr_nobucket, **kwargs):
            # it seems the mock requires ClassicAuthenticator to work (hence its use in the ClusterTestCase)
            # TODO: resolve this

            auth_type = ClassicAuthenticator if self.is_mock else PasswordAuthenticator
            mock_hack = {'bucket':self.cluster_info.bucket_name} if self.is_mock else {}
            return self.cluster_class(connection_string=str(connstr_nobucket),
                                        authenticator=auth_type(self.cluster_info.admin_username,
                                                     self.cluster_info.admin_password), **mock_hack)

        @property
        def cluster_class(self):
            return TxCluster

        def _get_connstr_and_bucket_name(self,
                                         args,  # type: List[Any]
                                         kwargs):
            connstr = args.pop(0) if args else kwargs.pop('connection_string')
            connstr_nobucket = ConnectionString.parse(connstr)
            bucket=connstr_nobucket.bucket
            connstr_nobucket.bucket = None
            return connstr_nobucket, bucket

        def gen_collection(self,
                           *args, **kwargs):
            bucket_result = self.gen_bucket(*args, **kwargs)
            return bucket_result.default_collection()

        def gen_bucket(self, *args, override_bucket=None, **kwargs):
            args = list(args)
            connstr_nobucket, bucket = self._get_connstr_and_bucket_name(args, kwargs)
            bucket = override_bucket or bucket
            return self._instantiate_txcluster(connstr_nobucket, **kwargs).bucket(bucket)

        @property
        def factory(self):
            return factory or self.gen_collection

        def setUp(self):
            super(_TxTestCase, self).setUp()
            self.cb = None

        def tearDown(self):
            super(_TxTestCase, self).tearDown()

        @classmethod
        def setUpClass(cls) -> None:
            import inspect
            if timeout:
                for name, method in inspect.getmembers(cls,inspect.isfunction):
                    try:
                        print("Setting {} timeout to 10 secs".format(name))
                        getattr(cls,name).timeout=timeout
                    except Exception as e:
                        print(e)

    return _TxTestCase
