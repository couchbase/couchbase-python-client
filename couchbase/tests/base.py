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

from __future__ import absolute_import

import sys
import types
import platform
import warnings

try:
    from unittest.case import SkipTest
except ImportError:
    from nose.exc import SkipTest

try:
    from configparser import ConfigParser
except ImportError:
    # Python <3.0 fallback
    from ConfigParser import SafeConfigParser as ConfigParser

from testresources import ResourcedTestCase, TestResourceManager

from couchbase.admin import Admin
from couchbase.mockserver import CouchbaseMock, BucketSpec, MockControlClient
from couchbase.result import (
    MultiResult, ValueResult, OperationResult, ObserveInfo, Result)
from couchbase._pyport import basestring

CONFIG_FILE = 'tests.ini' # in cwd

class ClusterInformation(object):
    def __init__(self):
        self.host = "localhost"
        self.port = 8091
        self.admin_username = "Administrator"
        self.admin_password = "password"
        self.bucket_name = "default"
        self.bucket_password = ""

    def make_connargs(self, **overrides):
        bucket = self.bucket_name
        if 'bucket' in overrides:
            bucket = overrides.pop('bucket')
        connstr = 'http://{0}:{1}/{2}?'.format(self.host, self.port, bucket)

        if 'config_cache' in overrides:
            connstr += 'config_cache='
            connstr += str(overrides.pop('config_cache'))

        ret = {
            'password': self.bucket_password,
            'connection_string': connstr
        }
        ret.update(overrides)
        return ret

    def make_connection(self, conncls, **kwargs):
        return conncls(**self.make_connargs(**kwargs))

    def make_admin_connection(self):
        return Admin(self.admin_username, self.admin_password,
                     self.host, self.port)


class ConnectionConfiguration(object):
    def __init__(self, filename=CONFIG_FILE):
        self._fname = filename
        self.load()

    def load(self):
        config = ConfigParser()
        config.read(self._fname)

        info = ClusterInformation()
        info.host = config.get('realserver', 'host')
        info.port = config.getint('realserver', 'port')
        info.admin_username = config.get('realserver', 'admin_username')
        info.admin_password = config.get('realserver', 'admin_password')
        info.bucket_name = config.get('realserver', 'bucket_name')
        info.bucket_password = config.get('realserver', 'bucket_password')

        if config.getboolean('realserver', 'enabled'):
            self.realserver_info = info
        else:
            self.realserver_info = None

        if (config.has_option("mock", "enabled") and
                              config.getboolean('mock', 'enabled')):

            self.mock_enabled = True
            self.mockpath = config.get("mock", "path")
            if config.has_option("mock", "url"):
                self.mockurl = config.get("mock", "url")
            else:
                self.mockurl = None
        else:
            self.mock_enabled = False


class MockResourceManager(TestResourceManager):
    def __init__(self, config):
        super(MockResourceManager, self).__init__()
        self._config = config
        self._info = None
        self._failed = False

    def _reset(self, *args, **kw):
        pass

    def make(self, *args, **kw):
        if not self._config.mock_enabled:
            return None

        if self._info:
            return self._info

        if self._failed:
            raise Exception('Not invoking failed mock!')

        bspec_dfl = BucketSpec('default', 'couchbase')
        mock = CouchbaseMock([bspec_dfl],
                             self._config.mockpath,
                             self._config.mockurl,
                             replicas=2,
                             nodes=4)

        try:
            mock.start()
        except:
            self._failed = True
            raise

        info = ClusterInformation()
        info.bucket_name = "default"
        info.port = mock.rest_port
        info.host = "127.0.0.1"
        info.admin_username = "Administrator"
        info.admin_password = "password"
        info.mock = mock
        self._info = info
        return info

    def isDirty(self):
        return False


class RealServerResourceManager(TestResourceManager):
    def __init__(self, config):
        super(RealServerResourceManager, self).__init__()
        self._config = config

    def make(self, *args, **kw):
        return self._config.realserver_info

    def isDirty(self):
        return False


class ApiImplementationMixin(object):
    """
    This represents the interface which should be installed by an implementation
    of the API during load-time
    """
    @property
    def factory(self):
        """
        Return the main Connection class used for this implementation
        """
        raise NotImplementedError()

    @property
    def viewfactory(self):
        """
        Return the view subclass used for this implementation
        """
        raise NotImplementedError()

    @property
    def should_check_refcount(self):
        """
        Return whether the instance's reference cound should be checked at
        destruction time
        """
        raise NotImplementedError()

    cls_MultiResult = MultiResult
    cls_ValueResult = ValueResult
    cls_OperationResult = OperationResult
    cls_ObserveInfo = ObserveInfo
    cls_Result = Result

GLOBAL_CONFIG = ConnectionConfiguration()


class CouchbaseTestCase(ResourcedTestCase):
    resources = [
        ('_mock_info', MockResourceManager(GLOBAL_CONFIG)),
        ('_realserver_info', RealServerResourceManager(GLOBAL_CONFIG))
    ]

    config = GLOBAL_CONFIG

    @property
    def cluster_info(self):
        for v in [self._realserver_info, self._mock_info]:
            if v:
                return v
        raise Exception("Neither mock nor realserver available")

    @property
    def is_realserver(self):
        return self.cluster_info is self._realserver_info

    @property
    def is_mock(self):
        return self.cluster_info is self._mock_info

    @property
    def realserver_info(self):
        if not self._realserver_info:
            raise SkipTest("Real server required")
        return self._realserver_info

    @property
    def mock(self):
        try:
            return self._mock_info.mock
        except AttributeError:
            return None

    @property
    def mock_info(self):
        if not self._mock_info:
            raise SkipTest("Mock server required")
        return self._mock_info

    def setUp(self):
        super(CouchbaseTestCase, self).setUp()

        if not hasattr(self, 'assertIsInstance'):
            def tmp(self, a, *bases):
                self.assertTrue(isinstance(a, bases))
            self.assertIsInstance = types.MethodType(tmp, self)
        if not hasattr(self, 'assertIsNone'):
            def tmp(self, a):
                self.assertTrue(a is None)
            self.assertIsNone = types.MethodType(tmp, self)

        self._key_counter = 0

        if not hasattr(self, 'factory'):
            from couchbase.bucket import Bucket
            from couchbase.views.iterator import View
            from couchbase.result import (
                MultiResult, Result, OperationResult, ValueResult,
                ObserveInfo)

            self.factory = Bucket
            self.viewfactory = View
            self.cls_Result = Result
            self.cls_MultiResult = MultiResult
            self.cls_OperationResult = OperationResult
            self.cls_ObserveInfo = ObserveInfo
            self.should_check_refcount = True
            warnings.warn('Using fallback (couchbase module) defaults')

    def skipLcbMin(self, vstr):
        """
        Test requires a libcouchbase version of at least vstr.
        This may be a hex number (e.g. 0x020007) or a string (e.g. "2.0.7")
        """
        if isinstance(vstr, basestring):
            components = vstr.split('.')
            hexstr = "0x"
            for comp in components:
                if len(comp) > 2:
                    raise ValueError("Version component cannot be larger than 99")
                hexstr += "{0:02}".format(int(comp))

            vernum = int(hexstr, 16)
        else:
            vernum = vstr
            components = []
            # Get the display
            for x in range(0, 3):
                comp = (vernum & 0xff << (x*8)) >> x*8
                comp = "{0:x}".format(comp)
                components = [comp] + components
            vstr = ".".join(components)

        rtstr, rtnum = self.factory.lcb_version()
        if rtnum < vernum:
            raise SkipTest(("Test requires {0} to run (have {1})")
                            .format(vstr, rtstr))

    def skipIfMock(self):
        pass

    def skipUnlessMock(self):
        pass

    def make_connargs(self, **overrides):
        return self.cluster_info.make_connargs(**overrides)

    def make_connection(self, **kwargs):
        return self.cluster_info.make_connection(self.factory, **kwargs)

    def make_admin_connection(self):
        return self.cluster_info.make_admin_connection()

    def gen_key(self, prefix=None):
        if not prefix:
            prefix = "python-couchbase-key_"

        ret = "{0}{1}".format(prefix, self._key_counter)
        self._key_counter += 1
        return ret

    def gen_key_list(self, amount=5, prefix=None):
        ret = [ self.gen_key(prefix) for x in range(amount) ]
        return ret

    def gen_kv_dict(self, amount=5, prefix=None):
        ret = {}
        keys = self.gen_key_list(amount=amount, prefix=prefix)
        for k in keys:
            ret[k] = "Value_For_" + k
        return ret


class ConnectionTestCase(CouchbaseTestCase):
    def checkCbRefcount(self):
        if not self.should_check_refcount:
            return

        import gc
        if platform.python_implementation() == 'PyPy':
            return

        gc.collect()
        for x in range(10):
            oldrc = sys.getrefcount(self.cb)
            if oldrc > 2:
                gc.collect()
            else:
                break

        self.assertEqual(oldrc, 2)

    def setUp(self):
        super(ConnectionTestCase, self).setUp()
        self.cb = self.make_connection()

    def tearDown(self):
        super(ConnectionTestCase, self).tearDown()
        if hasattr(self, '_implDtorHook'):
            self._implDtorHook()
        else:
            try:
                self.checkCbRefcount()
            finally:
                del self.cb


class RealServerTestCase(ConnectionTestCase):
    def setUp(self):
        super(RealServerTestCase, self).setUp()

        if not self._realserver_info:
            raise SkipTest("Need real server")

    @property
    def cluster_info(self):
        return self.realserver_info


# Class which sets up all the necessary Mock stuff
class MockTestCase(ConnectionTestCase):
    def setUp(self):
        super(MockTestCase, self).setUp()
        self.skipUnlessMock()
        self.mockclient = MockControlClient(self.mock.rest_port)

    def make_connection(self, **kwargs):
        return self.mock_info.make_connection(self.factory, **kwargs)

    @property
    def cluster_info(self):
        return self.mock_info


class DDocTestCase(ConnectionTestCase):
    pass


class ViewTestCase(ConnectionTestCase):
    pass
