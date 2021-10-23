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
from typing import *
from couchbase_core._libcouchbase import PYCBC_TRACING
from basictracer import BasicTracer, SpanRecorder
from couchbase_core._version import __version__ as cb_version
from couchbase_core._pyport import basestring
from couchbase_core.mockserver import CouchbaseMock, BucketSpec, MockControlClient
from couchbase.management.admin import Admin

import re
import gc
import logging
import os
import platform
import sys
import time
import traceback
import types
import warnings
from abc import abstractmethod
from datetime import timedelta
from functools import wraps

from deepdiff import DeepDiff
from flaky import flaky
from testfixtures import LogCapture
from testresources import ResourcedTestCase as ResourcedTestCaseReal, TestResourceManager
from utilspie.collectionsutils import frozendict

import couchbase
import couchbase_core
from couchbase.cluster import AsyncCluster
from collections import defaultdict, namedtuple
from couchbase.bucket import Bucket as V3Bucket
from couchbase.cluster import Cluster, ClusterOptions, ClusterTracingOptions, \
    ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator, ClassicAuthenticator, Authenticator
from couchbase.collection import CBCollection
from couchbase.exceptions import CollectionAlreadyExistsException, ScopeAlreadyExistsException, NotSupportedException, \
    CouchbaseException
from couchbase.management.analytics import CreateDatasetOptions
from couchbase.management.collections import CollectionSpec
from couchbase_core.client import Client as CoreClient
from couchbase_core.connstr import ConnectionString
from couchbase.diagnostics import ServiceType, PingState

import pathlib

BASEDIR = pathlib.Path(__file__).parent.parent

try:
    from unittest2.case import SkipTest
except ImportError:
    from nose.exc import SkipTest

try:
    from configparser import ConfigParser
except ImportError:
    # Python <3.0 fallback
    from fallback import configparser


try:
    import unittest2 as unittest
except ImportError:
    import unittest


if os.environ.get("PYCBC_TRACE_GC") in ['FULL', 'STATS_LEAK_ONLY']:
    gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)

SLOWCONNECT_PATTERN = re.compile(r'.*centos.*')


class FlakyCounter(object):
    def __init__(self, max_runs, min_passes, **kwargs):
        self.count = 0
        self.kwargs = kwargs
        self.kwargs['rerun_filter'] = self.flaky_count
        self.kwargs['max_runs'] = max_runs
        self.kwargs['min_passes'] = min_passes

    def __call__(self, func):
        return flaky(**self.kwargs)(func)

    def flaky_count(self, err, name, test, plugin):
        self.count += 1
        print("trying test {}: {}/{}".format(name,
              self.count, self.kwargs['max_runs']))
        return True


loglevel = os.environ.get("PYCBC_DEBUG_LOG_LEVEL")
if loglevel:
    ch = logging.StreamHandler()
    ch.setLevel(logging.getLevelName(loglevel))
    formatter = logging.Formatter(
        '%(asctime)s : %(message)s : %(levelname)s -%(name)s', datefmt='%d%m%Y %I:%M:%S %p')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)


def version_to_tuple(version_str, default=None):
    return tuple(map(int, str.split(version_str, "."))
                 ) if version_str else default


PYCBC_SERVER_VERSION = version_to_tuple(os.environ.get("PYCBC_SERVER_VERSION"))


def sanitize_json(input, ignored_parts):
    # types (Any,Dict) -> Any
    if isinstance(input, list):
        to_be_sorted = list(sanitize_json(x, ignored_parts) for x in input)
        return tuple(sorted(to_be_sorted, key=lambda x: x.__hash__()))
    elif isinstance(input, basestring):
        return input.replace("'", '"')
    elif isinstance(input, float):
        return round(input, 5)
    elif isinstance(input, dict):
        result = {}
        for key, value in input.items():
            sub_ignored_parts = None
            if isinstance(ignored_parts, dict):
                sub_ignored_parts = ignored_parts.get(key)
            elif isinstance(ignored_parts, str) and ignored_parts == key:
                continue
            result[key] = sanitize_json(value, sub_ignored_parts or {})
        input = frozendict(**result)
    return input


class ResourcedTestCase(ResourcedTestCaseReal):
    class CaptureContext(LogCapture):
        def __init__(self, *args, **kwargs):
            self.records = []
            kwargs['attributes'] = (lambda r: self.records.append(r))
            super(ResourcedTestCase.CaptureContext,
                  self).__init__(*args, **kwargs)

        @property
        def output(self):
            return map(str, self.records)

    def __init__(self, *args, **kwargs):
        super(ResourcedTestCase, self).__init__(*args, **kwargs)
        self.maxDiff = None

    def deepDiffComparator(self, expected, actual):
        self.assertEqual({}, DeepDiff(expected, actual, ignore_order=True, significant_digits=5,
                                      ignore_numeric_type_changes=True, ignore_type_subclasses=True,
                                      ignore_string_type_changes=True))

    def assertSanitizedEqual(self, actual, expected,
                             ignored=None, comparator=None):
        comparator = comparator or self.assertEqual
        ignored = ignored or {}
        actual_json_sanitized = sanitize_json(actual, ignored)
        expected_json_sanitized = sanitize_json(expected, ignored)
        logging.warning(("\n"
                         "comparing {} and\n"
                         "{}\n"
                         "sanitized actual:{} and\n"
                         "sanitized expected:{}").format(actual, expected, actual_json_sanitized,
                                                         expected_json_sanitized))
        comparator(expected_json_sanitized, actual_json_sanitized)

    def assertLogs(self, *args, **kwargs):
        try:
            return super(ResourcedTestCase, self).assertLogs(*args, **kwargs)
        except Exception as e:
            logging.warn(e)

            return ResourcedTestCase.CaptureContext(*args, **kwargs)

    def run_and_collect_exceptions(self, command):
        try:
            command()
        except BaseException:
            if not hasattr(self, "exceptions"):
                self.exceptions = []
            self.exceptions.append(traceback.format_exc())

    def check_exceptions(self):
        exceptions = getattr(self, "exceptions", [])
        self.assertListEqual([], exceptions)


PYCBC_CB_VERSION = 'PYCBC/' + cb_version

CONFIG_FILE = 'tests.ini'  # in cwd
if not os.path.exists(CONFIG_FILE):
    CONFIG_FILE = BASEDIR.joinpath(CONFIG_FILE).__str__()
ClientType = TypeVar('ClientType', bound=CoreClient)

MockHackArgs = NamedTuple(
    'MockHackArgs', [('auth', Authenticator), ('kwargs', Mapping[str, Any])])


class ClusterInformation(object):
    def __init__(self):
        self.host = "localhost"
        self.port = 8091
        self.admin_username = "Administrator"
        self.admin_password = "password"
        self.bucket_name = "default"
        self.bucket_password = ""
        self.ipv6 = "disabled"
        self.protocol = "http"
        self.enable_tracing = "off"
        self.tracingparms = {}
        self.bucket_username = None

    @staticmethod
    def filter_opts(options):
        return {key: value for key, value in
                options.items() if
                key in ["certpath", "keypath", "ipv6", "config_cache", "compression", "log_redaction", "enable_tracing",
                        "network", "enable_mutation_tokens"]}

    def make_connargs(self, **overrides):
        bucket = self.bucket_name
        if 'bucket' in overrides:
            bucket = overrides.pop('bucket')
        host = overrides.pop('host', self.host)
        if self.protocol.startswith('couchbase'):
            protocol_format = '{0}/{1}'.format(host, bucket)
        elif self.protocol.startswith('http'):
            protocol_format = '{0}:{1}/{2}'.format(host, self.port, bucket)
        else:
            raise CouchbaseException('Unrecognised protocol')
        connstr = self.protocol + '://' + protocol_format
        final_options = ClusterInformation.filter_opts(self.__dict__)
        override_options = ClusterInformation.filter_opts(overrides)
        for k, v in override_options.items():
            overrides.pop(k)
            if v:
                final_options[k] = v
            elif k == 'enable_mutation_tokens':
                final_options[k] = "false" if v is False else "true"

        conn_options = '&'.join((key + "=" + value) for key,
                                value in filter(lambda tpl: tpl[1], final_options.items()))
        connstr += ("?" + conn_options) if conn_options else ""
        if 'init_tracer' in overrides.keys():
            overrides['tracer'] = overrides.pop("init_tracer")(
                PYCBC_CB_VERSION, **self.tracingparms)
        ret = {
            'password': self.bucket_password,
            'connection_string': connstr
        }

        if self.bucket_username:
            ret['password'] = self.bucket_username
        ret.update(overrides)
        return ret

    def make_connection(self,
                        conncls,  # type: Type[ClientType]
                        **kwargs):
        # type: (...) -> ClientType
        connargs = self.make_connargs(**kwargs)
        return conncls(**connargs)

    def mock_hack_options(self,  # type: ClusterInformation
                          is_mock  # type: bool
                          ):
        # type: (...) -> MockHackArgs
        # FIXME: hack because the Mock seems to want a bucket name for cluster connections
        # We should not be using classic here!  But, somewhere in the tests, we need
        # this for hitting the mock, it seems

        return MockHackArgs(ClassicAuthenticator, {
                            'bucket': self.bucket_name}) if is_mock else MockHackArgs(PasswordAuthenticator, {})

    def make_admin_connection(self, is_mock):

        return Admin(self.admin_username, self.admin_password,
                     self.host, self.port, ipv6=self.ipv6, **self.mock_hack_options(is_mock).kwargs)


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
        info.ipv6 = config.get('realserver', 'ipv6', fallback='disabled')
        info.certpath = config.get('realserver', 'certpath', fallback=None)
        info.keypath = config.get('realserver', 'keypath', fallback=None)
        info.protocol = config.get('realserver', 'protocol', fallback="http")
        info.enable_tracing = config.get(
            'realserver', 'tracing', fallback=None)
        info.tracingparms['port'] = config.get(
            'realserver', 'tracing_port', fallback=None)
        info.analytics_host = config.get(
            'analytics', 'host', fallback=info.host)
        info.analytics_port = config.get(
            'analytics', 'host', fallback=info.port)
        info.network = config.get('realserver', 'network', fallback=None)
        logging.info("info is " + str(info.__dict__))
        self.enable_tracing = info.enable_tracing
        if config.getboolean('realserver', 'enabled'):
            self.realserver_info = info
        else:
            self.realserver_info = None

        if (config.has_option("mock", "enabled") and
                config.getboolean('mock', 'enabled')):

            self.mock_enabled = True
            self.mockpath = str(BASEDIR.joinpath(config.get("mock", "path")))
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
        if config.mock_enabled:
            self._info = self.create_mock()
        else:
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

        return self.create_mock()

    def isDirty(self):
        return False

    def create_mock(self):
        bspec_dfl = BucketSpec('default', 'couchbase')
        mock = CouchbaseMock([bspec_dfl],
                             self._config.mockpath,
                             self._config.mockurl,
                             replicas=2,
                             nodes=4)

        try:
            mock.start()
        except BaseException:
            self._failed = True
            raise

        info = ClusterInformation()
        info.bucket_name = "default"
        info.port = mock.rest_port
        info.host = "127.0.0.1"
        info.admin_username = "Administrator"
        info.admin_password = "password"
        info.network = None
        info.mock = mock
        info.enable_tracing = self._config.enable_tracing
        self._info = info
        return info


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

    from couchbase_core.result import (
        ValueResult, OperationResult, ObserveInfo, Result)
    from couchbase_core._libcouchbase import MultiResult

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
    def cluster_info(self  # type: CouchbaseTestCase
                     ):
        # type: (...) -> ClusterInformation
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
            from couchbase_core.views.iterator import View
            from couchbase_core.result import (
                MultiResult, Result, OperationResult, ValueResult,
                ObserveInfo)
            self.factory = V3Bucket
            self.cls_Result = Result
            self.cls_MultiResult = MultiResult
            self.cls_OperationResult = OperationResult
            self.cls_ObserveInfo = ObserveInfo
            self.should_check_refcount = True
            warnings.warn('Using fallback (couchbase module) defaults')
            return

        self.should_check_refcount = False

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
                    raise ValueError(
                        "Version component cannot be larger than 99")
                hexstr += "{0:02}".format(int(comp))

            vernum = int(hexstr, 16)
        else:
            vernum = vstr
            components = []
            # Get the display
            for x in range(0, 3):
                comp = (vernum & 0xff << (x * 8)) >> x * 8
                comp = "{0:x}".format(comp)
                components = [comp] + components
            vstr = ".".join(components)

        rtstr, rtnum = self.factory.lcb_version()
        if rtnum < vernum:
            raise SkipTest(("Test requires {0} to run (have {1})")
                           .format(vstr, rtstr))

    def skipIfMock(self):
        if self.is_mock:
            raise SkipTest("Not be run against mock")

    def skipUnlessMock(self):
        if not self.is_mock:
            raise SkipTest("Not to be run against non-mock")

    def make_connargs(self, **overrides):
        return self.cluster_info.make_connargs(**overrides)

    def make_connection(self, **kwargs):
        return self.cluster_info.make_connection(self.factory, **kwargs)

    def make_admin_connection(self):
        return self.cluster_info.make_admin_connection(self.is_mock)

    def gen_key(self, prefix=None):
        if not prefix:
            prefix = "python-couchbase-key_"

        ret = "{0}{1}".format(prefix, self._key_counter)
        self._key_counter += 1
        return ret

    def gen_key_list(self, amount=5, prefix=None):
        ret = [self.gen_key(prefix) for x in range(amount)]
        return ret

    def gen_kv_dict(self, amount=5, prefix=None):
        ret = {}
        keys = self.gen_key_list(amount=amount, prefix=prefix)
        for k in keys:
            ret[k] = "Value_For_" + k
        return ret

    # noinspection PyCompatibility
    def assertRegex(self, text, expected_regex, *args, **kwargs):
        try:
            return super(CouchbaseTestCase, self).assertRegex(
                text, expected_regex, *args, **kwargs)
        except NameError:
            pass
        except AttributeError:
            pass

        return super(CouchbaseTestCase, self).assertRegexpMatches(
            *args, **kwargs)

    # noinspection PyCompatibility
    def assertRaisesRegex(self, expected_exception,
                          expected_regex, *args, **kwargs):
        try:
            return super(CouchbaseTestCase, self).assertRaisesRegex(
                expected_exception, expected_regex, *args, **kwargs)
        except NameError:
            pass
        except AttributeError:
            pass

        super(CouchbaseTestCase, self).assertRaisesRegexp(*args, **kwargs)

    @staticmethod
    def skip_fail_if_plat(plat_pat, message, expected_exc=(Exception,)):
        try:
            import distro
            os_id = distro.id()
        except ImportError:
            import platform
            os_id = platform.system()

        os_id = os_id.lower()

        def real_decorator(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                try:
                    fn(*args, **kwargs)
                except expected_exc:
                    if re.match(plat_pat, os_id):
                        raise SkipTest("{}: {}".format(
                            message, traceback.format_exc()))
                    raise
            return wrapper
        return real_decorator

    def gen_obj_graph(self, attrib, attrib_name, graphdir):
        import objgraph

        options = dict(refcounts=True, max_depth=3,
                       too_many=10, shortnames=False)
        import os
        final_graph_dir = BASEDIR.joinpath(graphdir)
        os.makedirs(final_graph_dir, exist_ok=True)
        objgraph.show_refs(attrib,
                           filename=os.path.join(final_graph_dir, '{}_{}_refs.dot'.format(self._testMethodName,
                                                                                          attrib_name)),
                           **options)
        objgraph.show_backrefs(attrib,
                               filename=os.path.join(final_graph_dir,
                                                     '{}_{}_backrefs.dot'.format(self._testMethodName,
                                                                                 attrib_name)),
                               **options)
        logging.info("got referrents {}".format(
            repr(gc.get_referents(attrib))))
        logging.info("got referrers {}".format(repr(gc.get_referrers(attrib))))


class ConnectionTestCaseBase(CouchbaseTestCase):
    def __init__(self, *args, **kwargs):
        self.cb = None
        super(ConnectionTestCaseBase, self).__init__(*args, **kwargs)

    def checkCbRefcount(self):
        if not self.should_check_refcount:
            return

        import gc
        if platform.python_implementation() == 'PyPy':
            return
        if os.environ.get("PYCBC_TRACE_GC") in ['FULL', 'GRAPH_ONLY']:
            import objgraph
            graphdir = os.path.join(os.getcwd(), "ref_graphs")
            try:
                os.makedirs(graphdir)
            except BaseException:
                pass

            for attrib_name in ["cb.tracer.parent", "cb"]:
                try:
                    logging.info("evaluating " + attrib_name)
                    attrib = eval("self." + attrib_name)
                    self.gen_obj_graph(attrib, attrib_name, graphdir)
                except BaseException:
                    pass
        gc.collect()
        for x in range(10):
            oldrc = sys.getrefcount(self.cb)
            if oldrc > 2:
                gc.collect()
            else:
                break
        # commented out for now as GC seems to be unstable
        # self.assertEqual(oldrc, 2)

    def setUp(self, **kwargs):
        super(ConnectionTestCaseBase, self).setUp()
        self.cb = self.make_connection(**kwargs)

    def sleep(self, duration):
        expected_end = time.time() + duration
        while True:
            remaining_time = expected_end - time.time()
            if remaining_time <= 0:
                break
            try:
                self.cb.get("dummy", ttl=remaining_time)
            except BaseException:
                pass

    def tearDown(self):
        super(ConnectionTestCaseBase, self).tearDown()
        if hasattr(self, '_implDtorHook'):
            self._implDtorHook()
        else:
            try:
                self.checkCbRefcount()
            finally:
                del self.cb


class LogRecorder(SpanRecorder):
    def record_span(self, span):
        if os.environ.get("PYCBC_LOG_RECORDED_SPANS"):
            logging.info("recording span: " + str(span.__dict__))


def basic_tracer():
    return BasicTracer(LogRecorder())


try:
    from opentracing_pyzipkin.tracer import Tracer
    import requests

    def http_transport(encoded_span):
        # The collector expects a thrift-encoded list of spans.
        import logging
        requests.post(
            'http://localhost:9411/api/v1/spans',
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'}
        )

    def jaeger_tracer(service, port=9414, **kwargs):
        port = 9411
        tracer = Tracer(PYCBC_CB_VERSION, 100, http_transport, port)
        logging.error(tracer)
        return tracer

except Exception as f:
    def jaeger_tracer(service, port=None):
        logging.error("No Jaeger import available")
        return basic_tracer()


ConnectionTestCase = ConnectionTestCaseBase


class RealServerTestCase(ConnectionTestCase):
    def setUp(self, **kwargs):
        super(RealServerTestCase, self).setUp(**kwargs)

        if not self._realserver_info:
            raise SkipTest("Need real server")

    @property
    def cluster_info(self):
        return self.realserver_info


# Class which sets up all the necessary Mock stuff
class MockTestCase(ConnectionTestCase):
    def setUp(self, **kwargs):
        super(MockTestCase, self).setUp(**kwargs)
        self.skipUnlessMock()
        self.mockclient = MockControlClient(self.mock.rest_port)

    def make_connection(self, **kwargs):
        return self.mock_info.make_connection(self.factory, **kwargs)

    @property
    def cluster_info(self):
        return self.mock_info


class SkipUnsupported(SkipTest):
    def __init__(self,
                 cause
                 ):
        super(SkipUnsupported, self).__init__(traceback.format_exc())


QueryParams = NamedTuple(
    'QueryParams', [('statement', str), ('rowcount', int)])


def check_gc(execute_gc=False):
    import gc
    if execute_gc:
        before = gc.get_count()
        gc.collect()
        print('Executed GC:  count before: {}, count after: {}'
              .format(before, gc.get_count()))
    else:
        print('GC count: {}'.format(gc.get_count()))


def check_fds():
    # TODO:  add fd logic for windows
    if sys.platform not in ['linux', 'darwin']:
        return

    from subprocess import Popen, PIPE, TimeoutExpired
    python_pid = os.getpid()

    # other commands useful for debugging
    # args = ["lsof", "-iTCP", "-sTCP:LISTEN", "-n"]
    # to do this though will take more hoops (or use shell=True ?)
    # lsof -a -p python_pid | wc -l
    args = ["lsof", "-a", "-p", str(python_pid)]

    fds = Popen(args, stdout=PIPE, stderr=PIPE)

    try:
        output, _ = fds.communicate(timeout=3)
        fd_output = output.decode().split('\n')
        print('found {} fds'.format(len(fd_output) - 1))

    except TimeoutExpired:
        fds.kill()
        fds.communicate()
        raise


class MockRestartException(Exception):
    pass


class CouchbaseClusterResourceException(Exception):
    pass


class CouchbaseClusterResource(object):

    def __init__(self, test_resources):
        """
        Create a CouchbaseClusterResource object.
        Responsible for handling cluster and bucket creation.

        PYCBC-1097
            Temporary fix to help improve the test suite

        :param test_resources: list of tuples representing the CouchbaseTestCase resources
        """
        self.info = None
        self.is_mock = False
        self.cluster = None
        self.bucket = None
        self.bucket_name = None
        self.cluster_version = None
        self.set_test_resources(test_resources)

    def set_test_resources(self,
                           test_resources  # type: List[Tuple]
                           ):
        # type: (...) -> None

        # hack to grab info built in the resource's make() method
        mock = test_resources[0][1]._info
        real = test_resources[1][1]._config.realserver_info
        if real:
            self.info = real
        else:
            self.info = mock
            self.is_mock = True

    def setup_cluster(self,
                      **kwargs  # type: Any
                      ):
        # type: (...) -> None
        if self.is_mock:
            # less patience with the mock
            bucket_name = self.try_n_times(3,
                                           3, self.init_cluster_and_bucket, **kwargs)
        else:
            bucket_name = self.try_n_times(10,
                                           3, self.init_cluster_and_bucket, **kwargs)
        self.bucket = self.try_n_times(10, 3, self.cluster.bucket, bucket_name)
        self.bucket_name = bucket_name
        self.try_n_times(20, 3, self.is_ready)
        self.set_cluster_version()

    def init_cluster_and_bucket(self,
                                **kwargs  # type: Any
                                ):
        # type: (...) -> str
        opts = kwargs.pop('cluster_options', None)
        transcoder = kwargs.pop("transcoder", None)
        connargs = self.info.make_connargs(**kwargs)
        connstr_abstract, bucket_name = self.get_connstr_and_bucket_name(
            [], connargs)
        self.cluster = self.instantiate_cluster(
            connstr_abstract, opts, transcoder=transcoder)
        return bucket_name

    def get_connstr_and_bucket_name(self,
                                    args,  # type: List[Any]
                                    kwargs  # type: Any
                                    ):
        # type: (...) -> Tuple
        connstr = args.pop(0) if args else kwargs.pop('connection_string')
        connstr_nobucket = ConnectionString.parse(connstr)
        bucket = connstr_nobucket.bucket
        connstr_nobucket.bucket = None
        return connstr_nobucket, bucket

    def instantiate_cluster(self,
                            connstr_nobucket,  # type: str
                            opts=None,  # type: Any
                            **kwargs      # type: Any
                            ):
        # type: (...) -> Cluster
        mock_hack = self.info.mock_hack_options(self.is_mock)
        auth = mock_hack.auth(self.info.admin_username,
                              self.info.admin_password)
        if not opts:
            opts = ClusterOptions(auth)
        else:
            opts['authenticator'] = auth
        transcoder = kwargs.pop("transcoder", None)
        if transcoder:
            opts["transcoder"] = transcoder
        if SLOWCONNECT_PATTERN.match(platform.platform()):
            default_timeout_options = ClusterTimeoutOptions(
                config_total_timeout=timedelta(seconds=30))
            default_timeout_options.update(opts.get('timeout_options', {}))
            opts['timeout_options'] = default_timeout_options

        return Cluster.connect(connection_string=str(connstr_nobucket),
                               options=opts, **mock_hack.kwargs)

    def disconnect_cluster(self) -> None:
        self.cluster.disconnect()

    def set_cluster_version(self, cluster=None) -> None:
        clstr = cluster or self.cluster
        pools = self.try_n_times(
            10, 3, clstr._admin.http_request, path='/pools')
        self.cluster_version = pools.value['implementationVersion']

    def is_ready(self) -> bool:
        if self.is_mock:
            return True
        # NOTE: ping is broken -- returns the Analytics in the Query.  So, for now, we
        # are _probably_ ok if we just make sure the 4 services are up.  Could be more
        # tricky if needed...
        service_types = [ServiceType.KeyValue, ServiceType.Search, ServiceType.Query,
                         # ServiceType.Analytics,
                         ServiceType.View]
        resp = self.bucket.ping()
        # first make sure all are there:
        if all(k in resp.endpoints.keys() for k in service_types):
            print("all services are present ({})".format(service_types))
            for service in service_types:
                if not any(
                        x for x in resp.endpoints[service] if x.state == PingState.OK):
                    raise Exception("{} isn't ready yet".format(service))
            return True
        raise Exception("not all services present in {}".format(resp))

    def try_n_times(self,  # type: ClusterTestCase
                    num_times,  # type: int
                    seconds_between,  # type: SupportsFloat
                    func,  # type: Callable
                    *args,  # type: Any
                    **kwargs  # type: Any
                    ):
        # type: (...) -> Any

        for _ in range(num_times):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # helpful to have this print statement when tests fail
                logging.info("Got exception, sleeping: {}".format(
                    traceback.format_exc()))
                time.sleep(seconds_between)

        if self.is_mock:

            try:
                self.restart_mock()
                return func(*args, **kwargs)
            except MockRestartException:
                raise
            except Exception:
                pass

        raise CouchbaseClusterResourceException(
            "unsuccessful {} after {} times, waiting {} seconds between calls".format(func, num_times, seconds_between))

    def restart_mock(self) -> None:
        try:
            print('\nR.I.P. mock...')
            self.info.mock.stop()
            time.sleep(3)
            self.info.mock.start()
            self.info.port = self.info.mock.rest_port
        except Exception as ex:
            import traceback
            traceback.print_exc()
            raise MockRestartException('Error trying to restart mock')


class ClusterTestCase(CouchbaseTestCase):
    _cluster_resource = None

    def __init__(self, *args, **kwargs):
        super(ClusterTestCase, self).__init__(*args, **kwargs)
        self.validator = ClusterTestCase.ItemValidator(self)
        self.dataset_name = 'test_beer_dataset'

    @property
    def cluster_factory(self  # type: ClusterTestCase
                        ):
        # type: (...) -> Type[Cluster]
        return Cluster

    class ItemValidator(object):
        def __init__(self, parent):
            self._parent = parent

        def assertDsValue(self, expected, item):
            self._parent.assertEqual(expected, item)

        def assertSuccess(self, item):
            pass

        def assertCas(self, item):
            pass

    def assertValue(self, expected, result):
        self.assertEqual(expected, result.content)

    def assertDsValue(self, expected, item):
        self.validator.assertDsValue(expected, item)

    def assertSuccess(self, item):
        self.validator.assertSuccess(item)

    def assertCas(self, item):
        self.validator.assertCas(item)

    def try_n_times_till_exception(self,  # type: ClusterTestCase
                                   num_times,  # type: int
                                   seconds_between,  # type: SupportsFloat
                                   func,  # type: Callable
                                   *args,  # type: Any
                                   # type: Tuple[Type[Exception],...]
                                   expected_exceptions=(Exception,),
                                   **kwargs  # type: Any
                                   ):
        # type: (...) -> Any
        for _ in range(num_times):
            try:
                func(*args, **kwargs)
                time.sleep(float(seconds_between))
            except expected_exceptions as e:
                # helpful to have this print statement when tests fail
                logging.info("Got one of expected exceptions {}, returning: {}".format(
                    expected_exceptions, e))
                return
            except Exception as e:
                logging.info("Got unexpected exception, raising: {}".format(e))
                raise

        self.fail(
            "successful {} after {} times waiting {} seconds between calls".format(func, num_times, seconds_between))

    @staticmethod
    def _passthrough(result, *_, **__):
        return result

    def _fail(self, message):
        self.fail(message)

    def _success(self):
        return True

    def checkResult(self, result, callback):
        return callback(result)

    def try_n_times(self,  # type: ClusterTestCase
                    num_times,  # type: int
                    seconds_between,  # type: SupportsFloat
                    func,  # type: Callable
                    *args,  # type: Any
                    on_success=None,  # type: Callable
                    # type: Tuple[Type[Exception], ...]
                    expected_exceptions=(Exception,),
                    **kwargs  # type: Any
                    ):
        # type: (...) -> Any
        on_success = on_success or self._passthrough
        for _ in range(num_times):
            try:
                ret = func(*args, **kwargs)
                return on_success(ret)
            except expected_exceptions as e:
                # helpful to have this print statement when tests fail
                logging.info("Got exception, sleeping: {}".format(
                    traceback.format_exc()))
                time.sleep(seconds_between)
        return self._fail(
            "unsuccessful {} after {} times, waiting {} seconds between calls".format(func, num_times, seconds_between))

    Triable = TypeVar('Triable', bound=Callable)

    def try_n_times_decorator(self,
                              func,  # type: ClusterTestCase.Triable
                              num_times,
                              seconds_between,
                              on_success=None
                              ):
        # type: (...) -> ClusterTestCase.Triable
        def wrapper(*args, **kwargs):
            success_func = kwargs.pop(
                'on_success', on_success) or self._passthrough
            return self.try_n_times(
                num_times, seconds_between, func, *args, on_success=success_func, **kwargs)

        return wrapper

    def factory(self, *args, **kwargs):
        return V3Bucket(*args, username="default", **
                        kwargs).default_collection()

    def setUp(self, **kwargs):
        super(ClusterTestCase, self).setUp()

        # if kwargs are passed in, reset conneciton w/ specied options
        if kwargs:
            if type(self)._cluster_resource:
                type(self)._cluster_resource.disconnect_cluster()
                type(self)._cluster_resource = None

            type(self)._cluster_resource = CouchbaseClusterResource(
                type(self).resources)
            type(self)._cluster_resource.setup_cluster(**kwargs)

        if not type(self)._cluster_resource:
            type(self)._cluster_resource = CouchbaseClusterResource(
                type(self).resources)
            type(self)._cluster_resource.setup_cluster()

        self.cluster = type(self)._cluster_resource.cluster
        self.bucket = type(self)._cluster_resource.bucket
        self.bucket_name = type(self)._cluster_resource.bucket_name
        self.cluster_version = type(self)._cluster_resource.cluster_version
        self.query_props = QueryParams('SELECT mockrow', 1) if self.is_mock else \
            QueryParams("SELECT * FROM `beer-sample` LIMIT 2",
                        2)  # type: QueryParams
        self.empty_query_props = QueryParams('SELECT emptyrow', 0) if self.is_mock else \
            QueryParams("SELECT * FROM `beer-sample` LIMIT 0", 0)

    @classmethod
    def setUpClass(cls,
                   **kwargs  # type: Any
                   ) -> None:
        # PYCBC-1097: this is a temporary hack to help stabilize builds
        #   a larger refactor needs to be done
        super(ClusterTestCase, cls).setUpClass()
        if cls._cluster_resource:
            return

        cls._cluster_resource = CouchbaseClusterResource(cls.resources)
        cls._cluster_resource.setup_cluster(**kwargs)

    @classmethod
    def tearDownClass(cls) -> None:
        # PYCBC-1097: this is a temporary hack to help stabilize builds
        #   a larger refactor needs to be done
        if cls._cluster_resource:
            cls._cluster_resource.disconnect_cluster()
            cls._cluster_resource = None

        # PYCBC-1097 - helper methods to track gc + fds
        # check_gc(execute_gc=False)
        # check_fds()

    def _get_connstr_and_bucket_name(self,
                                     args,  # type: List[Any]
                                     kwargs):
        connstr = args.pop(0) if args else kwargs.pop('connection_string')
        connstr_nobucket = ConnectionString.parse(connstr)
        bucket = connstr_nobucket.bucket
        connstr_nobucket.bucket = None
        return connstr_nobucket, bucket

    T = TypeVar('T', bound=Cluster)

    def _instantiate_cluster(self,
                             connstr_nobucket,  # type: str
                             cluster_class=None,  # type: Type[Cluster]
                             opts=None  # type: Any
                             ):
        # type: (...) -> ClusterTestCase.T
        cluster_class = cluster_class or self.cluster_factory
        mock_hack = self.cluster_info.mock_hack_options(self.is_mock)
        auth = mock_hack.auth(self.cluster_info.admin_username,
                              self.cluster_info.admin_password)
        if not opts:
            opts = ClusterOptions(auth)
        else:
            opts['authenticator'] = auth
        if SLOWCONNECT_PATTERN.match(platform.platform()):
            default_timeout_options = ClusterTimeoutOptions(
                config_total_timeout=timedelta(seconds=30))
            default_timeout_options.update(opts.get('timeout_options', {}))
            opts['timeout_options'] = default_timeout_options

        if not self.is_mock:
            return self.try_n_times(10, 3, cluster_class.connect,
                                    connection_string=str(connstr_nobucket),
                                    options=opts, **mock_hack.kwargs)

        return cluster_class.connect(connection_string=str(connstr_nobucket),
                                     options=opts, **mock_hack.kwargs)

    # NOTE: this really is only something you can trust in homogeneous clusters, but then again
    # this is a test suite.
    def get_cluster_version(self, full=False):
        return self.cluster_version if full is True else self.cluster_version.split(
            '-')[0]

    def get_bucket_info(self):
        return self.cluster._admin.bucket_info(self.bucket_name).value

    def supports_sync_durability(self):
        info = self.get_bucket_info()
        return "durableWrite" in info['bucketCapabilities']


def skip_if_no_collections(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if not self.supports_collections():
            raise SkipTest('collections not supported (server < 6.5?)')
        func(self, *args, **kwargs)
        return wrap


class CollectionTestCase(ClusterTestCase):
    _beer_sample_collections = None  # type: namedtuple
    coll = None  # type: CBCollection
    initialised = defaultdict(lambda: {})
    cb = None  # type: CBCollection

    def __init__(self, *args, **kwargs):
        super(CollectionTestCase, self).__init__(*args, **kwargs)

    # soon we should have a Cluster function that does this (DP _or_ 7.0,
    # etc...)
    def supports_collections(self):
        try:
            v = float(self.get_cluster_version()[0:3])
            if v >= 7.0:
                return True
        except ValueError:
            # lets assume it is the mock
            return False
        # if < 7, check for DP
        return self.cluster._is_dev_preview()

    def setUp(self, default_collections=None, real_collections=None, **kwargs):
        default_collections = default_collections or {None: {None: "coll"}}
        real_collections = real_collections or {
            "bedrock": {"flintstones": 'coll'}}
        # prepare:
        # 1) Connect to a Cluster
        super(CollectionTestCase, self).setUp(**kwargs)
        cm = self.bucket.collections()
        # check for collection support.  Return use default_collection
        # otherwise
        if self.supports_collections():
            my_collections = real_collections
        else:
            self.cb = self.bucket.default_collection()
            self.coll = self.bucket.default_collection()
            return

        for scope_name, collections in my_collections.items():
            CollectionTestCase._upsert_scope(cm, scope_name)
            scope = self.bucket.scope(
                scope_name) if scope_name else self.bucket
            for collection_name, dest in collections.items():
                CollectionTestCase._upsert_collection(
                    cm, collection_name, scope_name)
                # 2) Open a Collection
                coll = scope.collection(
                    collection_name) if collection_name else scope.default_collection()
                setattr(self, dest, coll)

        self.cb = self.coll  # type: CBCollection

        if type(self)._beer_sample_collections:
            self.beer_sample_collections = type(self)._beer_sample_collections

    @classmethod
    def setUpClass(cls,
                   setup_beer_sample_collections=None,  # type: bool
                   **kwargs  # type: Any
                   ) -> None:
        super(CollectionTestCase, cls).setUpClass(**kwargs)
        try:
            cls._cluster_resource.bucket.collections().get_all_scopes()
            if setup_beer_sample_collections:
                BeerSampleCollections = namedtuple('BeerSampleCollections', [
                    'scope', 'beers', 'breweries'])
                cls._beer_sample_collections = BeerSampleCollections('beer-sample-scope', CollectionSpec(
                    'beers', 'beer-sample-scope'), CollectionSpec('breweries', 'beer-sample-scope'))
        except NotSupportedException:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._beer_sample_collections:
            cm = cls._cluster_resource.bucket.collections()
            cm.drop_collection(cls._beer_sample_collections.beers)
            cm.drop_collection(cls._beer_sample_collections.breweries)
            cm.drop_scope(cls._beer_sample_collections.scope)

        super(CollectionTestCase, cls).tearDownClass()

    def create_beer_sample_collections(self):
        self.beers_fqdn = '`{}`.`{}`.{}'.format(
            self.bucket_name, self.beer_sample_collections.scope,
            self.beer_sample_collections.beers.name)

        self.breweries_fqdn = '`{}`.`{}`.{}'.format(
            self.bucket_name, self.beer_sample_collections.scope,
            self.beer_sample_collections.breweries.name)

        if self.beer_sample_collections_exist():
            # collections might exist, need to validate doc counts
            counts_good = False
            for i in range(2):
                if not self.check_beer_sample_collections_docs():
                    if i == 1:
                        break
                    self.populate_beer_sample_collections()
                else:
                    counts_good = True
                    break

            if not counts_good:
                raise Exception("Unable to populate beer sample collections")
            return

        self.cm.create_scope(self.beer_sample_collections.scope)
        self.try_n_times(10, 3, self.get_scope,
                         self.beer_sample_collections.scope,
                         collection_mgr=self.cm)
        self.cm.create_collection(self.beer_sample_collections.beers)
        self.try_n_times(10, 3, self.get_collection,
                         self.beer_sample_collections.beers.name,
                         self.beer_sample_collections.scope,
                         collection_mgr=self.cm)
        self.cm.create_collection(self.beer_sample_collections.breweries)
        self.try_n_times(10, 3, self.get_collection,
                         self.beer_sample_collections.breweries.name,
                         self.beer_sample_collections.scope,
                         collection_mgr=self.cm)

        self.try_n_times(5, 3, self.populate_beer_sample_collections)

        if not self.check_beer_sample_collections_docs():
            raise Exception("Unable to populate beer sample collections")

    def populate_beer_sample_collections(self):
        self.cluster.query(
            "CREATE PRIMARY INDEX ON {}".format(self.beers_fqdn)).execute()
        query_str = """
        INSERT INTO {} (KEY id, VALUE doc)
        SELECT META(b).id AS id,
            b AS doc
        FROM `beer-sample` b
        WHERE b.type='beer'
        """.format(self.beers_fqdn)
        self.cluster.query(query_str).execute()

        self.cluster.query(
            "CREATE PRIMARY INDEX ON {}".format(self.breweries_fqdn)).execute()
        query_str = """
        INSERT INTO {} (KEY id, VALUE doc)
        SELECT META(b).id AS id,
            b AS doc
        FROM `beer-sample` b
        WHERE b.type='brewery'
        """.format(self.breweries_fqdn)
        self.cluster.query(query_str).execute()

    def beer_sample_collections_exist(self):
        scope = self.try_n_times(10, 3, self.get_scope,
                                 self.beer_sample_collections.scope,
                                 collection_mgr=self.cm)
        beer_coll = self.try_n_times(10, 3, self.get_collection,
                                     self.beer_sample_collections.scope,
                                     self.beer_sample_collections.beers.name,
                                     collection_mgr=self.cm)
        brewery_coll = self.try_n_times(10, 3, self.get_collection,
                                        self.beer_sample_collections.scope,
                                        self.beer_sample_collections.breweries.name,
                                        collection_mgr=self.cm)
        return scope and beer_coll and brewery_coll

    def check_beer_sample_collections_docs(self):
        beers_query_str = 'SELECT COUNT(1) AS beers FROM {};'.format(
            self.beers_fqdn)
        breweries_query_str = 'SELECT COUNT(1) AS breweries FROM {};'.format(
            self.breweries_fqdn)
        counts_good = False
        for i in range(10):
            res = self.try_n_times(10, 10, self.cluster.query, beers_query_str)
            beers = res.rows()[0]['beers']
            res = self.try_n_times(
                10, 10, self.cluster.query, breweries_query_str)
            breweries = res.rows()[0]['breweries']

            if beers > 100 and breweries > 100:
                counts_good = True
                break

            # should have increased population by now, break to retry
            # population queries
            if beers == 0 and breweries == 0 and i == 5:
                break

            print('Found {} beers, {} breweries in collection, waiting a bit...'.format(
                beers, breweries))
            time.sleep(5)

        return counts_good

    def get_scope(self, scope_name, bucket_name=None, collection_mgr=None):
        if collection_mgr:
            return next((s for s in collection_mgr.get_all_scopes()
                        if s.name == scope_name), None)

        bucket = self.try_n_times(10, 3, self.cluster.bucket, bucket_name)
        if bucket:
            cm = bucket.collections()
            return next((s for s in cm.get_all_scopes()
                        if s.name == scope_name), None)

        return None

    def get_collection(self, scope_name, coll_name,
                       bucket_name=None, collection_mgr=None):
        if collection_mgr:
            scope = self.get_scope(scope_name, collection_mgr=collection_mgr)
        else:
            scope = self.get_scope(scope_name, bucket_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    @staticmethod
    def _upsert_collection(cm, collection_name, scope_name):
        if not collection_name in CollectionTestCase.initialised[scope_name].keys(
        ):
            try:
                cm.create_collection(CollectionSpec(
                    collection_name, scope_name))
                CollectionTestCase.initialised[scope_name][collection_name] = None
            except CollectionAlreadyExistsException as e:
                warnings.warn(e.message)

    @staticmethod
    def _upsert_scope(cm, scope_name):
        try:
            if scope_name and not scope_name in CollectionTestCase.initialised.keys():
                cm.create_scope(scope_name)
        except ScopeAlreadyExistsException as e:
            warnings.warn(e.message)
            pass


AsyncClusterType = TypeVar('AsyncClusterType', bound=AsyncCluster)


class AsyncClusterTestCase(ClusterTestCase):

    def gen_cluster(self,  # type: AsyncClusterTestCase
                    *args,
                    **kwargs):
        # type: (...) -> AsyncClusterType
        args = list(args)
        connstr_nobucket, bucket = self._get_connstr_and_bucket_name(
            args, kwargs)
        return self._instantiate_cluster(connstr_nobucket, self.cluster_class)

    def gen_bucket(self, *args, override_bucket=None, **kwargs):
        args = list(args)
        connstr_nobucket, bucket = self._get_connstr_and_bucket_name(
            args, kwargs)
        bucket = override_bucket or bucket
        return self._instantiate_cluster(
            connstr_nobucket, self.cluster_class).bucket(bucket)

    def gen_collection(self,
                       *args, **kwargs):
        bucket_result = self.gen_bucket(*args, **kwargs)
        return bucket_result.default_collection()

    @property
    @abstractmethod
    def cluster_class(self  # type: AsyncClusterTestCase
                      ):
        # type: (...) -> Type[AsyncClusterType]
        pass


class DDocTestCase(RealServerTestCase):
    pass


class ViewTestCase(ConnectionTestCase):
    pass


class TracedCase(CollectionTestCase):
    _tracer = None

    def init_tracer(self, service, **kwargs):
        if not TracedCase._tracer:
            if self.using_jaeger:
                TracedCase._tracer = jaeger_tracer(service, **kwargs)
                self.using_jaeger = True
        if not TracedCase._tracer:
            TracedCase._tracer = basic_tracer()
            self.using_jaeger = False
        return TracedCase._tracer

    @property
    def tracer(self):
        return TracedCase._tracer

    def setUp(self, trace_all=True, flushcount=0, enable_logging=False,
              use_parent_tracer=False, *args, **kwargs):
        self.timeout = None
        # self.enable_logging = enable_logging or os.environ.get("PYCBC_ENABLE_LOGGING")
        self.use_parent_tracer = use_parent_tracer
        self.using_jaeger = (os.environ.get("PYCBC_USE_JAEGER") == "TRUE")
        self.flushdict = {k: v for k, v in zip(
            map(str, range(1, 100)), map(str, range(1, 100)))}
        self.trace_all = os.environ.get("PYCBC_TRACE_ALL") or trace_all
        self.flushcount = flushcount
        if self.using_jaeger and self.flushcount > 5:
            raise SkipTest("too slow when using jaeger")
        enable_logging |= bool(self.trace_all)
        if enable_logging:
            couchbase.enable_logging()
        if self.use_parent_tracer:
            kwargs['init_tracer'] = self.init_tracer
        kwargs['enable_tracing'] = "true"
        if self.trace_all:
            tracing_options = ClusterTracingOptions(
                tracing_orphaned_queue_flush_interval=timedelta(
                    milliseconds=1),
                tracing_orphaned_queue_size=9,
                tracing_threshold_queue_flush_interval=timedelta(
                    milliseconds=1),
                tracing_threshold_queue_size=9,
                tracing_threshold_kv=timedelta(milliseconds=1),
                # tracing_threshold_query=timedelta(milliseconds=1),
                tracing_threshold_view=timedelta(milliseconds=1),
                tracing_threshold_search=timedelta(milliseconds=1),
                tracing_threshold_analytics=timedelta(milliseconds=1)
            )
            dummy_auth = PasswordAuthenticator("default", "password")
            # the dummy_auth isn't really used, the base class decides between classic
            # and password dependng on mock or not.
            opts = ClusterOptions(authenticator=dummy_auth,
                                  tracing_options=tracing_options)
            kwargs["cluster_options"] = opts
        super(TracedCase, self).setUp(**kwargs)

    def flush_tracer(self):
        try:
            for entry in range(1, self.flushcount):
                self.cb.upsert_multi(self.flushdict)
        except Exception as e:
            logging.warning(str(e))

    def tearDown(self):
        if self.trace_all and not self.using_jaeger:
            self.flush_tracer()
        super(TracedCase, self).tearDown()
        couchbase.disable_logging()
        if self.tracer and getattr(self.tracer, "close", None):
            try:
                # yield to IOLoop to flush the spans -
                # https://github.com/jaegertracing/jaeger-client-python/issues/50
                time.sleep(2)
                self.tracer.close()  # flush any buffered spans
            except BaseException:
                pass


class AnalyticsTestCaseBase(CollectionTestCase):
    def setUp(self, *args, **kwargs):
        super(AnalyticsTestCaseBase, self).setUp(*args, **kwargs)
        if self.is_mock:
            raise SkipTest("analytics not mocked")
        if int(self.get_cluster_version().split('.')[0]) < 6:
            raise SkipTest("no analytics in {}".format(
                self.get_cluster_version()))
        self.mgr = self.cluster.analytics_indexes()
        # create a dataset to query
        self.mgr.create_dataset(
            self.dataset_name, 'beer-sample', CreateDatasetOptions(ignore_if_exists=True))

        def has_dataset(name, *args, **kwargs):
            datasets = self.mgr.get_all_datasets()
            return [d for d in datasets if d.dataset_name == name][0]

        def on_dataset(*args, **kwargs):
            # connect it...
            return self.mgr.connect_link()

        self.try_n_times(10, 3, has_dataset, self.dataset_name,
                         on_success=on_dataset)


class CouchbaseClusterInfoException(Exception):
    pass


class CouchbaseClusterInfo(object):
    def __init__(self, cluster_resource, loop=None):
        self._cluster_resource = cluster_resource
        self._loop = loop
        self._cluster = None
        self._bucket_name = None
        self._bucket = None
        self._scope = None
        self._collection = None

    @property
    def loop(self):
        return self._loop

    @property
    def cluster_resource(self):
        return self._cluster_resource

    @property
    def cluster_version(self):
        return self._cluster_resource.cluster_version

    @property
    def cluster(self):
        return self._cluster

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def bucket(self):
        return self._bucket

    @property
    def scope(self):
        return self._scope

    @property
    def collection(self):
        return self._collection

    def set_cluster(self, cluster_class, **kwargs):
        transcoder = kwargs.pop("transcoder", None)
        conn_args = self.cluster_resource.info.make_connargs(**kwargs)
        connstr = conn_args.pop('connection_string')
        connstr_nobucket = ConnectionString.parse(connstr)
        mock_hack = self.cluster_resource.info.mock_hack_options(
            self.cluster_resource.is_mock)
        auth = mock_hack.auth(self.cluster_resource.info.admin_username,
                              self.cluster_resource.info.admin_password)

        self.set_bucket_name('beer-sample')
        connstr_nobucket.bucket = None
        if transcoder is not None:
            mock_hack.kwargs["transcoder"] = transcoder
        self._cluster = cluster_class(
            str(connstr_nobucket), ClusterOptions(auth), **mock_hack.kwargs)

    def set_bucket_name(self, bucket_name):
        self._bucket_name = bucket_name

    def set_bucket(self, bucket_name=None, loop=None):
        if bucket_name:
            self._bucket_name = bucket_name

        if not self.bucket_name:
            return

        self._bucket = self.cluster.bucket(self.bucket_name)

        evloop = self.loop or loop
        if evloop:
            evloop.run_until_complete(self.bucket.on_connect())

    def set_cluster_version(self):
        if self.cluster_resource and self.cluster:
            self.cluster_resource.set_cluster_version(self.cluster)

    def set_collection(self, use_scopes_and_colls=None):
        if not self.bucket:
            return

        if use_scopes_and_colls and self.supports_scopes_and_collections():
            self.create_test_scopes_and_colls()
            self._scope = self.bucket.scope('test-scope')
            self._collection = self.scope.collection('test-collection')
        else:
            self._collection = self.bucket.default_collection()

    def supports_scopes_and_collections(self):
        if not self.cluster:
            return False

        if not self.cluster_version:
            self.set_cluster_version()

        try:
            v = float(self.cluster_version[0:3])
            if v >= 7.0:
                return True
            else:
                return False
        except ValueError:
            # lets assume it is the mock
            return False

    def create_test_scopes_and_colls(self):
        cm = self.bucket.collections()
        CouchbaseClusterInfo._upsert_scope(cm, 'test-scope')
        CouchbaseClusterInfo._upsert_collection(
            cm, 'test-scope', 'test-collection')
        self.cluster_resource.try_n_times(
            3, 3, CouchbaseClusterInfo._get_collection, cm, 'test-scope', 'test-collection')

    def drop_test_scopes_and_colls(self):
        cm = self.bucket.collections()
        CouchbaseClusterInfo._drop_scope(cm, 'test-scope')

    @staticmethod
    def _upsert_collection(cm, scope_name, collection_name):
        try:
            if scope_name and collection_name:
                cm.create_collection(CollectionSpec(
                    collection_name, scope_name))
        except CollectionAlreadyExistsException:
            pass

    @staticmethod
    def _upsert_scope(cm, scope_name):
        try:
            if scope_name:
                cm.create_scope(scope_name)
        except ScopeAlreadyExistsException:
            pass

    @staticmethod
    def _get_scope(cm, scope_name):
        return next((s for s in cm.get_all_scopes()
                    if s.name == scope_name), None)

    @staticmethod
    def _get_collection(cm, scope_name, coll_name):

        scope = CouchbaseClusterInfo._get_scope(cm, scope_name)
        collection = next(
            (c for c in scope.collections if c.name == coll_name), None)

        return collection, scope

    @staticmethod
    def _drop_scope(cm, scope_name):
        try:
            if scope_name:
                cm.drop_scope(scope_name)
        except ScopeAlreadyExistsException:
            pass
