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

from couchbase.management.analytics import CreateDatasetOptions
from collections import defaultdict
from functools import wraps
from parameterized import parameterized_class
from testfixtures import LogCapture

from testresources import ResourcedTestCase as ResourcedTestCaseReal, TestResourceManager

from couchbase.exceptions import CollectionAlreadyExistsException, ScopeAlreadyExistsException, NotSupportedError
import couchbase_core
from couchbase import Cluster, ClusterOptions, CBCollection, JSONDocument, CoreClient
from couchbase.cluster import ClassicAuthenticator
from couchbase_core.connstr import ConnectionString
import couchbase_core._libcouchbase as _LCB

try:
    import unittest2 as unittest
except ImportError:
    import unittest
import logging
import gc
import os
import time
from basictracer import BasicTracer, SpanRecorder
import couchbase_core._libcouchbase
import traceback

from typing import *
from couchbase_v2.bucket import Bucket as V2Bucket
if os.environ.get("PYCBC_TRACE_GC") in ['FULL', 'STATS_LEAK_ONLY']:
    gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)

from utilspie.collectionsutils import frozendict
from couchbase.management.collections import CollectionSpec
from couchbase.bucket import Bucket as V3Bucket
from flaky import flaky
from deepdiff import DeepDiff


class FlakyCounter(object):
    def __init__(self, max_runs, min_passes, **kwargs):
        self.count=0
        self.kwargs=kwargs
        self.kwargs['rerun_filter']=self.flaky_count
        self.kwargs['max_runs']=max_runs
        self.kwargs['min_passes']=min_passes

    def __call__(self, func):
        return flaky(**self.kwargs)(func)
    def flaky_count(self, err, name, test, plugin):
        self.count+=1
        print("trying test {}: {}/{}".format(name,self.count,self.kwargs['max_runs']))
        return True

loglevel=os.environ.get("PYCBC_DEBUG_LOG_LEVEL")
if loglevel:
    ch = logging.StreamHandler()
    ch.setLevel(logging.getLevelName(loglevel))
    logging.getLogger().addHandler(ch)


def version_to_tuple(version_str, default=None):
    return tuple(map(int, str.split(version_str, "."))) if version_str else default


PYCBC_SERVER_VERSION = version_to_tuple(os.environ.get("PYCBC_SERVER_VERSION"))


def sanitize_json(input, ignored_parts):
    # types (Any,Dict) -> Any
    if isinstance(input, list):
        to_be_sorted=list(sanitize_json(x, ignored_parts) for x in input)
        return tuple(sorted(to_be_sorted, key=lambda x: x.__hash__() ))
    elif isinstance(input,basestring):
        return input.replace("'",'"')
    elif isinstance(input,float):
        return round(input,5)
    elif isinstance(input,dict):
        result ={}
        for key, value in input.items():
            sub_ignored_parts = None
            if isinstance(ignored_parts, dict):
                sub_ignored_parts=ignored_parts.get(key)
            elif isinstance(ignored_parts,str) and ignored_parts == key:
                continue
            result[key]=sanitize_json(value, sub_ignored_parts or {})
        input = frozendict(**result)
    return input


class ResourcedTestCase(ResourcedTestCaseReal):

    class CaptureContext(LogCapture):
        def __init__(self, *args, **kwargs):
            self.records = []
            kwargs['attributes'] = (lambda r: self.records.append(r))
            super(ResourcedTestCase.CaptureContext, self).__init__(*args, **kwargs)

        @property
        def output(self):
            return map(str, self.records)

    def __init__(self,*args,**kwargs):
        super(ResourcedTestCase,self).__init__(*args,**kwargs)
        self.maxDiff = None

    def deepDiffComparator(self, expected, actual):
        self.assertEqual({}, DeepDiff(expected, actual, ignore_order=True, significant_digits=5,
                                      ignore_numeric_type_changes=True, ignore_type_subclasses=True,
                                      ignore_string_type_changes=True))

    def assertSanitizedEqual(self, actual, expected, ignored=None, comparator=None):
        comparator = comparator or self.assertEqual
        ignored = ignored or {}
        actual_json_sanitized = sanitize_json(actual, ignored)
        expected_json_sanitized = sanitize_json(expected, ignored)
        logging.warning(("\n"
                       "comparing {} and\n"
                       "{}\n"
                       "sanitized actual:{} and\n"
                       "sanitized expected:{}").format(actual, expected, actual_json_sanitized, expected_json_sanitized))
        comparator(expected_json_sanitized, actual_json_sanitized)

    def assertLogs(self, *args, **kwargs):
        try:
            return super(ResourcedTestCase,self).assertLogs(*args, **kwargs)
        except Exception as e:
            logging.warn(e)

            return ResourcedTestCase.CaptureContext(*args, **kwargs)

    def run_and_collect_exceptions(self, command):
        try:
            command()
        except:
            if not hasattr(self, "exceptions"):
                self.exceptions=[]
            self.exceptions.append(traceback.format_exc())

    def check_exceptions(self):
        exceptions = getattr(self, "exceptions", [])
        self.assertListEqual([], exceptions)

try:
    from unittest2.case import SkipTest
except ImportError:
    from nose.exc import SkipTest

try:
    from configparser import ConfigParser
except ImportError:
    # Python <3.0 fallback
    from fallback import configparser

from couchbase_v2.exceptions import CouchbaseError
from couchbase.management.admin import Admin
from couchbase_core.mockserver import CouchbaseMock, BucketSpec, MockControlClient
from couchbase_core.result import (
    ValueResult, OperationResult, ObserveInfo, Result)
from couchbase_core._libcouchbase import MultiResult
from couchbase_core._pyport import basestring
from couchbase_core._version import __version__ as cb_version
PYCBC_CB_VERSION = 'PYCBC/' + cb_version

CONFIG_FILE = 'tests.ini' # in cwd

ClientType = TypeVar('ClientType', bound=CoreClient)


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
                options.items() if key in ["certpath", "keypath", "ipv6", "config_cache", "compression", "log_redaction", "enable_tracing", "network"]}

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
            raise CouchbaseError('Unrecognised protocol')
        connstr = self.protocol + '://' + protocol_format
        final_options = ClusterInformation.filter_opts(self.__dict__)
        override_options = ClusterInformation.filter_opts(overrides)
        for k, v in override_options.items():
            overrides.pop(k)
            if v:
                final_options[k] = v

        conn_options = '&'.join((key + "=" + value) for key, value in filter(lambda tpl: tpl[1],final_options.items()))
        connstr += ("?" + conn_options) if conn_options else ""
        if 'init_tracer' in overrides.keys():
            overrides['tracer']=overrides.pop("init_tracer")(PYCBC_CB_VERSION
                                                             , **self.tracingparms)
        ret = {
            'password': self.bucket_password,
            'connection_string': connstr
        }

        if self.bucket_username:
            ret['password']=self.bucket_username
        ret.update(overrides)
        return ret

    def make_connection(self,
                        conncls,  # type: Type[ClientType]
                        **kwargs):
        # type: (type, **Any) -> ClientType
        connargs = self.make_connargs(**kwargs)
        return conncls(**connargs)

    def make_admin_connection(self):
        return Admin(self.admin_username, self.admin_password,
                     self.host, self.port, ipv6=self.ipv6)


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
        info.enable_tracing = config.get('realserver', 'tracing', fallback=None)
        info.tracingparms['port'] = config.get('realserver', 'tracing_port', fallback=None)
        info.analytics_host = config.get('analytics','host',fallback=info.host)
        info.analytics_port = config.get('analytics','host',fallback=info.port)
        info.network = config.get('realserver','network',fallback=None)
        logging.info("info is "+str(info.__dict__))
        self.enable_tracing = info.enable_tracing
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
        info.network = None
        info.mock = mock
        info.enable_tracing = self._config.enable_tracing
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
        # type: (Any) -> ClusterInformation
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
            from couchbase_v2.views.iterator import View
            from couchbase_core.result import (
                MultiResult, Result, OperationResult, ValueResult,
                ObserveInfo)
            self.factory = V2Bucket
            self.viewfactory = View
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

    def assertRegex(self, *args, **kwargs):
        try:
            return super(CouchbaseTestCase,self).assertRegex(*args,**kwargs)
        except NameError:
            pass
        except AttributeError:
            pass

        return super(CouchbaseTestCase,self).assertRegexpMatches(*args,**kwargs)

    def assertRaisesRegex(self, *args, **kwargs):
        try:
            return super(CouchbaseTestCase,self).assertRaisesRegex(*args,**kwargs)
        except NameError:
            pass
        except AttributeError:
            pass

        super(CouchbaseTestCase,self).assertRaisesRegexp(*args,**kwargs)

class ConnectionTestCaseBase(CouchbaseTestCase):
    def __init__(self, *args, **kwargs):
        self.cb =None
        super(ConnectionTestCaseBase,self).__init__(*args,**kwargs)
    def checkCbRefcount(self):
        if not self.should_check_refcount:
            return

        import gc
        if platform.python_implementation() == 'PyPy':
            return
        if os.environ.get("PYCBC_TRACE_GC") in ['FULL','GRAPH_ONLY']:
            import objgraph
            graphdir=os.path.join(os.getcwd(),"ref_graphs")
            try:
                os.makedirs(graphdir)
            except:
                pass

            for attrib_name in ["cb.tracer.parent", "cb"]:
                try:
                    logging.info("evaluating "+attrib_name)
                    attrib = eval("self." + attrib_name)
                    options = dict(refcounts=True, max_depth=3, too_many=10, shortnames=False)
                    objgraph.show_refs(attrib,
                                       filename=os.path.join(graphdir, '{}_{}_refs.dot'.format(self._testMethodName,
                                                                                               attrib_name)),
                                       **options)
                    objgraph.show_backrefs(attrib,
                                           filename=os.path.join(graphdir,
                                                                 '{}_{}_backrefs.dot'.format(self._testMethodName,
                                                                                             attrib_name)),
                                           **options)
                    logging.info("got referrents {}".format(repr(gc.get_referents(attrib))))
                    logging.info("got referrers {}".format(repr(gc.get_referrers(attrib))))
                except:
                    pass
        gc.collect()
        for x in range(10):
            oldrc = sys.getrefcount(self.cb)
            if oldrc > 2:
                gc.collect()
            else:
                break
        # commented out for now as GC seems to be unstable
        #self.assertEqual(oldrc, 2)

    def setUp(self, **kwargs):
        # type: (**Any) -> None
        super(ConnectionTestCaseBase, self).setUp()
        self.cb = self.make_connection(**kwargs)

    def sleep(self, duration):
        expected_end=time.time()+duration
        while True:
            remaining_time=expected_end-time.time()
            if remaining_time<=0:
                break
            try:
                self.cb.get("dummy",ttl=remaining_time)
            except:
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
            logging.info("recording span: "+str(span.__dict__))


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

    def jaeger_tracer(service, port = 9414, **kwargs ):
        port = 9411
        tracer= Tracer(PYCBC_CB_VERSION, 100, http_transport, port )
        logging.error(tracer)
        return tracer

except Exception as e:
    def jaeger_tracer(service, port = None):
        logging.error("No Jaeger import available")
        return basic_tracer()


class TracedCase(ConnectionTestCaseBase):
    _tracer = None

    def init_tracer(self, service, **kwargs):
        if not TracedCase._tracer:
            if self.using_jaeger:
                TracedCase._tracer =  jaeger_tracer(service,**kwargs)
                self.using_jaeger = True
        if not TracedCase._tracer:
            TracedCase._tracer = basic_tracer()
            self.using_jaeger = False
        return TracedCase._tracer

    @property
    def tracer(self):
        return TracedCase._tracer

    def setUp(self, trace_all = True, flushcount = 0, enable_logging = False, use_parent_tracer = False, *args, **kwargs):
        self.enable_logging = enable_logging or os.environ.get("PYCBC_ENABLE_LOGGING")
        self.use_parent_tracer = use_parent_tracer
        self.using_jaeger =(os.environ.get("PYCBC_USE_JAEGER") == "TRUE")
        self.flushdict = {k: v for k, v in zip(map(str, range(1, 100)), map(str, range(1, 100)))}
        self.trace_all = os.environ.get("PYCBC_TRACE_ALL") or trace_all
        self.flushcount = flushcount
        if self.using_jaeger and self.flushcount>5:
            raise SkipTest("too slow when using jaeger")
        enable_logging |= bool(self.trace_all)
        if enable_logging:
            couchbase_core.enable_logging()
        if self.use_parent_tracer:
            kwargs['init_tracer'] =self.init_tracer
        kwargs['enable_tracing']="true"
        super(TracedCase, self).setUp(**kwargs)
        if self.trace_all:
            self.cb.tracing_orphaned_queue_flush_interval = 0.0001
            self.cb.tracing_orphaned_queue_size =10
            self.cb.tracing_threshold_queue_flush_interval = 0.00001
            self.cb.tracing_threshold_queue_size = 10
            self.cb.tracing_threshold_kv = 0.00001
            self.cb.tracing_threshold_n1ql= 0.00001
            self.cb.tracing_threshold_view =0.00001
            self.cb.tracing_threshold_fts =0.00001
            self.cb.tracing_threshold_analytics =0.00001

    def flush_tracer(self):
        try:
            for entry in range(1, self.flushcount):
                self.cb.upsert_multi(self.flushdict)
        except Exception as e:
            logging.warning(str(e))

    def tearDown(self):
        if self.trace_all and not self.using_jaeger:
            self.flush_tracer()
        super(TracedCase,self).tearDown()
        couchbase_core.disable_logging()
        if self.tracer and getattr(self.tracer,"close", None):
            try:
                time.sleep(2)   # yield to IOLoop to flush the spans - https://github.com/jaegertracing/jaeger-client-python/issues/50
                self.tracer.close()  # flush any buffered spans
            except:
                pass


ConnectionTestCase = ConnectionTestCaseBase
if os.environ.get("PYCBC_TRACE_ALL") and couchbase_core._libcouchbase.PYCBC_TRACING:
    ConnectionTestCase = TracedCase


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


class ClusterTestCase(CouchbaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ClusterTestCase, self).__init__(*args, **kwargs)
        self.validator = ClusterTestCase.ItemValidator(self)
        self.dataset_name = 'test_beer_dataset'

    @property
    def cluster_factory(self  # type: ClusterTestCase
                        ):
        # type: (...) -> Type[Cluster]
        return Cluster.connect

    class ItemValidator(object):
        def __init__(self, parent):
            self._parent = parent

        def assertDsValue(self, expected, item):
            self._parent.assertEquals(expected, item)

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

    def try_n_times_till_exception(self, num_times, seconds_between, func, *args, **kwargs):
        for _ in range(num_times):
            try:
                ret = func(*args, **kwargs)
                time.sleep(seconds_between)
            except Exception as e:
                # helpful to have this print statement when tests fail
                print("got exception {}, returning".format(e))
                return
        self.fail("successful {} after {} times waiting {} seconds between calls".format(func, num_times, seconds_between))

    @staticmethod
    def _passthru(result, *args, **kwargs):
        return result

    def _fail(self, message):
        self.fail(message)

    def _success(self):
        return True

    def checkResult(self, result, callback):
        return callback(result)

    def try_n_times(self, num_times, seconds_between, func, *args, on_success=None, **kwargs):
        on_success = on_success or self._passthru
        for _ in range(num_times):
            try:
                ret = func(*args, **kwargs)
                return on_success(ret)
            except Exception as e:
                # helpful to have this print statement when tests fail
                print("got exception {}, sleeping...".format(e))
                time.sleep(seconds_between)
        return self._fail("unsuccessful {} after {} times, waiting {} seconds between calls".format(func, num_times, seconds_between))

    def factory(self, *args, **kwargs):
        return V3Bucket(*args, username="default", **kwargs).default_collection()

    def setUp(self, **kwargs):
        super(ClusterTestCase, self).setUp()
        bucket_name = self.init_cluster_and_bucket()
        self.bucket = self.cluster.bucket(bucket_name)
        self.bucket_name = bucket_name

    def init_cluster_and_bucket(self):
        connargs = self.cluster_info.make_connargs()
        connstr_abstract = ConnectionString.parse(connargs.pop('connection_string'))
        bucket_name = connstr_abstract.bucket
        connstr_abstract.bucket = None
        connstr_abstract.set_option('enable_collections', 'true')
        # FIXME: we should not be using classic here!  But, somewhere in the tests, we need
        # this for hitting the mock, it seems
        from couchbase.cluster import PasswordAuthenticator
        auth_type = ClassicAuthenticator if self.is_mock else PasswordAuthenticator
        # hack because the Mock seems to want a bucket name for cluster connections, odd
        mock_hack = {'bucket': bucket_name} if self.is_mock else {}
        self.cluster = self.cluster_factory(connection_string=connstr_abstract, authenticator=
        auth_type(self.cluster_info.admin_username, self.cluster_info.admin_password), **mock_hack)
        return bucket_name

    # NOTE: this really is only something you can trust in homogeneous clusters, but then again
    # this is a test suite.
    def get_cluster_version(self):
        pools = self.cluster._admin.http_request(path='/pools').value
        return pools['implementationVersion'].split('-')[0]

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
    coll = None  # type: CBCollection
    initialised = defaultdict(lambda: {})
    cb = None  # type: CBCollection
    def __init__(self, *args, **kwargs):
        super(CollectionTestCase, self).__init__(*args, **kwargs)

    def supports_collections(self):
        cm = self.bucket.collections()
        try:
            cm.get_all_scopes()
            return True
        except NotSupportedError:
            return False

    def setUp(self, default_collections=None, real_collections=None):
        default_collections = default_collections or {None: {None: "coll"}}
        real_collections = real_collections or {"bedrock": {"flintstones": 'coll'}}
        # prepare:
        # 1) Connect to a Cluster
        super(CollectionTestCase, self).setUp()
        cm = self.bucket.collections()
        # check for collection support.  Return use default_collection otherwise
        if self.supports_collections():
            my_collections = real_collections
        else:
            self.cb = self.bucket.default_collection()
            self.coll = self.bucket.default_collection()
            return

        for scope_name, collections in my_collections.items():
            CollectionTestCase._upsert_scope(cm, scope_name)
            scope = self.bucket.scope(scope_name) if scope_name else self.bucket
            for collection_name, dest in collections.items():
                CollectionTestCase._upsert_collection(cm, collection_name, scope_name)
                # 2) Open a Collection
                coll = scope.collection(collection_name) if collection_name else scope.default_collection()
                setattr(self, dest, coll)

        self.cb = self.coll  # type: CBCollection

    @staticmethod
    def _upsert_collection(cm, collection_name, scope_name):
        if not collection_name in CollectionTestCase.initialised[scope_name].keys():
            try:
                cm.create_collection(CollectionSpec(collection_name, scope_name))
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


class DDocTestCase(RealServerTestCase):
    pass


class ViewTestCase(ConnectionTestCase):
    pass


class AnalyticsTestCaseBase(CollectionTestCase):
    def setUp(self, *args, **kwargs):
        super(AnalyticsTestCaseBase, self).setUp(*args, **kwargs)
        if self.is_mock:
            raise SkipTest("analytics not mocked")
        if int(self.get_cluster_version().split('.')[0]) < 6:
            raise SkipTest("no analytics in {}".format(self.get_cluster_version()))
        self.mgr = self.cluster.analytics_indexes()
        # create a dataset to query
        self.mgr.create_dataset(self.dataset_name, 'beer-sample', CreateDatasetOptions(ignore_if_exists=True))

        def has_dataset(name, *args, **kwargs):
            datasets = self.mgr.get_all_datasets()
            return [d for d in datasets if d.dataset_name == name][0]

        def on_dataset(*args, **kwargs):
            # connect it...
            return self.mgr.connect_link()

        self.try_n_times(10, 3, has_dataset, self.dataset_name, on_success=on_dataset)
