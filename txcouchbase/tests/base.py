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
import os
import sys
import traceback
from types import CodeType, FrameType
from typing import *

import twisted.internet.base
import twisted.python.util
from twisted.internet import defer
from twisted.trial._synctest import SkipTest
from twisted.trial.unittest import TestCase

from couchbase_core.client import Client
from couchbase_tests.base import ClusterTestCase, AsyncClusterTestCase
from txcouchbase.cluster import TxCluster

T = TypeVar('T', bound=AsyncClusterTestCase)
Factory = Callable[[Any], Client]
twisted.internet.base.DelayedCall.debug = True

import logging
import re
import inspect

import couchbase.exceptions


candidate_pattern=re.compile(r'.*(twisted|tx|couchbase).*')


def validate_frame(frame,   # type: FrameType
                   s):
    module = get_module_or_filename(frame)
    return (not module) or candidate_pattern.match(module)


def get_module_or_filename(frame):
    return inspect.getmodule(frame.f_code) or frame.f_code.co_filename


def magicmethod(clazz, method):
    if method not in clazz.__dict__:  # Not defined in clazz : inherited
        return 'inherited'
    elif hasattr(super(clazz), method):  # Present in parent : overloaded
        return 'overloaded'
    else:  # Not present in parent : newly defined
        return 'newly defined'


def logged_spewer(frame,  # type: FrameType
                  s,
                  ignored):
    """
    A trace function for sys.settrace that prints every function or method call.
    """
    from twisted.python import reflect
    try:
        code = frame.f_code
        co_name = code.co_name
        co_filename = code.co_filename
        module_or_filename=get_module_or_filename(frame)
        if not validate_frame(frame, s):
            return
        if 'self' in frame.f_locals:
            se = frame.f_locals['self']
            if hasattr(se, '__class__'):
                clazz = se.__class__
            else:
                clazz = type(se)
            k = reflect.qual(clazz)
            if magicmethod(clazz,co_name) in ('inherited',):
                return
            logging.info('method %s.%s at %s' % (
                k, co_name, id(se)))
        else:
            import inspect
            logging.info('function %s in module %s at %s, line %s' % (
                co_name,
                module_or_filename,
                co_filename,
                frame.f_lineno))
    except:
        pass


class _TxTestCase(TestCase):
    def make_connection(self,  # type: _TxTestCase
                        **kwargs):
        # type: (...) -> Factory
        ret = super(_TxTestCase, self).make_connection(**kwargs)
        self.register_cleanup(ret)
        return ret

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

    def checkCbRefcount(self):
        pass

    @property
    def cluster_class(self):
        return TxCluster

    def setUp(self):
        self.setUpTrace(self)
        super(_TxTestCase, self).setUp()
        self.cb = None

    def tearDown(self):
        try:
            super(_TxTestCase, self).tearDown()
        except Exception as e:
            raise
        finally:
            self.tearDownTrace(self)

    @classmethod
    def setUpClass(cls) -> None:
        import inspect
        cls.setUpTrace(cls)
        if cls.timeout:
            for name, method in inspect.getmembers(cls,inspect.isfunction):
                try:
                    logging.info("Setting {} timeout to {} secs".format(name, cls.timeout))
                    getattr(cls,name).timeout=cls.timeout
                except:
                    logging.error(traceback.format_exc())

    @staticmethod
    def setUpTrace(subject):
        if os.getenv("PYCBC_DEBUG_SPEWER", "").upper() == "TRUE":
            # enable very detailed call logging
            subject._oldtrace = sys.gettrace()
            sys.settrace(logged_spewer)

    @staticmethod
    def tearDownTrace(subject):
        oldtrace = getattr(subject, '_oldtrace', None)
        if oldtrace:
            sys.settrace(oldtrace)

    @classmethod
    def tearDownClass(cls):
        super(_TxTestCase, cls).tearDownClass()
        cls.tearDownTrace(cls)


def gen_base(basecls,  # type: Type[AsyncClusterTestCase]
             timeout=5,
             factory=None  # type: Factory
             ):
    # type: (...) -> Type[AsyncClusterTestCase]
    class WrappedTest(_TxTestCase, basecls):
        @property
        @classmethod
        def timeout(cls):
            return timeout

        @property
        def factory(self):
            return factory or self.gen_collection

    return WrappedTest


def skip_PYCBC_894(func):
    def wrapped_func(self,  # type: TestCase
                     *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except couchbase.exceptions.TimeoutException:
            raise SkipTest("Fails on MacOS - to be fixed: https://issues.couchbase.com/browse/PYCBC-894")
    if sys.platform in ['darwin'] and not os.environ.get("PYCBC_894", None):
        return wrapped_func
    return func
