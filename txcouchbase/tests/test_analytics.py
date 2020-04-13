from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.trial._synctest import SkipTest

import couchbase.tests_v3.cases.analytics_t
from couchbase.cluster import Cluster as SyncCluster
from txcouchbase.cluster import TxCluster, BatchedAnalyticsResult
from txcouchbase.tests.base import gen_base
import os
import logging
import twisted.internet.defer


class TxAnalyticsTest(gen_base(couchbase.tests_v3.cases.analytics_t.AnalyticsTestCase,timeout=20)):
    def setUp(self):
        if not os.getenv("PYCBC_ASYNC_ANALYTICS"):
            raise SkipTest("Async Analytics tests blocking as is")
        self._factory = SyncCluster
        super(TxAnalyticsTest, self).setUp()
        self._factory = self.gen_cluster
        self.cluster = self.make_connection()

    def tearDown(self):
        try:
            self._factory = SyncCluster
            self.cluster = self.make_connection()
        except Exception as e:
            pass
        super(TxAnalyticsTest, self).tearDown()

    @staticmethod
    def sleep(secs, callback):
        return deferLater(reactor, secs, callable=callback)

    def _passthrough(self, result, *args, **kwargs):
        return result

    def try_n_times(self, num_times, seconds_between, func, *args, on_success=None, **kwargs):
        on_success = on_success or self._passthrough
        if not isinstance(self.cluster, TxCluster):
            return super(TxAnalyticsTest, self).try_n_times(num_times, seconds_between, func, *args, on_success=on_success, **kwargs)

        class ResultHandler(object):
            def __init__(self, parent):
                self.remaining=num_times
                self._parent=parent

            def start(self, *exargs, **exkwargs):
                ret = func(*args, **kwargs)
                def kicker(result):
                    return self.success(result, args, kwargs, *exargs, **exkwargs)
                result = ret.addCallback(kicker)
                ret.addErrback(self.on_fail)
                return result

            def success(self, result, *exargs, **kwargs):
                return on_success(result, *exargs, **kwargs)

            def on_fail(self, deferred_exception):
                deferred_exception.printDetailedTraceback()
                if self.remaining:
                    self.remaining -= 1
                    deferred=TxAnalyticsTest.sleep(seconds_between,self.start)
                    deferred.addErrback(self._parent.fail)
                    return deferred
                else:
                    return self._parent.fail("unsuccessful {} after {} times, waiting {} seconds between calls".format(func, num_times, seconds_between))
        return ResultHandler(self).start()

    def checkResult(self, result, callback):
        if self._factory == SyncCluster:
            return super(TxAnalyticsTest, self).checkResult(result)

        def check(answer, *args, **kwargs):
            import logging
            result=callback(answer, *args, **kwargs)
            logging.error("Calling verifier {} with {}, {}, {} and got {}".format(callback, result, args, kwargs, result))
            return result
        result.addErrback(defer.fail)
        return result.addCallback(check)

    def _fail(self, message):
        if self._factory == SyncCluster:
            return super(TxAnalyticsTest, self)._fail(message)
        logging.error(message)
        return defer.fail

    def _success(self):
        if self._factory == SyncCluster:
            return True
        return twisted.internet.defer.Deferred()

    def _verify(self, d  # type: Base
                    ):
        def verify(o):
            logging.error("in callback with {}".format(o))
            self.assertIsInstance(o, BatchedAnalyticsResult)
            rows = [r for r in o]
            logging.error("End of callback")

        result= d.addCallback(verify)
        d.addErrback(self.mock_fallback)
        logging.error("ready to return")
        return result

    def assertQueryReturnsRows(self, query, *options, **kwargs):
        if self._factory == SyncCluster:
            return super(TxAnalyticsTest, self).assertQueryReturnsRows(query, *options, **kwargs)
        d = self.cluster.analytics_query(query, *options, **kwargs)

        def query_callback(result):
            self.assertIsInstance(result, BatchedAnalyticsResult)

            rows = result.rows()
            if len(rows) > 0:
                return rows
            raise Exception("no rows in result")
        d.addErrback(self.fail)
        return d.addCallback(query_callback)

    @property
    def factory(self):
        return self._factory