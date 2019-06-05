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

"""
This file shows how to make a simple connection pool using Couchbase.
"""
from couchbase_v2.bucket import Bucket
from Queue import Queue, Empty
from threading import Lock, Thread
from time import time
from argparse import ArgumentParser


class ClientUnavailableError(Exception):
    pass


class BucketWrapper(Bucket):
    """
    This is a simple subclass which adds usage statistics to inspect later on
    """
    def __init__(self, **kwargs):
        super(BucketWrapper, self).__init__(**kwargs)
        self.use_count = 0
        self.use_time = 0
        self.last_use_time = 0

    def start_using(self):
        self.last_use_time = time()

    def stop_using(self):
        self.use_time += time() - self.last_use_time
        self.use_count += 1


class Pool(object):
    def __init__(self, initial=4, max_clients=10, **connargs):
        """
        Create a new pool
        :param int initial: The initial number of client objects to create
        :param int max_clients: The maximum amount of clients to create. These
          clients will only be created on demand and will potentially be
          destroyed once they have been returned via a call to
          :meth:`release_client`
        :param connargs: Extra arguments to pass to the Connection object's
        constructor
        """
        self._q = Queue()
        self._l = []
        self._connargs = connargs
        self._cur_clients = 0
        self._max_clients = max_clients
        self._lock = Lock()

        for x in range(initial):
            self._q.put(self._make_client())
            self._cur_clients += 1

    def _make_client(self):
        ret = BucketWrapper(**self._connargs)
        self._l.append(ret)
        return ret

    def get_client(self, initial_timeout=0.05, next_timeout=200):
        """
        Wait until a client instance is available
        :param float initial_timeout:
          how long to wait initially for an existing client to complete
        :param float next_timeout:
          if the pool could not obtain a client during the initial timeout,
          and we have allocated the maximum available number of clients, wait
          this long until we can retrieve another one

        :return: A connection object
        """
        try:
            return self._q.get(True, initial_timeout)
        except Empty:
            try:
                self._lock.acquire()
                if self._cur_clients == self._max_clients:
                    raise ClientUnavailableError("Too many clients in use")
                cb = self._make_client()
                self._cur_clients += 1
                cb.start_using()
                return cb
            except ClientUnavailableError as ex:
                try:
                    return self._q.get(True, next_timeout)
                except Empty:
                    raise ex
            finally:
                self._lock.release()

    def release_client(self, cb):
        """
        Return a Connection object to the pool
        :param Connection cb: the client to release
        """
        cb.stop_using()
        self._q.put(cb, True)


class CbThread(Thread):
    def __init__(self, pool, opcount=10, remaining=10000):
        super(CbThread, self).__init__()
        self.pool = pool
        self.remaining = remaining
        self.opcount = opcount

    def run(self):
        while self.remaining:
            cb = self.pool.get_client()
            kv = dict(
                ("Key_{0}".format(x), str(x)) for x in range(self.opcount)
            )
            cb.upsert_multi(kv)
            self.pool.release_client(cb)
            self.remaining -= 1


def main():

    ap = ArgumentParser()
    ap.add_argument('-U', '--connstr', help="Connection string",
                    default='couchbase://localhost/default')
    ap.add_argument("-O", "--opcount", help="How many operations to perform "
                    "at once", type=int,
                    default=10)
    ap.add_argument('--pool-min',
                    help="Minimum pool size", default=4, type=int)
    ap.add_argument('--pool-max',
                    help="Maximum pool size", default=10, type=int)
    ap.add_argument('-t', '--threads', type=int, default=10,
                    help="Number of threads to launch")

    options = ap.parse_args()

    pool = Pool(initial=options.pool_min,
                max_clients=options.pool_max,
                connstr=options.connstr)

    thrs = [
        CbThread(pool, opcount=options.opcount) for _ in range(options.threads)
    ]

    map(lambda thr: thr.start(), thrs)
    map(lambda thr: thr.join(), thrs)

    for c in pool._l:
        print "Have client {0}".format(c)
        print "\tTime In Use: {0}, use count: {1}".format(c.use_time,
                                                          c.use_count)


if __name__ == "__main__":
    main()
