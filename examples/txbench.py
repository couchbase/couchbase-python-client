#!/usr/bin/env python
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

import argparse
from time import time

from twisted.internet import reactor

from txcouchbase.bucket import RawBucket, Bucket
from couchbase import FMT_BYTES
from couchbase.transcoder import Transcoder

ap = argparse.ArgumentParser()

ap.add_argument('-t', '--threads', default=4, type=int,
                help="Number of threads to spawn. 0 means no threads "
                "but workload will still run in the main thread")

ap.add_argument('-d', '--delay', default=0, type=float,
                help="Number of seconds to wait between each op. "
                "may be a fraction")

ap.add_argument('-C', '--clients', default=1, type=int,
                help="Number of clients (nthreads are per-client)")

ap.add_argument('--deferreds', action='store_true', default=False,
                help="Whether to use Deferreds (or normal callbacks)")
ap.add_argument('-U', '--connstr', default='couchbase://localhost/default',
                help="Connection string")
ap.add_argument('-p', '--password', default=None, type=str)
ap.add_argument('-D', '--duration', default=10, type=int,
                help="Duration of run (in seconds)")

ap.add_argument('-T', '--transcoder', default=False,
                action='store_true',
                help="Use the Transcoder object rather than built-in "
                "conversion routines")

ap.add_argument('--ksize', default=12, type=int,
                help="Key size to use")

ap.add_argument('--vsize', default=128, type=int,
                help="Value size to use")

ap.add_argument('--batch', '-N', type=int, default=1, help="Batch size to use")

options = ap.parse_args()

class Runner(object):
    def __init__(self, cb):
        self.cb = cb
        self.delay = options.delay
        self.key = 'K' * options.ksize
        self.value = b'V' * options.vsize
        self.kv = {}
        for x in range(options.batch):
            self.kv[self.key + str(x)] = self.value
        self.wait_time = 0
        self.opcount = 0
        self.end_time = time() + options.duration
        self._do_stop = False
        self.start()

    def _schedule_raw(self, *args):
        opres = self.cb.upsert_multi(self.kv, format=FMT_BYTES)
        opres.callback = self._schedule_raw
        self.opcount += 1

    def _schedule_deferred(self, *args):
        rv = self.cb.upsertMulti(self.kv, format=FMT_BYTES)
        rv.addCallback(self._schedule_deferred)
        self.opcount += options.batch

    def start(self):
        if options.deferreds:
            self._schedule_deferred()
        else:
            self.cb._async_raw = True
            self._schedule_raw()

    def stop(self):
        self._do_stop = True

global_begin = time()
runners = []
clients = []
kwargs = {
    'connstr' : options.connstr,
    'password': options.password,
    'unlock_gil': False
}
if options.transcoder:
    kwargs['transcoder'] = Transcoder()

for _ in range(options.clients):
    cls = Bucket if options.deferreds else RawBucket
    cb = cls(**kwargs)
    clients.append(cb)
    d = cb.connect()

    def _on_connected(unused, client):
        for _ in range(options.threads):
            r = Runner(client)
            runners.append(r)
    d.addCallback(_on_connected, cb)

def stop_all():
    [r.stop() for r in runners]
    reactor.stop()

reactor.callLater(options.duration, stop_all)
reactor.run()


global_duration = time() - global_begin
total_ops = sum([r.opcount for r in runners])
total_time = 0
for r in runners:
    total_time += r.wait_time

print("Total run took an absolute time of %0.2f seconds" % (global_duration,))
print("Did a total of %d operations" % (total_ops,))
print("[ABS] %0.2f ops/second" % (float(total_ops)/float(global_duration),))
