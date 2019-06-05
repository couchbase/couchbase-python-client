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
from time import sleep, time

import gevent

from couchbase_core import FMT_BYTES
from gcouchbase.bucket import Bucket

ap = argparse.ArgumentParser()

ap.add_argument('-t', '--threads', default=4, type=int,
                help="Number of threads to spawn. 0 means no threads "
                "but workload will still run in the main thread")

ap.add_argument('-d', '--delay', default=0, type=float,
                help="Number of seconds to wait between each op. "
                "may be a fraction")
ap.add_argument('-U', '--connstr', default='couchbase://localhost/default',
                help="Connection string")
ap.add_argument('-p', '--password', default=None, type=str)
ap.add_argument('-D', '--duration', default=10, type=int,
                help="Duration of run (in seconds)")

ap.add_argument('--ksize', default=12, type=int,
                help="Key size to use")

ap.add_argument('--vsize', default=128, type=int,
                help="Value size to use")
ap.add_argument('--iops', default=None, type=str,
                help="Use Pure-Python IOPS plugin")
ap.add_argument('-g', '--global-instance',
                help="Use global instance", default=False,
                action='store_true')
ap.add_argument('--batch', '-N', type=int, help="Batch size", default=1)

options = ap.parse_args()

GLOBAL_INSTANCE = None
CONN_OPTIONS = {
        'connstr' : options.connstr,
        'password': options.password
}

GLOBAL_INSTANCE = Bucket(**CONN_OPTIONS)

def make_instance():
    if options.global_instance:
        return GLOBAL_INSTANCE
    else:
        return Bucket(**CONN_OPTIONS)

class Worker(object):
    def __init__(self):
        self.delay = options.delay
        self.key = 'K' * options.ksize
        self.value = b'V' * options.vsize
        self.kv = {}
        for x in range(options.batch):
            self.kv[self.key + str(x)] = self.value

        self.wait_time = 0
        self.opcount = 0

    def run(self, *args, **kwargs):
        self.end_time = time() + options.duration
        cb = make_instance()

        while time() < self.end_time:
            begin_time = time()
            rv = cb.upsert_multi(self.kv, format=FMT_BYTES)
            assert rv.all_ok, "Operation failed: "
            self.wait_time += time() - begin_time
            self.opcount += options.batch

global_begin = None
gthreads = []
worker_threads = []
for x in range(options.threads):
    w = Worker()
    worker_threads.append(w)
    t = gevent.spawn(w.run)
    gthreads.append(t)

global_begin = time()
for t in gthreads:
    t.join()

global_duration = time() - global_begin
total_ops = sum([w.opcount for w in worker_threads])
total_time = 0
for t in worker_threads:
    total_time += t.wait_time

print("Total run took an absolute time of %0.2f seconds" % (global_duration,))
print("Did a total of %d operations" % (total_ops,))
print("Total wait time of %0.2f seconds" % (total_time,))
print("[WAIT] %0.2f ops/second" % (float(total_ops)/float(total_time),))
print("[ABS] %0.2f ops/second" % (float(total_ops)/float(global_duration),))
