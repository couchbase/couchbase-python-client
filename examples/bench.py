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

#!/usr/bin/env python
import argparse
from threading import Thread
from time import sleep, time
from couchbase.libcouchbase import Connection, FMT_BYTES

ap = argparse.ArgumentParser()

ap.add_argument('-t', '--threads', default=4, type=int,
                help = "Number of threads to spawn. 0 means no threads "
                "but workload will still run in the main thread")

ap.add_argument('-d', '--delay', default=0, type=float,
                help = "Number of seconds to wait between each op. "
                "may be a fraction")

ap.add_argument('-u', '--username', default='Administrator', type=str)
ap.add_argument('-b', '--bucket', default='default', type=str)
ap.add_argument('-p', '--password', default="123456", type=str)
ap.add_argument('-H', '--hostname', default='localhost', type=str)
ap.add_argument('-D', '--duration', default=10, type=int,
                help = "Duration of run (in seconds)")

ap.add_argument('--ksize', default=12, type=int,
                help = "Key size to use")

ap.add_argument('--vsize', default=128, type=int,
                help = "Value size to use")

options = ap.parse_args()
DO_UNLOCK_GIL = options.threads > 0

class Worker(Thread):
    def __init__(self):
        self.delay = options.delay
        self.key = 'K' * options.ksize
        self.value = b'V' * options.vsize
        self.wait_time = 0
        self.opcount = 0
        self.cb = Connection(bucket='default',
                        host=options.hostname,
                        unlock_gil=DO_UNLOCK_GIL)
        self.end_time = time() + options.duration
        super(Worker, self).__init__()

    def run(self, *args, **kwargs):
        cb = self.cb

        while time() < self.end_time:
            begin_time = time()
            rv = cb.set(self.key, self.value, format=FMT_BYTES)
            assert rv.rc == 0, "Operation failed: " + str(rv.rc)
            self.wait_time += time() - begin_time

            if self.delay:
                sleep(self.delay)

            self.opcount += 1


global_begin = None
worker_threads = []
if not options.threads:
    # No threding requested:
    w = Worker()
    worker_threads.append(w)
    global_begin = time()
    w.run()
else:
    for x in range(options.threads):
        worker_threads.append(Worker())

    global_begin = time()
    for t in worker_threads:
        t.start()

    for t in worker_threads:
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
