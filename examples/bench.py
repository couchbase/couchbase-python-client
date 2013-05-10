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

ap.add_argument('-n', '--iterations', default=10000, type=int,
                help = "Number of operations, per thread, to perform")

ap.add_argument('--ksize', default=12, type=int,
                help = "Key size to use")

ap.add_argument('--vsize', default=128, type=int,
                help = "Value size to use")

options = ap.parse_args()
DO_UNLOCK_GIL = options.threads > 0

class Worker(Thread):
    def __init__(self):
        self.delay = options.delay
        self.iterations = options.iterations
        self.key = 'K' * options.ksize
        self.value = 'V' * options.vsize
        self.wait_time = 0
        self.cb = Connection(bucket='default',
                        host=options.hostname,
                        unlock_gil=DO_UNLOCK_GIL)
        super(Worker, self).__init__()

    def run(self, *args, **kwargs):
        iterations = self.iterations
        cb = self.cb

        while iterations:
            begin_time = time()
            rv = cb.set(self.key, self.value, format=FMT_BYTES)
            assert rv.rc == 0, "Operation failed: " + str(rv.rc)
            self.wait_time += time() - begin_time

            iterations -= 1
            if self.delay:
                sleep(self.delay)


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
total_ops = options.iterations * len(worker_threads)
total_time = 0
for t in worker_threads:
    total_time += t.wait_time

print("Total run took an absolute time of %0.2f seconds" % (global_duration,))
print("Did a total of %d operations" % (total_ops,))
print("Total wait time of %0.2f seconds" % (total_time,))
print("[WAIT] %0.2f ops/second" % (float(total_ops)/float(total_time),))
print("[ABS] %0.2f ops/second" % (float(total_ops)/float(global_duration),))
