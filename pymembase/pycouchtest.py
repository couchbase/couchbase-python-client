#!/usr/bin/python
import random
from threading import Thread
import uuid
import time
from exception import MemcachedTimeoutException
from membaseclient import VBucketAwareMembaseClient
from optparse import OptionParser
from util import ProgressBar, StringUtil
import sys

class SharedProgressBar(object):
    def __init__(self, number_of_items):
        self.bar = ProgressBar(0, number_of_items, 77)
        self.number_of_items = number_of_items
        self.counter = 0
        self.old_bar_string = ""

    def update(self):
        self.counter += 1
        if self.old_bar_string != str(self.bar):
            sys.stdout.write(str(self.bar) + '\r')
            sys.stdout.flush()
            self.old_bar_string = str(self.bar)
        self.bar.updateAmount(self.counter)

    def flush(self):
        self.bar.updateAmount(self.number_of_items)
        sys.stdout.write(str(self.bar) + '\r')
        sys.stdout.flush()

class SmartLoader(object):
    def __init__(self, options, server, sharedProgressBar, thread_id):
        self._options = options
        self._server = server
        self._thread = None
        self.shut_down = False
        self._stats = {"total_time": 0, "max": 0, "min": 1 * 1000 * 1000, "samples": 0, "timeouts": 0}
        self._bar = sharedProgressBar
        self._thread_id = thread_id

    def start(self):
        self._thread = Thread(target=self._run)
        self._thread.start()

    def _run(self):
        v = None
        try:
            options = self._options
            v = VBucketAwareMembaseClient(self._server, options.bucket, options.verbose)
            number_of_items = (int(options.items) / int(options.num_of_threads))
            value = StringUtil.create_value("*", int(options.value_size))
            for i in range(0, number_of_items):
                if self.shut_down:
                    break
                key = "{0}-{1}".format(options.key_prefix, str(uuid.uuid4())[:5])
                if options.load_json:
                    document = "\"name\":\"pymc-{0}\"".format(key, key)
                    document = document + ",\"age\":{0}".format(random.randint(0, 1000))
                    document = "{" + document + "}"
                    try:
                        self._profile_before()
                        v.set(key, 0, 0, document)
                        self._profile_after()
                    except MemcachedTimeoutException:
                        self._stats["timeouts"] += 1
                else:
                    try:
                        self._profile_before()
                        v.set(key, 0, 0, value)
                        self._profile_after()
                    except MemcachedTimeoutException:
                        self._stats["timeouts"] += 1
                self._bar.update()
            v.done()
            v = None
        except:
            if v:
                v.done()

    def print_stats(self):
        msg = "Thread {0} - average set time : {1} seconds , min : {2} seconds , max : {3} seconds , operation timeouts {4}"
        if self._stats["samples"]:
            print msg.format(self._thread_id, self._stats["total_time"] / self._stats["samples"],
                             self._stats["min"], self._stats["max"], self._stats["timeouts"])

    def wait(self, block=False):
        if block:
            self._thread.join()
        else:
            return not self._thread.is_alive()


    def stop(self):
        self.shut_down = True
        if v:
            v.done()

    def _profile_before(self):
        self.start = time.time()

    def _profile_after(self):
        self.end = time.time()
        diff = self.end - self.start
        self._stats["samples"] += 1
        self._stats["total_time"] = self._stats["total_time"] + diff
        if self._stats["min"] > diff:
            self._stats["min"] = diff
        if self._stats["max"] < diff:
            self._stats["max"] = diff

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--node", dest="node",
                      help="node's ns_server ip:port", metavar="192.168.1.1:8091")
    parser.add_option("-u", "--user", dest="username",
                      help="node username", metavar="Administrator", default="Administrator")
    parser.add_option("-p", "--password", dest="password",
                      help="node password", metavar="asdasd")
    parser.add_option("-b", "--bucket", dest="bucket", help="which bucket to insert data",
                      default="default", metavar="default")
    parser.add_option("-j", "--json", dest="load_json", help="insert json data",
                      default=False, metavar="True")

    parser.add_option("-v", "--verbose", dest="verbose", help="run in verbose mode",
                      default=False, metavar="False")

    parser.add_option("-i", "--items", dest="items", help="number of items to be inserted",
                      default=100, metavar="100")

    parser.add_option("--size", dest="value_size", help="value size,default is 256 byte",
                      default=512, metavar="100")

    parser.add_option("--threads", dest="num_of_threads", help="number of threads to run load",
                      default=1, metavar="100")

    parser.add_option("--prefix", dest="key_prefix",
                      help="prefix to use for memcached keys and json _ids,default is uuid string",
                      default=str(uuid.uuid4())[:5], metavar="pymc")

    options, args = parser.parse_args()

    node = options.node

    if not node:
        parser.print_help()
        exit()
        #if port is not given use :8091
    if node.find(":") == -1:
        hostname = node
        port = 8091
    else:
        hostname = node[:node.find(":")]
        port = node[node.find(":") + 1:]
    server = {"ip": hostname,
              "port": port,
              "username": options.username,
              "password": options.password}
    v = None
    workers = []
    try:
        no_threads = options.num_of_threads
        number_of_items = int(options.items)
        sharedProgressBar = SharedProgressBar(number_of_items)
        for i in range(0, int(no_threads)):
            worker = SmartLoader(options, server, sharedProgressBar, i)
            worker.start()
            workers.append(worker)
        while True:
            all_finished = True
            for worker in workers:
                all_finished &= worker.wait()
            if all_finished:
                break
            else:
                time.sleep(0.5)
        sharedProgressBar.flush()
        for worker in workers:
            worker.print_stats()
    except :
        print ""
        for worker in workers:
            worker.stop()