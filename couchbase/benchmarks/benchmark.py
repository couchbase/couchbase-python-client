#
# Copyright 2012, Couchbase, Inc.
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

import random
import string
import uuid
import json
import time
import os
import sys

import cProfile

here = os.path.dirname(os.path.abspath(__file__))

# Benchmark parameters
HOST = "127.0.0.1"
ITEMS = 50000  # Total number of documents to create/read
VALUES_PER_DOC = 1
VALUE_LENGTH = 92  # together with previous parameter it defines document size
PROFILES = os.path.join(here, 'profiles', time.strftime('%Y-%m-%d_%H-%M-%S',
                                                        time.localtime()))

if not os.path.exists(PROFILES):
    os.makedirs(PROFILES)


def prepare_data():
    # Test data
    print "Preparing data..."

    # Random string generator
    randstr = lambda length: ''.join(random.choice(string.letters +
                                                   string.digits)
                                     for _ in range(length))

    data = dict()
    for item in range(ITEMS):
        key = uuid.uuid4().hex
        value = dict((randstr(10), randstr(VALUE_LENGTH))
                     for _ in range(VALUES_PER_DOC))
        data[key] = json.dumps(value)

    return data


class Bench:
    """Base Bench class for handling setup, data prep, and execution"""
    def __init__(self):
        """Override this with library integration"""
        raise NotImplementedError

    def do(self, test_func, data, title):
        # Insert all items
        print "  " + title + " data..."

        def do_it():
            start_time = time.time()
            for key, value in data.items():
                test_func(key, value)
            end_time = time.time()
            print "    cmds/sec: {0}".format(ITEMS / (end_time - start_time))

        cProfile.runctx('do_it()', globals(), locals(),
                        os.path.join(PROFILES,
                                     str(self.__class__)[9:] + '.' +
                                     test_func.__name__))


class PythonMemcacheBench(Bench):
    def __init__(self):
        import memcache
        self.client = memcache.Client(["{0}:11210".format(HOST)], debug=0)

    def sets(self, key, value):
        self.client.set(key, value)

    def gets(self, key, value):
        self.client.get(key)


class MemcachedClientBench(Bench):
    def __init__(self):
        from couchbase.memcachedclient import MemcachedClient
        self.client = MemcachedClient(port=11210)

    def sets(self, key, value):
        self.client.set(key, 0, 0, value)

    def gets(self, key, value):
        self.client.get(key)


class VBucketAwareClientBench(MemcachedClientBench):
    def __init__(self):
        from couchbase.vbucketawareclient import VBucketAwareClient
        self.client = VBucketAwareClient(port=11210)


class CouchbaseClientBench(MemcachedClientBench):
    def __init__(self):
        from couchbase.couchbaseclient import CouchbaseClient
        self.client = \
            CouchbaseClient('http://{0}:8091/pools/default'.format(HOST),
                            'default')


class CouchbaseBench(MemcachedClientBench):
    def __init__(self):
        from couchbase import Couchbase
        self.client = Couchbase(HOST, 'Administrator', 'asdasd')['default']


class PyLibCbBench(Bench):
    def __init__(self):
        from pylibcb import Client
        self.client = Client(host=HOST, user='Administrator', passwd='asdasd',
                             bucket='default')

    def sets(self, key, value):
        self.client.set(key, value)

    def gets(self, key, value):
        self.client.get(key)


def main():
    data = prepare_data()
    for name in ["PythonMemcache", "MemcachedClient",
                 "VBucketAwareClient", "CouchbaseClient", "Couchbase",
                 "PyLibCb"]:
        print name
        bench = getattr(sys.modules[__name__], name + 'Bench')()
        for (doing, func) in [('Set', 'sets'), ('Get', 'gets')]:
            bench.do(getattr(bench, func), data, doing)


if __name__ == "__main__":
    main()
    os._exit(0)
