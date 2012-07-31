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

sources = [{'type':'couchbase', 'class':'CouchbaseReader',
            'example':'couchbase://bucket:password@example.com:8091/bucket'}]
destinations = [{'type':'couchbase', 'class':'CouchbaseWriter',
                 'example':
                 'couchbase://bucket:password@example.com:8091/bucket'}]

from urlparse import urlparse
import urllib

import couchbase
from couchbase.migrator.migrator import Reader, Writer


class CouchbaseReader(Reader):
    def __init__(self, source):
        # couchbase://username:password@example.com:8091/bucket
        url = urlparse(source)
        self.username = url.username
        self.password = url.password
        self.host = url.hostname
        self.port = url.port
        self.bucket_name = url.path[1:]

        self.page_limit = 100

        cb = couchbase.Server(self.host + ":" + str(self.port),
                              username=self.username,
                              password=self.password)
        self.bucket = cb[self.bucket_name]

        self.items = self.bucket.view('_all_docs', limit=self.page_limit + 1,
                                      stale=False, reduce=False,
                                      include_docs=True)

    def __iter__(self):
        return self

    def next(self):
        if len(self.items) < 1:
            raise StopIteration()
        elif len(self.items) == 1:
            next_startkey = (urllib.quote_plus(self.items[0]['key']
                             .replace('"', '\\"').encode('utf-8')))
            next_startkey_docid = urllib.quote_plus(self.items[0]['doc']['_id']
                                                    .replace('"', '\\"')
                                                    .encode('utf-8'))
            self.items = self.bucket.view('_all_docs',
                                          limit=self.page_limit + 1,
                                          startkey=next_startkey,
                                          startkey_docid=next_startkey_docid,
                                          stale=False, reduce=False,
                                          include_docs=True)
            data = self.items.pop(0)
        else:
            data = self.items.pop(0)

        record = {'id': data['doc']['_id']}
        record['value'] = (dict((k, v) for (k, v) in data['doc'].iteritems()
                           if not k.startswith('$')))
        return record


class CouchbaseWriter(Writer):
    def __init__(self, destination):
        # couchbase://username:password@example.com:8091/bucket
        url = urlparse(destination)
        self.username = url.username
        self.password = url.password
        self.host = url.hostname
        self.port = url.port
        self.bucket_name = url.path[1:]

        self.verbose = False

        cb = couchbase.Server(self.host + ":" + str(self.port),
                              username=self.username,
                              password=self.password)
        self.bucket = cb[self.bucket_name]

    def write(self, record):
        record_save = record['value']
        record_save['_id'] = record['id'].encode('utf-8')
        self.bucket.save(record_save)
