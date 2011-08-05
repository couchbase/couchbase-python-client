
sources=[]
destinations=[{'type':'couchbase','class':'CouchbaseWriter','example':'couchbase:username:password@example.com:8091/bucket'}]

import re
import json

from couchbase.couchbaseclient import VBucketAwareCouchbaseClient, MemcachedTimeoutException
#import mc_bin_client

import migrator

class CouchbaseReader(migrator.Reader):
    def __init__(self, source):
        # username:password@example.com:8091/bucket
        m = re.match('^([^:]+):([^@]+)@([^:]+):([^/]+)/(.+)$', source)
        self.username = m.group(1)
        self.password = m.group(2)
        self.host = m.group(3)
        self.port = m.group(4)
        self.bucket = m.group(5)

        self.bucket_port = 11211
        self.bucket_password = ''

#    def __iter__(self):
#        return self

#    def next(self):
#        data = self.reader.next()
#        if data:
#            record = {'id':data['id']}
#            record['value'] = dict((k,v) for (k,v) in json_data['value'].iteritems() if not k.startswith('_'))
#            return record
#        else:
#            raise StopIteration()
#        raise StopIteration()


class CouchbaseWriter(migrator.Writer):
    def __init__(self, destination):
        # username:password@example.com:8091/bucket
        m = re.match('^([^:]+):([^@]+)@([^:]+):([^/]+)/(.+)$', destination)
        self.username = m.group(1)
        self.password = m.group(2)
        self.host = m.group(3)
        self.port = m.group(4)
        self.bucket = m.group(5)

        self.bucket_port = 11211
        self.bucket_password = ''

        self.verbose = False

        # todo: use server username/password to query the bucket password/port if needed
        self.server = "http://{2}:{3}/pools/default".format(self.username, self.password, self.host, self.port)
        self.client = VBucketAwareCouchbaseClient(self.server, self.bucket, self.bucket_password, self.verbose)

    def write(self, record):
        for i in range(5):
            try:
                # todo: check for timeout and flags
                self.client.set(str(record['id']), 0, 0, json.dumps(record['value']))
                return
            except MemcachedTimeoutException as e:
                pass
            except:
                self.client.done()
                self.client = VBucketAwareCouchbaseClient(self.server, self.bucket, self.bucket_password, self.verbose)
        print 'unable to set key {0}'.format(str(record['id']))

    def close(self):
        self.client.done()
