
sources=[{'type':'couchdb','class':'CouchdbReader','example':'couchdb:example.com:5984/database'}]
destinations=[{'type':'couchdb','class':'CouchdbWriter','example':'couchdb:example.com:5984/database'}]

import re
import json

try:
    import couchdb
except:
    sources=[]
    destinations=[]

import migrator

class CouchdbReader(migrator.Reader):
    def __init__(self, source):
        # example.com:5984/database
        m = re.match('^([^:]+):([^/]+)/(.+)$', source)
        self.host = m.group(1)
        self.port = m.group(2)
        self.database = m.group(3)

        self.couch = couchdb.Server('http://{0}:{1}'.format(self.host,self.port))
        self.db = self.couch[self.database]
        self.rows = list(self.db.view('_all_docs'))

    def __iter__(self):
        return self

    def next(self):
        if not self.rows:
            raise StopIteration()

        data = self.rows.pop()
        if data:
            record = {'id':data['id']}
            record['value'] = dict((k,v) for (k,v) in self.db[data['id']].iteritems() if not k.startswith('_'))
            return record
        else:
            raise StopIteration()
        raise StopIteration()


class CouchdbWriter(migrator.Writer):
    def __init__(self, destination):
        # example.com:5984/database

        m = re.match('^([^:]+):([^/]+)/(.+)$', destination)
        self.host = m.group(1)
        self.port = m.group(2)
        self.database = m.group(3)

        self.couch = couchdb.Server('http://{0}:{1}'.format(self.host,self.port))
        self.db = self.couch[self.database]

    def write(self, record):
        record_save = record['value']
        record_save['_id'] = record['id']
        self.db.save(record_save)
