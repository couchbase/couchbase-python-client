
sources=[{'type':'couchdb','class':'CouchdbReader','example':'couchdb://example.com:5984/database'}]
destinations=[{'type':'couchdb','class':'CouchdbWriter','example':'couchdb://example.com:5984/database'}]

import re
import json
import urllib
from urlparse import urlparse

try:
    import couchdb
except:
    sources=[]
    destinations=[]

import migrator

class CouchdbReader(migrator.Reader):
    def __init__(self, source):
        # couchdb://example.com:5984/database
        url = urlparse(source)
        self.host = url.hostname
        self.port = url.port
        self.database = url.path[1:]

        self.page_limit = 100

        self.couch = couchdb.Server('http://%s:%s' % (self.host,self.port))
        self.db = self.couch[self.database]

        self.items = list(self.db.view('_all_docs', limit=self.page_limit+1, include_docs=True))

    def __iter__(self):
        return self

    def next(self):
        if len(self.items) < 1:
            raise StopIteration()
        elif len(self.items) == 1:
            next_startkey = self.items[0]['key'].replace('"','\\"').encode('utf-8')
            next_startkey_docid = self.items[0]['key'].replace('"','\\"').encode('utf-8')
            self.items = list(self.db.view('_all_docs', limit=self.page_limit+1, startkey=next_startkey, startkey_docid=next_startkey_docid, include_docs=True))

        data = self.items.pop(0)

        record = {'id':data['doc']['_id']}
        record['value'] = dict((k,v) for (k,v) in data['doc'].iteritems())
        return record


class CouchdbWriter(migrator.Writer):
    def __init__(self, destination):
        # couchdb://example.com:5984/database
        url = urlparse(destination)
        self.host = url.hostname
        self.port = url.port
        self.database = url.path[1:]

        self.couch = couchdb.Server('http://%s:%s' % (self.host,self.port))
        self.db = self.couch[self.database]

    def write(self, record):
        record_save = record['value']
        record_save['_id'] = record['id']
        self.db.save(record_save)
