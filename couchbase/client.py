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

import uuid
import json
import time
from copy import deepcopy
from threading import Thread, Lock
import warnings
from collections import Set

import requests

from couchbase.logger import logger
from couchbase.rest_client import RestConnection
from couchbase.couchbaseclient import CouchbaseClient
from couchbase.exception import BucketUnavailableException

log = logger("client")


class Couchbase(object):
    """This is the primary class through which the Python "smart client"
    communicates with the cluster. It combines and simplifies access to both
    the memcached and HTTP protocols used in Couchbase Server."""
    def __init__(self, host, username, password):
        if (':' in host):
            [ip, port] = host.split(':')
        else:
            [ip, port] = host, 8091

        server = {'ip': ip,
                  'port': port,
                  'username': username,
                  'password': password
                  }

        self.servers = [server]
        self.servers_lock = Lock()

        self.rest_username = username
        self.rest_password = password

        server_config_uri = "http://%s:%s/pools/default" % (ip, port)
        config = requests.get(server_config_uri).json
        #couchApiBase will not be in node config before Couchbase Server 2.0
        self.couch_api_base = config["nodes"][0].get("couchApiBase")

        self.streaming_thread = Thread(name="streaming",
                                       target=self._start_streaming, args=())
        self.streaming_thread.daemon = True
        self.streaming_thread.start()

    def _start_streaming(self):
        # this will dynamically update servers

        url = "http://%s:%s/poolsStreaming/default" % (self.servers[0]["ip"],
                                                       self.servers[0]["port"])
        response = requests.get(url)
        for line in response.iter_lines():
            if line:
                data = json.loads(line)

                new_servers = []
                nodes = data["nodes"]
                for node in nodes:
                    if (node["clusterMembership"] == "active" and
                            node["status"] in ["healthy", "warmup"]):
                        ip, port = node["hostname"].split(":")
                        couch_api_base = node.get("couchApiBase")
                        new_servers.append({"ip": ip,
                                            "port": port,
                                            "username": self.rest_username,
                                            "password": self.rest_password,
                                            "couchApiBase": couch_api_base
                                            })
                new_servers.sort()
                self.servers_lock.acquire()
                self.servers = deepcopy(new_servers)
                self.servers_lock.release()

    def bucket(self, bucket_name):
        return Bucket(bucket_name, self)

    def buckets(self):
        """Get a list of all buckets as Buckets"""
        rest = self._rest()
        buckets = []
        for rest_bucket in rest.get_buckets():
            buckets.append(Bucket(rest_bucket.name, self))
        return buckets

    def create(self, bucket_name, bucket_password='', ram_quota_mb=100,
               replica=0):
        rest = self._rest()
        rest.create_bucket(bucket=bucket_name,
                           ramQuotaMB=ram_quota_mb,
                           authType='sasl',
                           saslPassword=bucket_password,
                           replicaNumber=replica,
                           bucketType='membase')

        ip, port, _, _ = self._rest_info()

        while True:
            try:
                content = '{"basicStats":{"quotaPercentUsed":0.0}}'
                api = "/pools/default/buckets/{0}".format(bucket_name)
                status, content = rest._http_request(api,
                                                     method='GET', params='',
                                                     headers=None, timeout=120)
                content = json.loads(content)
            except ValueError:
                pass
            if content['basicStats']['quotaPercentUsed'] > 0.0:
                time.sleep(2)
                break
            time.sleep(1)

        return Bucket(bucket_name, self)

    def delete(self, bucket_name):
        rest = self._rest()
        rest.delete_bucket(bucket_name)

    def __getitem__(self, key):
        return self.bucket(key)

    def __iter__(self):
        return BucketIterator(self.buckets())

    def _rest(self):
        self.servers_lock.acquire()
        server_info = deepcopy(self.servers[0])
        self.servers_lock.release()
        server_info['username'] = self.rest_username
        server_info['password'] = self.rest_password
        server_info['couchApiBase'] = self.couch_api_base
        rest = RestConnection(server_info)
        return rest

    def _rest_info(self):
        self.servers_lock.acquire()
        server_info = deepcopy(self.servers[0])
        self.servers_lock.release()
        return (server_info['ip'], server_info['port'],
                server_info['username'], server_info['password'])


class Server(Couchbase):
    """This is the earlier name of the Couchbase() class. It is now deprecated.
    """
    def __init__(self, host, username, password):
        warnings.warn("Server is deprecated; use Couchbase instead",
                      DeprecationWarning)
        Couchbase.__init__(self, host, username, password)


class BucketIterator(object):
    """Pythonic Iterator for Bucket objects."""
    def __init__(self, buckets):
        self.buckets = buckets

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.buckets.pop(0)
        except IndexError:
            raise StopIteration


class Bucket(object):
    """Handles Bucket management as well as key/value access for a specific
    bucket."""
    def __init__(self, name, server):
        self.server = server

        self.name = name
        rest = server._rest()
        self.info = rest.get_bucket(self.name)
        self.password = self.info.saslPassword

        ip, port, rest_username, rest_password = server._rest_info()
        formatter_uri = "http://%s:%s/pools/default"
        self.mc_client = CouchbaseClient(formatter_uri % (ip, port), self.name,
                                         self.password)

    def __del__(self):
        self.mc_client.done()

    def append(self, key, value, cas=0):
        return self.mc_client.append(key, value, cas)

    def prepend(self, key, value, cas=0):
        return self.mc_client.prepend(key, value, cas)

    def incr(self, key, amt=1, init=0, exp=0):
        return self.mc_client.incr(key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        return self.mc_client.decr(key, amt, init, exp)

    def set(self, key, expiration, flags, value):
        if isinstance(value, dict):
            value = json.dumps(value)
        return self.mc_client.set(key, expiration, flags, value)

    def add(self, key, exp, flags, val):
        return self.mc_client.add(key, exp, flags, val)

    def replace(self, key, exp, flags, val):
        return self.mc_client.replace(key, exp, flags, val)

    def get(self, key):
        return self.mc_client.get(key)

    def getl(self, key, exp=15):
        return self.mc_client.getl(key, exp)

    def cas(self, key, exp, flags, oldVal, val):
        return self.mc_client.cas(key, exp, flags, oldVal, val)

    def touch(self, key, exp):
        return self.mc_client.touch(key, exp)

    def gat(self, key, exp):
        return self.mc_client.gat(key, exp)

    def stats(self, sub=''):
        return self.mc_client.stats(sub)

    def delete(self, key, cas=0):
        if key.startswith('_design/'):
            # this is a design doc, we need to handle it differently
            view = key.split('/')[1]

            rest = self.server._rest()
            rest.delete_view(self.name, view)
        else:
            return self.mc_client.delete(key, cas)

    def save(self, document):
        warnings.warn("save is deprecated; use set instead",
                      DeprecationWarning)
        value = deepcopy(document)
        if '_id' in value:
            key = value['_id']
            del value['_id']
        else:
            key = str(uuid.uuid4())
        if '$flags' in value:
            flags = value['$flags']
            del value['$flags']
        else:
            flags = 0
        if '$expiration' in value:
            expiration = value['$expiration']
            del value['$expiration']
        else:
            expiration = 0

        if key.startswith('_design/'):
            self[key] = value
        else:
            if '_rev' in value:
                # couchbase works in clobber mode so for "set" _rev is useless
                del value['_rev']
            self.set(key, expiration, flags, json.dumps(value))

        return key

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            if 'expiration' in value or 'flags' in value:
                assert 'value' in value
                if isinstance(value['value'], dict):
                    v = json.dumps(value['value'])
                else:
                    v = value['value']
                self.set(key, value.get('expiration', 0),
                         value.get('flags', 0), v)
            elif key.startswith('_design/'):
                rest = self.server._rest()
                rest.create_design_doc(self.name, key[8:], json.dumps(value))
            else:
                self.set(key, 0, 0, json.dumps(value))
        else:
            self.set(key, 0, 0, value)

    def __getitem__(self, key):
        if key.startswith("_design/"):
            rest = self.server._rest()
            doc = rest.get_design_doc(self.name, key[8:])
            return DesignDoc(key[8:], doc, self)
        else:
            return self.get(key)

    def view(self, view, **options):
        params = deepcopy(options)
        limit = None
        if 'limit' in params:
            limit = params['limit']
            del params['limit']

        if view.startswith("_design/"):
            view_s = view.split('/')
            view_doc = view_s[1]
            view_map = view_s[3]
        else:
            view_doc = view
            view_map = None

        rest = self.server._rest()

        results = rest.view_results(self.name, view_doc, view_map,
                                    params, limit)
        if 'rows' in results:
            return results['rows']
        else:
            return None

    def design_docs(self):
        """List all design documents and return DesignDoc objects for each"""
        (ip, port, _, _) = self.server._rest_info()
        api = ''.join(['http://{0}:{1}'.format(ip, port),
                       self.info.ddocs['uri']])
        r = requests.get(api, auth=(self.server.rest_username,
                                    self.server.rest_password))
        ddocs = []
        for ddoc in r.json.get('rows'):
            ddocs.append(DesignDoc(ddoc['doc']['meta']['id'],
                                   ddoc['doc']['json'], bucket=self))

        return ddocs

    def flush(self):
        """RESTful Bucket flushing - will destory all the data in a bucket."""
        ip, port, rest_username, rest_password = self.server._rest_info()
        api = ''.join(["http://{0}:{1}".format(ip, port),
                      self.info.controllers['flush']])
        response = requests.post(api, auth=(rest_username, rest_password))

        if response.status_code is 503:
            raise Exception("Only buckets of type 'memcached' support flush.")
        elif response.status_code is 404:
            raise BucketUnavailableException
        elif response.status_code is 200:
            return True


class DesignDoc(object):
    """Object representation of a Couchbase Server Design Document--the thing
    that holds the MapReduce Views.

    This Object handles the core logic behind creating and updating views to
    a specific design doc. It also handles copying/publishing a design doc into
    (or out of) production."""
    def __init__(self, name, ddoc=None, bucket=None):
        assert isinstance(name, (str, unicode)), \
            "name parameter must be of type string or unicode"
        assert isinstance(ddoc, (str, unicode, dict)), \
            "ddoc parameter must be of type string, unicode, or dictionary"

        if name.startswith('_design/'):
            name = name[8:]
        self.name = name
        self.bucket = bucket

        if ddoc is None:
            self.ddoc = {'type': 'javascript', 'views': {}}
        elif isinstance(ddoc, unicode):
            self.ddoc = json.loads(ddoc)
        else:
            self.ddoc = ddoc

    def __str__(self):
        """Return the name of the Design Doc when using print"""
        return self.name

    def __eq__(self, other):
        """Compare name or "views" section of the Design Doc. This allows the
        use of "for ddoc in" style syntax when used with
        Bucket().design_docs()"""
        if isinstance(other, str) and "{" not in other and "}" not in other:
            return other == self.name
        elif isinstance(other, dict):
            return other['views'] == self.ddoc['views']

    def __neq__(self, other):
        return not self.__eq__(other)

    def __getitem__(self, name):
        return View(name, self.ddoc['views'][name], self)

    def views(self):
        return [View(view, self.ddoc['views'][view], self)
                for view in self.ddoc['views']]


class View(object):
    def __init__(self, name, view, ddoc=None):
        """Object for holding View information.

        Keyword arguments:
        name (str):  Name of the View within the Design Doc
        view (dict): View dictionary containing a 'map' and/or 'reduce' key
                     who's value should be of type string or unicode and
                     contain the JS MapReduce function
        """
        assert isinstance(name, (str, unicode)), \
            "name parameter must be of type string or unicode"
        assert isinstance(view, dict), \
            "view parameter must be of type dictionary"
        assert 'map' in view, \
            "name parameter must be of type string or unicode"
        assert isinstance(view['map'], (str, unicode)), \
            "name parameter must be of type string or unicode"
        self.name = name
        self.view = view
        self.ddoc = ddoc

    def __str__(self):
        """Return the name of the View when using print"""
        return self.name

    def __eq__(self, other):
        """Compare name or "views" section of the Design Doc. This allows the
        use of "for ddoc in" style syntax when used with
        Bucket().design_docs()"""
        if isinstance(other, str) and "{" not in other and "}" not in other:
            return other == self.name
        elif isinstance(other, dict):
            return other == {self.name: self.view}

    def __neq__(self, other):
        return not self.__eq__(other)

    def results(self, params={}):
        assert self.ddoc is not None, \
            "View must be connected to a saved Design Document to retrieve" \
            " results"

        rest = self.ddoc.bucket.server._rest()
        results = rest.view_results(self.ddoc.bucket.name, self.ddoc.name,
                                    self.name, params)

        return ViewResultsIterator(results)


class ViewResultsIterator(Set):
    def __init__(self, results):
        self.results = results['rows']
        if 'total_rows' in results:
            self.total_rows = results['total_rows']
        else:
            # reduced values don't really have a "length" so setting to 1
            self.total_rows = 1

    def __eq__(self, other):
        if len(self.results) == 1 and self.results[0]['key'] is None:
            return self.results[0]['value'] == other
        else:
            return self.results == other

    def __neq__(self, other):
        return not self.__eq__(other)

    def __iter__(self):
        return self

    def __len__(self):
        return self.total_rows

    def __contains__(self, item):
        return item in self.results

    def next(self):
        try:
            return self.results.pop(0)
        except IndexError:
            raise StopIteration
