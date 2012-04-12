#
# Copyright 2011, Couchbase, Inc.
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
import urllib

import logging

from rest_client import RestConnection
from couchbaseclient import VBucketAwareCouchbaseClient

logging.disable(logging.ERROR)


class Server(object):
    def __init__(self, host, username, password):
        server = {'ip':host.split(':')[0],
                  'port':host.split(':')[1],
                  'username':username,
                  'password':password
                  }
        if server['ip'] == server['port']:
            server['port'] = 8091

        self.servers = [server]
        self.servers_lock = Lock()

        self.rest_username = username
        self.rest_password = password

        server_config_uri = "http://{0}:{1}/pools/default".format(server['ip'], server['port'])
        config = ServerHelper.parse_server_config(server_config_uri, username, password)
        #couchApiBase will not be in node config before Couchbase Server 2.0
        self.couch_api_base = config["nodes"][0].get("couchApiBase")

        self.streaming_thread = Thread(name="streaming", target=self._start_streaming, args=())
        self.streaming_thread.daemon = True
        self.streaming_thread.start()


    def _start_streaming(self):
        # this will dynamically update servers
        urlopener = urllib.FancyURLopener()
        urlopener.prompt_user_passwd = lambda host, realm: (self.rest_username, self.rest_password)
        current_servers = True
        while current_servers:
            self.servers_lock.acquire()
            current_servers = deepcopy(self.servers)
            self.servers_lock.release()
            for server in current_servers:
                url = "http://{0}:{1}/poolsStreaming/default".format(server["ip"], server["port"])
                f = urlopener.open(url)
                while f:
                    try:
                        d = f.readline()
                        if not d:
                            # try next server if we get an EOF
                            f.close()
                            break
                    except:
                        # try next server if we fail to read
                        f.close()
                        break
                    try:
                        data = json.loads(d)
                    except:
                        continue

                    new_servers = []
                    nodes = data["nodes"]
                    for node in nodes:
                        if node["clusterMembership"] == "active" and node["status"] == "healthy":
                            hostport = node["hostname"]
                            couch_api_base = node.get("couchApiBase")
                            new_servers.append({"ip":hostport.split(":")[0],
                                                "port":int(hostport.split(":")[1]),
                                                "username":self.rest_username,
                                                "password":self.rest_password,
                                                "couchApiBase" : couch_api_base
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
        for rest_bucket in  rest.get_buckets():
            buckets.append(Bucket(rest_bucket.name, self))
        return buckets


    def create(self, bucket_name, bucket_password='', ram_quota_mb=100, replica=0):
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
                status, content = rest._http_request("http://{0}:{1}/pools/default/buckets/{2}".format(ip, port, bucket_name), method='GET', params='', headers=None, timeout=120)
            except ValueError:
                pass
            if json.loads(content)['basicStats']['quotaPercentUsed'] > 0.0:
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
        return server_info['ip'], server_info['port'], server_info['username'], server_info['password']



class BucketIterator(object):
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
    def __init__(self, bucket_name, server):
        self.server = server

        self.bucket_name = bucket_name
        rest = server._rest()
        self.bucket_password = rest.get_bucket(bucket_name).saslPassword


        ip, port, rest_username, rest_password = server._rest_info()
        self.mc_client = VBucketAwareCouchbaseClient("http://{0}:{1}/pools/default".format(ip, port), self.bucket_name, self.bucket_password)


    def append(self, key, value, cas=0):
        return self.mc_client.append(key, value, cas)

    def prepend(self, key, value, cas=0):
        return self.mc_client.prepend(key, value, cas)

    def incr(self, key, amt=1, init=0, exp=0):
        return self.mc_client.incr(key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        return self.mc_client.decr(key, amt, init, exp)

    def set(self, key, expiration, flags, value, cas=0):
        self.mc_client.set(key, expiration, flags, value, cas)

    def add(self, key, exp, flags, val):
        return self.mc_client.add(key, exp, flags, val)

    def replace(self, key, exp, flags, val):
        return self.mc_client.replace(key, exp, flags, val)

    def get(self, key):
        return self.mc_client.get(key)

    def send_get(self, key):
        return self.mc_client.send_get(key)

    def getl(self, key, exp=15):
        return self.mc_client.getl(key, exp)

    def cas(self, key, exp, flags, oldVal, val):
        return self.mc_client.cas(key, exp, flags, oldVal, val)

    def touch(self, key, exp):
        return self.mc_client.touch(key, exp)

    def gat(self, key, exp):
        return self.mc_client.gat(key, exp)

    def getMulti(self, keys):
        return self.mc_client.getMulti(keys)

    def stats(self, sub=''):
        return self.mc_client.stats(sub)

    def delete(self, key, cas=0):
        if key.startswith('_design/'):
            # this is a design doc, we need to handle it differently
            view = key.split('/')[1]

            rest = self.server._rest()
            rest.delete_view(self.bucket_name, view)
        else:
            return self.mc_client.delete(key, cas)


    def save(self, document):
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
            # this is a design doc, we need to handle it differently
            view = key.split('/')[1]

            rest = self.server._rest()
            rest.create_view(self.bucket_name, view, json.dumps(value))
        else:
            if '_rev' in value:
                # couchbase works in clobber mode so for a "set" _rev is useless
                del value['_rev']
            self.set(key, expiration, flags, json.dumps(value))

        return key


    def __setitem__(self, key, value):
        if isinstance(value, dict):
            self.set(key, value['expiration'], value['flags'], value['value'])
        else:
            self.set(key, 0, 0, value)


    def __getitem__(self, key):
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

        results = rest.view_results(self.bucket_name, view_doc, view_map, params, limit)
        if 'rows' in results:
            return results['rows']
        else:
            return None

class ServerHelper(object):
    @staticmethod
    def parse_server_config(uri, username = "", password = ""):
        urlopener = urllib.FancyURLopener()
        if len(username) > 0 and len(password) > 0:
            urlopener.prompt_user_passwd = lambda host, realm: (username, password)
        response = urlopener.open(uri)

        try:
            line = response.readline()
            data = json.loads(line)
            return data
        except:
            raise Exception("unexpected error - unable to parse server config at {0}".format(uri))

