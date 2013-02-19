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

from Queue import Queue, Full, Empty
from threading import Thread, Lock

import socket
import zlib
import urllib
import warnings
import time

try:
    import json
except ImportError:
    import simplejson as json
from copy import deepcopy

from couchbase.logger import logger
from couchbase.rest_client import RestHelper, RestConnection
from couchbase.exception import MemcachedError, MemcachedTimeoutException, \
    InvalidArgumentException
from couchbase.memcachedclient import MemcachedClient
from couchbase.vbucketawareclient import VBucketAwareClient
from couchbase.event import Event


class CouchbaseClient(object):
    #poll server every few seconds to see if the vbucket-map
    #has changes
    def __init__(self, url, bucket, password="", verbose=False):
        self.log = logger("CouchbaseClient")
        self.bucket = bucket
        self.rest_username = bucket
        self.rest_password = password
        self._memcacheds = {}
        self._vBucketMap = {}
        self._vBucketMap_lock = Lock()
        self._vBucketMapFastForward = {}
        self._vBucketMapFastForward_lock = Lock()
        #TODO: use regular expressions to parse the url
        server = {}
        if not bucket:
            raise InvalidArgumentException("bucket can not be an empty string",
                                           parameters="bucket")
        if not url:
            raise InvalidArgumentException("url can not be an empty string",
                                           parameters="url")
        if (url.find("http://") != -1 and url.rfind(":") != -1 and
            url.find("/pools/default") != -1):
            server["ip"] = (url[url.find("http://")
                            + len("http://"):url.rfind(":")])
            server["port"] = url[url.rfind(":") + 1:url.find("/pools/default")]
            server["username"] = self.rest_username
            server["password"] = self.rest_password
        else:
            raise InvalidArgumentException("invalid url string",
                                           parameters=url)
        self.servers = [server]
        self.servers_lock = Lock()
        self.rest = RestConnection(server)
        self.reconfig_vbucket_map()
        self.init_vbucket_connections()
        self.dispatcher = CommandDispatcher(self)
        self.dispatcher_thread = Thread(name="dispatcher-thread",
                                        target=self._start_dispatcher)
        self.dispatcher_thread.daemon = True
        self.dispatcher_thread.start()
        self.streaming_thread = Thread(name="streaming",
                                       target=self._start_streaming, args=())
        self.streaming_thread.daemon = True
        self.streaming_thread.start()
        self.verbose = verbose

    def _start_dispatcher(self):
        self.dispatcher.dispatch()

    def _start_streaming(self):
        # This will dynamically update vBucketMap, vBucketMapFastForward,
        # and servers
        urlopener = urllib.FancyURLopener()
        urlopener.prompt_user_passwd = lambda: (self.rest_username,
                                                self.rest_password)
        current_servers = True
        while current_servers:
            self.servers_lock.acquire()
            current_servers = deepcopy(self.servers)
            self.servers_lock.release()
            for server in current_servers:
                response = urlopener.open("http://%s:%s/pools/default/bucketsS"
                                          "treaming/%s" % (server["ip"],
                                          server["port"], self.bucket))
                while response:
                    try:
                        line = response.readline()
                        if not line:
                            # try next server if we get an EOF
                            response.close()
                            break
                    except:
                        # try next server if we fail to read
                        response.close()
                        break
                    try:
                        data = json.loads(line)
                    except:
                        continue

                    serverlist = data['vBucketServerMap']['serverList']
                    vbucketmapfastforward = {}
                    index = 0
                    if 'vBucketMapForward' in data['vBucketServerMap']:
                        for vbucket in\
                            data['vBucketServerMap']['vBucketMapForward']:
                            vbucketmapfastforward[index] =\
                                serverlist[vbucket[0]]
                            index += 1
                        self._vBucketMapFastForward_lock.acquire()
                        self._vBucketMapFastForward =\
                            deepcopy(vbucketmapfastforward)
                        self._vBucketMapFastForward_lock.release()
                    vbucketmap = {}
                    index = 0
                    for vbucket in data['vBucketServerMap']['vBucketMap']:
                        vbucketmap[index] = serverlist[vbucket[0]]
                        index += 1

                    # only update vBucketMap if we don't have a fastforward
                    # on a not_mb_vbucket error, we already update the
                    # vBucketMap from the fastforward map
                    if not vbucketmapfastforward:
                        self._vBucketMap_lock.acquire()
                        self._vBucketMap = deepcopy(vbucketmap)
                        self._vBucketMap_lock.release()

                    new_servers = []
                    nodes = data["nodes"]
                    for node in nodes:
                        if (node["clusterMembership"] == "active" and
                            node["status"] == "healthy"):
                            ip, port = node["hostname"].split(":")
                            new_servers.append({"ip": ip,
                                                "port": port,
                                                "username": self.rest_username,
                                                "password": self.rest_password
                                                })
                    new_servers.sort()
                    self.servers_lock.acquire()
                    self.servers = deepcopy(new_servers)
                    self.servers_lock.release()

    def init_vbucket_connections(self):
        # start up all vbucket connections
        self._vBucketMap_lock.acquire()
        vbucketcount = len(self._vBucketMap)
        self._vBucketMap_lock.release()
        for i in range(vbucketcount):
            self.start_vbucket_connection(i)

    def start_vbucket_connection(self, vbucket):
        self._vBucketMap_lock.acquire()
        server = deepcopy(self._vBucketMap[vbucket])
        self._vBucketMap_lock.release()
        serverIp, serverPort = server.split(":")
        if not server in self._memcacheds:
            self._memcacheds[server] =\
                MemcachedClientHelper.direct_client(self.rest, serverIp,
                                                    serverPort, self.bucket)

    def start_vbucket_fastforward_connection(self, vbucket):
        self._vBucketMapFastForward_lock.acquire()
        if not vbucket in self._vBucketMapFastForward:
            self._vBucketMapFastForward_lock.release()
            return
        server = deepcopy(self._vBucketMapFastForward[vbucket])
        self._vBucketMapFastForward_lock.release()
        serverIp, serverPort = server.split(":")
        if not server in self._memcacheds:
            self._memcacheds[server] =\
                MemcachedClientHelper.direct_client(self.rest, serverIp,
                                                    serverPort, self.bucket)

    def restart_vbucket_connection(self, vbucket):
        self._vBucketMap_lock.acquire()
        server = deepcopy(self._vBucketMap[vbucket])
        self._vBucketMap_lock.release()
        serverIp, serverPort = server.split(":")
        if server in self._memcacheds:
            self._memcacheds[server].close()
        self._memcacheds[server] =\
            MemcachedClientHelper.direct_client(self.rest, serverIp,
                                                serverPort, self.bucket)

    def reconfig_vbucket_map(self, vbucket=-1):
        vb_ready = RestHelper(self.rest).vbucket_map_ready(self.bucket, 60)
        if not vb_ready:
            raise Exception("vbucket map is not ready for bucket %s" %
                            (self.bucket))
        vBuckets = self.rest.get_vbuckets(self.bucket)
        self.vbucket_count = len(vBuckets)

        self._vBucketMap_lock.acquire()
        for vBucket in vBuckets:
            if vBucket.id == vbucket or vbucket == -1:
                self._vBucketMap[vBucket.id] = vBucket.master
        self._vBucketMap_lock.release()

    def memcached(self, key, fastforward=False):
        self._vBucketMap_lock.acquire()
        self._vBucketMapFastForward_lock.acquire()
        vBucketId = (zlib.crc32(key) >> 16) & (len(self._vBucketMap) - 1)

        if fastforward and vBucketId in self._vBucketMapFastForward:
            # only try the fastforward if we have an entry
            # otherwise we just wait for the main map to update
            self.start_vbucket_fastforward_connection(vBucketId)
            self._vBucketMap[vBucketId] =\
                self._vBucketMapFastForward[vBucketId]

        if vBucketId not in self._vBucketMap:
            msg = "vbucket map does not have an entry for vb : %s"
            self._vBucketMapFastForward_lock.release()
            self._vBucketMap_lock.release()
            raise Exception(msg % (vBucketId))
        if self._vBucketMap[vBucketId] not in self._memcacheds:
            msg = "smart client does not have a mc connection for server : %s"
            self._vBucketMapFastForward_lock.release()
            self._vBucketMap_lock.release()
            raise Exception(msg % (self._vBucketMap[vBucketId]))
        r = self._memcacheds[self._vBucketMap[vBucketId]]
        self._vBucketMapFastForward_lock.release()
        self._vBucketMap_lock.release()
        return r

    def vbucketid(self, key):
        self._vBucketMap_lock.acquire()
        r = (zlib.crc32(key) >> 16) & (len(self._vBucketMap) - 1)
        self._vBucketMap_lock.release()
        return r

    def done(self):
        if self.dispatcher:
            self.dispatcher.shutdown()
            if self.verbose:
                self.log.info("dispatcher shutdown invoked")
            [self._memcacheds[ip].close() for ip in self._memcacheds]
            if self.verbose:
                self.log.info("closed all memcached open connections")
            self.dispatcher = None

    def _respond(self, item, event):
        timeout = 30
        event.wait(timeout)
        if not event.isSet():
            # if we timeout, then try to reconnect to the server
            # responsible for this vbucket
            self.restart_vbucket_connection(self.vbucketid(item['key']))
            raise MemcachedTimeoutException(item, timeout)
        if "error" in item["response"]:
            raise item["response"]["error"]
        return item["response"]["return"]

    def get(self, key):
        event = Event()
        item = {"operation": "get", "key": key, "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def gat(self, key, expiry):
        event = Event()
        item = {"operation": "gat", "key": key, "expiry": expiry,
                "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def touch(self, key, expiry):
        event = Event()
        item = {"operation": "touch", "key": key, "expiry": expiry,
                "event": event,
                "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def cas(self, key, expiry, flags, old_value, value):
        event = Event()
        item = {"operation": "cas", "key": key, "expiry": expiry,
                "flags": flags, "old_value": old_value, "value": value,
                "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def decr(self, key, amount=1, init=0, expiry=0):
        event = Event()
        item = {"operation": "decr", "key": key, "amount": amount,
                "init": init, "expiry": expiry, "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def set(self, key, expiry, flags, value):
        event = Event()
        item = {"operation": "set", "key": key, "expiry": expiry,
                "flags": flags, "value": value, "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def add(self, key, expiry, flags, value):
        event = Event()
        item = {"operation": "add", "key": key, "expiry": expiry,
                "flags": flags, "value": value, "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def append(self, key, value, cas=0):
        event = Event()
        item = {"operation": "append", "key": key, "cas": cas, "value": value,
                "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def delete(self, key, cas=0):
        event = Event()
        item = {"operation": "delete", "key": key, "cas": cas, "event": event,
                "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def prepend(self, key, value, cas=0):
        event = Event()
        item = {"operation": "prepend", "key": key, "cas": cas, "value": value,
                "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def getl(self, key, expiry=15):
        event = Event()
        item = {"operation": "getl", "key": key, "expiry": expiry,
                "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def replace(self, key, expiry, flags, value):
        event = Event()
        item = {"operation": "replace", "key": key, "expiry": expiry,
                "flags": flags, "value": value, "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def incr(self, key, amount=1, init=0, expiry=0):
        event = Event()
        item = {"operation": "incr", "key": key, "amount": amount,
                "init": init, "expiry": expiry, "event": event, "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)

    def flush(self, wait_time=0):
        event = Event()
        item = {"operation": "flush", "expiry": wait_time, "event": event,
                "response": {}}
        self.dispatcher.put(item)
        return self._respond(item, event)


class VBucketAwareCouchbaseClient(CouchbaseClient):
    def __init__(self, host, username, password):
        warnings.warn("VBucketAwareCouchbaseClient is deprecated; use "
                      "CouchbaseClient instead", DeprecationWarning)
        CouchbaseClient.__init__(self, host, username, password)


class CommandDispatcher(object):
    #this class contains a queue where request

    def __init__(self, vbaware, verbose=False):
        #have a queue , in case of not my vbucket error
        #let's reinitialize the config/memcached socket connections ?
        self.queue = Queue(10000)
        self.status = "initialized"
        self.vbaware = vbaware
        self.reconfig_callback = self.vbaware.reconfig_vbucket_map
        self.start_connection_callback = self.vbaware.start_vbucket_connection
        self.restart_connection_callback =\
            self.vbaware.restart_vbucket_connection
        self.verbose = verbose
        self.log = logger("CommandDispatcher")
        self._dispatcher_stopped_event = Event()

    def put(self, item):
        try:
            self.queue.put(item, False)
        except Full:
            #TODO: add a better error message here
            raise Exception("queue is full")

    def shutdown(self):
        if self.status != "shutdown":
            self.status = "shutdown"
            if self.verbose:
                self.log.info("dispatcher shutdown command received")
        self._dispatcher_stopped_event.wait(2)

    def reconfig_completed(self):
        self.status = "ok"

    def dispatch(self):
        while (self.status != "shutdown" or (self.status == "shutdown" and
               self.queue.qsize() > 0)):
            #wait if its reconfiguring the vbucket-map
            if self.status == "vbucketmap-configuration":
                continue
            try:
                item = self.queue.get(block=False, timeout=1)
                if item:
                    try:
                        self.do(item)
                        # do will only raise not_my_vbucket_exception,
                        # EOF and socket.error
                    except MemcachedError, ex:
                        # if we get a not_my_vbucket then requeue item
                        #  with fast forward map vbucket
                        self.log.error(ex)
                        self.reconfig_callback(ex.vbucket)
                        self.start_connection_callback(ex.vbucket)
                        item["fastforward"] = True
                        self.queue.put(item)
                    except EOFError, ex:
                        # we go an EOF error, restart the connection
                        self.log.error(ex)
                        self.restart_connection_callback(ex.vbucket)
                        self.queue.put(item)
                    except socket.error, ex:
                        # we got a socket error, restart the connection
                        self.log.error(ex)
                        self.restart_connection_callback(ex.vbucket)
                        self.queue.put(item)

            except Empty:
                time.sleep(0.5)
                pass
        if self.verbose:
            self.log.info("dispatcher stopped")
            self._dispatcher_stopped_event.set()

    def _raise_if_recoverable(self, ex, item):
        if isinstance(ex, MemcachedError) and ex.status == 7:
            ex.vbucket = item["vbucket"]
            print ex
            self.log.error("got not my vb error. key: %s, vbucket: %s" %
                           (item["key"], item["vbucket"]))
            raise ex
        if isinstance(ex, EOFError):
            ex.vbucket = item["vbucket"]
            print ex
            self.log.error("got EOF")
            raise ex
        if isinstance(ex, socket.error):
            ex.vbucket = item["vbucket"]
            print ex
            self.log.error("got socket.error")
            raise ex
        item["response"]["error"] = ex

    def do(self, item):
        #find which vbucket this belongs to and then run the operation on that
        if "key" in item:
            item["vbucket"] = self.vbaware.vbucketid(item["key"])
        if not "fastforward" in item:
            item["fastforward"] = False
        item["response"]["return"] = None

        if item["operation"] == "get":
            key = item["key"]
            try:
                item["response"]["return"] =\
                    self.vbaware.memcached(key, item["fastforward"]).get(key)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "set":
            key = item["key"]
            expiry = item["expiry"]
            flags = item["flags"]
            value = item["value"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.set(key, expiry, flags,
                                                      value)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "add":
            key = item["key"]
            expiry = item["expiry"]
            flags = item["flags"]
            value = item["value"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.add(key, expiry, flags,
                                                      value)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "replace":
            key = item["key"]
            expiry = item["expiry"]
            flags = item["flags"]
            value = item["value"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.replace(key, expiry, flags,
                                                          value)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "delete":
            key = item["key"]
            cas = item["cas"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.delete(key, cas)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "prepend":
            key = item["key"]
            cas = item["cas"]
            value = item["value"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.prepend(key, value, cas)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "append":
            key = item["key"]
            cas = item["cas"]
            value = item["value"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.append(key, value, cas)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "getl":
            key = item["key"]
            expiry = item["expiry"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.getl(key, expiry)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "gat":
            key = item["key"]
            expiry = item["expiry"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.gat(key, expiry)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "touch":
            key = item["key"]
            expiry = item["expiry"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.touch(key, expiry)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "incr":
            key = item["key"]
            amount = item["amount"]
            init = item["init"]
            expiry = item["expiry"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.incr(key, amount, init,
                                                       expiry)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "decr":
            key = item["key"]
            amount = item["amount"]
            init = item["init"]
            expiry = item["expiry"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.decr(key, amount, init,
                                                       expiry)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()

        elif item["operation"] == "cas":
            key = item["key"]
            expiry = item["expiry"]
            flags = item["flags"]
            old_value = item["old_value"]
            value = item["value"]
            try:
                conn = self.vbaware.memcached(key, item["fastforward"])
                item["response"]["return"] = conn.cas(key, expiry, flags,
                                                      old_value, value)
            except Exception, ex:
                self._raise_if_recoverable(ex, item)
            item["event"].set()
        elif item["operation"] == "flush":
            wait_time = item["expiry"]
            for key, conn in self.vbaware._memcacheds.items():
                conn.flush(wait_time)
            item["response"]["return"] = True
            item["event"].set()


class MemcachedClientHelper(object):
    @staticmethod
    def direct_client(rest, ip, port, bucket):
        bucket_info = rest.get_bucket(bucket)
        vBuckets = bucket_info.vbuckets
        vbucket_count = len(vBuckets)
        for node in bucket_info.nodes:
            if node.ip == ip and node.memcached == int(port):
                if vbucket_count == 0:
                    client = MemcachedClient(ip.encode('ascii', 'ignore'),
                                             int(port))
                else:
                    client = VBucketAwareClient(ip.encode('ascii', 'ignore'),
                                             int(port))
                client.vbucket_count = vbucket_count
                client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                       bucket_info.saslPassword
                                       .encode('ascii'))
                return client
        raise Exception(("unexpected error - unable to find ip:%s in this"
                         " cluster" % (ip)))
