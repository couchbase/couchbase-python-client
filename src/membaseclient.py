#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""
from Queue import Queue
from threading import Thread

import logger
import hmac
from multiprocessing import Event
import socket
import random
import struct
import exceptions
import crc32
from membase.api.rest_client import RestHelper, RestConnection

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
from memcacheConstants import TOUCH_PKT_FMT, GAT_PKT_FMT, GETL_PKT_FMT
import memcacheConstants

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        supermsg = 'Memcached error #' + `status`
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status = status
        self.msg = msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)


class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211):
        self.host = host
        self.port = port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect_ex((host, port))
        self.r = random.Random()
        self.log = logger.logger("MemcachedClient")

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=REQ_PKT_FMT, magic=REQ_MAGIC_BYTE):
        msg = struct.pack(fmt, magic,
                          cmd, len(key), len(extraHeader), dtype, vbucketId,
                          len(key) + len(extraHeader) + len(val), opaque, cas)
        self.s.send(msg + extraHeader + key + val)

    def _recvMsg(self):
        response = ""
        while len(response) < MIN_RECV_PACKET:
            data = self.s.recv(MIN_RECV_PACKET - len(response))
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
            response += data
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas =\
        struct.unpack(RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            data = self.s.recv(remaining)
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
            rv += data
            remaining -= len(data)

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), "Got magic: %d" % magic
        return cmd, errcode, opaque, cas, keylen, extralen, rv

    def _handleKeyedResponse(self, myopaque):
        cmd, errcode, opaque, cas, keylen, extralen, rv = self._recvMsg()
        assert myopaque is None or opaque == myopaque,\
        "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode:
            raise MemcachedError(errcode, rv)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0):
        """Send a command and await its response."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val):
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
                           cas)

    def _cat(self, cmd, key, cas, val):
        return self._doCmd(cmd, key, val, '', cas)

    def append(self, key, value, cas=0):
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key, value, cas=0):
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val = self._doCmd(cmd, key, '',
                                          struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp))
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0):
        """Increment or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        """Decrement or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp)

    def set(self, key, exp, flags, val, vbucket=-1):
        if vbucket == -1:
            self.vbucketId = crc32.crc32_hash(key) & 1023
        else:
            self.vbucketId = vbucket
        """Set a value in the memcached server."""
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val)

    def send_set(self, key, exp, flags, val):
        """Set a value in the memcached server without handling the response"""
        self.vbucketId = crc32.crc32_hash(key) & 1023
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_SET, key, val, opaque, struct.pack(SET_PKT_FMT, flags, exp), 0)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
                            val)

    def __parseGet(self, data, klen=0):
        flags = struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        if vbucket == -1:
            self.vbucketId = crc32.crc32_hash(key) & 1023
        else:
            self.vbucketId = vbucket
        parts = self._doCmd(memcacheConstants.CMD_GET, key, '')

        return self.__parseGet(parts)

    def send_get(self, key):
        """ sends a get message without parsing the response """
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_GET, key, '', opaque)

    def getl(self, key, exp=15):
        """Get the value for a given key within the memcached server."""
        parts = self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '',
                            struct.pack(memcacheConstants.GETL_PKT_FMT, exp))
        return self.__parseGet(parts)

    def cas(self, key, exp, flags, oldVal, val):
        """CAS in a new value for the given key and comparison value."""
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
                     oldVal, val)

    def touch(self, key, exp):
        """Touch a key in the memcached server."""
        return self._doCmd(memcacheConstants.CMD_TOUCH, key, '',
                           struct.pack(memcacheConstants.TOUCH_PKT_FMT, exp))

    def gat(self, key, exp):
        """Get the value for a given key and touch it within the memcached server."""
        parts = self._doCmd(memcacheConstants.CMD_GAT, key, '',
                            struct.pack(memcacheConstants.GAT_PKT_FMT, exp))
        return self.__parseGet(parts)

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(memcacheConstants.CMD_SASL_LIST_MECHS,
                               '', '')[2].split(' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(memcacheConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user, password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        challenge = None
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError, e:
            if e.status != memcacheConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(memcacheConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def stop_persistence(self):
        return self._doCmd(memcacheConstants.CMD_STOP_PERSISTENCE, '', '')

    def start_persistence(self):
        return self._doCmd(memcacheConstants.CMD_START_PERSISTENCE, '', '')

    def set_flush_param(self, key, val):
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val)

    def stop_replication(self):
        return self._doCmd(memcacheConstants.CMD_STOP_REPLICATION, '', '')

    def start_replication(self):
        return self._doCmd(memcacheConstants.CMD_START_REPLICATION, '', '')

    def start_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_START_ONLINEUPDATE, '', '')

    def complete_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_COMPLETE_ONLINEUPDATE, '', '')

    def revert_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_REVERT_ONLINEUPDATE, '', '')

    def set_tap_param(self, key, val):
        return self._doCmd(memcacheConstants.CMD_SET_TAP_PARAM, key, val)

    def set_vbucket_state(self, vbucket, stateName):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(memcacheConstants.VB_SET_PKT_FMT,
                            memcacheConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(memcacheConstants.CMD_SET_VBUCKET_STATE, '',
                           state)

    def get_vbucket_state(self, vbucket):
        return self._doCmd(memcacheConstants.CMD_GET_VBUCKET_STATE,
                           str(vbucket), '')

    def delete_vbucket(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_DELETE_VBUCKET, '', '')

    def evict_key(self, key):
        return self._doCmd(memcacheConstants.CMD_EVICT_KEY, key, '')

    def getMulti(self, keys):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued = dict(enumerate(keys))
        terminal = len(opaqued) + 10
        # Send all of the keys in quiet
        for k, v in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k)

        self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        rv = {}
        done = False
        while not done:
            opaque, cas, data = self._handleSingleResponse(None)
            if opaque != terminal:
                rv[opaqued[opaque]] = self.__parseGet((opaque, cas, data))
            else:
                done = True

        return rv

    def stats(self, sub=''):
        """Get stats."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0, vbucket=-1):
        if vbucket == -1:
            self.vbucketId = crc32.crc32_hash(key) & 1023
        """Delete the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
                           struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name):
        return self._doCmd(memcacheConstants.CMD_SELECT_BUCKET, name, '')

    def sync_persistence(self, keyspecs):
        payload = self._build_sync_payload(0x8, keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return opaque, cas, self._parse_sync_response(data)


    def sync_mutation(self, keyspecs):
        payload = self._build_sync_payload(0x4, keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_replication(self, numReplicas, keyspecs):
        payload = self._build_sync_payload((numReplicas & 0x0f) << 4, keyspecs)

        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_replication_or_persistence(self, numReplicas, keyspecs):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0x8, keyspecs)

        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_replication_and_persistence(self, numReplicas, keyspecs):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0xA, keyspecs)

        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return opaque, cas, self._parse_sync_response(data)

    def _build_sync_payload(self, flags, keyspecs):
        payload = struct.pack(">I", flags)
        payload += struct.pack(">H", len(keyspecs))

        for spec in keyspecs:
            if not isinstance(spec, dict):
                raise TypeError("each keyspec must be a dict")
            if not spec.has_key('vbucket'):
                raise TypeError("missing vbucket property in keyspec")
            if not spec.has_key('key'):
                raise TypeError("missing key property in keyspec")

            payload += struct.pack(">Q", spec.get('cas', 0))
            payload += struct.pack(">H", spec['vbucket'])
            payload += struct.pack(">H", len(spec['key']))
            payload += spec['key']

        return payload


    def _parse_sync_response(self, data):
        keyspecs = []
        nkeys = struct.unpack(">H", data[0: struct.calcsize("H")])[0]
        offset = struct.calcsize("H")
        for i in xrange(nkeys):
            spec = {}
            width = struct.calcsize("QHHB")
            (spec['cas'], spec['vbucket'], keylen, eventid) = struct.unpack(">QHHB", data[offset: offset + width])

        offset += width
        spec['key'] = data[offset: offset + keylen]
        offset += keylen

        if eventid == memcacheConstants.CMD_SYNC_EVENT_PERSISTED:
            spec['event'] = 'persisted'
        elif eventid == memcacheConstants.CMD_SYNC_EVENT_MODIFED:
            spec['event'] = 'modified'
        elif eventid == memcacheConstants.CMD_SYNC_EVENT_DELETED:
            spec['event'] = 'deleted'
        elif eventid == memcacheConstants.CMD_SYNC_EVENT_REPLICATED:
            spec['event'] = 'replicated'
        elif eventid == memcacheConstants.CMD_SYNC_INVALID_KEY:
            spec['event'] = 'invalid key'
        elif spec['event'] == memcacheConstants.CMD_SYNC_INVALID_CAS:
            spec['event'] = 'invalid cas'
        else:
            spec['event'] = eventid

        keyspecs.append(spec)
        return keyspecs

    def restore_file(self, filename):
        """Initiate restore of a given file."""
        return self._doCmd(memcacheConstants.CMD_RESTORE_FILE, filename, '', '', 0)


    def restore_complete(self):
        """Notify the server that we're done restoring."""
        return self._doCmd(memcacheConstants.CMD_RESTORE_COMPLETE, '', '')


    def deregister_tap_client(self, tap_name):
        """Deregister the TAP client with a given name."""
        return self._doCmd(memcacheConstants.CMD_DEREGISTER_TAP_CLIENT, tap_name, '')


class VBucketAwareMemcachedClient(object):
    #poll server every few seconds to see if the vbucket-map
    #has changes
    def __init__(self, server, bucket):
        self.log = logger.logger("VBucketAwareMemcachedClient")
        self.bucket = bucket
        self._memcacheds = {}
        self._vBucketMap = {}
        self.rest = RestConnection(server)
        self.__init__vbucket_map(self.rest, bucket)
        self.dispatcher = CommandDispatcher(self)
        self.dispatcher_thread = Thread(name="dispatcher-thread", target=self._start_dispatcher)
        self.dispatcher_thread.start()
        #kick off dispatcher

    def _start_dispatcher(self):
        self.dispatcher.dispatch()

    def reconfig_vbucket_map(self):
        self.__init__vbucket_map(self.rest, self.bucket)

    def __init__vbucket_map(self, rest, bucket):
        vb_ready = RestHelper(rest).vbucket_map_ready(bucket, 60)
        if not vb_ready:
            raise Exception("vbucket map is not ready for bucket {0}".format(bucket))
        vBuckets = rest.get_vbuckets(bucket)
        nodes = rest.get_nodes()
        for vBucket in vBuckets:
            masterIp = vBucket.master.split(":")[0]
            masterPort = int(vBucket.master.split(":")[1])
            self._vBucketMap[vBucket.id] = vBucket.master
            if not vBucket.master in self._memcacheds:
                server = {"ip": masterIp, "port": rest.port, "rest_username": rest.username,
                          "rest_password": rest.password, "username": rest.username, "password": rest.password}
                try:
                    for node in nodes:
                        if node.ip == masterIp and node.memcached == masterPort:
                            self._memcacheds[vBucket.master] =\
                            MemcachedClientHelper.direct_client(server, bucket)
                            break
                except Exception as ex:
                    msg = "unable to establish connection to {0}.cleanup open connections"
                    self.log.warn(msg.format(masterIp))
                    self.done()
                    raise ex


    def memcached(self, key):
        vBucketId = crc32.crc32_hash(key) & 1023
        if vBucketId not in self._vBucketMap:
            msg = "vbucket map does not have an entry for vb : {0}"
            raise Exception(msg.format(vBucketId))
        if self._vBucketMap[vBucketId] not in self._memcacheds:
            msg = "poxi does not have a mc connection for server : {0}"
            raise Exception(msg.format(self._vBucketMap[vBucketId]))
        return self._memcacheds[self._vBucketMap[vBucketId]]

    def not_my_vbucket_memcached(self, key):
        vBucketId = crc32.crc32_hash(key) & 1023
        which_mc = self._vBucketMap[vBucketId]
        for server in self._memcacheds:
            if server != which_mc:
                return self._memcacheds[server]


    def done(self):
        [self._memcacheds[ip].close() for ip in self._memcacheds]

    def _respond(self,item,event):
        event.wait(3)
        if not event.is_set():
            raise Exception("timeout")
        return item["response"]["return"]

    def get(self, key, vbucket=-1):
        event = Event()
        item = {"operation": "get", "key": key, "event": event, "response": {}}
        self.dispatcher.queue.put(item)
        return self._respond(item,event)

    def gat(self, key, exp):
        return super(VBucketAwareMemcachedClient, self).gat(key, exp)

    def touch(self, key, exp):
        return super(VBucketAwareMemcachedClient, self).touch(key, exp)

    def cas(self, key, exp, flags, oldVal, val):
        super(VBucketAwareMemcachedClient, self).cas(key, exp, flags, oldVal, val)

    def delete_vbucket(self, vbucket):
        return super(VBucketAwareMemcachedClient, self).delete_vbucket(vbucket)

    def decr(self, key, amt=1, init=0, exp=0):
        return super(VBucketAwareMemcachedClient, self).decr(key, amt, init, exp)

    def set(self, key, expiry, flags, value):
        event = Event()
        item = {"operation": "set", "key": key, "expiry": expiry, "flags": flags, "value": value, "event": event,
                "response": {}}
        self.dispatcher.queue.put(item)
        return self._respond(item, event)

    def add(self, key, exp, flags, val):
        return super(VBucketAwareMemcachedClient, self).add(key, exp, flags, val)

    def delete(self, key, cas=0, vbucket=-1):
        return super(VBucketAwareMemcachedClient, self).delete(key, cas, vbucket)

    def prepend(self, key, value, cas=0):
        return super(VBucketAwareMemcachedClient, self).prepend(key, value, cas)

    def getl(self, key, exp=15):
        return super(VBucketAwareMemcachedClient, self).getl(key, exp)

    def replace(self, key, exp, flags, val):
        return super(VBucketAwareMemcachedClient, self).replace(key, exp, flags, val)

    def getMulti(self, keys):
        return super(VBucketAwareMemcachedClient, self).getMulti(keys)

    def flush(self, timebomb=0):
        return super(VBucketAwareMemcachedClient, self).flush(timebomb)

    def incr(self, key, amt=1, init=0, exp=0):
        return super(VBucketAwareMemcachedClient, self).incr(key, amt, init, exp)


class CommandDispatcher(object):
    #this class contains a queue where request

    def __init__(self, vbaware):
        #have a queue , in case of not my vbucket error
        #let's reinitialize the config/memcached socket connections ?
        self.queue = Queue(10000)
        self.status = "initialized"
        self.vbaware = vbaware
        self.reconfig_callback = self.vbaware.reconfig_vbucket_map


    def shutdown(self):
        self.status = "shutdown"

    def reconfig_completed(self):
        self.status = "ok"

    def dispatch(self):
        while self.status != "shutdown":
            #wait if its reconfiguring the vbucket-map
            if self.status == "vbucketmap-configuration":
                continue
            item = self.queue.get()
            if item:
                try:
                    response = self.do(item)
                except Exception as ex:
                    if isinstance(ex, MemcachedError):
                        #put the item back in the queue
                        self.queue.put(item)
                        if ex.status == 7:
                            self.reconfig_callback(self.reconfig_callback)


    def do(self, item):
        #find which vbucket this belongs to and then run the operation on that ?
        if item["operation"] == "get":
            key = item["key"]
            item["response"]["return"] = self.vbaware.memcached(key).get(key)
            item["event"].set()
        elif item["operation"] == "set":
            key = item["key"]
            expiry = item["expiry"]
            flags = item["flags"]
            value = item["value"]
            item["response"]["return"] = self.vbaware.memcached(key).set(key, expiry, flags, value)
            item["event"].set()


class MemcachedClientHelper(object):
    @staticmethod
    def direct_client(server, bucket):
        node = RestConnection(server).get_nodes_self()
        if isinstance(server, dict):
            ip = server["ip"]
        else:
            ip = server.ip
        client = MemcachedClient(ip, node.memcached)
        bucket_info = RestConnection(server).get_bucket(bucket)
        #todo raise exception for not bucket_info
        client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                               bucket_info.saslPassword.encode('ascii'))
        return client

    @staticmethod
    def proxy_client(server, bucket):
        #for this bucket on this node what is the proxy ?
        rest = RestConnection(server)
        bucket_info = rest.get_bucket(bucket)
        nodes = bucket_info.nodes
        for node in nodes:
            if node.ip == server.ip and int(node.port) == int(server.port):
                client = MemcachedClient(server.ip, node.moxi)
                if bucket_info.authType == "sasl":
                    client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                           bucket_info.saslPassword.encode('ascii'))
                return client
        raise Exception("unable to find {0} in get_nodes()".format(server.ip))






