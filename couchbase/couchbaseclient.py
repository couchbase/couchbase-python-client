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
from threading import Thread, Event, Lock
from exception import MemcachedTimeoutException, InvalidArgumentException

import logger
import hmac
import socket
import random
import exceptions
import zlib
import struct
import urllib
import warnings
try:
    import json
except:
    import simplejson as json
from copy import deepcopy
from rest_client import RestHelper, RestConnection


class MemcachedConstants(object):
    # Command constants
    CMD_GET = 0
    CMD_SET = 1
    CMD_ADD = 2
    CMD_REPLACE = 3
    CMD_DELETE = 4
    CMD_INCR = 5
    CMD_DECR = 6
    CMD_QUIT = 7
    CMD_FLUSH = 8
    CMD_GETQ = 9
    CMD_NOOP = 10
    CMD_VERSION = 11
    CMD_STAT = 0x10
    CMD_APPEND = 0x0e
    CMD_PREPEND = 0x0f
    CMD_TOUCH = 0x1c
    CMD_GAT = 0x1d

    # SASL stuff
    CMD_SASL_LIST_MECHS = 0x20
    CMD_SASL_AUTH = 0x21
    CMD_SASL_STEP = 0x22

    # Bucket extension
    CMD_CREATE_BUCKET = 0x85
    CMD_DELETE_BUCKET = 0x86
    CMD_LIST_BUCKETS = 0x87
    CMD_EXPAND_BUCKET = 0x88
    CMD_SELECT_BUCKET = 0x89

    CMD_STOP_PERSISTENCE = 0x80
    CMD_START_PERSISTENCE = 0x81
    CMD_SET_FLUSH_PARAM = 0x82
    CMD_RESTORE_FILE = 0x83
    CMD_RESTORE_ABORT = 0x84
    CMD_RESTORE_COMPLETE = 0x85
    #Online update
    CMD_START_ONLINEUPDATE = 0x86
    CMD_COMPLETE_ONLINEUPDATE = 0x87
    CMD_REVERT_ONLINEUPDATE = 0x88

    CMD_START_REPLICATION = 0x90
    CMD_STOP_REPLICATION = 0x91
    CMD_SET_TAP_PARAM = 0x92
    CMD_EVICT_KEY = 0x93

    # Replication
    CMD_TAP_CONNECT = 0x40
    CMD_TAP_MUTATION = 0x41
    CMD_TAP_DELETE = 0x42
    CMD_TAP_FLUSH = 0x43
    CMD_TAP_OPAQUE = 0x44
    CMD_TAP_VBUCKET_SET = 0x45
    CMD_TAP_CHECKPOINT_START = 0x46
    CMD_TAP_CHECKPOINT_END = 0x47

    # vbucket stuff
    CMD_SET_VBUCKET_STATE = 0x3d
    CMD_GET_VBUCKET_STATE = 0x3e
    CMD_DELETE_VBUCKET = 0x3f

    CMD_GET_LOCKED = 0x94

    CMD_SYNC = 0x96

    # TAP client registration
    CMD_DEREGISTER_TAP_CLIENT = 0x89

    # event IDs for the SYNC command responses
    CMD_SYNC_EVENT_PERSISTED = 1
    CMD_SYNC_EVENT_MODIFED = 2
    CMD_SYNC_EVENT_DELETED = 3
    CMD_SYNC_EVENT_REPLICATED = 4
    CMD_SYNC_INVALID_KEY = 5
    CMD_SYNC_INVALID_CAS = 6

    VB_STATE_ACTIVE = 1
    VB_STATE_REPLICA = 2
    VB_STATE_PENDING = 3
    VB_STATE_DEAD = 4
    VB_STATE_NAMES = {'active': VB_STATE_ACTIVE,
                      'replica': VB_STATE_REPLICA,
                      'pending': VB_STATE_PENDING,
                      'dead': VB_STATE_DEAD}

    COMMAND_NAMES = (dict(((globals()[k], k) for k in globals()
                     if k.startswith("CMD_"))))

    # TAP_OPAQUE types
    TAP_OPAQUE_ENABLE_AUTO_NACK = 0
    TAP_OPAQUE_INITIAL_VBUCKET_STREAM = 1
    TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC = 2
    TAP_OPAQUE_OPEN_CHECKPOINT = 3

    # TAP connect flags
    TAP_FLAG_BACKFILL = 0x01
    TAP_FLAG_DUMP = 0x02
    TAP_FLAG_LIST_VBUCKETS = 0x04
    TAP_FLAG_TAKEOVER_VBUCKETS = 0x08
    TAP_FLAG_SUPPORT_ACK = 0x10
    TAP_FLAG_REQUEST_KEYS_ONLY = 0x20
    TAP_FLAG_CHECKPOINT = 0x40
    TAP_FLAG_REGISTERED_CLIENT = 0x80

    TAP_FLAG_TYPES = {TAP_FLAG_BACKFILL: ">Q",
                      TAP_FLAG_REGISTERED_CLIENT: ">B"}

    # TAP per-message flags
    TAP_FLAG_ACK = 0x01
    TAP_FLAG_NO_VALUE = 0x02  # The value for key is not included in the packet

    # Flags, expiration
    SET_PKT_FMT = ">II"

    # flags
    GET_RES_FMT = ">I"

    # How long until the deletion takes effect.
    DEL_PKT_FMT = ""

    ## TAP stuff
    # eng-specific length, flags, ttl, [res, res, res]; item flags, exp
    TAP_MUTATION_PKT_FMT = ">HHbxxxII"
    TAP_GENERAL_PKT_FMT = ">HHbxxx"

    # amount, initial value, expiration
    INCRDECR_PKT_FMT = ">QQI"
    # Special incr expiration that means do not store
    INCRDECR_SPECIAL = 0xffffffff
    INCRDECR_RES_FMT = ">Q"

    # Time bomb
    FLUSH_PKT_FMT = ">I"

    # Touch commands
    # expiration
    TOUCH_PKT_FMT = ">I"
    GAT_PKT_FMT = ">I"
    GETL_PKT_FMT = "I"

    # 2 bit integer.  :/
    VB_SET_PKT_FMT = ">I"

    MAGIC_BYTE = 0x80
    REQ_MAGIC_BYTE = 0x80
    RES_MAGIC_BYTE = 0x81

    # magic, opcode, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
    REQ_PKT_FMT = ">BBHBBHIIQ"
    # magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
    RES_PKT_FMT = ">BBHBBHIIQ"
    # min recv packet size
    MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
    # The header sizes don't deviate
    assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

    EXTRA_HDR_FMTS = {
        CMD_SET: SET_PKT_FMT,
        CMD_ADD: SET_PKT_FMT,
        CMD_REPLACE: SET_PKT_FMT,
        CMD_INCR: INCRDECR_PKT_FMT,
        CMD_DECR: INCRDECR_PKT_FMT,
        CMD_DELETE: DEL_PKT_FMT,
        CMD_FLUSH: FLUSH_PKT_FMT,
        CMD_TAP_MUTATION: TAP_MUTATION_PKT_FMT,
        CMD_TAP_DELETE: TAP_GENERAL_PKT_FMT,
        CMD_TAP_FLUSH: TAP_GENERAL_PKT_FMT,
        CMD_TAP_OPAQUE: TAP_GENERAL_PKT_FMT,
        CMD_TAP_VBUCKET_SET: TAP_GENERAL_PKT_FMT,
        CMD_SET_VBUCKET_STATE: VB_SET_PKT_FMT,
        }

    EXTRA_HDR_SIZES = dict(
        [(k, struct.calcsize(v)) for (k, v) in EXTRA_HDR_FMTS.items()])

    ERR_UNKNOWN_CMD = 0x81
    ERR_NOT_FOUND = 0x1
    ERR_EXISTS = 0x2
    ERR_AUTH = 0x20
    ERR_AUTH_CONTINUE = 0x21


class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        supermsg = 'Memcached error #' + repr(status)
        if msg:
            supermsg += ":  " + msg
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
        self.s = socket.socket()
        self.s.connect_ex((host, port))
        self.r = random.Random()
        self.log = logger.logger("MemcachedClient")
        self.vbucket_count = 1024

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=MemcachedConstants.REQ_PKT_FMT,
                 magic=MemcachedConstants.REQ_MAGIC_BYTE):
        msg = struct.pack(fmt, magic,
                          cmd, len(key), len(extraHeader), dtype, vbucketId,
                          len(key) + len(extraHeader) + len(val), opaque, cas)
        self.s.send(msg + extraHeader + key + val)

    def _recvMsg(self):
        response = ""
        while len(response) < MemcachedConstants.MIN_RECV_PACKET:
            data = self.s.recv(MemcachedConstants.MIN_RECV_PACKET
                               - len(response))
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?)."
                                          " from %s" % (self.host))
            response += data
        assert len(response) == MemcachedConstants.MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas =\
        struct.unpack(MemcachedConstants.RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            data = self.s.recv(remaining)
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?)."
                                          " from %s" % (self.host))
            rv += data
            remaining -= len(data)

        assert (magic in (MemcachedConstants.RES_MAGIC_BYTE,
                          MemcachedConstants.REQ_MAGIC_BYTE)),\
            "Got magic: %d" % magic
        return cmd, errcode, opaque, cas, keylen, extralen, rv

    def _handleKeyedResponse(self, myopaque):
        cmd, errcode, opaque, cas, keylen, extralen, rv = self._recvMsg()
        assert myopaque is None or opaque == myopaque, \
        "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode:
            raise MemcachedError(errcode, rv)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data =\
            self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0):
        """Send a command and await its response."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val):
        return self._doCmd(cmd, key, val,
                           struct.pack(MemcachedConstants.SET_PKT_FMT, flags,
                                       exp), cas)

    def _cat(self, cmd, key, cas, val):
        return self._doCmd(cmd, key, val, '', cas)

    def append(self, key, value, cas=0, vbucket=-1):
        self._set_vbucket_id(key, vbucket)
        return self._cat(MemcachedConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key, value, cas=0, vbucket=-1):
        self._set_vbucket_id(key, vbucket)
        return self._cat(MemcachedConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val =\
            self._doCmd(cmd, key, '',
                        struct.pack(MemcachedConstants.INCRDECR_PKT_FMT,
                                amt, init, exp))
        return struct.unpack(MemcachedConstants.INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0, vbucket=-1):
        """Increment or create the named counter."""
        self._set_vbucket_id(key, vbucket)
        return self.__incrdecr(MemcachedConstants.CMD_INCR, key, amt, init,
                               exp)

    def decr(self, key, amt=1, init=0, exp=0, vbucket=-1):
        """Decrement or create the named counter."""
        self._set_vbucket_id(key, vbucket)
        return self.__incrdecr(MemcachedConstants.CMD_DECR, key, amt, init,
                               exp)

    def _set_vbucket_id(self, key, vbucket):
        if vbucket == -1:
            self.vbucketId = (zlib.crc32(key) >> 16) & (self.vbucket_count - 1)
        else:
            self.vbucketId = vbucket

    def set(self, key, exp, flags, val, vbucket=-1):
        """Set a value in the memcached server."""
        self._set_vbucket_id(key, vbucket)
        return self._mutate(MemcachedConstants.CMD_SET, key, exp, flags, 0,
                            val)

    def add(self, key, exp, flags, val, vbucket=-1):
        """Add a value in the memcached server iff it doesn't already exist."""
        self._set_vbucket_id(key, vbucket)
        return self._mutate(MemcachedConstants.CMD_ADD, key, exp, flags, 0,
                            val)

    def replace(self, key, exp, flags, val, vbucket=-1):
        """Replace a value in the memcached server iff it already exists."""
        self._set_vbucket_id(key, vbucket)
        return self._mutate(MemcachedConstants.CMD_REPLACE, key, exp, flags, 0,
                            val)

    def __parseGet(self, data, klen=0):
        flags = struct.unpack(MemcachedConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(MemcachedConstants.CMD_GET, key, '')

        return self.__parseGet(parts)

    def getl(self, key, exp=15, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(MemcachedConstants.CMD_GET_LOCKED, key, '',
                            struct.pack(MemcachedConstants.GETL_PKT_FMT, exp))
        return self.__parseGet(parts)

    def cas(self, key, exp, flags, oldVal, val, vbucket=-1):
        """CAS in a new value for the given key and comparison value."""
        self._set_vbucket_id(key, vbucket)
        self._mutate(MemcachedConstants.CMD_SET, key, exp, flags,
                     oldVal, val)

    def touch(self, key, exp, vbucket=-1):
        """Touch a key in the memcached server."""
        self._set_vbucket_id(key, vbucket)
        return self._doCmd(MemcachedConstants.CMD_TOUCH, key, '',
                           struct.pack(MemcachedConstants.TOUCH_PKT_FMT, exp))

    def gat(self, key, exp, vbucket=-1):
        """Get the value for a given key and touch it."""
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(MemcachedConstants.CMD_GAT, key, '',
                            struct.pack(MemcachedConstants.GAT_PKT_FMT, exp))
        return self.__parseGet(parts)

    def version(self):
        """Get the version of the server."""
        return self._doCmd(MemcachedConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""

        return set(self._doCmd(MemcachedConstants.CMD_SASL_LIST_MECHS,
                               '', '')[2].split(' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(MemcachedConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user,
                                                        password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        challenge = None
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError, e:
            if e.status != MemcachedConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(MemcachedConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def stop_persistence(self):
        return self._doCmd(MemcachedConstants.CMD_STOP_PERSISTENCE, '', '')

    def start_persistence(self):
        return self._doCmd(MemcachedConstants.CMD_START_PERSISTENCE, '', '')

    def set_flush_param(self, key, val):
        return self._doCmd(MemcachedConstants.CMD_SET_FLUSH_PARAM, key, val)

    def stop_replication(self):
        return self._doCmd(MemcachedConstants.CMD_STOP_REPLICATION, '', '')

    def start_replication(self):
        return self._doCmd(MemcachedConstants.CMD_START_REPLICATION, '', '')

    def start_onlineupdate(self):
        return self._doCmd(MemcachedConstants.CMD_START_ONLINEUPDATE, '', '')

    def complete_onlineupdate(self):
        return self._doCmd(MemcachedConstants.CMD_COMPLETE_ONLINEUPDATE, '',
                           '')

    def revert_onlineupdate(self):
        return self._doCmd(MemcachedConstants.CMD_REVERT_ONLINEUPDATE, '', '')

    def set_tap_param(self, key, val):
        return self._doCmd(MemcachedConstants.CMD_SET_TAP_PARAM, key, val)

    def set_vbucket_state(self, vbucket, stateName):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(MemcachedConstants.VB_SET_PKT_FMT,
                            MemcachedConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(MemcachedConstants.CMD_SET_VBUCKET_STATE, '',
                           state)

    def get_vbucket_state(self, vbucket):
        return self._doCmd(MemcachedConstants.CMD_GET_VBUCKET_STATE,
                           str(vbucket), '')

    def delete_vbucket(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(MemcachedConstants.CMD_DELETE_VBUCKET, '', '')

    def evict_key(self, key):
        return self._doCmd(MemcachedConstants.CMD_EVICT_KEY, key, '')

    def getMulti(self, keys):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued = dict(enumerate(keys))
        terminal = len(opaqued) + 10
        # Send all of the keys in quiet
        for k, v in opaqued.iteritems():
            self._sendCmd(MemcachedConstants.CMD_GETQ, v, '', k)

        self._sendCmd(MemcachedConstants.CMD_NOOP, '', '', terminal)

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
        self._sendCmd(MemcachedConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data =\
                self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(MemcachedConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0, vbucket=-1):
        """Delete the value for a given key within the memcached server."""
        self._set_vbucket_id(key, vbucket)
        return self._doCmd(MemcachedConstants.CMD_DELETE, key, '', '', cas)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(MemcachedConstants.CMD_FLUSH, '', '',
                           struct.pack(MemcachedConstants.FLUSH_PKT_FMT,
                                       timebomb))

    def bucket_select(self, name):
        return self._doCmd(MemcachedConstants.CMD_SELECT_BUCKET, name, '')

    def sync_persistence(self, keyspecs):
        payload = self._build_sync_payload(0x8, keyspecs)
        opaque, cas, data = self._doCmd(MemcachedConstants.CMD_SYNC, "",
                                        payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_mutation(self, keyspecs):
        payload = self._build_sync_payload(0x4, keyspecs)
        opaque, cas, data = self._doCmd(MemcachedConstants.CMD_SYNC, "",
                                        payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_replication(self, numReplicas, keyspecs):
        payload = self._build_sync_payload((numReplicas & 0x0f) << 4, keyspecs)
        opaque, cas, data = self._doCmd(MemcachedConstants.CMD_SYNC, "",
                                        payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_replication_or_persistence(self, numReplicas, keyspecs):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) |
                                            0x8, keyspecs)

        opaque, cas, data = self._doCmd(MemcachedConstants.CMD_SYNC, "",
                                        payload)
        return opaque, cas, self._parse_sync_response(data)

    def sync_replication_and_persistence(self, numReplicas, keyspecs):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) |
                                            0xA, keyspecs)

        opaque, cas, data = self._doCmd(MemcachedConstants.CMD_SYNC, "",
                                        payload)
        return opaque, cas, self._parse_sync_response(data)

    def _build_sync_payload(self, flags, keyspecs):
        payload = struct.pack(">I", flags)
        payload += struct.pack(">H", len(keyspecs))

        for spec in keyspecs:
            if not isinstance(spec, dict):
                raise TypeError("each keyspec must be a dict")
            if 'vbucket' not in spec:
                raise TypeError("missing vbucket property in keyspec")
            if 'key' not in spec:
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
            spec['cas'], spec['vbucket'], keylen, eventid =\
                struct.unpack(">QHHB", data[offset: offset + width])

            offset += width
            spec['key'] = data[offset: offset + keylen]
            offset += keylen

            if eventid == MemcachedConstants.CMD_SYNC_EVENT_PERSISTED:
                spec['event'] = 'persisted'
            elif eventid == MemcachedConstants.CMD_SYNC_EVENT_MODIFED:
                spec['event'] = 'modified'
            elif eventid == MemcachedConstants.CMD_SYNC_EVENT_DELETED:
                spec['event'] = 'deleted'
            elif eventid == MemcachedConstants.CMD_SYNC_EVENT_REPLICATED:
                spec['event'] = 'replicated'
            elif eventid == MemcachedConstants.CMD_SYNC_INVALID_KEY:
                spec['event'] = 'invalid key'
            elif spec['event'] == MemcachedConstants.CMD_SYNC_INVALID_CAS:
                spec['event'] = 'invalid cas'
            else:
                spec['event'] = eventid

            keyspecs.append(spec)
        return keyspecs

    def restore_file(self, filename):
        """Initiate restore of a given file."""
        return self._doCmd(MemcachedConstants.CMD_RESTORE_FILE, filename, '')

    def restore_complete(self):
        """Notify the server that we're done restoring."""
        return self._doCmd(MemcachedConstants.CMD_RESTORE_COMPLETE, '', '')

    def deregister_tap_client(self, tap_name):
        """Deregister the TAP client with a given name."""
        return self._doCmd(MemcachedConstants.CMD_DEREGISTER_TAP_CLIENT,
                           tap_name, '')


class CouchbaseClient(object):
    #poll server every few seconds to see if the vbucket-map
    #has changes
    def __init__(self, url, bucket, password="", verbose=False):
        self.log = logger.logger("CouchbaseClient")
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
        warnings.warn("Server is deprecated; use VBucketAwareCouchbaseClient"
                      " instead", DeprecationWarning)
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
        self.log = logger.logger("CommandDispatcher")
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
                item = self.queue.get(block=True, timeout=1)
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
                    except exceptions.EOFError, ex:
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
        if isinstance(ex, exceptions.EOFError):
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
        for node in bucket_info.nodes:
            if node.ip == ip and node.memcached == int(port):
                client = MemcachedClient(ip.encode('ascii', 'ignore'),
                                         int(port))
                client.vbucket_count = len(vBuckets)
                client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                       bucket_info.saslPassword
                                       .encode('ascii'))
                return client
        raise Exception(("unexpected error - unable to find ip:%s in this"
                         " cluster" % (ip)))
