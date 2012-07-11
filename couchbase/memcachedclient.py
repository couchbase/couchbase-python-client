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

import hmac
import socket
import random
import zlib
import struct
import warnings

from couchbase.logger import logger
from couchbase.constants import MemcachedConstants, VBucketAwareConstants
from couchbase.exception import MemcachedError


class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211):
        self.host = host
        self.port = port
        self.s = socket.socket()
        self.s.connect_ex((host, port))
        self.r = random.Random()
        self.log = logger("MemcachedClient")
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
                raise EOFError("Got empty data (remote died?)."
                                          " from %s" % (self.host))
            response += data
        assert len(response) == MemcachedConstants.MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas =\
        struct.unpack(MemcachedConstants.RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            data = self.s.recv(remaining)
            if data == '':
                raise EOFError("Got empty data (remote died?)."
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

    def _parse_get(self, data, klen=0):
        flags = struct.unpack(MemcachedConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(MemcachedConstants.CMD_GET, key, '')

        return self._parse_get(parts)

    def getl(self, key, exp=15, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        warnings.warn("MemcachedClient.getl is deprecated; use "
                      "VBucketAwareClient.getl instead", DeprecationWarning)
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(VBucketAwareConstants.CMD_GET_LOCKED, key, '',
                            struct.pack(VBucketAwareConstants.GETL_PKT_FMT,
                                        exp))
        return self._parse_get(parts)

    def cas(self, key, exp, flags, oldVal, val, vbucket=-1):
        """CAS in a new value for the given key and comparison value."""
        self._set_vbucket_id(key, vbucket)
        self._mutate(MemcachedConstants.CMD_SET, key, exp, flags,
                     oldVal, val)

    def touch(self, key, exp, vbucket=-1):
        """Touch a key in the memcached server."""
        warnings.warn("MemcachedClient.touch is deprecated; use "
                      "VBucketAwareClient.touch instead", DeprecationWarning)
        self._set_vbucket_id(key, vbucket)
        return self._doCmd(VBucketAwareConstants.CMD_TOUCH, key, '',
                           struct.pack(VBucketAwareConstants.TOUCH_PKT_FMT,
                                       exp))

    def gat(self, key, exp, vbucket=-1):
        """Get the value for a given key and touch it."""
        warnings.warn("MemcachedClient.gat is deprecated; use "
                      "VBucketAwareClient.gat instead", DeprecationWarning)
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(VBucketAwareConstants.CMD_GAT, key, '',
                            struct.pack(VBucketAwareConstants.GAT_PKT_FMT,
                                        exp))
        return self._parse_get(parts)

    def version(self):
        """Get the version of the server."""
        return self._doCmd(MemcachedConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Return an immutable, fronzenset of the supported SASL methods."""

        return frozenset(self._doCmd(MemcachedConstants.CMD_SASL_LIST_MECHS,
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
                rv[opaqued[opaque]] = self._parse_get((opaque, cas, data))
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
