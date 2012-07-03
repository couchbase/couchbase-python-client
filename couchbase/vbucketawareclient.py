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

import struct

from constants import VBucketAwareConstants
from memcachedclient import MemcachedClient


class VBucketAwareClient(MemcachedClient):
    def getl(self, key, exp=15, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket_id(key, vbucket)
        parts = self._doCmd(VBucketAwareConstants.CMD_GET_LOCKED, key, '',
                            struct.pack(VBucketAwareConstants.GETL_PKT_FMT,
                                        exp))
        return self._parse_get(parts)

    def touch(self, key, exp, vbucket=-1):
        """Touch a key in the memcached server."""
        self._set_vbucket_id(key, vbucket)
        return self._doCmd(VBucketAwareConstants.CMD_TOUCH, key, '',
                           struct.pack(VBucketAwareConstants.TOUCH_PKT_FMT,
                                       exp))
