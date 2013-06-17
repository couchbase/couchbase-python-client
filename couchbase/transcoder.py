#
# Copyright 2013, Couchbase, Inc.
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

import warnings
import json
import pickle

from couchbase import (FMT_JSON, FMT_BYTES, FMT_UTF8, FMT_PICKLE, FMT_MASK)
from couchbase.exceptions import ValueFormatError
from couchbase._libcouchbase import Transcoder


class TranscoderPP(object):
    """
    This is a pure-python Transcoder class. It is here only to show a reference
    implementation. It is recommended that you subclass from
    the :class:`Transcoder` object instead if all the methods are not
    implemented.
    """

    def encode_key(self, key):
        ret = (self.encode_value(key, FMT_UTF8))[0]
        return ret

    def decode_key(self, key):
        return self.decode_value(key, FMT_UTF8)

    def encode_value(self, value, format):
        fbase = format & FMT_MASK

        if fbase not in (FMT_PICKLE, FMT_JSON, FMT_BYTES, FMT_UTF8):
            raise ValueError("Unrecognized format")

        if fbase == FMT_BYTES:
            if isinstance(value, bytes):
                pass

            elif isinstance(value, bytearray):
                value = bytes(value)

            else:
                raise TypeError("Expected bytes")

            return (value, format)

        elif fbase == FMT_UTF8:
            return (value.encode('utf-8'), format)

        elif fbase == FMT_PICKLE:
            return (pickle.dumps(value), FMT_PICKLE)

        elif fbase == FMT_JSON:
            return (json.dumps(value).encode('utf-8'), FMT_JSON)

        else:
            raise ValueError("Unrecognized format '%r'" % (format,))

    def decode_value(self, value, flags):
        is_recognized_format = True
        fbase = flags & FMT_MASK

        if fbase not in (FMT_JSON, FMT_UTF8, FMT_BYTES, FMT_PICKLE):
            fbase = FMT_BYTES
            is_recognized_format = False

        if fbase == FMT_BYTES:
            if not is_recognized_format:
                warnings.warn("Received unrecognized flags %d" % (flags,))
            return value

        elif fbase == FMT_UTF8:
            return value.decode("utf-8")

        elif fbase == FMT_JSON:
            return json.loads(value.decode("utf-8"))

        elif fbase == FMT_PICKLE:
            return pickle.loads(value)
