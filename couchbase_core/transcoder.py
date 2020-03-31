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

from couchbase_core._libcouchbase import (FMT_JSON, FMT_AUTO,
                       FMT_BYTES, FMT_UTF8, FMT_PICKLE,
                       FMT_LEGACY_MASK, FMT_COMMON_MASK)

from couchbase_core._libcouchbase import Transcoder
from couchbase_core._pyport import unicode

# Initialize our dictionary

UNIFIED_FORMATS = (FMT_JSON, FMT_BYTES, FMT_UTF8, FMT_PICKLE)
LEGACY_FORMATS = tuple([x & FMT_LEGACY_MASK for x in UNIFIED_FORMATS])
COMMON_FORMATS = tuple([x & FMT_COMMON_MASK for x in UNIFIED_FORMATS])

COMMON2UNIFIED = {}
LEGACY2UNIFIED = {}

for fl in UNIFIED_FORMATS:
    COMMON2UNIFIED[fl & FMT_COMMON_MASK] = fl
    LEGACY2UNIFIED[fl & FMT_LEGACY_MASK] = fl


def get_decode_format(flags):
    """
    Returns a tuple of format, recognized
    """
    c_flags = flags & FMT_COMMON_MASK
    l_flags = flags & FMT_LEGACY_MASK

    if c_flags:
        if c_flags not in COMMON_FORMATS:
            return FMT_BYTES, False
        else:
            return COMMON2UNIFIED[c_flags], True
    else:
        if not l_flags in LEGACY_FORMATS:
            return FMT_BYTES, False
        else:
            return LEGACY2UNIFIED[l_flags], True


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
        if format == 0:
            format = FMT_JSON
        elif format == FMT_AUTO:
            if isinstance(value, unicode):
                format = FMT_UTF8
            elif isinstance(value, (bytes, bytearray)):
                format = FMT_BYTES
            elif isinstance(value, (list, tuple, dict, bool)) or value is None:
                format = FMT_JSON
            else:
                format = FMT_PICKLE

        if format not in (FMT_PICKLE, FMT_JSON, FMT_BYTES, FMT_UTF8):
            raise ValueError("Unrecognized format")

        if format == FMT_BYTES:
            if isinstance(value, bytes):
                pass

            elif isinstance(value, bytearray):
                value = bytes(value)

            else:
                raise TypeError("Expected bytes")

            return value, format

        elif format == FMT_UTF8:
            return value.encode('utf-8'), format

        elif format == FMT_PICKLE:
            return self._do_pickle_encode(value), FMT_PICKLE

        elif format == FMT_JSON:
            return self._do_json_encode(value).encode('utf-8'), FMT_JSON

        else:
            raise ValueError("Unrecognized format '%r'" % (format,))

    def decode_value(self, value, flags):
        format, is_recognized = get_decode_format(flags)

        if format == FMT_BYTES:
            if not is_recognized:
                warnings.warn("Received unrecognized flags %d" % (flags,))
            return value

        elif format == FMT_UTF8:
            return value.decode("utf-8")

        elif format == FMT_JSON:
            return self._do_json_decode(value.decode('utf-8'))

        elif format == FMT_PICKLE:
            return self._do_pickle_decode(value)

    def _do_json_encode(self, value):
        """
        Can be overidden by subclasses. This should do the same as `json.dumps`
        :param value: Python object
        :return: JSON string
        """
        return json.dumps(value, ensure_ascii=False)

    def _do_json_decode(self, value):
        """
        Can be overidden by subclasses. This should do the same as `json.loads`
        :param value: The JSON string
        :return: The decoded Python value
        """
        return json.loads(value)

    def _do_pickle_encode(self, value):
        """
        Can be overidden by subclasses. This should do the same as
        `pickle.dumps`
        :param value: The value to pickle
        :return: The pickled buffer
        """
        return pickle.dumps(value)

    def _do_pickle_decode(self, value):
        """
        Can be overidden by subclasses. This should do the same as
        `pickle.loads`
        :param value: The pickled buffer
        :return: The unpickled python object
        """
        return pickle.loads(value)


class LegacyTranscoderPP(TranscoderPP):
    def encode_value(self, value, format):
        encoded, flags = super(LegacyTranscoderPP, self).encode_value(value, format)
        return encoded, flags & FMT_LEGACY_MASK
