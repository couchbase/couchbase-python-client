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
import json

from couchbase.tests.base import ConnectionTestCase
from couchbase.transcoder import TranscoderPP, Transcoder
from couchbase.bucket import Bucket
from couchbase import FMT_UTF8
import couchbase.exceptions as E

# This won't test every single permutation of the transcoder, but will check
# mainly to see if error messages are appropriate and how the application handles
# a misbehaving transcoder. The Transcoder API is fairly simple.. so

def gen_func(fname):
    def fn(self, *args):
        if fname in self._op_next:
            val = self._op_next[fname]
            if hasattr(val, '__call__'):
                return val()
            return self._op_next[fname]

        return getattr(self._tc, fname)(*args)
    return fn

class MangledTranscoder(object):
    """
    This is a custom transcoder class where we can optionally set a 'next_value'
    field for a specific operation. If this field is empty, then the default
    method is used
    """
    def __init__(self):
        self._tc = TranscoderPP()
        self._op_next = {}

    def set_all(self, val):
        for n in ('encode_key', 'encode_value', 'decode_key', 'decode_value'):
            self._op_next[n] = val

    def set_next(self, ftype, val):
        self._op_next[ftype] = val

    decode_key = gen_func('decode_key')
    encode_key = gen_func('encode_key')
    decode_value = gen_func('decode_value')
    encode_value = gen_func('encode_value')

class TranscoderTest(ConnectionTestCase):

    def test_simple_transcoder(self):
        tc = TranscoderPP()
        self.cb.transcoder = tc

        key = self.gen_key("simple_transcoder")
        obj_values = ({}, [], -1, None, False, True)
        for curval in obj_values:
            self.cb.upsert(key, curval)
            ret = self.cb.get(key)
            self.assertEqual(ret.value, curval)


    # Try to test some bad transcoders:

    def test_empty_transcoder(self):
        for v in (None, False, 0):
            self.cb.transcoder = v
            self.cb.upsert("foo", "bar")

    def test_bad_transcoder(self):
        self.cb.transcoder = None

        key = self.gen_key("bad_transcoder")
        self.cb.upsert(key, "value")
        self.cb.transcoder = object()
        self.assertRaises(E.ValueFormatError, self.cb.upsert, key, "bar")
        self.assertRaises(E.ValueFormatError, self.cb.get, key)


        mangled = MangledTranscoder()
        # Ensure we actually work
        self.cb.transcoder = mangled
        self.cb.upsert(key, "value")
        self.cb.get(key)


        for badret in (None, (), [], ""):
            mangled.set_all(badret)
            self.assertRaises(E.ValueFormatError, self.cb.upsert, key, "value")
            self.assertRaises(E.ValueFormatError, self.cb.get, key)

        mangled._op_next.clear()
        # Try with only bad keys:
        mangled._op_next['encode_key'] = None
        self.assertRaises(E.ValueFormatError, self.cb.upsert, key, "value")


    def test_transcoder_bad_encvals(self):
        mangled = MangledTranscoder()
        self.cb.transcoder = mangled

        key = self.gen_key("transcoder_bad_encvals")

        # Various tests for 'bad_value':
        encrets = (

            # None
            None,

            # Valid string, but not inside tuple
            b"string",

            # Tuple, but invalid contents
            (None, None),

            # Tuple, valid string, but invalid size (no length)
            (b"valid string"),

            # Tuple, valid flags but invalid string
            (None, 0xf00),

            # Valid tuple, but flags are too big
            (b"string", 2**40),

            # Tuple, but bad leading string
            ([], 42)
        )

        for encret in encrets:
            print(encret)
            mangled._op_next['encode_value'] = encret
            self.assertRaises(E.ValueFormatError, self.cb.upsert, key, "value")

    def test_transcoder_kdec_err(self):
        key = self.gen_key("transcoder_kenc_err")
        mangled = MangledTranscoder()
        self.cb.transcoder = mangled
        key = self.gen_key('kdec_err')
        self.cb.upsert(key, 'blah', format=FMT_UTF8)
        def exthrow():
            raise UnicodeDecodeError()

        mangled.set_next('decode_value', exthrow)
        self.assertRaises(E.ValueFormatError, self.cb.get, key)



    def test_transcoder_anyobject(self):
        # This tests the versatility of the transcoder object
        key = self.gen_key("transcoder_anyobject")
        mangled = MangledTranscoder()
        self.cb.transcoder = mangled

        mangled._op_next['encode_key'] = key.encode("utf-8")
        mangled._op_next['encode_value'] = (b"simple_value", 10)

        objs = (object(), None, MangledTranscoder(), True, False)
        for o in objs:
            mangled._op_next['decode_key'] = o
            mangled._op_next['decode_value'] = o
            self.cb.upsert(o, o, format=o)
            rv = self.cb.get(o)
            self.assertEqual(rv.value, o)


    def test_transcoder_unhashable_keys(self):
        key = self.gen_key("transcoder_unhashable_keys")
        mangled = MangledTranscoder()
        mangled._op_next['encode_key'] = key.encode("utf-8")
        mangled._op_next['encode_value'] = (b"simple_value", 10)
        self.cb.transcoder = mangled

        # As MultiResult objects must be able to store its keys in a dictionary
        # we cannot allow unhashable types. These are such examples
        unhashable = ({}, [], set())
        for o in unhashable:
            mangled._op_next['decode_key'] = o
            mangled._op_next['decode_value'] = o
            self.assertRaises(E.ValueFormatError, self.cb.upsert, o, o)
            self.assertRaises(E.ValueFormatError, self.cb.get, o, quiet=True)

    def test_transcoder_class(self):
        # Test whether we can pass a class for a transcoder
        key = self.gen_key("transcoder_class")
        c = Bucket(**self.make_connargs(transcoder=TranscoderPP))
        c.upsert(key, "value")

        c = Bucket(**self.make_connargs(transcoder=TranscoderPP))
        c.upsert(key, "value")

    def test_mask_sanity(self):
        from couchbase import FMT_COMMON_MASK, FMT_LEGACY_MASK
        self.assertEqual(FMT_COMMON_MASK, 0xFF000000)
        self.assertEqual(FMT_LEGACY_MASK, 0x00000007)

    def test_pycbc295(self):
        # Test that we ignore the legacy flags and use the common flags
        # instead
        custom_tc = MangledTranscoder()
        orig_tc = Transcoder()

        c = self.make_connection()
        c.transcoder = custom_tc
        custom_tc._op_next['encode_value'] = (
            json.dumps({'Hello': 'World'}).encode('utf8'),
            0x02000001
        )
        key = self.gen_key('pycbc295')
        c.upsert(key, 'whatevs')
        c.transcoder = orig_tc
        rv = c.get(key)
        self.assertIsInstance(rv.value, (dict,))
