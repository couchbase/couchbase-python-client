#!/usr/bin/env python

from couchbase.transcoder import Transcoder
from couchbase.connection import Connection


class ReverseTranscoder(Transcoder):
    def encode_key(self, key):
        return super(ReverseTranscoder, self).encode_key(key[::-1])

    def decode_key(self, key):
        key = super(ReverseTranscoder, self).decode_key(key)
        return key[::-1]


c_reversed = Connection(bucket='default', transcoder=ReverseTranscoder())
c_plain = Connection(bucket='default')

c_plain.delete_multi(('ABC', 'CBA', 'XYZ', 'ZYX'), quiet=True)

c_reversed.set("ABC", "This is a reversed key")

rv = c_plain.get("CBA")
print("Got value for reversed key '{0}'".format(rv.value))

rv = c_reversed.get("ABC")
print("Got value for reversed key '{0}' again".format(rv.value))

c_plain.set("ZYX", "This is really ZYX")

rv = c_reversed.get("XYZ")
print("Got value for '{0}': '{1}'".format(rv.key, rv.value))
