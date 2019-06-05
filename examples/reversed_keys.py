from couchbase_core.transcoder import Transcoder
from couchbase_v2.bucket import Bucket


class ReverseTranscoder(Transcoder):
    def encode_key(self, key):
        return super(ReverseTranscoder, self).encode_key(key[::-1])

    def decode_key(self, key):
        key = super(ReverseTranscoder, self).decode_key(key)
        return key[::-1]


c_reversed = Bucket('couchbase://localhost/default',
        transcoder=ReverseTranscoder())
c_plain = Bucket('couchbase://localhost/default')

c_plain.remove_multi(('ABC', 'CBA', 'XYZ', 'ZYX'), quiet=True)

c_reversed.upsert("ABC", "This is a reversed key")

rv = c_plain.get("CBA")
print("Got value for reversed key '{0}'".format(rv.value))

rv = c_reversed.get("ABC")
print("Got value for reversed key '{0}' again".format(rv.value))

c_plain.upsert("ZYX", "This is really ZYX")

rv = c_reversed.get("XYZ")
print("Got value for '{0}': '{1}'".format(rv.key, rv.value))
