import warnings
import json
import pickle
from couchbase import (FMT_JSON, FMT_BYTES, FMT_UTF8, FMT_PICKLE, FMT_MASK)
from couchbase.exceptions import ValueFormatError

class Transcoder(object):
    """
    This class is the base for custom key/value transcoders. It is largely
    experimental and the interface may change.
    """

    def encode_key(self, key):
        """Encode the key as a bytearray.

        :param key: This is an object passed as a string key.
          There is no restriction on this type

        :return: a bytearray
          The default implementation encodes the key as UTF-8
        """
        ret = (self.encode_value(key, FMT_UTF8))[0]
        return ret

    def decode_key(self, key):
        """Decode the key into something your application uses.

        :param bytearray key: The key, in the form of a bytearray

        :return: a string or other object your application will use
          The default implementation decodes the keys from UTF-8
        """
        return self.decode_value(key, FMT_UTF8)

    def encode_value(self, value, format):
        """Encode the value into something meaningful

        :param any value: A value. This may be a string or a complex python
          object.
        :param any format: The `format` argument as passed to the mutator

        :return: A tuple of (value, flags)
        """
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
        """
        Decode the value from the raw bytes representation into something
        meaningful

        :param value bytearray: Raw bytes, as stored on the server
        :param int flags: The flags for the value

        :return: Something meaningful to be used as a value within the
          application
        """
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
