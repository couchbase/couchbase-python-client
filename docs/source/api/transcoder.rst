====================
Transcoder Interface
====================

.. module:: couchbase_core.transcoder

The **Transcoder** interface may be used to provide custom value and
key formats. Among the uses of the transcoder class, one may:

* Implement compatibility with other client libraries

    The ``format`` option in the operations correspond to a flag value
    which is stored in the server as meta data along with the key. These
    flags are utilized by client libraries to determine how to interpret
    the value on the server (which is just a set of bytes) into a more
    complex and user-friendly type in the native client language. Some
    clients may have different ideas about which flags mean which value format,
    and some clients may use formats which are specific to that platform
    (for example, :const:`~couchbase_core.FMT_PICKLE` which
    is typically only native to Python objects). One may implement a
    custom transcoder class which understands a wider variety of types
    and flags

* Implement Compression

    If storing large values, it may be handy to compress them before
    storing them on the server. This provides for lower network overhead
    and storage space on the server, at the expense of the computational
    overhead of compressing and decompressing objects. One may add extra
    flags to indicate a value has been compressed, and with which format.

* Automatic conversion of different value types into custom classes

    The built-in transcoder only uses native Python types. One may wish
    to interpret values as belong to user-defined classes.

The :class:`Transcoder` class is implemented in C for efficiency, but
a pure-python implementation is available as the :class:`~TranscoderPP`
class in ``couchbase/transcoder.py``.

Typically one would subclass the :class:`Transcoder` class, and
implement the needed methods, allowing for high efficient built-in
methods to perform the un-needed operations.

Note that the :class:`~couchbase_core.client.Client` does not
use a :class:`Transcoder` object by default (however it internally uses
the same routines that the C-implemented :class:`Transcoder` does). Thus,
if no custom transcoding is needed, it is more efficient to set the
:attr:`~couchbase_core.client.Client.transcoder` to ``None``, which
is the default.

.. class:: Transcoder


    .. automethod:: encode_key(key)
    .. automethod:: encode_value(value, flags)
    .. automethod:: decode_key(key)
    .. automethod:: decode_value(value, flags)
    .. automethod:: determine_format(value)

.. autoclass:: TranscoderPP
