====================
Conversion Functions
====================

.. module:: couchbase

By default, this library uses the default ``pickle`` and ``json``
modules of the standard Python installation to perform conversion
to and from those formats.

Sometimes there may be a wish to use a different implementation of
those functions (for example, ``cPickle`` or a faster JSON encoder/
decoder).

There are two functions available to change the default Pickle and JSON
converters.

Note that these immediately affect *all*
:class:`~couchbase.bucket.Bucket` objects. If you wish to have
a finer grained control over which object uses which converters, you
may wish to consider writing your own
:class:`~couchbase.transcoder.Transcoder`
implementation instead.

.. autofunction:: set_json_converters

.. autofunction:: set_pickle_converters
