====================
Conversion Functions
====================

.. module:: couchbase_core

By default, this library uses the default ``pickle`` and ``json``
modules of the standard Python installation to perform conversion
to and from those formats.

Sometimes there may be a wish to use a different implementation of
those functions (for example, ``cPickle`` or a faster JSON encoder/
decoder).

There are two functions available to change the default Pickle and JSON
converters.

Note that these immediately affect *all*
:class:`~couchbase_core.client.Client` objects. If you wish to have
a finer grained control over which object uses which converters, you
may wish to consider writing your own
:class:`~couchbase_core.transcoder.Transcoder`
implementation instead.

.. autofunction:: set_json_converters

.. autofunction:: set_pickle_converters
