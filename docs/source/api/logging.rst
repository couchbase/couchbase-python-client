=======
Logging
=======

.. currentmodule::couchbase

.. versionadded:: 2.0.0

Logging may be enabled programmatically via :meth:`couchbase.enable_logging()`
or via the environment, setting the ``LCB_LOGLEVEL`` environment variable
to a value between `0` and `5`.

"Programmatic" logging uses Python's standard ``logging`` module. If you
are not familiar with Python's logging module, see https://docs.python.org/2/howto/logging.html#logging-basic-tutorial
for a basic tutorial (note, the `logging` module is also available on
Python 3)

Note the environment variable method is actually a variable interpreted
by the underlying C library (`libcouchbase`) and is available on all
C library versions starting from 2.4.0.

Note that logging messages themselves are currently limited to output from the C
library



.. autofunction:: couchbase.enable_logging