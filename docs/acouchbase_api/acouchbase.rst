==========
acouchbase
==========

.. note::
    Further updates to the acouchbase docs will come with future 4.x releases.  In the meantime,
    check out the provided examples in the :acouchbase_examples:`Github repo <>`.

Each public ``couchbase`` module has a counterpart at the same path in ``acouchbase``, so asyncio
code imports from its own package, for example ``from acouchbase.options import QueryOptions``.  A
name the two APIs share, such as an options class, an enum or an exception, is the same object at
either path.

.. toctree::
   :maxdepth: 2

   acouchbase_core
   acouchbase_binary_collection
   acouchbase_results
