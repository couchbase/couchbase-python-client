===========
txcouchbase
===========

.. note::
    Further updates to the acouchbase docs will come with future 4.x releases.  In the meantime,
    check out the provided examples in the :txcouchbase_examples:`Github repo <>`.

.. note::
    The minimum required Twisted version is 21.7.0.

Each public ``couchbase`` module has a counterpart at the same path in ``txcouchbase``, apart from the
features the Twisted API does not provide (transactions, the datastructures, the eventing function
manager and range scans).  So Twisted code imports from its own package, for example
``from txcouchbase.options import QueryOptions``.  A name the two APIs share, such as an options class,
an enum or an exception, is the same object at either path.

.. warning::
    The 4.x SDK introduced a breaking change where the ``txcouchbase`` package must be imported *prior* to importing the reactor.  This is so that the asyncio reactor can be installed.

.. toctree::
   :maxdepth: 2

   txcouchbase_core
