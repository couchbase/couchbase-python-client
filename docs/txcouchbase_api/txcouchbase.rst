===========
txcouchbase
===========

.. note::
    Further updates to the acouchbase docs will come with future 4.0.x releases.  In the meantime,
    checkout the provided examples in the :txcouchbase_examples:`Github repo <>`.

.. note::
    The minimum required Twisted version is 21.7.0.

.. warning::
    The 4.x SDK introduced a breaking change where the ``txcouchbase`` package must be imported *prior* to importing the reactor.  This is so that the asyncio reactor can be installed.

.. toctree::
   :maxdepth: 2

   txcouchbase_core
