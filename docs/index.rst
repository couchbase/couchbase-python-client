===================================================
Welcome to the Couchbase Python SDK documentation!
===================================================

Guide
-----

:doc:`using_the_python_sdk`
   Installing the SDK, connecting to a cluster, and migrating from earlier versions.

:doc:`parallelism`
   Sharing a cluster across threads, and using the SDK from more than one process.

Reference
---------

The SDK has three APIs that share one set of types.  Each API section documents the classes it writes
itself; the types they all take and return are documented once, in the common reference.

:doc:`reference/couchbase/index`
   The ``couchbase`` package, where every operation blocks until it completes.

:doc:`reference/acouchbase/index`
   The ``acouchbase`` package, for asyncio.

:doc:`reference/txcouchbase/index`
   The ``txcouchbase`` package, for Twisted.

:doc:`reference/common/index`
   Options, results, exceptions and the other types every API shares.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :caption: Guide
   :maxdepth: 1
   :hidden:

   using_the_python_sdk
   parallelism

.. toctree::
   :caption: Reference
   :maxdepth: 1
   :hidden:

   reference/couchbase/index
   reference/acouchbase/index
   reference/txcouchbase/index
   reference/common/index
