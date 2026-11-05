===================================================
Welcome to the Couchbase Python SDK documentation!
===================================================

Guide
-----

:doc:`using_the_python_sdk`
   Installing the SDK and connecting to a cluster.

:doc:`migration`
   The import paths kept for existing code and what replaces each, and moving from 3.x to 4.x.

:doc:`logging_and_telemetry`
   Logging, the reports of slow operations and latencies, and sending traces and metrics to OpenTelemetry.

:doc:`transactions`
   Running a transaction, handling its errors, and configuring it.

:doc:`parallelism`
   Sharing a cluster across threads, and using the SDK from more than one process.

:doc:`environment_variables`
   The variables the SDK reads at run time, for logging, option validation and loading OpenSSL.

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
   migration
   logging_and_telemetry
   transactions
   parallelism
   environment_variables

.. toctree::
   :caption: Reference
   :maxdepth: 1
   :hidden:

   reference/couchbase/index
   reference/acouchbase/index
   reference/txcouchbase/index
   reference/common/index
