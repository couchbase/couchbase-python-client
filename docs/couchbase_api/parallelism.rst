============
Parallelism
============

.. contents::
    :local:

Thread-based Parallelism
==========================

The 4.x version of the Python SDK significantly improves how the SDK handles the
`Global Interpreter Lock (GIL) <https://docs.python.org/3/glossary.html#term-global-interpreter-lock>`_.
As part of the improvements, options available in previous series of the SDK (2.x and 3.x) are no longer needed.

Notable options that are no longer utilized (or available) in the 4.x SDK:

* ``lockmode`` ClusterOption has been deprecated as it is a no-op (i.e. has no functionality) and will be removed in a future version of the SDK
* ``unlock_gil`` option is no longer available

Due to the underlying architecture of the 4.x SDK it is recommended to share a cluster instance across multiple threads.  However, creating a
cluster instance per thread can work as well.  As should be the standard for *all* applications, we recommend sufficient testing to determine
the path that best fits the needs of *your* application.

See the `Python threading docs <https://docs.python.org/3/library/threading.html>`_ for details on thread-based parallelism.

Process-based Parallelism
==========================

Due to the underlying architecture of the 4.x SDK a cluster instance **cannot** be shared across across multiple processes; therefore,
a cluster instance must be created for each process used in a multiprocessing scenario. As should be the standard for *all* applications,
we recommend sufficient testing to determine the path that best fits the needs of *your* application.

See the `Python multiprocessing docs <https://docs.python.org/3/library/multiprocessing.html>`_ for details on process-based parallelism.
