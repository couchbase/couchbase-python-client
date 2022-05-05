.. Couchbase Python Client Library documentation master file, created by
   sphinx-quickstart on Thu Apr 14 13:34:44 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

===================================================
Welcome to the Couchbase Python SDK documentation!
===================================================

Getting Started with the Python SDK
-------------------------------------

:doc:`using_the_python_sdk`
   Useful information for getting started and using the Python SDK.

Synchronous API
---------------

:doc:`couchbase_api/couchbase_core`
   API reference for Cluster, Bucket, Scope and Collection objects.

:doc:`couchbase_api/couchbase_n1ql`
   API reference for query (SQL++) operations.

:doc:`couchbase_api/couchbase_analytics`
   API reference for analytics operations.

:doc:`couchbase_api/couchbase_search`
   API reference for full text search (FTS) operations.

:doc:`couchbase_api/couchbase_transactions`
   API reference for Distributed ACID transactions with the Python SDK.

:doc:`couchbase_api/couchbase_diagnostics`
   API reference for diagnostic operations.

:doc:`couchbase_api/couchbase_binary_collection`
   API reference for BinaryCollection operations.

:doc:`couchbase_api/couchbase_datastructures`
   API reference for datastructure operations.

:doc:`couchbase_api/couchbase_management`
   API reference for management operations.

Global API
-----------------
:doc:`couchbase_api/exceptions`
   API reference for Exceptions.

:doc:`couchbase_api/management_options`
   API reference for mangement operation options.

:doc:`couchbase_api/options`
   API reference for operation options.

:doc:`couchbase_api/results`
   API reference for operation results.

:doc:`couchbase_api/subdocument`
   API reference for subdocument operation Specs.

Asynchronous APIs
-----------------
:doc:`acouchbase_api/acouchbase`
   API reference for the asyncio (acouchbase) API.

:doc:`txcouchbase_api/txcouchbase`
   API reference for the Twisted (txcouchbase) API.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. Hidden TOCs

.. toctree::
   :caption: Using the Couchbase Python SDK
   :maxdepth: 2
   :hidden:

   using_the_python_sdk

.. toctree::
   :caption: Synchronous API
   :maxdepth: 2
   :hidden:

   couchbase_api/couchbase_analytics
   couchbase_api/couchbase_binary_collection
   couchbase_api/couchbase_core
   couchbase_api/couchbase_datastructures
   couchbase_api/couchbase_diagnostics
   couchbase_api/couchbase_n1ql
   couchbase_api/couchbase_management
   couchbase_api/couchbase_search
   couchbase_api/couchbase_transactions
   couchbase_api/couchbase_views

.. toctree::
   :caption: Global API
   :maxdepth: 2
   :hidden:

   couchbase_api/exceptions
   couchbase_api/management_options
   couchbase_api/options
   couchbase_api/results
   couchbase_api/subdocument


.. toctree::
   :caption: Asynchronous APIs
   :maxdepth: 2
   :hidden:

   acouchbase_api/acouchbase
   txcouchbase_api/txcouchbase
