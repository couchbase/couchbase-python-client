=================
Full Text Search
=================

.. note::
    Further updates to the search docs will come with future 4.x releases.

.. contents::
    :local:
    :depth: 2

Enumerations
===============

.. module:: couchbase.search
.. autoenum:: SearchScanConsistency

Options
===============

.. module:: couchbase.options
    :noindex:
.. autoclass:: SearchOptions
    :noindex:

SearchRequest
===============

.. module:: couchbase.search
    :noindex:
.. autoclass:: SearchRequest
    :members:

Results
===============

.. module:: couchbase.search
    :noindex:

SearchMetaData
+++++++++++++++++++

.. autoclass:: SearchMetaData
    :members:

SearchMetrics
+++++++++++++++++++

.. autoclass:: SearchMetrics
    :members:

SearchResult
+++++++++++++++++++

.. module:: couchbase.result
    :noindex:

.. class:: SearchResult
    :noindex:

    .. automethod:: rows
        :noindex:
    .. automethod:: metadata
        :noindex:

Vector Search
===============

.. module:: couchbase.vector_search
    :noindex:

.. autoclass:: VectorQuery
    :members:

.. autoclass:: VectorSearch
    :members:

Enumerations
+++++++++++++++++++

.. module:: couchbase.vector_search
    :noindex:
.. autoenum:: VectorQueryCombination

Options
+++++++++++++++++++

.. module:: couchbase.options
    :noindex:
.. autoclass:: VectorSearchOptions
