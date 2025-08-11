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

SearchQuery
===============

.. module:: couchbase.search
    :noindex:

QueryString Queries
++++++++++++++++++++

.. autoclass:: QueryStringQuery
    :members:

Analytic Queries
+++++++++++++++++++

.. autoclass:: MatchQuery
    :members:
.. autoclass:: MatchPhraseQuery
    :members:

Non-Analytic Queries
+++++++++++++++++++++

.. autoclass:: BooleanFieldQuery
    :members:
.. autoclass:: DocIdQuery
    :members:
.. autoclass:: PrefixQuery
    :members:
.. autoclass:: PhraseQuery
    :members:
.. autoclass:: RegexQuery
    :members:
.. autoclass:: TermQuery
    :members:
.. autoclass:: WildcardQuery
    :members:

Range Queries
+++++++++++++++++++

.. autoclass:: DateRangeQuery
    :members:
.. autoclass:: NumericRangeQuery
    :members:
.. autoclass:: TermRangeQuery
    :members:

Compound Queries
+++++++++++++++++++

.. autoclass:: BooleanQuery
.. autoclass:: ConjunctionQuery
.. autoclass:: DisjunctionQuery

Geo Queries
+++++++++++++++++++

.. autoclass:: GeoBoundingBoxQuery
.. autoclass:: GeoDistanceQuery
.. autoclass:: GeoPolygonQuery

Special Queries
+++++++++++++++++++

.. autoclass:: MatchAllQuery
    :members:
.. autoclass:: MatchNoneQuery
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
