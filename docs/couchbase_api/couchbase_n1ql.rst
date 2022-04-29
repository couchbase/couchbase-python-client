==============
Query (SQL++)
==============

.. contents::
    :local:
    :depth: 2

Enumerations
===============

.. module:: couchbase.n1ql
.. autoenum:: QueryProfile
.. autoenum:: QueryScanConsistency
.. autoenum:: QueryStatus

Options
===============

.. module:: couchbase.options
    :noindex:
.. autoclass:: QueryOptions
    :noindex:

Results
===============
.. module:: couchbase.n1ql
    :noindex:

QueryMetaData
+++++++++++++++++++
.. autoclass:: QueryMetaData
    :members:

QueryMetrics
+++++++++++++++++++
.. autoclass:: QueryMetrics
    :members:

QueryResult
+++++++++++++++++++
.. module:: couchbase.result
    :noindex:

.. class:: QueryResult
    :noindex:

    .. automethod:: rows
        :noindex:
    .. automethod:: metadata
        :noindex:
