==============
Diagnostics
==============

.. contents::
    :local:
    :depth: 2

Ping
==================

Bucket
++++++++++

.. module:: couchbase.bucket
    :noindex:
.. class:: Bucket
    :noindex:

    .. automethod:: ping
        :noindex:

Cluster
++++++++++

.. module:: couchbase.cluster
    :noindex:
.. class:: Cluster
    :noindex:

    .. automethod:: ping
        :noindex:

Options
++++++++++

.. module:: couchbase.options
    :noindex:
.. autoclass:: PingOptions
    :noindex:

Results
++++++++++

.. module:: couchbase.result
    :noindex:

.. class:: PingResult
    :noindex:

    .. autoproperty:: id
        :noindex:
    .. autoproperty:: endpoints
        :noindex:
    .. autoproperty:: sdk
        :noindex:
    .. autoproperty:: version
        :noindex:
    .. automethod:: as_json
        :noindex:


Diagnostics
=====================

Cluster
++++++++++

.. module:: couchbase.cluster
    :noindex:
.. class:: Cluster
    :noindex:

    .. automethod:: diagnostics
        :noindex:

Options
++++++++++

.. module:: couchbase.options
    :noindex:
.. autoclass:: DiagnosticsOptions
    :noindex:

Results
++++++++++

.. module:: couchbase.result
    :noindex:

.. class:: DiagnosticsResult
    :noindex:

    .. autoproperty:: id
        :noindex:
    .. autoproperty:: endpoints
        :noindex:
    .. autoproperty:: state
        :noindex:
    .. autoproperty:: sdk
        :noindex:
    .. autoproperty:: version
        :noindex:
    .. automethod:: as_json
        :noindex:
