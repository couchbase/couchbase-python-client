=================
Range Scan
=================

.. note::
    Use this API for low concurrency batch queries where latency is not a critical as the system may have to scan a lot of documents to find the matching documents.
    For low latency range queries, it is recommended that you use SQL++ with the necessary indexes.

.. contents::
    :local:
    :depth: 2

Scan Types
===============

.. module:: couchbase.kv_range_scan
    :noindex:

ScanTerm
+++++++++++++++++++

.. autoclass:: ScanTerm
    :members:

RangeScan
+++++++++++++++++++

.. class:: RangeScan

    .. autoproperty:: start
        :noindex:
    .. autoproperty:: end
        :noindex:

PrefixScan
+++++++++++++++++++

.. class:: PrefixScan

    .. autoproperty:: prefix
        :noindex:

SamplingScan
+++++++++++++++++++

.. class:: SamplingScan

    .. autoproperty:: limit
        :noindex:
    .. autoproperty:: seed
        :noindex:

Options
===============

.. module:: couchbase.options
    :noindex:
.. autoclass:: ScanOptions
    :noindex:

Results
===============

.. module:: couchbase.result
    :noindex:

ScanResult
+++++++++++++++++++

.. class:: ScanResult
    :noindex:

    .. autoproperty:: id
        :noindex:
    .. autoproperty:: ids_only
        :noindex:
    .. autoproperty:: cas
        :noindex:
    .. autoproperty:: content_as
        :noindex:
    .. autoproperty:: expiry_time
        :noindex:

ScanResultIterable
+++++++++++++++++++

.. class:: ScanResultIterable
    :noindex:

    .. automethod:: rows
        :noindex:
    .. automethod:: cancel_scan
        :noindex:
