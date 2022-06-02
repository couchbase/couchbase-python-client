==============
Results
==============

.. module:: couchbase.result

AnalyticsResult
=================

.. class:: AnalyticsResult

    .. automethod:: rows
    .. automethod:: metadata

ClusterInfoResult
=================

.. class:: ClusterInfoResult

    .. autoproperty:: is_community
    .. autoproperty:: is_enterprise
    .. autoproperty:: server_version
    .. autoproperty:: server_version_full
    .. autoproperty:: server_version_short

CounterResult
=================

.. class:: CounterResult

    .. autoproperty:: cas
    .. autoproperty:: content
    .. autoproperty:: key

DiagnosticsResult
=================

.. class:: DiagnosticsResult

    .. autoproperty:: id
    .. autoproperty:: endpoints
    .. autoproperty:: state
    .. autoproperty:: sdk
    .. autoproperty:: version
    .. automethod:: as_json

ExistsResult
=================

.. class:: ExistsResult

    .. autoproperty:: exists

GetResult
=================

.. class:: GetResult

    .. autoproperty:: cas
    .. autoproperty:: content_as
    .. autoproperty:: key
    .. autoproperty:: expiry_time


LookupInResult
=================

.. class:: LookupInResult

    .. autoproperty:: cas
    .. autoproperty:: content_as

MultiCounterResult
=====================

.. class:: MultiCounterResult

    .. autoproperty:: all_ok
    .. autoproperty:: exceptions
    .. autoproperty:: results

MultiGetResult
=====================

.. class:: MultiGetResult

    .. autoproperty:: all_ok
    .. autoproperty:: exceptions
    .. autoproperty:: results

MultiExistsResult
=====================

.. class:: MultiExistsResult

    .. autoproperty:: all_ok
    .. autoproperty:: exceptions
    .. autoproperty:: results

MultiMutationResult
=====================

.. class:: MultiMutationResult

    .. autoproperty:: all_ok
    .. autoproperty:: exceptions
    .. autoproperty:: results

MutateInResult
=================

.. class:: MutateInResult

    .. autoproperty:: cas
    .. autoproperty:: content_as

MutationResult
=================

.. class:: MutationResult

    .. autoproperty:: cas
    .. automethod:: mutation_token

PingResult
=================

.. class:: PingResult

    .. autoproperty:: id
    .. autoproperty:: endpoints
    .. autoproperty:: sdk
    .. autoproperty:: version
    .. automethod:: as_json


QueryResult
=================

.. class:: QueryResult

    .. automethod:: rows
    .. automethod:: metadata

SearchResult
=================

.. class:: SearchResult

    .. automethod:: rows
    .. automethod:: metadata

ViewResult
=================

.. class:: ViewResult

    .. automethod:: rows
    .. automethod:: metadata
