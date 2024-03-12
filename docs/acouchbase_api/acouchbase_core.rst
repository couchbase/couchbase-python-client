==============================
Asyncio (acouchbase) Core API
==============================

.. note::
    Further updates to the acouchbase docs will come with future 4.x releases.  In the meantime,
    check out the provided examples in the :acouchbase_examples:`Github repo <>`.


.. contents::
    :local:

Cluster
==============

.. module:: acouchbase.cluster
.. autoclass:: Cluster

.. autoclass:: AsyncCluster

    .. automethod:: connect
    .. automethod:: on_connect
    .. autoproperty:: connected
    .. automethod:: bucket
    .. automethod:: cluster_info
    .. automethod:: ping
    .. automethod:: diagnostics
    .. automethod:: wait_until_ready
    .. automethod:: query
    .. automethod:: search_query
    .. automethod:: analytics_query
    .. autoproperty:: transactions
    .. automethod:: buckets
    .. automethod:: users
    .. automethod:: query_indexes
    .. automethod:: analytics_indexes
    .. automethod:: search_indexes
    .. automethod:: eventing_functions
    .. automethod:: close

Authentication
================

See :ref:`Global API Authentication<authentication-ref>`

Bucket
==============

.. module:: acouchbase.bucket
.. autoclass:: Bucket

.. autoclass:: AsyncBucket

    .. autoproperty:: name
    .. autoproperty:: connected
    .. automethod:: scope
    .. automethod:: collection
    .. automethod:: default_collection
    .. automethod:: ping
    .. automethod:: view_query
    .. automethod:: collections
    .. automethod:: view_indexes

Scope
==============

.. module:: acouchbase.scope
.. autoclass:: Scope

.. autoclass:: AsyncScope

    .. autoproperty:: name
    .. autoproperty:: bucket_name
    .. automethod:: query
    .. automethod:: search_query
    .. automethod:: analytics_query

Collection
==============

.. module:: acouchbase.collection

.. autoclass:: Collection

.. class:: AsyncCollection

    .. autoproperty:: name
    .. automethod:: exists
    .. automethod:: get
    .. automethod:: get_and_lock
    .. automethod:: get_and_touch
    .. automethod:: insert
    .. automethod:: lookup_in
    .. automethod:: mutate_in
    .. automethod:: remove
    .. automethod:: replace
    .. automethod:: touch
    .. automethod:: unlock
    .. automethod:: upsert
    .. automethod:: scan
    .. automethod:: binary
    .. automethod:: couchbase_list
    .. automethod:: couchbase_map
    .. automethod:: couchbase_set
    .. automethod:: couchbase_queue
