===================
Core couchbase API
===================

.. contents::
    :local:

Cluster
==============

.. module:: couchbase.cluster
.. autoclass:: Cluster

    .. automethod:: connect
    .. autoproperty:: connected
    .. automethod:: bucket
    .. automethod:: cluster_info
    .. automethod:: ping
    .. automethod:: diagnostics
    .. automethod:: wait_until_ready
    .. automethod:: query
    .. automethod:: search_query
    .. automethod:: search
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

.. module:: couchbase.bucket
.. autoclass:: Bucket

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

.. module:: couchbase.scope

.. autoclass:: Scope

    .. autoproperty:: name
    .. autoproperty:: bucket_name
    .. automethod:: query
    .. automethod:: search_query
    .. automethod:: search
    .. automethod:: analytics_query
    .. automethod:: search_indexes
    .. automethod:: eventing_functions

Collection
==============

.. module:: couchbase.collection

.. class:: Collection

    .. autoproperty:: name
    .. automethod:: exists
    .. automethod:: get
    .. automethod:: get_all_replicas
    .. automethod:: get_and_lock
    .. automethod:: get_and_touch
    .. automethod:: get_any_replica
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
    .. automethod:: list_append
    .. automethod:: list_prepend
    .. automethod:: list_set
    .. automethod:: list_get
    .. automethod:: list_remove
    .. automethod:: list_size
    .. automethod:: couchbase_map
    .. automethod:: map_add
    .. automethod:: map_get
    .. automethod:: map_remove
    .. automethod:: map_size
    .. automethod:: couchbase_set
    .. automethod:: set_add
    .. automethod:: set_remove
    .. automethod:: set_size
    .. automethod:: set_contains
    .. automethod:: couchbase_queue
    .. automethod:: queue_push
    .. automethod:: queue_pop
    .. automethod:: queue_size
    .. automethod:: get_multi
    .. automethod:: get_and_lock_multi
    .. automethod:: get_all_replicas_multi
    .. automethod:: get_any_replica_multi
    .. automethod:: exists_multi
    .. automethod:: insert_multi
    .. automethod:: lock_multi
    .. automethod:: remove_multi
    .. automethod:: replace_multi
    .. automethod:: touch_multi
    .. automethod:: unlock_multi
    .. automethod:: upsert_multi
    .. automethod:: query_indexes
