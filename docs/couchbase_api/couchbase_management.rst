==============
Management
==============

.. note::
    Further updates to the management API docs will come with future 4.0.x releases.

.. contents::
    :local:

Bucket Management
=================================

.. module:: couchbase.management.buckets
.. autoclass:: BucketManager

    .. automethod:: create_bucket
    .. automethod:: drop_bucket
    .. automethod:: flush_bucket
    .. automethod:: get_all_buckets
    .. automethod:: get_bucket
    .. automethod:: update_bucket

Collection Management
=================================

.. module:: couchbase.management.collections

.. autoclass:: CollectionManager

    .. automethod:: create_collection
    .. automethod:: create_scope
    .. automethod:: drop_collection
    .. automethod:: drop_scope
    .. automethod:: get_all_scopes

.. autoclass:: CollectionSpec

    .. autoproperty:: name
    .. autoproperty:: scope_name
    .. autoproperty:: max_ttl

.. autoclass:: ScopeSpec

    .. autoproperty:: name
    .. autoproperty:: collections

Query Index Management
=================================

.. module:: couchbase.management.queries

.. autoclass:: QueryIndexManager

    .. automethod:: build_deferred_indexes
    .. automethod:: create_index
    .. automethod:: create_primary_index
    .. automethod:: drop_index
    .. automethod:: drop_primary_index
    .. automethod:: get_all_indexes
    .. automethod:: watch_indexes

User Management
=================================

.. module:: couchbase.management.users

.. autoclass:: UserManager

    .. automethod:: drop_user
    .. automethod:: drop_group
    .. automethod:: get_all_groups
    .. automethod:: get_all_users
    .. automethod:: get_group
    .. automethod:: get_roles
    .. automethod:: get_user
    .. automethod:: upsert_group
    .. automethod:: upsert_user
