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

    .. py:method:: create_collection(collection: CollectionSpec, *options: CreateCollectionOptions, **kwargs: Dict[str, Any]) -> None
        :noindex:

        .. deprecated:: 4.1.9
            Use ``create_collection(scope_name, collection_name, settings=None, *options, **kwargs)`` instead.

        Creates a new collection in a specified scope.

        :param collection: The collection details.
        :type collection: :class:`.CollectionSpec`
        :param \*options: Optional parameters for this operation.
        :type \*options: :class:`~couchbase.management.options.CreateCollectionOptions`
        :param \*\*kwargs: keyword arguments that can be used as optional parameters for this operation.
        :type \*\*kwargs: Dict[str, Any]
        :raises `~couchbase.exceptions.CollectionAlreadyExistsException`: If the collection already exists.
        :raises `~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.

    .. automethod:: create_collection
    .. automethod:: create_scope

    .. py:method:: drop_collection(collection: CollectionSpec, *options: DropCollectionOptions, **kwargs: Dict[str, Any]) -> None
        :noindex:

        .. deprecated:: 4.1.9
            Use ``drop_collection(scope_name, collection_name, *options, **kwargs)`` instead.

        Drops a collection from a specified scope.

        :param collection: The collection details.
        :type collection: :class:`.CollectionSpec`
        :param \*options: Optional parameters for this operation.
        :type \*options: :class:`~couchbase.management.options.DropCollectionOptions`
        :param \*\*kwargs: keyword arguments that can be used as optional parameters for this operation.
        :type \*\*kwargs: Dict[str, Any]
        :raises `~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.

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
