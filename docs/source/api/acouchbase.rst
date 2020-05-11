===================
`asyncio` Interface
===================

.. module:: acouchbase

The asyncio interface is for use with the Async IO Python standard library
that ships with Python 3.4 and greater. This documentation
contains the API reference for how to use the ``acouchbase`` module with
asyncio.

.. currentmodule:: acouchbase.cluster

For the most part, the ``acouchbase`` API functions like its synchronous
counterpart, :class:`~couchbase`, except for its
asynchronous nature. Where the synchronous API returns a
:class:`~couchbase.result.Result` object, the ``acouchbase`` API returns
an :class:`AsyncResult` which will have its callback invoked with a result.

As such, we will omit the mentions of the normal key value operations, which
function identially to their synchronous conterparts documented in the
:class:`~couchbase.cluster.Cluster` :class:`~couchbase.bucket.Bucket`,
and :class:`~couchbase.collection.Collection` classes.

.. class:: AIOClientMixin

    .. automethod:: __init__
    .. automethod:: on_connect
    .. autoattribute:: connected

.. class:: Cluster

    .. automethod:: __init__
    .. automethod:: analytics_query
    .. automethod:: search_query
    .. automethod:: query

.. class:: Bucket

    .. automethod:: view_query

.. class:: Collection

    .. automethod:: __init__

.. currentmodule:: acouchbase.iterator

.. class:: AQueryResult

    .. automethod:: __iter__
    .. automethod:: __init__

.. class:: ASearchResult

    .. automethod:: __iter__
    .. automethod:: __init__

.. class:: AViewResult

    .. automethod:: __iter__
    .. automethod:: __init__

.. class:: AAnalyticsResult

    .. automethod:: __iter__
    .. automethod:: __init__

