===================
`Twisted` Interface
===================

.. module:: txcouchbase

The Twisted interface is for use with the Twisted Python event and networking
library which may be found at http://www.twistedmatrix.com. This documentation
contains the API reference for how to use the ``txcouchbase`` module with
Twisted.

.. currentmodule:: txcouchbase.cluster

For the most part, the ``txcouchbase`` API functions like its synchronous
counterpart, :class:`~couchbase`, except for its
asynchronous nature. Where the synchronous API returns a
:class:`~couchbase.result.Result` object, the ``txcouchbase`` API returns
a :class:`Deferred` which will have its callback invoked with a result.

As such, we will omit the mentions of the normal key value operations, which
function identially to their synchronous conterparts documented in the
:class:`~couchbase.cluster.Cluster` :class:`~couchbase.bucket.Bucket`,
and :class:`~couchbase.collection.Collection` classes.

The :class:`TxDeferredClient` mixin for Twisted is subclassed from the lower-level
:class:`TxRawClient` which returns :class:`~couchbase.result.AsyncGetResult` etc
objects rather than `Deferred` objects. This is largely due to performance
reasons (Deferreds result in a 3x performance slowdown).

.. class:: TxRawClientMixin

    .. automethod:: __init__
    .. automethod:: registerDeferred
    .. automethod:: on_connect
    .. automethod:: defer
    .. autoattribute:: connected

.. class:: TxDeferredClientMixin

    .. automethod:: __init__

.. class:: TxRawCluster

    .. automethod:: __init__
    .. automethod:: analytics_query
    .. automethod:: search_query
    .. automethod:: query
    .. automethod:: query_ex

.. class:: TxCluster

    .. automethod:: __init__

.. class:: TxRawBucket

    .. automethod:: __init__

.. class:: TxBucket

    .. automethod:: view_query
    .. automethod:: view_query_ex

.. class:: TxRawCollection

    .. automethod:: __init__

.. class:: TxCollection

    .. automethod:: __init__

.. class:: BatchedViewResult

    .. automethod:: __iter__
    .. automethod:: __init__

.. class:: BatchedQueryResult

    .. automethod:: __iter__
    .. automethod:: __init__

.. class:: BatchedAnalyticsResult

    .. automethod:: __iter__
    .. automethod:: __init__

.. class:: BatchedSearchResult

    .. automethod:: __iter__
    .. automethod:: __init__
