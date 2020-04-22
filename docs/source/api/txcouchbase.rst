===================
`Twisted` Interface
===================

.. _JIRA: https://issues.couchbase.com/browse/PYCBC-590

.. warning::
    The async APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. They will
    be updated to support SDK3 shortly. See JIRA_.

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

The :class:`TxDeferredClient` interface for Twisted is subclassed from the lower-level
:class:`TxRawClient` which returns :class:`~couchbase_core.result.AsyncResult`
objects rather than `Deferred` objects. This is largely due to performance
reasons (Deferreds result in a 3x performance slowdown).

.. class:: TxRawClientFactory.gen_raw.<locals>.TxRawClient

    .. automethod:: __init__
    .. automethod:: registerDeferred
    .. automethod:: connect
    .. automethod:: defer
    .. autoattribute:: connected

.. class:: TxClientFactory.gen_client.<locals>.TxDeferredClient

    .. automethod:: __init__
    .. automethod:: query
    .. automethod:: query_ex
    .. automethod:: view_query
    .. automethod:: view_query_ex
    .. automethod:: analytics_query
    .. automethod:: search_query


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
