==================
`gevent` Interface
==================

.. _JIRA: https://issues.couchbase.com/browse/PYCBC-590

.. warning::
    The async APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. They will
    be updated to support SDK3 shortly. See JIRA_.

.. module:: gcouchbase

The ``gcouchbase`` module offers a complete API which is fully compatible
with the :class:`couchbase_v2.bucket.Bucket` API, but is fully aware
and optimized for the gevent :class:`~gevent.hub.Hub`.

Currently, this has been tested with `gevent` version 0.13 and 1.0.0.


As the `gcouchbase` implementation relies on `gevent` internal APIs
itself there may be incompatibilities between minor gevent releases,
although this is not expected.

Example usage::

    from gcouchbase import Bucket
    cb = Bucket('couchbase://localhost/default')

    # Like the normal Bucket API
    res = cb.upsert("foo", "bar")
    res = cb.get("foo")


    viewiter = cb.query("beer", "brewery_beers", limit=4)
    for row in viewiter:
        print("Have row {0}".format(row))



.. module:: gcouchbase.bucket

.. autoclass:: Bucket
    :show-inheritance: