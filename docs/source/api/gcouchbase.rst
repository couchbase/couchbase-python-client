========================
GEvent Couchbase Support
========================


.. module:: gcouchbase

The ``gcouchbase`` module offers a complete API which is fully compatible
with the :class:`couchbase.connection.Connection` API, but is fully aware
and optimized for the gevent :class:`~gevent.hub.Hub`.

Currently, this has been tested with `gevent` version 0.13, and it is
unknown if it will work with the 1.0 series.

Example usage::

    from couchbase import experimental
    experimental.enable()

    from gcouchbase import GConnection
    cb = GConnection(bucket='default')

    # Like the normal Connection API
    res = cb.set("foo", "bar")
    res = cb.get("foo")


    viewiter = cb.query("beer", "brewery_beers", limit=4)
    for row in viewiter:
        print("Have row {0}".format(row))
