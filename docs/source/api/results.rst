##############
Result Objects
##############

.. currentmodule:: couchbase.result

This is the base class for all result operations

.. autoclass:: Result

   .. autoattribute:: rc

   .. autoattribute:: success

   .. autoattribute:: errstr

   .. autoattribute:: key


.. autoclass:: OperationResult
   :show-inheritance:
   :no-undoc-members:

   .. autoattribute cas


.. autoclass:: ValueResult
   :show-inheritance:
   :members:

   .. autoattribute:: cas


.. autoclass:: HttpResult
    :show-inheritance:
    :members:
    :no-inherited-members:




.. class:: MultiResult


    This class is intended to serve as a container for multiple results returned
    from an operation. It is a subclass of `dict` and may be used as such. The
    keys will be the keys on which the operations were performed and the values
    will be the results of the operation (i.e. a :class:`OperationResult` object)

    The :attr:`all_ok` field can be used to quickly examine the object for errors
    (in case something like ``quiet`` was passed to
    :meth:`~couchbase.bucket.Bucket.get_multi`), e.g.

    Using the `all_ok` field::

        results = cb.get_multi(("foo", "bar", "baz"), quiet=True)
        if not results.all_ok:
            # process error handling here
            print "Some results did not complete successfully"


    If an exception is propagated during the operation, the ``MultiResult`` class
    will still contain valid contents, except than being a return value, it will
    be available via the thrown exceptions'
    :attr:`~couchbase.exceptions.CouchbaseError.all_results` field. From this field
    you can inspect the non-failed operations and handle them as approrpiate, while
    only invoking error handling for those items which explicitly contained an error

    Using the ``MultiResult`` class from an exception handler::

        try:
            cb.insert({"foo":"fooval","bar":"barval"})
        except CouchbaseDataError as e:
            for key, result in e.all_results.items():
                if not result.success:
                    print "Could not add {0}. Got error code {1}".format(key, result.rc)


    .. autoattribute:: all_ok

.. _observe_info:

===============
Observe Results
===============

-----------------
Observe Constants
-----------------

These constants are returned as values for :attr:`ObserveInfo.flags`
field.

.. data:: couchbase.OBS_FOUND

    The key exists on the given node's cache, though it may not have been
    stored to disk yet.

.. data:: couchbase.OBS_PERSISTED

    The key is persisted to the given node's disk.

.. data:: couchbase.OBS_NOTFOUND

    The key is not present in the node's cache.

.. data:: couchbase.OBS_LOGICALLY_DELETED

    The key is not present in the node's cache, however it is still present
    on the persistent store. If the node would crash at this moment, the key
    would still be present when it starts up again.

    This is equivalent to ``OBS_NOTFOUND | OBS_PERSISTED``

--------------------
`ObserveInfo` Object
--------------------

.. module:: couchbase.result

.. autoclass:: couchbase.result.ObserveInfo
    :members:
