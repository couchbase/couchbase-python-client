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
