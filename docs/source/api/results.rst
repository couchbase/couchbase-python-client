Result Objects
===============

.. module:: couchbase.libcouchbase

This is the base class for all result operations

.. autoclass:: couchbase.libcouchbase.Result

   .. autoattribute:: rc

   .. autoattribute:: success

   .. autoattribute:: errstr

   .. autoattribute:: key

.. autoclass:: couchbase.libcouchbase.OperationResult
   :show-inheritance:
   :members:

.. autoclass:: couchbase.libcouchbase.ValueResult
   :show-inheritance:
   :members:

This object is returned by multi-key operations

.. autoclass:: couchbase.libcouchbase.MultiResult
   :members:
   :no-inherited-members:

This object is returned by generic HTTP operations

.. autoclass:: couchbase.libcouchbase.HttpResult
    :show-inheritance:
    :members:
    :no-inherited-members:

.. _observe_info:

Observe Results
===============

Observe Constants
-----------------

.. currentmodule:: couchbase.libcouchbase

These constants are returned as bits in the :attr:`ObserveInfo.flags`
field.

.. data:: OBS_FOUND

    The key exists on the given node's cache, though it may not have been
    stored to disk yet.

.. data:: OBS_PERSISTED

    The key is persisted to the given node's disk.

.. data:: OBS_NOTFOUND

    The key is not present in the node's cache.

`ObserveInfo` Object
----------------------

.. module:: couchbase.libcouchbase

.. autoclass:: couchbase.libcouchbase.ObserveInfo
    :members:
