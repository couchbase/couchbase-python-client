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
