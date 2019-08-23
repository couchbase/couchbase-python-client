============
Item Objects
============

.. versionadded:: 1.1.0

.. module:: couchbase_core.items

The :class:`~couchbase_core.items.Item` class is a subclass of
:class:`~couchbase_core.result.ValueResult`. It differs from its parent in that
it is instantiable by the user, and can also contain fields not originally
defined in the superclass (i.e. it has a ``__dict__`` field).

These objects may be passed (via either the
:class:`couchbase_core.items.ItemOptionDict` or
:class:`couchbase_core.items.ItemSequence` containers) to any of the ``_multi``
functions of the :class:`~couchbase_core.client.Client` objects.

Since the `Item` structure is backwards-compatible (and therefore,
interchangeable) with any of the key-value subtypes of the
:class:`~couchbase_core.results.Result` object, a new `Result` object is not
created for each operation in the returned
:class:`~couchbase_core.result.MultiResult` dictionary.

This approach allows you to maintain a persistent object representing your
data locally; periodically updating it from the Couchbase server.

Using the `Item` collections also allows per-item options for any of the
``_multi`` methods.


--------------
Creating Items
--------------

`Item` objects may be created simply by calling the zero-arg constructor::

    from couchbase_core.items import Item
    it = Item()

Before an `Item` object can be passed to any of the `Bucket` methods,
it *must* have its key set. You can simply assign the key to the object's
`key` property::

    it.key = "some_key"

In order to store the actual item, you should assign it a value, and place
it inside one of the collections mentioned before. Here we'll use the
:class:`~couchbase_core.items.ItemOptionDict` which can also contain per-item
options::

    from couchbase_core.items import ItemOptionDict
    itmdict = ItemOptionDict()

    # Need to add the value:
    it.value = "some string"

    itmdict.add(it, format=couchbase_core.FMT_UTF8)

To actually store the item, you pass the *collection* to the
:meth:`~couchbase_core.client.Client.upsert_multi` method, and it will function as
normally::

    mres = cb.set_multi(itmdict)

``mres`` is a `MultiResult` object. The value for each key will now contain
the `Item` passed originally within the collection. The normal fields including
``cas``, ``flags``.


---------------
Class Reference
---------------

.. versionadded:: 1.1.0

.. autoclass:: Item
    :members:

.. autoclass:: ItemCollection

.. autoclass:: ItemOptionDict
    :show-inheritance:
    :members:

.. autoclass:: ItemSequence
    :show-inheritance:
