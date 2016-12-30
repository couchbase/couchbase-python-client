.. module:: couchbase.datastructures


==================
Datastructures API
==================

.. currentmodule:: couchbase.bucket

.. versionadded:: 2.1.1

The datastructure API allows your application to view Couchbase documents as
common data structures such as lists, maps, and queues.

Datastructure operations are implemented largely using
:mod:`couchbase.subdocument` operations, and also carry with some more
efficiency.


Couchbase datatypes are still JSON underneath, and use the
:meth:`couchbase.bucket.Bucket.mutate_in` and
:meth:`couchbase.bucket.Bucket.lookup_in` methods to access the structures
themselves.

The datastructures are:

- Map: This is similar to a Python `dict`. It is represented as a JSON object
  (i.e. ``{}``). Maps can be manipulated using the `map_*` methods below.

- List: This is similar to a Python `list`. It is represented as a JSON array
  (``[]``). Lists can be accessed using the `list_*` methods below.

- Set: This is like a List, but also contains methods allowing to avoid inserting
  duplicates. All List methods may be used on Sets, and vice versa. Sets can
  be accessed using the `set_*` methods below, as well as various `list_*`
  methods.

- Queues: This is like a list, but also features a ``queue_pop`` method
  which can make it suitable for a light weight FIFO queue.
  You can access queues using the `queue_*` methods below.


.. note::

    Datastructures are just JSON documents and can be accessed using any of the
    full-document (e.g. :meth:`~.Bucket.upsert` and :meth:`~.Bucket.get`) or
    sub-document (e.g. :meth:`~.Bucket.lookup_in` and :meth:`~.Bucket.mutate_in`)
    methods.


.. currentmodule:: couchbase.bucket

.. class:: Bucket
    :noindex:

    .. automethod:: map_add
    .. automethod:: map_get
    .. automethod:: map_size
    .. automethod:: map_remove

    .. automethod:: list_append
    .. automethod:: list_prepend
    .. automethod:: list_set
    .. automethod:: list_get
    .. automethod:: list_remove
    .. automethod:: list_size


    .. automethod:: set_size
    .. automethod:: set_add
    .. automethod:: set_remove
    .. automethod:: set_contains

    .. automethod:: queue_push
    .. automethod:: queue_pop
    .. automethod:: queue_size
