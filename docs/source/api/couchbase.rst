=================
Bucket object
=================

.. module:: couchbase.bucket
.. class:: Bucket

    .. automethod:: __init__
    .. automethod:: scope
    .. automethod:: default_collection

.. _argtypes:





=================
Scope object
=================

.. module:: couchbase.collection
.. class:: Scope

    .. automethod:: __init__
    .. automethod:: default_collection
    .. automethod:: open_collection


=================
Collection object
=================

.. class:: Collection

    .. automethod:: __init__


Used internally by the SDK.
This constructor is not intended for external use.


Passing Arguments
=================

.. currentmodule:: couchbase.collection

All keyword arguments passed to methods should be specified as keyword
arguments, and the user should not rely on their position within the keyword
specification - as this is subject to change.

Thus, if a function is prototyped as::

    def foo(self, key, foo=None, bar=1, baz=False)

then arguments passed to ``foo()`` should *always* be in the form of ::

    obj.foo(key, foo=fooval, bar=barval, baz=bazval)

and never like ::

    obj.foo(key, fooval, barval, bazval)



Key and Value Format
====================

.. currentmodule:: couchbase

By default, keys are encoded as UTF-8, while values are encoded as JSON;
which was selected to be the default for compatibility and ease-of-use
with views.


Format Options
--------------

The following constants may be used as values to the `format` option
in methods where this is supported. This is also the value returned in the
:attr:`~couchbase_core.result.ValueResult.flags` attribute of the
:class:`~couchbase_core.result.ValueResult` object from a
:meth:`~couchbase.collection.Collection.get` operation.

Each format specifier has specific rules about what data types it accepts.

.. data:: FMT_JSON

    Indicates the value is to be converted to JSON. This accepts any plain
    Python object and internally calls :meth:`json.dumps(value)`. See
    the Python `json` documentation for more information.
    It is recommended you use this format if you intend to examine the value
    in a MapReduce view function

.. data:: FMT_PICKLE

    Convert the value to Pickle. This is the most flexible format as it accepts
    just about any Python object. This should not be used if operating in
    environments where other Couchbase clients in other languages might be
    operating (as Pickle is a Python-specific format)

.. data:: FMT_BYTES

    Pass the value as a byte string. No conversion is performed, but the value
    must already be of a `bytes` type. In Python 2.x `bytes` is a synonym
    for `str`. In Python 3.x, `bytes` and `str` are distinct types. Use this
    option to store "binary" data.
    An exception will be thrown if a `unicode` object is passed, as `unicode`
    objects do not have any specific encoding. You must first encode the object
    to your preferred encoding and pass it along as the value.

    Note that values with `FMT_BYTES` are retrieved as `byte` objects.

    `FMT_BYTES` is the quickest conversion method.

.. data:: FMT_UTF8

    Pass the value as a UTF-8 encoded string. This accepts `unicode` objects.
    It may also accept `str` objects if their content is encodable as UTF-8
    (otherwise a :exc:`~couchbase.exceptions.ValueFormatError` is
    thrown).

    Values with `FMT_UTF8` are retrieved as `unicode` objects (for Python 3
    `unicode` objects are plain `str` objects).

.. data:: FMT_AUTO

    Automatically determine the format of the input type. The value of this
    constant is an opaque object.

    The rules are as follows:

    If the value is a ``str``, :const:`FMT_UTF8` is used. If it is a ``bytes``
    object then :const:`FMT_BYTES` is used. If it is a ``list``, ``tuple``
    or ``dict``, ``bool``, or ``None`` then :const:`FMT_JSON` is used.
    For anything else :const:`FMT_PICKLE` is used.


Key Format
----------

The above format options are only valid for *values* being passed to one
of the storage methods (see :meth:`couchbase.collection.Collection.upsert`).

For *keys*, the acceptable inputs are those for :const:`FMT_UTF8`

Single-Key Data Methods
=======================

These methods all return a :class:`~couchbase_core.result.Result` object containing
information about the operation (such as status and value).

.. currentmodule:: couchbase.collection


Storing Data
------------

.. currentmodule:: couchbase.collection
.. class:: Collection

    These methods set the contents of a key in Couchbase. If successful,
    they replace the existing contents (if any) of the key.

    .. automethod:: upsert

    .. automethod:: insert

    .. automethod:: replace


Retrieving Data
---------------

.. currentmodule:: couchbase.collection
.. class:: Collection

    .. automethod:: get

Modifying Data
--------------

These methods modify existing values in Couchbase

.. currentmodule:: couchbase.collection
.. class:: Collection


    .. automethod:: append

    .. automethod:: prepend

Entry Operations
----------------

These methods affect an entry in Couchbase. They do not
directly modify the value, but may affect the entry's accessibility
or duration.


.. currentmodule:: couchbase.collection
.. class:: Collection

    .. automethod:: remove

    .. automethod:: lock

    .. automethod:: unlock

    .. automethod:: touch


Sub-Document Operations
-----------------------

These methods provide entry points to modify *parts* of a document in
Couchbase.

.. note::

    Sub-Document API methods are available in Couchbase Server 4.5
    (currently in Developer Preview).

    The server and SDK implementations and APIs are subject to change


.. currentmodule:: couchbase.collection
.. class:: Collection

    .. automethod:: lookup_in
    .. automethod:: mutate_in

Counter Operations
------------------

These are atomic counter operations for Couchbase. They increment
or decrement a counter. A counter is a key whose value can be parsed
as an integer. Counter values may be retrieved (without modification)
using the :meth:`couchbase.collection.Collection.get` method

.. currentmodule:: couchbase.options
.. class:: SignedInt64

    .. automethod:: __init__


.. currentmodule:: couchbase.collection
.. class:: DeltaValue

    .. automethod:: __init__

.. class:: Collection

    .. automethod:: increment
    .. automethod:: decrement


Multi-Key Data Methods
======================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

These methods tend to be more efficient than their single-key
equivalents. They return a :class:`couchbase.result.MultiResult` object (which is
a dict subclass) which contains class:`couchbase.result.Result` objects as the
values for its keys

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

    .. automethod:: upsert_multi

    .. automethod:: get_multi

    .. automethod:: insert_multi

    .. automethod:: replace_multi

    .. automethod:: append_multi

    .. automethod:: prepend_multi

    .. automethod:: remove_multi

    .. automethod:: counter_multi

    .. automethod:: lock_multi

    .. automethod:: unlock_multi

    .. automethod:: touch_multi

Batch Operation Pipeline
========================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

In addition to the multi methods, you may also use the `Pipeline` context
manager to schedule multiple operations of different types

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

    .. automethod:: pipeline

.. class:: Pipeline

    .. autoattribute:: results


MapReduce/View Methods
======================

.. currentmodule:: couchbase.cluster
.. class:: Cluster

    .. automethod:: query

N1QL Query Methods
==================

.. currentmodule:: couchbase.cluster
.. class:: Cluster

    .. automethod:: query


Full-Text Search Methods
========================

.. currentmodule:: couchbase.cluster
.. class:: Cluster

    .. automethod:: search_query


Design Document Management
==========================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

.. currentmodule:: couchbase_core.bucketmanager


To perform design document management operations, you must first get
an instance of the :class:`BucketManager`. You can do this by invoking
the :meth:`~couchbase_v2.bucket.Bucket.bucket_manager` method on the
:class:`~couchbase_v2.bucket.Bucket` object.

.. note::
    Design document management functions are async. This means that any
    successful return value simply means that the operation was *scheduled*
    successfuly on the server. It is possible that the view or design will
    still not yet exist after creating, deleting, or publishing a design
    document. Therefore it may be recommended to verify that the view exists
    by "polling" until the view does not fail. This may be accomplished by
    specifying the ``syncwait`` parameter to the various design methods which
    accept them.

.. note::
    The normal process for dealing with views and design docs is to first
    create a `development` design document. Such design documents are
    prefixed with the string ``dev_``. They operate on a small subset of
    cluster data and as such are ideal for testing as they do not impact
    load very much.

    Once you are satisfied with the behavior of the development design doc,
    you can `publish` it into a production mode design doc. Such design
    documents always operate on the full cluster dataset.

    The view and design functions accept a ``use_devmode`` parameter which
    prefixes the design name with ``dev_`` if not already prefixed.


.. class:: BucketManager


    .. automethod:: design_create
    .. automethod:: design_get
    .. automethod:: design_publish
    .. automethod:: design_delete
    .. automethod:: design_list

N1QL Index Management
=====================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

.. currentmodule:: couchbase_core.bucketmanager

Before issuing any N1QL query using :cb_bmeth:`n1ql_query`, the bucket being
queried must have an index defined for the query. The simplest index is the
primary index.

To create a primary index, use::

    mgr.n1ql_index_create_primary(ignore_exists=True)

You can create additional indexes using::

    mgr.n1ql_create_index('idx_foo', fields=['foo'])

.. class:: BucketManager

    .. automethod:: n1ql_index_create
    .. automethod:: n1ql_index_create_primary
    .. automethod:: n1ql_index_drop
    .. automethod:: n1ql_index_drop_primary
    .. automethod:: n1ql_index_build_deferred
    .. automethod:: n1ql_index_watch
    .. automethod:: n1ql_index_list

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

    .. automethod:: bucket_manager


Flushing (clearing) the Bucket
==============================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

For some stages of development and/or deployment, it might be useful
to be able to clear the bucket of its contents.

.. currentmodule:: couchbase_core.bucket
.. class:: Bucket

    .. automethod:: flush


Informational Methods
=====================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

These methods do not operate on keys directly, but offer various
information about things

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

    .. automethod:: stats

    .. automethod:: lcb_version

    .. automethod:: observe

    .. automethod:: observe_multi

Item API Methods
================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

These methods are specifically for the :class:`~couchbase.items.Item`
API. Most of the `multi` methods will accept `Item` objects as well,
however there are some special methods for this interface

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

    .. automethod:: append_items
    .. automethod:: prepend_items

Durability Constraints
======================

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

Durability constraints ensure safer protection against data loss.

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

    .. automethod:: endure
    .. automethod:: endure_multi
    .. automethod:: durability

Attributes
==========

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

.. currentmodule:: couchbase_core.bucket
.. class:: Bucket

    .. autoattribute:: quiet

    .. autoattribute:: transcoder

    .. autoattribute:: data_passthrough

    .. autoattribute:: unlock_gil

    .. autoattribute:: timeout

    .. autoattribute:: views_timeout

    .. autoattribute:: n1ql_timeout

    .. autoattribute:: bucket

    .. autoattribute:: server_nodes

    .. autoattribute:: is_ssl

    .. autoattribute:: compression

    .. attribute:: default_format

        Specify the default format (default: :const:`~couchbase.FMT_JSON`)
        to encode your data before storing in Couchbase. It uses the
        flags field to store the format.

        See :ref:`format_info` for the possible values

        On a :meth:`~couchbase.bucket.Bucket.get` the
        original value will be returned. This means the JSON will be
        decoded, respectively the object will be unpickled.

        .. seealso::

            :ref:`format_info` and :attr:`data_passthrough`

    .. attribute:: quiet

        It controlls whether to raise an exception when the client
        executes operations on non-existent keys (default: `False`).
        If it is `False` it will raise
        :exc:`couchbase.exceptions.NotFoundError` exceptions. When
        set to `True` the operations will not raise an exception, but
        still set an error inside the :class:`~couchbase.result.Result` object.

    .. autoattribute:: lockmode


Private APIs
============

.. warning::
    The APIs below are from SDK2 and currently only available
    from the couchbase_v2 legacy support package. We plan
    to update these to support SDK3 shortly.

.. currentmodule:: couchbase_v2.bucket
.. class:: Bucket

   The following APIs are not supported because using them is inherently
   dangerous. They are provided as workarounds for specific problems which
   may be encountered by users, and for potential testing of certain states
   and/or modifications which are not attainable with the public API.

   .. automethod:: _close

   .. automethod:: _cntl

   .. automethod:: _cntlstr

   .. automethod:: _vbmap


.. _connopts:

Additional Connection Options
=============================

.. currentmodule:: couchbase.bucket

This section is intended to document some of the less common connection
options and formats of the connection string
(see :meth:`couchbase.bucket.Bucket.__init__`).


Using Custom Ports
-------------------

If you require to connect to an alternate port for bootstrapping the client
(either because your administrator has configured the cluster to listen on
alternate ports, or because you are using the built-in ``cluster_run``
script provided with the server source code), you may do so in the host list
itself.

Simply provide the host in the format of ``host:port``.

Note that the port is dependent on the *scheme* used. In this case, the scheme
dictates what specific service the port points to.


=============== ========
Scheme          Protocol
=============== ========
``couchbase``   memcached port (default is ``11210``)
``couchbases``  SSL-encrypted memcached port (default is ``11207``)
``http``        REST API/Administrative port (default is ``8091``)
=============== ========


Options in Connection String
----------------------------

Additional client options may be specified within the connection
string itself. These options are derived from the underlying
*libcouchbase* library and thus will accept any input accepted
by the library itself. The following are some influential options:


- ``config_total_timeout``. Number of seconds to wait for the client
  bootstrap to complete.

- ``config_node_timeout``. Maximum number of time to wait (in seconds)
  to attempt to bootstrap from the current node. If the bootstrap times
  out (and the ``config_total_timeout`` setting is not reached), the
  bootstrap is then attempted from the next node (or an exception is
  raised if no more nodes remain).

- ``config_cache``. If set, this will refer to a file on the
  filesystem where cached "bootstrap" information may be stored. This
  path may be shared among multiple instance of the Couchbase client.
  Using this option may reduce overhead when using many short-lived
  instances of the client.

  If the file does not exist, it will be created.
