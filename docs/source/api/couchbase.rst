=================
Connection object
=================

.. module:: couchbase
.. autoclass:: Couchbase
    :members:
    :no-undoc-members:

.. module:: couchbase.libcouchbase

.. autoclass:: Connection
    :no-undoc-members:


.. _argtypes:

Passing Arguments
=================

.. currentmodule:: couchbase.libcouchbase

All keyword arguments passed to methods should be specified as keyword
arguments, and the user should not rely on their position within the keyword
specification - as this is subject to change.

Thus, if a function is prototyped as::

    def foo(self, key, foo=None, bar=1, baz=False)

then arguments passed to ``foo()`` should *always* be in the form of ::

    obj.foo(key, foo=fooval, bar=barval, baz=bazval)

and never like ::

    obj.foo(key, fooval, barval, bazval)

Arguments To ``*_multi`` Methods
--------------------------------

Arguments passed to ``*_multi`` methods involves passing an iterable of keys.
The iterable must have ``__len__`` and ``__iter__`` implemented.

For operations which require values (i.e. the
:meth:`~couchbase.libcouchbase.Connection.set_multi` family), a ``dict`` must
be passed with the values set as the values which should be stored for the keys.

Some of the multi methods accept keyword arguments; these arguments apply to
*all* the keys within the iterable passed.


.. _format_info:

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
:attr:`~couchbase.libcouchbase.Result.flags` attribute of the
:class:`~couchbase.libcouchbase.Result` object from a
:meth:`~couchbase.libcouchbase.Connection.get` operation.

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


Key Format
----------

The above format options are only valid for *values* being passed to one
of the storage methods (see :meth:`couchbase.libcouchbase.Connection.set`).

For *keys*, the acceptable inputs are those for :const:`FMT_UTF8`

Single-Key Data Methods
=======================

These methods all return a :class:`Result` object containing
information about the operation (such as status and value).

.. currentmodule:: couchbase.libcouchbase


Storing Data
------------

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    These methods set the contents of a key in Couchbase. If successful,
    they replace the existing contents (if any) of the key.

    .. automethod:: set

    .. automethod:: add

    .. automethod:: replace

    .. automethod:: add

Retrieving Data
---------------

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. automethod:: get

Modifying Data
--------------

These methods modify existing values in Couchbase

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection


    .. automethod:: append

    .. automethod:: prepend

Entry Operations
----------------

These methods affect an entry in Couchbase. They do not
directly modify the value, but may affect the entry's accessibility
or duration.


.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. automethod:: delete

    .. automethod:: lock

    .. automethod:: unlock

    .. automethod:: touch


Counter Operations
------------------

These are atomic counter operations for Couchbase. They increment
or decrement a counter. A counter is a key whose value can be parsed
as an integer. Counter values may be retrieved (without modification)
using the :meth:`get` method

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. automethod:: incr

    .. automethod:: decr


Multi-Key Data Methods
======================

These methods tend to be more efficient than their single-key
equivalents. They return a :class:`MultiResult` object (which is
a dict subclass) which contains clasS:`Result` objects as the
values for its keys

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. automethod:: set_multi

    .. automethod:: get_multi

    .. automethod:: add_multi

    .. automethod:: replace_multi

    .. automethod:: append_multi

    .. automethod:: prepend_multi

    .. automethod:: delete_multi

    .. automethod:: incr_multi

    .. automethod:: decr_multi

    .. automethod:: lock_multi

    .. automethod:: unlock_multi

    .. automethod:: touch_multi

MapReduce/View Methods
======================

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. warning::
    
        The view API is not considered stable. It will likely change in
        the future

    .. automethod:: _view

    .. automethod:: _design

Informational Methods
=====================

These methods do not operate on keys directly, but offer various
information about things

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. automethod:: stats

    .. automethod:: errors

    .. automethod:: lcb_version

Attributes
==========

.. currentmodule:: couchbase.libcouchbase
.. class:: Connection

    .. autoattribute:: quiet

    .. autoattribute:: transcoder

    .. autoattribute:: data_passthrough

    .. autoattribute:: unlock_gil

    .. autoattribute:: timeout

    .. autoattribute:: server_nodes

    .. attribute:: default_format

        Specify the default format (default: const:`~couchbase.FMT_JSON`)
        to encode your data before storing in Couchbase. It uses the
        flags field to store the format.

        See :ref:`format_info` for the possible values

        On a :meth:`~couchbase.libcouchbase.Connection.get` the
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
        still set an error inside the :class:`Result` object.
