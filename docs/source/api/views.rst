============
View Options
============

.. module:: couchbase.views.params


This document explains the various view options, and how they are treated
by the Couchbase library.


Many of the view options correspond to those listed here
http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-querying-rest-api.html


Note that these explain the view options and their values as they are passed
along to the server.


Value Type For Options
----------------------

.. currentmodule:: couchbase.views.params

Different options accept different types, which shall be enumerated here


.. _viewtype_boolean:

Boolean Type
^^^^^^^^^^^^

.. currentmodule:: couchbase.views.params

Options which accept booleans may accept the following Python types:

    * Standard python ``bool`` types, like ``True`` and ``False``
    * Numeric values which evaluate to booleans
    * Strings containing either ``"true"`` or ``"false"``

Other options passed as booleans will raise an error, as it is assumed that
perhaps it was passed accidentally due to a bug in the application.


.. _viewtype_num:

Numeric Type
^^^^^^^^^^^^

.. currentmodule:: couchbase.views.params

Options which accept numeric values accept the following Python types:

    * ``int``, ``long`` and ``float`` objects
    * Strings which contain values convertible to said native numeric types

It is an error to pass a ``bool`` as a number, despite the fact that in Python,
``bool`` are actually a subclass of ``int``.


.. _viewtype_jsonvalue:

JSON Value
^^^^^^^^^^

.. currentmodule:: couchbase.views.params

Options which accept JSON values accept native Python types (and any user-
defined classes) which can successfully be passed through ``json.dumps``.

Do *not* pass an already-encoded JSON string, and do not URI-escape the
string either - as this will be done by the option handling layer (but see
:ref:`passthrough_values` for a way to circumvent this)

Note that it is perfectly acceptable to pass JSON primitives (such as numbers,
strings, and booleans).


.. _viewtype_jsonarray:

JSON Array
^^^^^^^^^^

.. currentmodule:: couchbase.views.params

Options which accept JSON array values should be pass a Python type which
can be converted to a JSON array. This typically means any ordered Python
sequence (such as ``list`` and ``tuple``). Like :ref:`viewtype_jsonvalue`,
the contents of the list should *not* be URI-escaped, as this will be done
at the option handling layer


.. _viewtype_string:

String
^^^^^^

.. currentmodule:: couchbase.views.params

Options which accept strings accept so-called "semantic strings", specifically;
the following Python types are acceptable:

    * ``str`` and ``unicode`` objects
    * ``int`` and ``long`` objects

Note that ``bool``, ``none`` and other objects are not accepted - this is to
ensure that random objects passed don't simply end up being ``repr()``'d
and causing confusion in your view results.

If you have a custom object which has a ``__str__`` method and would like to
use it as a string, you must explicitly do so prior to passing it as an option.

.. _viewtype_range:

Range Value
^^^^^^^^^^^

.. currentmodule:: couchbase.view.params

Range specifiers take a sequence (list or tuple) of one or two elements.

If the sequence contains two items, the first is taken to be the *start*
of the range, and the second is taken to be its (non-inclusive) *end*

If the sequence contains only a single item, it is taken to be the
*start* of the range, and no *end* will be specified.

To specify a range which has an *end* but not a start, pass a two-element
sequence with the first element being an :data:`UNSPEC` value.


The type of each element is parameter-specific.


.. _viewtype_unspec:

Unspecified Value
^^^^^^^^^^^^^^^^^

.. currentmodule:: couchbase.views.params

Conventionally, it is common for APIs to treat the value ``None`` as being
a default parameter of some sort. Unfortunately since view queries deal with
JSON, and ``None`` maps to a JSON ``null``, it is not possible for the view
processing functions to ignore ``None``.

As an alternative, a special constant is provided as
:data:`UNSPEC`. You may use this as a placeholder value for any
option. When the view processing code encounters this value, it will
discard the option-value pair.


Common View Parameters
======================

.. currentmodule:: couchbase.views.params


Note that only view parameters which are client-only are documented here.
More documentation as to the meaning of each parameter may be found in the
server manual.

.. class:: Params

Range Parameters
----------------

These parameters affect the sorting, ordering, and filtering of rows.

.. currentmodule:: couchbase.views.params

.. class:: Params

    .. data:: MAPKEY_RANGE

        Specify the range based on the contents of the keys emitted by the
        view's ``map`` function.

        :Server Option: Maps to both ``startkey`` and ``endkey``
        :String Literal: ``"mapkey_range"``
        :Value Type:
            :ref:`viewtype_range` of :ref:`viewtype_jsonvalue` elements

    .. data:: DOCKEY_RANGE

        Specify the range based on the contents of the keys as they are stored
        by :meth:`~couchbase.libcouchbase.Connection.set`. These are
        returned as the "Document IDs" in each view result.

        :Server Option: Maps to both ``startkey_docid`` and ``endkey_docid``
        :String Literal: ``"dockey_range"``
        :Value Type:
            :ref:`viewtype_range` of :ref:`viewtype_string` elements.

    .. data:: MAPKEY_SINGLE

        Limit the view results to those keys which match the value to this
        option

        :String Literal: ``"mapkey_single"``
        :Server Option: Maps to ``key``
        :Value Type: :ref:`viewtype_jsonvalue`

    .. data:: MAPKEY_MULTI

        Like :data:`MAPKEY_SINGLE`, but specify a sequence of keys.
        Only rows whose emitted keys match any of the keys specified here
        will be returned.

        :String Literal: ``"mapkey_multi"``
        :Server Option: ``keys``
        :Value Type: :ref:`viewtype_jsonarray`

    .. data:: INCLUSIVE_END

        :Server Option: ``inclusive_end``
        :Value Type: :ref:`viewtype_boolean`

    .. data:: DESCENDING

        :Server Option: ``descending``
        :Value Type: :ref:`viewtype_boolean`


Reduce Function Parameters
--------------------------

.. currentmodule:: couchbase.views.params

These options are valid only for views which have a ``reduce`` function,
and for which the :attr:`Params.REDUCE` value is enabled

.. class:: Params

    .. data:: REDUCE

        :Server Option: ``reduce``
        :Value Type: :ref:`viewtype_boolean`

    .. data:: GROUP

        :Server Option: ``group``
        :Value Type: :ref:`viewtype_boolean`

    .. data:: GROUP_LEVEL

        :Server Option: ``group_level``
        :Value Type: :ref:`viewtype_num`

Pagination and Sampling
-----------------------
.. currentmodule:: couchbase.views.params

These options limit or paginate through the results

.. class:: Params

    .. data:: SKIP

        :Server Option: ``skip``
        :Value Type: :ref:`viewtype_num`

    .. data:: LIMIT

        :Server Option: ``limit``
        :Value Type: :ref:`viewtype_num`


Miscellaneous Options
---------------------

.. currentmodule:: couchbase.views.params

These do not particularly affect the actual query behavior, but may
control some other behavior which may indirectly impact performance or
indexing operations.

.. class:: Params

    .. data:: STALE

        :Server Option: ``stale``
        :Value Type:
            A string containing either ``"ok"``, ``"false"`` or
            ``"update_after"``.
            The constants :data:`STALE_OK`, :data:`STALE_UPDATE_BEFORE`, and
            :data:`STALE_UPDATE_AFTER` may be used as well

            A :ref:`viewtype_boolean` may be used as well, in which case
            ``True`` is converted to ``"ok"``

    .. data:: ON_ERROR

        :Server Option: ``on_error``
        :Value Type:
            A string of either ``"stop"`` or ``"continue"``. You may use
            the symbolic constants :data:`ONERROR_STOP` or
            :data:`ONERROR_CONTINUE`

    .. data:: CONNECTION_TIMEOUT

        This parameter is a server-side option indicating how long
        a given node should wait for another node to respond. This does
        *not* directly set the client-side timeout.

        :Server Option: ``connection_timeout``
        :Value Type: :ref:`viewtype_num`

    .. data:: DEBUG

        :Server Option: ``debug``
        :Value Type: :ref:`viewtype_boolean`

    .. data:: FULL_SET

        :Server Option: ``full_set``
        :Value Type: :ref:`viewtype_boolean`


Raw Server Parameters
---------------------

.. currentmodule:: couchbase.views.params

These constants are present in the view API but are wrapped by various
other options above. Nevertheless, using the raw options are supported.

.. class:: Params

    .. data:: STARTKEY

        :Server Option: ``startkey``
        :Value Type: :ref:`viewtype_jsonvalue`

    .. data:: ENDKEY

        :Server Option: ``endkey``
        :Value Type: :ref:`viewtype_jsonvalue`

    .. data:: STARTKEY_DOCID

        :Server Option: ``startkey_docid``
        :Value Type: :ref:`viewtype_string`

    .. data:: ENDKEY_DOCID

        :Server Option: ``endkey_docid``
        :Value Type: :ref:`viewtype_string`



Convenience Constants
---------------------

.. currentmodule:: couchbase.views.params

These are convenience *value* constants for some of the options

.. autoattribute:: couchbase.views.params.ONERROR_CONTINUE
.. autoattribute:: couchbase.views.params.ONERROR_STOP
.. autoattribute:: couchbase.views.params.STALE_OK
.. autoattribute:: couchbase.views.params.STALE_UPDATE_BEFORE
.. autoattribute:: couchbase.views.params.STALE_UPDATE_AFTER
.. autoattribute:: couchbase.views.params.UNSPEC




.. _passthrough_values:

Circumventing Parameter Constraints
-----------------------------------

.. currentmodule:: couchbase.views.params

Sometimes it may be necessary to circumvent existing constraints placed by
the client library regarding view option validation.

For this, there are ``passthrough`` and ``allow_unrecognized`` options
which may be set in order to allow the client to be more lax in its conversion
process.

These options are present under various names in the various view query
functions.


* Passthrough

    Passthrough removes any conversion functions applied. It simply assumes
    values for all options are strings, and then encodes them rather simply

* Allowing Unrecognized Options

    If a newer version of a server is released has added a new option, older
    versions of this library will not know about it, and will raise an error
    when it is being used. In this scenario, one can use the 'allow unrecognized'
    mode to *add* extra options, with their values being treated as simple
    strings.

    This has the benefit of providing normal behavior for known options.
