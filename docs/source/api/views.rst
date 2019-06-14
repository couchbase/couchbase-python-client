##############
Querying Views
##############

===============
``View`` Object
===============

.. module:: couchbase_core.views.iterator

.. class:: View

    .. automethod:: __init__

    .. automethod:: __iter__

^^^^^^^^^^
Attributes
^^^^^^^^^^

    .. attribute:: errors

        Errors returned from the view engine itself


    .. attribute:: indexed_rows

        Number of total rows indexed by the view. This is the number of results
        before any filters or limitations applied.
        This is only valid once the iteration has started


    .. attribute:: row_processor

        An object to handle a single page of the paginated results. This
        object should be an instance of a class conforming to the
        :class:`RowProcessor` interface. By default, it is an instance of
        :class:`RowProcessor` itself.


    .. attribute:: raw

        The actual :class:`couchbase_core.bucket.HttpResult` object.
        Note that this is only the *last* result returned. If using paginated
        views, the view comprises several such objects, and is cleared each
        time a new page is fetched.


    .. attribute:: design

        Name of the design document being used


    .. attribute:: view

        Name of the view being queired


    .. attribute:: include_docs

        Whether documents are fetched along with each row



^^^^^^^^^^^^^^
Row Processing
^^^^^^^^^^^^^^

.. class:: RowProcessor

    .. automethod:: handle_rows


.. class:: ViewRow

    This is the default class returned by the :class:`RowProcessor`

    .. attribute:: key

        The key emitted by the view's ``map`` function (first argument to ``emit``)


    .. attribute:: value

        The value emitted by the view's ``map`` function (second argument to
        ``emit``). If the view was queried with ``reduce`` enabled, then this
        contains the reduced value after being processed by the ``reduce``
        function.


    .. attribute:: docid

        This is the document ID for the row. This is always ``None`` if
        ``reduce`` was specified. Otherwise it may be passed to one of the
        ``get`` or ``set`` method to retrieve or otherwise access the
        underlying document. Note that if ``include_docs`` was specified,
        the :attr:`doc` already contains the document


    .. attribute:: doc

        If ``include_docs`` was specified, contains the actual
        :class:`couchbase_core.bucket.Result` object for the document.



================
``Query`` Object
================

.. module:: couchbase_core.views.params


.. class:: Query

    .. automethod:: __init__

    .. automethod:: update

    .. autoattribute:: encoded


.. _view_options:

^^^^^^^^^^^^
View Options
^^^^^^^^^^^^

This document explains the various view options, and how they are treated
by the Couchbase library.


Many of the view options correspond to those listed here
http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-querying-rest-api.html


Note that these explain the view options and their values as they are passed
along to the server.

.. _param_listings:

These attributes are available as properties (with get and set)
and can also be used as keys within a constructor.


^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Result Range and Sorting Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following properties allow you to

* Define a range to limit your results (i.e. `between foo and bar`)
* Define a specific subset of keys for which results should be yielded
* Reverse the sort order

.. class:: Query


    .. attribute:: mapkey_range

        Specify the range based on the contents of the keys emitted by the
        view's ``map`` function.

        :Server Option: Maps to both ``startkey`` and ``endkey``
        :Value Type:
            :ref:`viewtype_range` of :ref:`viewtype_jsonvalue` elements

        The result output depends on the type of keys and ranges used.

        One may specify a "full" range (that is, an exact match of the first
        and/or last key to use), or a partial range where the start and end
        ranges specify a *subset* of the key to be used as the start and end.
        In such a case, the results begin with the first key which matches
        the partial start key, and ends with the first key that matches the
        partial end key.

        Additionally, keys may be *compound* keys, i.e. complex data types
        such as lists.

        You may use the :const:`STRING_RANGE_END` to specify a wildcard
        for an end range.

        Match all keys that start with "a" through keys starting with "f"::

            q.mapkey_range = ["a", "f"+q.STRING_RANGE_END]
            q.inclusive_end = True

        If you have a view function that looks something like this::

            function(doc, meta) {
                if (doc.city && doc.event) {
                    emit([doc.country, doc.state, doc.city], doc.event)
                }
            }

        Then you may query for all events in a specific state by using::

            q.mapkey_range = [
                ["USA", "NV", ""]
                ["USA", "NV", q.STRING_RANGE_END]
            ]

        While the first two elements are an exact match (i.e. only keys which
        have ``["USA","NV", ...]`` in them, the third element should accept
        anything, and thus has its start value as the empty string (i.e. lowest
        range) and the magic ``q.STRING_RANGE_END`` as its lowest value.

        As such, the results may look like::

            ViewRow(key=[u'USA', u'NV', u'Reno'], value=u'Air Races', docid=u'air_races_rno', doc=None)
            ViewRow(key=[u'USA', u'NV', u'Reno'], value=u'Reno Rodeo', docid=u'rodeo_rno', doc=None)
            ViewRow(key=[u'USA', u'NV', u'Reno'], value=u'Street Vibrations', docid=u'street_vibrations_rno', doc=None)

            # etc.

    .. autoattribute:: STRING_RANGE_END

    .. attribute:: dockey_range

        :Server Option: Maps to both ``startkey_docid`` and ``endkey_docid``
        :Value Type:
            :ref:`viewtype_range` of :ref:`viewtype_string` elements.

        Specify the range based on the contents of the keys as they are stored
        by :meth:`~couchbase.bucket.Bucket.upsert`. These are
        returned as the "Document IDs" in each view result.

        You *must* use this attribute in conjunction with
        :attr:`mapkey_range` option. Additionally, this option only has
        any effect if you are emitting duplicate keys for different
        document IDs. An example of this follows:

        Documents::

            c.upsert("id_1", { "type" : "dummy" })
            c.upsert("id_2", { "type" : "dummy" })
            # ...
            c.upsert("id_9", { "type" : "dummy" })


        View::

            // This will emit "dummy" for ids 1..9

            function map(doc, meta) {
                emit(doc.type);
            }



        Only get information about ``"dummy"`` docs for IDs 3 through 6::

            q = Query()
            q.mapkey_range = ["dummy", "dummy" + Query.STRING_RANGE_END]
            q.dockey_range = ["id_3", "id_6"]
            q.inclusive_end = True

        .. warning::

            Apparently, only the first element of this parameter has any
            effect. Currently the example above will start returning rows
            from ``id_3`` (as expected), but does not stop after reaching
            ``id_6``.

    .. attribute:: key

    .. attribute:: mapkey_single

        :Server Option: ``key``
        :Value Type: :ref:`viewtype_jsonvalue`

        Limit the view results to those keys which match the value to this
        option exactly.

        View::

            function(doc, meta) {
                if (doc.type == "brewery") {
                    emit([meta.id]);
                } else {
                    emit([doc.brewery_id, meta.id]);
                }
            }

        Example::

            q.mapkey_single = "abbaye_de_maredsous"


        Note that as the ``map`` function can return more than one result with
        the same key, you may still get more than one result back.


    .. attribute:: keys

    .. attribute:: mapkey_multi

        :Server Option: ``keys``
        :Value Type: :ref:`viewtype_jsonarray`

        Like :attr:`mapkey_single`, but specify a sequence of keys.
        Only rows whose emitted keys match any of the keys specified here
        will be returned.

        Example::

            q.mapkey_multi = [
                ["abbaye_de_maresdous"],
                ["abbaye_de_maresdous", "abbaye_de_maresdous-8"],
                ["abbaye_do_maresdous", "abbaye_de_maresdous-10"]
            ]


    .. attribute:: inclusive_end

        :Server Option: ``inclusive_end``
        :Value Type: :ref:`viewtype_boolean`

        Declare that the range parameters' (for e.g. :attr:`mapkey_range` and
        :attr:`dockey_range`) end key should also be returned for rows that
        match it. By default, the resultset is terminated once the first key
        matching the end range is found.

    .. attribute:: descending

        :Server Option: ``descending``
        :Value Type: :ref:`viewtype_boolean`


^^^^^^^^^^^^^^^^^^^^^^^^^^
Reduce Function Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^

These options are valid only for views which have a ``reduce`` function,
and for which the ``reduce`` value is enabled

.. class:: Query

    .. attribute:: reduce

        :Server Option: ``reduce``
        :Value Type: :ref:`viewtype_boolean`

        Note that if the view specified in the query (to e.g.
        :meth:`couchbase.bucket.Bucket.view_query`) does not have a
        reduce function specified, an exception will be thrown once the query
        begins.

    .. attribute:: group

        :Server Option: ``group``
        :Value Type: :ref:`viewtype_boolean`

        Specify this option to have the results contain a breakdown of the
        ``reduce`` function based on keys produced by ``map``. By default,
        only a single row is returned indicating the aggregate value from all
        the ``reduce`` invocations.

        Specifying this option will show a breakdown of the aggregate ``reduce``
        value based on keys. Each unique key in the result set will have its
        own value.

        Setting this property will also set :attr:`reduce` to ``True``

    .. attribute:: group_level

        :Server Option: ``group_level``
        :Value Type: :ref:`viewtype_num`

        This is analoguous to ``group``, except that it places a constraint
        on how many elements of the compound key produced by ``map`` should be
        displayed in the summary. For example if this parameter is set to
        ``1`` then the results are returned for each unique first element in
        the mapped keys.

        Setting this property will also set :attr:`reduce` to ``True``

^^^^^^^^^^^^^^^^^^^^^^^
Pagination and Sampling
^^^^^^^^^^^^^^^^^^^^^^^

These options limit or paginate through the results

.. class:: Query

    .. attribute:: skip

        :Server Option: ``skip``
        :Value Type: :ref:`viewtype_num`

        .. warning::
            Consider using :attr:`mapkey_range` instead. Using this property
            with high values is typically inefficient.

    .. attribute:: limit

        :Server Option: ``limit``
        :Value Type: :ref:`viewtype_num`

        Set an absolute limit on how many rows should be returned in this
        query. The number of rows returned will always be less or equal to
        this number.

^^^^^^^^^^^^^^^
Control Options
^^^^^^^^^^^^^^^

These do not particularly affect the actual query behavior, but may
control some other behavior which may indirectly impact performance or
indexing operations.

.. class:: Query

    .. attribute:: stale

        :Server Option: ``stale``

        Specify the (re)-indexing behavior for the view itself. Views return
        results based on indexes - which are not updated for each query by
        default. Updating the index for each query would cause much performance
        issues. However it is sometimes desirable to ensure consistency of data
        (as sometimes there may be a delay between recently-updated keys and
        the view index).

        This option allows to specify indexing behavior. It accepts a string
        which can have one of the following values:

        * ``ok``

            Stale indexes are allowable. This is the default. The constant
            :data:`STALE_OK` may be used instead.

        * ``false``

            Stale indexes are not allowable. Re-generate the index before
            returning the results. Note that if there are many results, this
            may take a considerable amount of time (on the order of several
            seconds, typically).
            The constant :data:`STALE_UPDATE_BEFORE` may be used instead.

        * ``update_after``

            Return stale indexes for this result (so that the query does not
            take a long time), but re-generated the index immediately after
            returning.
            The constant :data:`STALE_UPDATE_AFTER` may be used instead.

        A :ref:`viewtype_boolean` may be used as well, in which case
        ``True`` is converted to ``"ok"``, and ``False``
        is converted to ``"false"``

    .. attribute:: on_error

        :Server Option: ``on_error``
        :Value Type:
            A string of either ``"stop"`` or ``"continue"``. You may use
            the symbolic constants :data:`ONERROR_STOP` or
            :data:`ONERROR_CONTINUE`

    .. attribute:: connection_timeout

        This parameter is a server-side option indicating how long
        a given node should wait for another node to respond. This does
        *not* directly set the client-side timeout.

        :Server Option: ``connection_timeout``
        :Value Type: :ref:`viewtype_num`

    .. attribute:: debug

        :Server Option: ``debug``
        :Value Type: :ref:`viewtype_boolean`

        If enabled, various debug output will be dumped in the resultset.

    .. attribute:: full_set

        :Server Option: ``full_set``
        :Value Type: :ref:`viewtype_boolean`

        If enabled, development views will operate over the entire data within
        the bucket (and not just a limited subset).



----------------------
Value Type For Options
----------------------

.. currentmodule:: couchbase_core.views.params

Different options accept different types, which shall be enumerated here


.. _viewtype_boolean:

^^^^^^^^^^^^
Boolean Type
^^^^^^^^^^^^

.. currentmodule:: couchbase_core.views.params

Options which accept booleans may accept the following Python types:

    * Standard python ``bool`` types, like ``True`` and ``False``
    * Numeric values which evaluate to booleans
    * Strings containing either ``"true"`` or ``"false"``

Other options passed as booleans will raise an error, as it is assumed that
perhaps it was passed accidentally due to a bug in the application.


.. _viewtype_num:

^^^^^^^^^^^^
Numeric Type
^^^^^^^^^^^^

.. currentmodule:: couchbase_core.views.params

Options which accept numeric values accept the following Python types:

    * ``int``, ``long`` and ``float`` objects
    * Strings which contain values convertible to said native numeric types

It is an error to pass a ``bool`` as a number, despite the fact that in Python,
``bool`` are actually a subclass of ``int``.


.. _viewtype_jsonvalue:

^^^^^^^^^^
JSON Value
^^^^^^^^^^

.. currentmodule:: couchbase_core.views.params

Options which accept JSON values accept native Python types (and any user-
defined classes) which can successfully be passed through ``json.dumps``.

Do *not* pass an already-encoded JSON string, and do not URI-escape the
string either - as this will be done by the option handling layer (but see
:ref:`passthrough_values` for a way to circumvent this)

Note that it is perfectly acceptable to pass JSON primitives (such as numbers,
strings, and booleans).


.. _viewtype_jsonarray:

^^^^^^^^^^
JSON Array
^^^^^^^^^^

.. currentmodule:: couchbase_core.views.params

Options which accept JSON array values should be pass a Python type which
can be converted to a JSON array. This typically means any ordered Python
sequence (such as ``list`` and ``tuple``). Like :ref:`viewtype_jsonvalue`,
the contents of the list should *not* be URI-escaped, as this will be done
at the option handling layer


.. _viewtype_string:

^^^^^^
String
^^^^^^

.. currentmodule:: couchbase_core.views.params

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

^^^^^^^^^^^
Range Value
^^^^^^^^^^^

.. currentmodule:: couchbase_core.view.params

Range specifiers take a sequence (list or tuple) of one or two elements.

If the sequence contains two items, the first is taken to be the *start*
of the range, and the second is taken to be its (non-inclusive) *end*

If the sequence contains only a single item, it is taken to be the
*start* of the range, and no *end* will be specified.

To specify a range which has an *end* but not a start, pass a two-element
sequence with the first element being an :data:`UNSPEC` value.


The type of each element is parameter-specific.


.. _viewtype_unspec:

^^^^^^^^^^^^^^^^^
Unspecified Value
^^^^^^^^^^^^^^^^^

.. currentmodule:: couchbase_core.views.params

Conventionally, it is common for APIs to treat the value ``None`` as being
a default parameter of some sort. Unfortunately since view queries deal with
JSON, and ``None`` maps to a JSON ``null``, it is not possible for the view
processing functions to ignore ``None``.

As an alternative, a special constant is provided as
:data:`UNSPEC`. You may use this as a placeholder value for any
option. When the view processing code encounters this value, it will
discard the option-value pair.

^^^^^^^^^^^^^^^^^^^^^
Convenience Constants
^^^^^^^^^^^^^^^^^^^^^

.. currentmodule:: couchbase_core.views.params

These are convenience *value* constants for some of the options

.. autoattribute:: couchbase_core.views.params.ONERROR_CONTINUE
.. autoattribute:: couchbase_core.views.params.ONERROR_STOP
.. autoattribute:: couchbase_core.views.params.STALE_OK
.. autoattribute:: couchbase_core.views.params.STALE_UPDATE_BEFORE
.. autoattribute:: couchbase_core.views.params.STALE_UPDATE_AFTER
.. autoattribute:: couchbase_core.views.params.UNSPEC




.. _passthrough_values:

-----------------------------------
Circumventing Parameter Constraints
-----------------------------------

.. currentmodule:: couchbase_core.views.params

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

