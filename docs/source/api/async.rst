======================
Asynchronous Interface
======================

.. module:: couchbase_core.asynchronous


.. _async_intro:

The asynchronous interface to the SDK is a work in progress, and is currently
intended to be used by integrators into higher level async wrappers. See
the ``txcouchbase`` package for an example.

This document largely explains the current internals of how the Couchbase
async module works in a lower level. For a higher level overview, see:
http://blog.couchbase.com/python-sdk-and-twisted


Key-Value Interface
===================

The Key-Value interface of the async subsystem functions as closely
as possible like its synchronous counterpart. The primary difference is that
where the synchronous interface would return an instance of a
:class:`~couchbase_core.result.Result` or a
:class:`~couchbase_core.result.MultiResult`,
the asynchronous interface returns an
:class:`~couchbase_core.result.AsyncResult` object.

The :class:`~couchbase_core.result.AsyncResult`
object contains fields for two callbacks which are
invoked when the result is ready. One is the
:attr:`~couchbase_core.result.AsyncResult.callback`
field which is called with a ``Result`` or ``MultiResult`` upon success,
and the other is the
:attr:`~couchbase_core.result.AsyncResult.errback` field which is invoked
with an exception object upon error.

The semantics of when an exception is passed follows the rules of the
``quiet`` parameter just like the synchronous API.

.. currentmodule:: couchbase_core.asynchronous.bucket

.. class:: couchbase_core.result.AsyncResult

    .. autoattribute:: callback

    .. autoattribute:: errback


.. autoclass:: AsyncBucket
    :members:
    :show-inheritance:

Views Interface
===============

Different from the key-value interface, the synchronous view API
returns a :class:`~couchbase_core.views.iterator.View` object which
is itself an iterator which yields results. Because this is a synchronous
API, the iterator interface must be replaced with a class interface
which must be subclassed by a user.

.. currentmodule:: couchbase_core.asynchronous.view

.. class:: AsyncViewBase

    .. automethod:: __init__
    .. automethod:: on_rows
    .. automethod:: on_error
    .. automethod:: on_done
    .. automethod:: start

I/O Interface
=============

The async API is divided into several sections. In order to have an async
client which interacts with other async libraries and frameworks, it is
necessary to make the Couchbase extension aware of that environment. To this
end, the ``IOPS`` interface is provided. The ``IOPS``
API is entirely separate from the key-value API and should be treated as
belonging to a different library. It is simply the extension's I/O
abstraction.

.. currentmodule:: couchbase_core.iops.base

.. class:: Event

    This class represents an `Event`. This concept should be familiar
    to the intended audience, who should be familiar with event loops
    and their terminology. It represents a certain event in the future,
    which shall be triggered either by something happening or by the
    passage of time.

    When said event takes place, the object should be signalled via
    the :meth:`ready` method.

    .. automethod:: ready

.. class:: IOEvent

    A subclass of :class:`Event`, this represents a socket. Events applied
    to this socket are triggered when the socket becomes available for reading
    or writing.

    .. automethod:: ready_r
    .. automethod:: ready_w
    .. automethod:: ready_rw
    .. automethod:: fileno

.. class:: TimerEvent

    Subclass of :class:`Event` which represents a passage of time.

.. class:: IOPS

    .. automethod:: __init__
    .. automethod:: update_event
    .. automethod:: update_timer
    .. automethod:: start_watching
    .. automethod:: stop_watching
    .. automethod:: io_event_factory
    .. automethod:: timer_event_factory

Action Constants
----------------

.. data:: PYCBC_EVACTION_WATCH

    Action indicating the specific event should be added to the event
    loop's "watcher" list, and should be have its :meth:`~Event.ready`
    method called when the IO implementation has detected the specific event
    is ready

.. data:: PYCBC_EVACTION_UNWATCH

    Action indicating that the specific object should not be notified
    when the IO state changes. This is typically done by removing it from
    the watcher list

.. data:: PYCBC_EVACTION_CLEANUP

    Action to permanently erase any references to this event


IO Event Constants
------------------
.. data:: LCB_READ_EVENT

    IO Flag indicating that this event should be notified on file readbility

.. data:: LCB_WRITE_EVENT

    IO flag indicating that this event should be notified on file writeability


.. data:: LCB_RW_EVENT

    Equivalent to ``LCB_READ_EVENT|LCB_WRITE_EVENT``
