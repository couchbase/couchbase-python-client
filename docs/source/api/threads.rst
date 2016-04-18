==============================
Using With and Without Threads
==============================

.. module:: couchbase.bucket

You can use a single :class:`~couchbase.bucket.Bucket` object in
a single thread, and attain reasonable performance by having one
`Bucket` object per thread. However, you **cannot** use the same object
from multiple threads concurrently (but see below)

As `couchbase` is a C extension, it is helpful to know how Python
deals with threads in general, and how it handles C extensions in
this context.

Python utilizes something called a *Global Interpreter Lock* (*GIL*) to
allow Python to simulate concurrency. The basic idea is that the interpreter
can only ever utilize a single CPU core. Threading is acheived by having the
interpreter switch to a different thread every *n* instructions (where *n*)
is typically `100` - but is set in :meth:`sys.getcheckinterval` and
:meth:`sys.setcheckinterval`. When C extension code is being executed
however, Python has no way of knowing how many 'instructions' have passed
and thus by default keeps the interpreter lock on for the duration of the
extension function.

See http://docs.python.org/2/c-api/init.html#thread-state-and-the-global-interpreter-lock
for more details on how Python handles the GIL.

Since `couchbase` does I/O operations, it is inefficient to keep the entire
interpreter locked during the wait for I/O responses; thus it has the ability
to unlock the *GIL* right before it begins an I/O operation, and lock it
back again when the operation is complete.

When operating in a threaded environment (i.e. with *other* Python threads)
running, it is helpful to have the GIL handling enabled, as it may potentially
block other threads' execution. When using in non-threaded mode (i.e. where
no other Python threads operate within the entire application), it would be
good to disable the GIL handling. This reduces locking/unlocking overhead and
potentially makes your program faster.

The default is to have GIL handling enabled, so as to not unexpectedly hang
other threads in the case where an I/O operation takes a prolonged amount
of time.

This behavior itself can be controlled by the
:attr:`~couchbase.bucket.Bucket.unlock_gil` attribute


.. _multiple_threads:

Using a :class:`Bucket` from multiple threads
---------------------------------------------------

Sometimes it may be necessary to use a :class:`Bucket` object from
multiple threads. This is normally not a good option as there is no concurrency
gained from multiple Python threads (as they do not run in parallel, as above)
it might be necessary to have a single object which is being used from
an already-existing framework using threads, where there is typically little
contention between them.

You may utilize the ``lockmode`` constructor option to enforce a specific
behavior when the :class:`Bucket` object is accessed from multiple
threads

.. data:: LOCKMODE_EXC

*This is the default lockmode*. If it is detected that the object is being used
from multiple threads, an exception will be raised indicating such.

Internally this uses the C equivalent of the ``threading.Lock`` object (i.e.
``PyThread_allocate_lock()``). Upon each entry to a function it will try
to acquire the lock (without waiting). If the acquisition fails, the
exception is raised.

.. data:: LOCKMODE_WAIT

In this mode, a lock is also used, only in this case an exception is not
raised. Rather, the current thread patiently waits until the other thread
has completed its operation and the lock is then acquired. It is released once
the current thread has finished performing the operation.

Without this option, odd behavior may be exhibited (including some crashes).
If you are sure that the :class:`Bucket` object will never be used from
multiple threads, or if you have some other locking mechanism in place, then
you may use :const:`LOCKMODE_NONE`

.. data:: LOCKMODE_NONE

No thread safety checks
