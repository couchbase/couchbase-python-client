==============================
Using With and Without Threads
==============================

.. module:: couchbase.threads

You can use a single :class:`~couchbase.libcouchbase.Connection` object in
a single thread, and attain reasonable performance by having one
`Connection` object per thread. However, you **cannot** use the same object
from multiple threads concurrently.

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
:attr:`~couchbase.libcouchbase.Connection.unlock_gil` attribute
