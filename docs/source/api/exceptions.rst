=================
Exception Objects
=================

.. module:: couchbase.exceptions

-------------------------------
Exception Types and Classifiers
-------------------------------

.. versionadded:: 1.2.1

Couchbase exceptions may be caught in two flavors. You can catch an exception
either by its explicit subclass or by its base class.


Normally you should catch exception classes for cases you don't have a specific
handler for, and error details for things that need special handling. Thus
for example:

::

    try:
        cb.get("foo")
    except NotFoundError:
        print("Item does not exist")
    except CouchbaseTransientError:
        print("Transient error received. Handling and backing off")

Where `NotFoundError` is a specific error detail code indicating the item has
not been found, and `CouchbaseTransientError` is an error category indicating
the specific cause is likely transient.

As your application evolves you may wish to examine the specific error code
received as well as log the information to the screen.

Employing the error classifier pattern will prove scalable when additional
error codes are added to the library. Sticking to catching error codes rather
than specific error categories will allow your application to deal gracefully
with future error codes so long as they match a specific category.


You may also employ a different use model, for example:

::

    try:
        cb.get("foo")
    except CouchbaseError as e:
        if e.is_data and isinstance(e, NotFoundError):
            # handle not found
            pass
        elif e.is_network:
            print("Got network error")
        elif e.is_data:
            print("Got other data-related error")
        else:
            print("Got unhandled error code")


---------------------
Base Exception Object
---------------------

This object is the base class for all exceptions thrown by Couchbase which
are specific to the library. Other standard Python exceptions may still be
thrown depending on the condition.

.. autoexception:: CouchbaseError
   :members:

--------------------
Exception Categories
--------------------

These categories form the base exception classes

.. autoexception:: CouchbaseInternalError
.. autoexception:: CouchbaseNetworkError
.. autoexception:: CouchbaseInputError
.. autoexception:: CouchbaseFatalError
.. autoexception:: CouchbaseDataError
.. autoexception:: CouchbaseTransientError



-----------------
Exception Details
-----------------

The following codes are exception details. They all derive from
:exc:`CouchbaseError`. Many of them will have multiple error categories and thus
be inherited from multiple exception categories.

.. autoexception:: ArgumentError
   :show-inheritance:
.. autoexception:: ValueFormatError
   :show-inheritance:
.. autoexception:: AuthError
   :show-inheritance:
.. autoexception:: DeltaBadvalError
   :show-inheritance:
.. autoexception:: TooBigError
   :show-inheritance:
.. autoexception:: BusyError
   :show-inheritance:
.. autoexception:: InternalError
   :show-inheritance:
.. autoexception:: InvalidError
   :show-inheritance:
.. autoexception:: NoMemoryError
   :show-inheritance:
.. autoexception:: RangeError
   :show-inheritance:
.. autoexception:: LibcouchbaseError
   :show-inheritance:
.. autoexception:: TemporaryFailError
   :show-inheritance:
.. autoexception:: KeyExistsError
   :show-inheritance:
.. autoexception:: NotFoundError
   :show-inheritance:
.. autoexception:: DlopenFailedError
   :show-inheritance:
.. autoexception:: DlsymFailedError
   :show-inheritance:
.. autoexception:: NetworkError
   :show-inheritance:
.. autoexception:: NotMyVbucketError
   :show-inheritance:
.. autoexception:: NotStoredError
   :show-inheritance:
.. autoexception:: NotSupportedError
   :show-inheritance:
.. autoexception:: UnknownCommandError
   :show-inheritance:
.. autoexception:: UnknownHostError
   :show-inheritance:
.. autoexception:: ProtocolError
   :show-inheritance:
.. autoexception:: TimeoutError
   :show-inheritance:
.. autoexception:: ConnectError
   :show-inheritance:
.. autoexception:: BucketNotFoundError
   :show-inheritance:
.. autoexception:: ClientNoMemoryError
   :show-inheritance:
.. autoexception:: ClientTemporaryFailError
   :show-inheritance:
.. autoexception:: BadHandleError
   :show-inheritance:
.. autoexception:: HTTPError
   :show-inheritance:
.. autoexception:: SubdocPathNotFoundError
   :show-inheritance:
.. autoexception:: SubdocPathExistsError
   :show-inheritance: