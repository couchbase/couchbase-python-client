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
    except DocumentNotFoundException:
        print("Item does not exist")
    except CouchbaseTransientException:
        print("Transient error received. Handling and backing off")

Where `DocumentNotFoundException` is a specific error detail code indicating the item has
not been found, and `CouchbaseTransientException` is an error category indicating
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
    except CouchbaseException as e:
        if e.is_data and isinstance(e, DocumentNotFoundException):
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

.. autoexception:: CouchbaseException
   :members:

--------------------
Exception Categories
--------------------

These categories form the base exception classes

.. autoexception:: CouchbaseInternalException
.. autoexception:: NetworkException
.. autoexception:: CouchbaseInputException
.. autoexception:: CouchbaseFatalException
.. autoexception:: CouchbaseDataException
.. autoexception:: CouchbaseTransientException



-----------------
Exception Details
-----------------

The following codes are exception details. They all derive from
:exc:`CouchbaseException`. Many of them will have multiple error categories and thus
be inherited from multiple exception categories.

.. autoexception:: InvalidArgumentException
   :show-inheritance:
.. autoexception:: ValueFormatException
   :show-inheritance:
.. autoexception:: AuthenticationException
   :show-inheritance:
.. autoexception:: DeltaBadvalException
   :show-inheritance:
.. autoexception:: TooBigException
   :show-inheritance:
.. autoexception:: BusyException
   :show-inheritance:
.. autoexception:: InternalException
   :show-inheritance:
.. autoexception:: InvalidException
   :show-inheritance:
.. autoexception:: NoMemoryException
   :show-inheritance:
.. autoexception:: RangeException
   :show-inheritance:
.. autoexception:: LibcouchbaseException
   :show-inheritance:
.. autoexception:: TemporaryFailException
   :show-inheritance:
.. autoexception:: DocumentExistsException
   :show-inheritance:
.. autoexception:: DocumentNotFoundException
   :show-inheritance:
.. autoexception:: DlopenFailedException
   :show-inheritance:
.. autoexception:: DlsymFailedException
   :show-inheritance:
.. autoexception:: NetworkException
   :show-inheritance:
.. autoexception:: NotMyVbucketException
   :show-inheritance:
.. autoexception:: NotStoredException
   :show-inheritance:
.. autoexception:: NotSupportedException
   :show-inheritance:
.. autoexception:: UnknownCommandException
   :show-inheritance:
.. autoexception:: UnknownHostException
   :show-inheritance:
.. autoexception:: ProtocolException
   :show-inheritance:
.. autoexception:: TimeoutException
   :show-inheritance:
.. autoexception:: ConnectException
   :show-inheritance:
.. autoexception:: BucketNotFoundException
   :show-inheritance:
.. autoexception:: ClientNoMemoryException
   :show-inheritance:
.. autoexception:: ClientTemporaryFailException
   :show-inheritance:
.. autoexception:: BadHandleException
   :show-inheritance:
.. autoexception:: HTTPException
   :show-inheritance:
.. autoexception:: PathNotFoundException
   :show-inheritance:
.. autoexception:: PathExistsException
   :show-inheritance: