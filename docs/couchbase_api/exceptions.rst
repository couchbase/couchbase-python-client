==============
Exceptions
==============

.. contents::
    :local:

.. module:: couchbase.exceptions

Base
==================

CouchbaseException
++++++++++++++++++++++++++++++++
.. autoclass:: CouchbaseException

Common
==================

AmbiguousTimeoutException
++++++++++++++++++++++++++++++++
.. autoclass:: AmbiguousTimeoutException

InternalServerFailureException
++++++++++++++++++++++++++++++++
.. autoclass:: InternalServerFailureException

InvalidArgumentException
++++++++++++++++++++++++++++++++
.. autoclass:: InvalidArgumentException

UnAmbiguousTimeoutException
++++++++++++++++++++++++++++++++
.. autoclass:: UnAmbiguousTimeoutException

Authentication
==================

AuthenticationException
++++++++++++++++++++++++++++++++
.. autoclass:: AuthenticationException

Bucket
==================

BucketNotFoundException
++++++++++++++++++++++++++++++++
.. autoclass:: BucketNotFoundException

Key-Value
==================

DocumentExistsException
++++++++++++++++++++++++++++++++
.. autoclass:: DocumentExistsException

DocumentLockedException
++++++++++++++++++++++++++++++++
.. autoclass:: DocumentLockedException

DocumentNotFoundException
++++++++++++++++++++++++++++++++
.. autoclass:: DocumentNotFoundException

DurabilityImpossibleException
++++++++++++++++++++++++++++++++
.. autoclass:: DurabilityImpossibleException

DurabilityInvalidLevelException
++++++++++++++++++++++++++++++++
.. autoclass:: DurabilityInvalidLevelException

DurabilitySyncWriteAmbiguousException
++++++++++++++++++++++++++++++++++++++++
.. autoclass:: DurabilitySyncWriteAmbiguousException

DurabilitySyncWriteInProgressException
++++++++++++++++++++++++++++++++++++++++
.. autoclass:: DurabilitySyncWriteInProgressException

Subdocument
==================

InvalidValueException
++++++++++++++++++++++++++++++++
.. autoclass:: InvalidValueException

PathExistsException
++++++++++++++++++++++++++++++++
.. autoclass:: PathExistsException

PathMismatchException
++++++++++++++++++++++++++++++++
.. autoclass:: PathMismatchException

PathNotFoundException
++++++++++++++++++++++++++++++++
.. autoclass:: PathNotFoundException

.. warning::
    This is a *deprecated* class, use :class:`.InvalidValueException` instead.

SubdocCantInsertValueException
++++++++++++++++++++++++++++++++
.. autoclass:: SubdocCantInsertValueException

.. warning::
    This is a *deprecated* class, use :class:`.PathMismatchException` instead.

SubdocPathMismatchException
++++++++++++++++++++++++++++++++
.. autoclass:: SubdocPathMismatchException
