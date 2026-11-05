=========
Migration
=========

.. contents::
    :local:

Import paths
============

Every public name has one import path, the module its reference page documents.  Some names can also
still be imported from an older path, so that code written against it keeps working.  Those paths are
kept only for that code: new code should use the path in the right-hand column below, and none of the
old paths will gain anything new.

Importing a name from an old path emits a :class:`~couchbase.warnings.CouchbaseDeprecationWarning` that
points at the import and says what to use instead.  The SDK's type information leaves the old paths out,
so a type checker reports them too.  To find them all in a test run, turn the warning into an error:

.. code-block:: python

    import warnings

    from couchbase.warnings import CouchbaseDeprecationWarning

    warnings.simplefilter('error', CouchbaseDeprecationWarning)

The table lists every old path the SDK still serves.  "Nothing" marks a name that was never meant for
applications, or one with no replacement.

.. include:: reference/deprecated_paths.inc

Migrating from 3.x to 4.x
=========================

The Python SDK 4.x implements the :python_sdk_api_version:`SDK API 3 spec <>`, so all the steps outlined
in the :python_sdk_api_version:`SDK 3 migration docs <>` apply to a migration from a Python SDK 2.x
directly to Python SDK 4.x.

Importantly, the Python SDK 4.x has been substantially reworked to use a new backend (Couchbase++ instead
of libcouchbase).  Though the API surfaces are intended to be compatible, any code that relies on
undocumented or uncommitted internal details is not guaranteed to work.  Key areas that have been
reworked:

* The ``couchbase_core`` package has been removed.  The 4.x SDK provides appropriate import paths within
  the ``couchbase`` package (or the ``acouchbase`` and ``txcouchbase`` packages, for the async APIs) for
  anything that is needed with respect to the APIs provided by the SDK.
* As there is a new backend, the previous ``_libcouchbase`` C extension has been removed.
* Remnants of the 2.x API in previous Python 3.x SDK versions have been removed or deprecated.

  * Key items that have been **removed**:

    * The ``ClassicAuthenticator`` class.
    * Key-value operations are no longer available with a ``bucket`` instance.  Use a ``collection``
      instance for key-value operations.
    * A ``cluster`` and ``bucket`` instance do not inherit from the same base class.
    * The ``Client`` class.
    * The ``Items`` API.
    * The ``Admin`` cluster.

  * Key items that have been **deprecated**:

    * The datastructure methods of a ``collection`` instance, replaced by
      :class:`~couchbase.datastructures.CouchbaseList`, :class:`~couchbase.datastructures.CouchbaseMap`,
      :class:`~couchbase.datastructures.CouchbaseQueue` and :class:`~couchbase.datastructures.CouchbaseSet`.
    * :class:`~couchbase.result.OperationResult` and :class:`~couchbase.result.ValueResult`, still
      available from :mod:`couchbase.result`.

* Import paths have been reorganized to follow consistent patterns.  The paths that existed in the 3.x
  SDK are mostly still available, apart from the ``couchbase_core`` package; the ones that are
  deprecated are listed under `Import paths`_.

  * Authenticators are imported from :mod:`couchbase.auth`.
  * Constants are imported from ``couchbase.constants``.
  * Options are imported from :mod:`couchbase.options`.
  * Management options are imported from :mod:`couchbase.management.options`.
  * Results are imported from :mod:`couchbase.result`.
  * Exceptions are imported from :mod:`couchbase.exceptions`.
  * Enumerations and classes related to an operation are imported from that operation's module.  For
    example, :class:`~couchbase.n1ql.QueryScanConsistency` is imported from :mod:`couchbase.n1ql`.

* Changes to the async APIs (``acouchbase`` and ``txcouchbase``):

  * The multi-document operations (``get_multi``, ``upsert_multi`` and the rest) exist only in the
    ``couchbase`` API.  Each async API is built on a library with its own way to run operations
    together: ``asyncio.gather()`` for asyncio, and ``DeferredList`` for Twisted.
  * The ``txcouchbase`` API runs on Twisted's ``asyncioreactor``, which importing ``txcouchbase``
    installs.  So ``txcouchbase`` has to be imported before the reactor is:

    .. code-block:: python

        import txcouchbase

        from twisted.internet import reactor
