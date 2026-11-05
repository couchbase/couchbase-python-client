=====================
Environment Variables
=====================

The SDK reads the variables on this page at run time.  Each one sets something an application can also
set in code, and is meant for the case where the code is not yours to change, or not worth changing to
investigate a problem.

.. contents::
    :local:

Logging
=======

The logging variables are read once, when the SDK is first imported, so they have to be set before the
process starts.  Setting them in ``os.environ`` after ``import couchbase`` has no effect.

They configure two things together: the native core's log, and the SDK's Python loggers under
``couchbase``.  An application that would rather send the native core's log through its own Python
logging calls :func:`couchbase.configure_logging` instead.  The two cannot be combined:
:func:`~couchbase.configure_logging` raises ``RuntimeError`` when :envvar:`PYCBC_LOG_LEVEL` is set.

.. envvar:: PYCBC_LOG_LEVEL

    Turns logging on, at the given level: ``trace``, ``debug``, ``info``, ``warning``, ``error``,
    ``critical`` or ``off``.  ``warn`` is accepted as ``warning``, and the value is not case sensitive.
    An unrecognized value logs a warning and uses ``info``.

    Unset or empty, the native core does not log, the SDK's Python loggers are left to the
    application's logging configuration, and the other logging variables are ignored.

    Without :envvar:`PYCBC_LOG_FILE`, the native core logs to standard error.  The ``couchbase`` Python
    logger gets the same level.  If the application has already configured a handler that would
    receive its records, on the ``couchbase`` logger or the root logger, the SDK leaves them to it;
    otherwise it adds a console handler of its own.  The root logger is never changed.

    .. code-block:: console

        $ PYCBC_LOG_LEVEL=debug python my_app.py

.. envvar:: PYCBC_LOG_FILE

    Writes the log to files named from this path instead of to the console.  Read only when
    :envvar:`PYCBC_LOG_LEVEL` is set.

    The native core writes ``<path>.000000.txt`` and starts the next numbered file when the current
    one passes 100 MiB.  A restarted process appends to the highest-numbered file, and older files are
    never deleted, so at ``trace`` and ``debug`` they need cleaning up.  The SDK's Python records go to
    the path exactly as given.

    .. code-block:: console

        $ PYCBC_LOG_LEVEL=debug PYCBC_LOG_FILE=/var/log/couchbase-sdk.log python my_app.py

.. envvar:: PYCBC_ENABLE_CONSOLE

    Logs to the console as well as to :envvar:`PYCBC_LOG_FILE`.  ``1``, ``true``, ``yes`` and ``on``
    turn it on, ignoring case; any other value, or none, leaves it off.  Without a log file the console
    is always on, so this changes nothing.

.. envvar:: COUCHBASE_CXX_CLIENT_MAXIMIZE_LOGGER_CYCLE_SIZE

    Raises the size at which the native core starts its next log file from 100 MiB to 1 GiB.  Any
    non-empty value turns it on.  It matters only with :envvar:`PYCBC_LOG_FILE`, for a long capture at
    ``trace`` that would otherwise be spread over many files.  The variable belongs to the native core,
    which is shared with other Couchbase SDKs, hence the name.

Option validation
=================

.. envvar:: PYCBC_OPTION_VALIDATION

    What the SDK does when an operation is given an option key that its options class does not carry,
    or one the protocol in use cannot act on: ``ignore``, ``warn`` or ``strict``.  The default is
    ``warn``.

    ``warn`` emits :class:`~couchbase.warnings.CouchbaseOptionWarning` for an unknown key, naming the
    closest known key where there is one, and :class:`~couchbase.warnings.CouchbaseApplicabilityWarning`
    for a key that changes nothing.  ``strict`` raises :class:`~couchbase.exceptions.InvalidArgumentException` instead.
    ``ignore`` says nothing.

    The variable is the default for every cluster in the process.  A cluster given ``option_validation``
    in :class:`~couchbase.options.ClusterOptions` uses that instead, for its own options and every
    operation under it.  A cluster given none reads the variable at each operation, so changing it in
    ``os.environ`` takes effect on the next one.

    A value that is none of the three is reported with a :class:`~couchbase.warnings.CouchbaseOptionWarning` and treated as
    ``warn``.  It never raises, since the operation it is read for may come hours into a run.

    .. code-block:: console

        $ PYCBC_OPTION_VALIDATION=strict python -m pytest

Windows
=======

.. envvar:: PYCBC_OPENSSL_DIR

    The directory holding the OpenSSL libraries, ``libssl`` and ``libcrypto``, that the SDK's native
    extension loads.  Read only on Windows, and only when the extension fails to load at import; the
    SDK then registers this directory with the DLL search path and tries once more.  Unset, it uses
    the interpreter's own ``DLLs`` directory, which ships both libraries.

    The released wheels link their TLS library statically and never need it.  It is for an SDK built
    from source against a separately installed OpenSSL, and the build reads the same variable, as the
    OpenSSL root directory to build against.
