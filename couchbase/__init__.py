import couchbase_core._bootstrap
import couchbase_core._logutil

couchbase_core._bootstrap.do_init()


def enable_logging():
    """
    Enables integration with Python's `logging` module.

    This function enables the C library's logging to be propagated to
    the Python standard `logging` module.

    Calling this function affects any :class:`~couchbase_core.client.Client` objects created
    afterwards (but not before). Note that currently this will also
    override any ``LCB_LOGLEVEL`` directive inside the environment as
    well.

    The "root" couchbase_core logger is ``couchbase``.
    """
    import couchbase_core._logutil
    couchbase_core._logutil.configure(True)


def disable_logging():
    import couchbase_core._logutil
    couchbase_core._logutil.configure(False)