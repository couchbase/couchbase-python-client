==================================
Asyncio (acouchbase) Results
==================================

.. note::
    Further updates to the acouchbase docs will come with future 4.x releases.  In the meantime,
    check out the provided examples in the :acouchbase_examples:`Github repo <>`.

The streaming results below are the *acouchbase* counterparts of the results described in
:doc:`../couchbase_api/results`.  Their ``rows()`` returns an async iterable, so loop over it with
``async for``.  Every other result type is shared with the blocking API and is documented there;
import it from ``acouchbase.result`` all the same.

.. contents::
    :local:

.. module:: acouchbase.result

AsyncAnalyticsResult
======================

.. class:: AsyncAnalyticsResult

    .. automethod:: rows
    .. automethod:: metadata

AsyncQueryResult
======================

.. class:: AsyncQueryResult

    .. automethod:: rows
    .. automethod:: metadata

AsyncScanResultIterable
=========================

.. class:: AsyncScanResultIterable

    .. automethod:: rows
    .. automethod:: cancel_scan

AsyncSearchResult
======================

.. class:: AsyncSearchResult

    .. automethod:: rows
    .. automethod:: metadata

AsyncViewResult
======================

.. class:: AsyncViewResult

    .. automethod:: rows
    .. automethod:: metadata
