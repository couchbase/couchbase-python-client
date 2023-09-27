=======================
Using the Python SDK
=======================

The Couchbase Python SDK library allows you to connect to a Couchbase cluster from Python.
The Python SDK uses the high-performance C++ library, Couchbase++,  to handle communicating to the cluster over the Couchbase binary protocol.

Useful Links
=======================

* :python_sdk_github:`Source <>`
* :python_sdk_jira:`Bug Tracker <>`
* :python_sdk_docs:`Python docs on the Couchbase website <>`
* :python_sdk_release_notes:`Release Notes <>`
* :python_sdk_compatibility:`Compatibility Guide <>`
* :couchbase_dev_portal:`Couchbase Developer Portal <>`

How to Engage
=======================

* :couchbase_discord:`Join Discord and contribute <>`.
    The Couchbase Discord server is a place where you can collaborate about all things Couchbase.
    Connect with others from the community, learn tips and tricks, and ask questions.
* Ask and/or answer questions on the :python_sdk_forums:`Python SDK Forums <>`.


Installing the SDK
=======================

.. note::
    Best practice is to use a Python virtual environment such as venv or pyenv.
    Checkout:

        * Linux/MacOS: `pyenv <https://github.com/pyenv/>`_
        * Windows: `pyenv-win <https://github.com/pyenv-win/pyenv-win>`_


.. note::
    The Couchbase Python SDK provides wheels for Windows, MacOS and Linux platforms (via manylinux) for Python 3.8 - 3.11.

Prereqs
++++++++++

If not on platform that has a binary wheel availble, the following is needed:

* Python version 3.8 - 3.11 (see `Python Version Compatibility <https://docs.couchbase.com/python-sdk/current/project-docs/compatibility.html#python-version-compat>`_ for details)
* A C++ compiler supporting C++ 17
* CMake (version >= 3.18)
* Git (if not on a platform that offers wheels)
* OpenSSL 1.1.1
* If using the Twisted Framework and the txcouchbase API, Twisted >= 21.7.0 is required.

.. warning::
    Some older linux platforms to not provide defaults (Python version, OpenSSL, C++ 17 support, etc.) that meet the Python SDK's minimum requirements.  Be sure to update to the minimum requirements prior to installing the SDK.
    See the `dockerfiles folder <https://github.com/couchbase/couchbase-python-client/tree/master/examples/dockerfiles>`_ in the Python SDK examples folder for references to working setups for various linux platforms.

.. note::
    Starting with Python 3.11.5, macOS installers and Windows builders from python.org now use `OpenSSL 3.0 <https://docs.python.org/3/whatsnew/3.11.html#notable-changes-in-3-11-5>`_.
    A potential side-effect of this change is an ``ImportError: DLL load failed while importing pycbc_core`` error when using the Python SDK. As a work-around,
    set the ``PYCBC_OPENSSL_DIR`` environment variable to the path where the OpenSSL 1.1 libraries can be found (``libssl-1_1.dll`` and ``libcrypto-1_1.dll`` for Windows; ``libssl.1.1.dylib`` and ``libcrypto.1.1.dylib`` for macOS).

After the above have been installed, pip install ``setuptools`` and ``wheel`` (see command below).

.. code-block:: console

    $ python3 -m pip install --upgrade pip setuptools wheel

Install
++++++++++

.. code-block:: console

    $ python3 -m pip install couchbase

Introduction
=======================

Connecting to a Couchbase cluster is as simple as creating a new ``Cluster`` instance to represent the ``Cluster``
you are using, and then using the ``bucket`` and ``collection`` commands against this to open a connection to open
your specific ``bucket`` and ``collection``. You are able to execute most operations immediately, and they will be
queued until the connection is successfully established.

Here is a simple example of creating a ``Cluster`` instance, retrieving a document and using SQL++ (a.k.a. N1QL).

.. code-block:: python

    # needed for any cluster connection
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    # options for a cluster and SQL++ (N1QL) queries
    from couchbase.options import ClusterOptions, QueryOptions

    # get a reference to our cluster
    auth = PasswordAuthenticator('username', 'password')
    cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth))

    # get a reference to our bucket
    cb = cluster.bucket('travel-sample')

    # get a reference to the default collection
    cb_coll = cb.default_collection()

    # get a document
    result = cb_coll.get('airline_10')
    print(f'Document content: {result.content_as[dict]}')

    # using SQL++ (a.k.a N1QL)
    call_sign = 'CBS'
    sql_query = 'SELECT VALUE name FROM `travel-sample` WHERE type = "airline" AND callsign = $1'
    query_res = cluster.query(sql_query, QueryOptions(positional_parameters=[call_sign]))
    for row in query_res:
        print(f'Found row: {row}')

Source Control
=======================

The source control is available  on :python_sdk_github:`Github <>`.
Once you have cloned the repository, you may contribute changes through our gerrit server.
For more details see :python_sdk_contribute:`CONTRIBUTING.md <>`.

Migrating from 3.x to 4.x
===========================

The Python SDK 4.x implements the :python_sdk_api_version:`SDK API 3 spec <>`, so all the steps outlined in the :python_sdk_api_version:`SDK 3 migration docs <>` apply to a migration from a Python SDK 2.x directly to Python SDK 4.x.

Importantly, the Python SDK 4.x has been substantially reworked to use a new backend (Couchbase++ instead of libcouchbase.)
Though the API surfaces are intended to be compatible, any code that relies on undocumented or uncommitted internal details is not guaranteed to work.
Key areas that have been reworked:

* The ``couchbase_core`` package has been removed. The 4.x SDK provides appropriate import paths within the ``couchbase`` package (or possibly the ``acouchbase``/``txcouchbase`` packages if using one of the async APIs) for anything that is needed with respect to the APIs provided by the SDK.
* As there is a new backend, the previous ``_libcouchbase`` c-extension has been removed
* Remnants of the 2.x API in previous Python 3.x SDK versions have been removed or deprecated

  * Key items that have been **removed**:

    * The ``ClassicAuthenticator`` class
    * Key-value operations are no longer available with a ``bucket`` instance. Use a ``collection`` instance for key-value operations.
    * A ``cluster`` and ``bucket`` instance do not inherit from the same base class
    * The ``Client`` class has been removed
    * ``Items`` API
    * ``Admin`` cluster

  * Key items that have been **deprecated**:

    * Datastructure methods provided by the ``collection`` instance have been deprecated and replaced with their respective APIs (i.e. ``CouchbaseList``, ``CouchbaseMap``, ``CouchbaseQueue`` and ``CouchbaseSet``)
    * ``OperationResult`` (deprecated, still available from ``couchbase.result``)
    * ``ValueResult`` (deprecated, still available from ``couchbase.result``)

* Import paths have been reorganized to follow consistent patterns.  While the import paths that existed in 3.x SDK are mostly available (see previous points on removal of ``couchbase_core`` package), some paths are deprecated and will be removed in a future release.

  * All authenticators should be imported from ``couchbase.auth``
  * All constants should be imported from ``couchbase.constants``
  * All options should be imported from ``couchbase.options``
  * All management options should be imported from ``couchbase.management.options``
  * All results should be imported from ``couchbase.result``
  * All exceptions should be imported from ``couchbase.exceptions``
  * Enumerations and Classes related to operations should be imported from that operation's path.  For example, ``QueryScanConsistency`` should be imported from ``couchbase.n1ql`` (i.e. ``from couchbase.n1ql import QueryScanConsistency``)

* Changes to the async APIs (``acouchbase`` and ``txcouchbase``):

  * While multi-operations (``get_multi``, ``upsert_multi``, etc.) still exist for the ``couchbase`` API they have been removed from the async APIs (``acouchbase`` and ``txcouchbase``) as each of the async APIs are built with libraries that have mechanisms to handle multi/bulk operations (``asyncio`` has ``asyncio.gather(...)`` and ``Twisted`` has ``DeferredList(...)``).
  * If using the ``txcouchbase`` API, the reactor that should be installed is the ``asyncioreactor``.  Therefore, the ``txcouchbase`` package *needs* to be imported prior to importing the ``reactor``.  See example import below.

    .. code-block:: python

        # this is new with Python SDK 4.x, it needs to be imported prior to
        # importing the twisted reactor
        import txcouchbase

        from twisted.internet import reactor

License
=======================

The Couchbase Python SDK is licensed under the Apache License 2.0.

See :python_sdk_license:`LICENSE <>` for further details.
