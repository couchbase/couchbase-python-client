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
    Wheels are currently available on Windows and MacOS for Python 3.7 - 3.10. We are working on providing manylinux wheels in the near future..

Prereqs
++++++++++

If not on platform that has a binary wheel availble, the following is needed:

* Python version 3.7 - 3.10
* A C++ compiler supporting C++ 17
* CMake (version >= 3.18)
* Git (if not on a platform that offers wheels)
* OpenSSL 1.1.1
* If using the Twisted Framework and the txcouchbase API, Twisted >= 21.7.0 is required.

.. warning::
    Some older linux platforms to not provide defaults (Python version, OpenSSL, C++ 17 support, etc.) that meet the Python SDK's minimum requirements.  Be sure to update to the minimum requirements prior to installing the SDK.

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

License
=======================

The Couchbase Python SDK is licensed under the Apache License 2.0.

See :python_sdk_license:`LICENSE <>` for further details.
