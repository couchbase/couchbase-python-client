=======================
Couchbase Python Client
=======================

Client for Couchbase_.

.. note::

    This is the documentation for the 3.x version of the client. This is
    mostly compatible with the older version. Please refer to the
    *release25* branch for the older version.

-----------------------
Building and Installing
-----------------------

This only applies to building from source. If you are using a Windows
installer then everything (other than the server) is already included.
See below for windows snapshot releases.

Also note that these instructions apply to building from source.
You can always get the latest supported release version from pypi_.


If you have a recent version of *pip*, you may use the latest development
version by issuing the following incantation

.. code-block:: sh

    pip install git+git://github.com/couchbase/couchbase-python-client

~~~~~~~~~~~~~
Prerequisites
~~~~~~~~~~~~~

- Couchbase Server (http://couchbase.com/download)
- You may need a C compiler and Python development files, unless a
  binary wheel is available for your platform. These are available for
  at least Python 3.7 on Windows, but we will endeavour to add more.
- Git, if a binary wheel is not available.

~~~~~~~~
Building
~~~~~~~~
You will need a few tools installed to be able to build locally. A compiler
and CMake (2.8.9 or above), to compile the C extensions that communicate with
libcouchbase, git, pip and python's setuptools.

Linux (example is for debian-like linux):

.. code-block:: sh

    sudo apt install git-all
    sudo apt install python3-dev
    sudo apt install python3-pip
    sudo apt install python3-setuptools
    sudo apt install cmake
    sudo apt install build-essential

MacOSX:

.. code-block:: sh

    # mac (I'm assuming you use homebrew)
    git --version # this will prompt you to install if not already installed
    brew install cmake
    xcode-select --install
    pip3 install setuptools


The following will compile the module locally; you can then test basic
functionality including running the examples.

.. code-block:: sh

    python setup.py build_ext --inplace


If you have a libcouchbase install already (in, for example,
`/opt/local/libcouchbase`), you may build using it by setting PYCBC_BUILD=DISTUTILS
and some add extra directives, like so:

.. code-block:: sh

    export PYCBC_BUILD=DISTUTILS
    python setup.py build_ext --inplace \
        --library-dir /opt/local/libcouchbase/lib \
        --include-dir /opt/local/libcouchbase/include

Or you can modify the environment ``CFLAGS`` and ``LDFLAGS`` variables.


.. warning::

    If you do not intend to install this module, ensure you set the
    ``PYTHONPATH`` environment variable to this directory before running
    any scripts depending on it. Failing to do so may result in your script
    running against an older version of this module (if installed), or
    throwing an exception stating that the ``couchbase`` module could not
    be found.

^^^^^^^^^^
Installing
^^^^^^^^^^
.. code-block:: sh

    pip install .

.. warning::

    If you are on *macOS x* you may need to remove the build directory:
    `rm -rf ./build` before installing with pip: `pip3 install .`.


-----
Using
-----

Authentication is handled differently depending on what version of Couchbase Server
you are using:

~~~~~~~~~~~~~~~~~~~~~~~
Couchbase Server >= 5.0
~~~~~~~~~~~~~~~~~~~~~~~
Role-Based Access Control (RBAC) provides discrete username and passwords for an
application that allow fine-grained control. The authenticator is always required.

.. code-block:: pycon

    >>> from couchbase.cluster import Cluster, PasswordAuthenticator, ClusterOptions
    >>> from couchbase.auth import PasswordAuthenticator
    >>> cluster = Cluster('couchbase://localhost', ClusterOptions(PasswordAuthenticator('username', 'password')))
    >>> bucket = cluster.bucket('bucket-name')
    >>> collection = bucket.default_collection()

Here's an example code snippet which sets a key and then reads it

.. code-block:: pycon

    >>> collection.upsert("key", "value")
    >>> res = collection.get("key")
    >>> res.content
    u'value'
    >>>

You can also use views

.. code-block:: pycon

    >>> resultset = cluster.view_query("beer", "brewery_beers", limit=5)
    >>> resultset
    View<Design=beer, View=brewery_beers, Query=Query:'limit=5', Rows Fetched=0>
    >>> for row in resultset: print row.key
    ...
    [u'21st_amendment_brewery_cafe']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-21a_ipa']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-563_stout']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-amendment_pale_ale']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-bitter_american']

.. warning::
    The gcouchbase API has not yet been fully migrated to version 3,
    so is omitted for this version.

~~~~~~~~~~~
Twisted API
~~~~~~~~~~~

The Python client now has support for the Twisted async network framework.
To use with Twisted, simply import ``txcouchbase.cluster`` instead of
``couchbase.cluster``

.. code-block:: python

    from twisted.internet import reactor
    from txcouchbase.cluster import TxCluster

    cluster = TxCluster('couchbase://localhost', ClusterOptions(PasswordAuthenticator('username', 'password')))
    bucket = cluster.bucket("default")
    cb = bucket.default_collection()

    def on_upsert(ret):
        print("Set key. Result", ret)

    def on_get(ret):
        print("Got key. Result", ret)
        reactor.stop()

    cb.upsert("key", "value").addCallback(on_upsert)
    cb.get("key").addCallback(on_get)
    reactor.run()

    # Output:
    # Set key. Result OperationResult<RC=0x0, Key=key, CAS=0x9a78cf56c08c0500>
    # Got key. Result ValueResult<RC=0x0, Key=key, Value=u'value', CAS=0x9a78cf56c08c0500, Flags=0x0>


The ``txcouchbase`` API is identical to the ``couchbase`` API, except that where
the synchronous API will block until it receives a result, the async API will
return a `Deferred` which will be called later with the result or an appropriate
error.



------------------------
Asynchronous (Tulip) API
------------------------

This module also supports Python 3.4/3.5 asynchronous I/O. To use this
functionality, import the `couchbase.experimental` module (since this
functionality is considered experimental) and then import the `acouchbase`
module. The `acouchbase` module offers an API similar to the synchronous
client:

.. code-block:: python

    import asyncio

    from acouchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator


    async def write_and_read(key, value):
        cluster = Cluster('couchbase://10.0.0.31/', ClusterOptions(PasswordAuthenticator("username","password")))
        cb = cluster.bucket("default")
        cb_coll = cb.default_collection()
        await cb_coll.connect()
        await cb_coll.upsert(key, value)
        return await cb_coll.get(key)

    loop = asyncio.get_event_loop()
    rv = loop.run_until_complete(write_and_read('foo', 'bar'))
    print(rv.content)


~~~~~~~~~~~~~~
Other Examples
~~~~~~~~~~~~~~

There are other examples in the `examples` directory. To run them from the
source tree, do something like

.. code-block:: sh

    PYTHONPATH=$PWD ./examples/bench.py -U couchbase://localhost/default

----------------------
Building documentation
----------------------


The documentation is using Sphinx and also needs the numpydoc Sphinx extension.
In order for the documentation to build properly, the C extension must have
been built, since there are embedded docstrings in there as well.

To build the documentation, go into the `docs` directory and run

.. code-block:: sh

    make html

The HTML output can be found in `docs/build/html/`.


Alternatively, you can also build the documentation (after building the module
itself) from the top-level directory:

.. code-block:: sh

    python setup.py build_sphinx

Once built, the docs will be in in `build/sphinx/html`

-------
Testing
-------

For running the tests, you need the standard `unittest` module, shipped
with Python. Additionally, the `testresources` package is required.

To run them, use either `py.test`, `unittest` or `trial`.

The tests need a running Couchbase instance. For this, a `tests.ini`
file must be present, containing various connection parameters.
An example of this file may be found in `tests.ini.sample`.
You may copy this file to `tests.ini` and modify the values as needed.

To run the tests::

    nosetests

------------------------------
Support & Additional Resources
------------------------------

If you found an issue, please file it in our JIRA_.
You can ask questions in our forums_ or in the `#libcouchbase` channel on
freenode_.

The `official documentation`_ can be consulted as well for
general Couchbase concepts and offers a more didactic approach to using the
SDK.

-------
License
-------

The Couchbase Python SDK is licensed under the Apache License 2.0.

.. _Couchbase: http://couchbase.com
.. _libcouchbase: https://github.com/couchbase/libcouchbase
.. _official documentation: https://docs.couchbase.com/python-sdk/3.0/hello-world/start-using-sdk.html
.. _JIRA: http://couchbase.com/issues/browse/pycbc
.. _freenode: http://freenode.net/irc_servers.shtml
.. _pypi: http://pypi.python.org/pypi/couchbase
.. _forums: https://forums.couchbase.com
