=======================
Couchbase Python Client
=======================

Client for Couchbase_.

.. image:: https://travis-ci.org/couchbase/couchbase-python-client.png
    :target: https://travis-ci.org/couchbase/couchbase-python-client


.. note::

    This is the documentation for the 2.x version of the client. This is
    mostly compatible with the older version. Please refer to the
    *release12* branch for the older version.

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
- libcouchbase_. version 2.6.0 or greater (Bundled in Windows installer)
- libcouchbase development files.
- Python development files
- A C compiler.

~~~~~~~~
Building
~~~~~~~~

The following will compile the module locally; you can then test basic
functionality including running the examples.

.. code-block:: sh

    python setup.py build_ext --inplace


If your libcouchbase install is in an alternate location (for example,
`/opt/local/libcouchbase`), you may add extra directives, like so

.. code-block:: sh

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

    python setup.py install

-----
Using
-----

Here's an example code snippet which sets a key and then reads it

.. code-block:: pycon

    >>> from couchbase.bucket import Bucket
    >>> c = Bucket('couchbase://localhost/default')
    >>> c
    <couchbase.bucket.Bucket bucket=default, nodes=['localhost:8091'] at 0x105991cd0>
    >>> c.upsert("key", "value")
    OperationResult<RC=0x0, Key=key, CAS=0x31c0e3f3fc4b0000>
    >>> res = c.get("key")
    >>> res
    ValueResult<RC=0x0, Key=key, Value=u'value', CAS=0x31c0e3f3fc4b0000, Flags=0x0>
    >>> res.value
    u'value'
    >>>

You can also use views

.. code-block:: pycon

    >>> from couchbase.bucket import Bucket
    >>> c = Bucket('couchbase://localhost/beer-sample')
    >>> resultset = c.query("beer", "brewery_beers", limit=5)
    >>> resultset
    View<Design=beer, View=brewery_beers, Query=Query:'limit=5', Rows Fetched=0>
    >>> for row in resultset: print row.key
    ...
    [u'21st_amendment_brewery_cafe']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-21a_ipa']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-563_stout']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-amendment_pale_ale']
    [u'21st_amendment_brewery_cafe', u'21st_amendment_brewery_cafe-bitter_american']

~~~~~~~~~~~
Twisted API
~~~~~~~~~~~

The Python client now has support for the Twisted async network framework.
To use with Twisted, simply import ``txcouchbase.connection`` instead of
``couchbase.bucket``

.. code-block:: python

    from twisted.internet import reactor
    from txcouchbase.bucket import Bucket

    cb = Bucket('couchbase://localhost/default')
    def on_upsert(ret):
        print "Set key. Result", ret

    def on_get(ret):
        print "Got key. Result", ret
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

~~~~~~~~~~
GEvent API
~~~~~~~~~~

.. code-block:: python

    from gcouchbase.bucket import Bucket

    conn = Bucket('couchbase://localhost/default')
    print conn.upsert("foo", "bar")
    print conn.get("foo")

The API functions exactly like the normal Bucket API, except that the
implementation is significantly different.

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

    import couchbase.experimental
    couchbase.experimental.enable()
    from acouchbase.bucket import Bucket


    async def write_and_read(key, value):
        cb = Bucket('couchbase://10.0.0.31/default')
        await cb.connect()
        await cb.upsert(key, value)
        return await cb.get(key)

    loop = asyncio.get_event_loop()
    rv = loop.run_until_complete(write_and_read('foo', 'bar'))
    print(rv.value)


~~~~
PyPy
~~~~

`PyPy <http://pypy.org>`_ is an alternative high performance Python
implementation. Since PyPy does not work well with C extension modules,
this module will not work directly. You may refer to the alternate
implementation based on the *cffi* module: https://github.com/couchbaselabs/couchbase-python-cffi

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
To build the documentation, go into the `docs` directory and run

.. code-block:: sh

    make html

The HTML output can be found in `docs/build/html/`.

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

The simplest way to run the tests is to declare a `bucket_prefix` in
the `tests.ini` file and run the `setup_tests.py` script to create
them for you.

.. code-block:: sh

    python setup_tests.py

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
.. _libcouchbase: http://developer.couchbase.com/documentation/server/4.5/sdk/c/start-using-sdk.html
.. _official documentation: http://developer.couchbase.com/documentation/server/4.5/sdk/python/start-using-sdk.html
.. _JIRA: http://couchbase.com/issues/browse/pycbc
.. _freenode: http://freenode.net/irc_servers.shtml
.. _pypi: http://pypi.python.org/pypi/couchbase
.. _forums: https://forums.couchbase.com
