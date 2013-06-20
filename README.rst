=======================
Couchbase Python Client
=======================

Client for Couchbase_.

-----------------------
Building and Installing
-----------------------

This only applies to building from source. If you are using a Windows
installer then everything (other than the server) is already included.
See below for windows snapshot releases.

Also note that these instructions apply to building from source.
You can always get the latest supported release version from
`PyPi <http://pypi.python.org/pypi/couchbase>`_

~~~~~~~~~~~~~
Prerequisites
~~~~~~~~~~~~~

- Couchbase Server (http://couchbase.com/download)
- libcouchbase_. version 2.0.5 or greater (Bundled in Windows installer)
- libcouchbase development files.
- Python development files
- A C compiler.

~~~~~~~~
Building
~~~~~~~~

.. code-block:: sh

    python setup.py build_ext --inplace


If your libcouchbase install is in an alternate location (for example,
`/opt/local/libcouchbase`), you may add extra directives, like so

.. code-block:: sh

    python setup.py build_ext --inplace \
        --library-dir /opt/local/libcouchbase/lib \
        --include-dir /opt/local/libcouchbase/include

Or you can modify the environment ``CFLAGS`` and ``LDFLAGS`` variables.

.. _windowsbuilds:

~~~~~~~~~~~~~~~~~
Windows Snapshots
~~~~~~~~~~~~~~~~~

A list of recent snapshot builds for Windows may be found
`here <http://packages.couchbase.com/clients/python/snapshots>`.

You can always get release binaries from PyPi (as above).

-----
Using
-----

Here's an example code snippet which sets a key and then reads it

.. code-block:: pycon

    >>> from couchbase import Couchbase
    >>> c = Couchbase.connect(bucket='default')
    >>> c
    <couchbase.connection.Connection bucket=default, nodes=['127.0.0.1:8091'] at 0xb21a50>
    >>> c.set("key", "value")
    OperationResult<RC=0x0, Key=key, CAS=0x31c0e3f3fc4b0000>
    >>> res = c.get("key")
    >>> res
    ValueResult<RC=0x0, Key=key, Value=u'value', CAS=0x31c0e3f3fc4b0000, Flags=0x0>
    >>> res.value
    u'value'
    >>>

You can also use views

.. code-block:: pycon

    >>> from couchbase import Couchbase
    >>> c = Couchbase.connect(bucket='beer-sample')
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


~~~~~~~~~~~~~~
Other Examples
~~~~~~~~~~~~~~

There are other examples in the `examples` directory.

---------------------
Building documentaion
---------------------


The documentation is using Sphinx and also needs the numpydoc Sphinx extension.
To build the documentation, go into the `docs` directory and run

.. code-block:: sh

    make html

The HTML output can be found in `docs/build/html/`.

-------
Testing
-------

The tests need a running Couchbase instance. For this, a `tests/tests.ini`
file must be present, containing various connection parameters.
An example of this file may be found in `tests/tests.ini.sample`.
You may copy this file to `tests/tests.ini` and modify the values as needed.

The test suite need several buckets which need to be created before the tests
are run. They will all have the common prefix as specified in the test
configuration file. To create them, run:


.. code-block:: sh

    python tests/setup_tests.py

To run the tests::

    nosetests

-------
Support
-------

If you found an issue, please file it in our JIRA_. You may also ask in the
`#libcouchbase` IRC channel at freenode_. (which is where the author(s)
of this module may be found).

-------
License
-------

The Couchbase Python SDK is licensed under the Apache License 2.0.

.. _Couchbase: http://couchbase.com
.. _libcouchbase: http://couchbase.com/develop/c/current
.. _JIRA: http://couchbase.com/issues/browse/pycbc
.. _freenode: http://freenode.net/irc_servers.shtml
