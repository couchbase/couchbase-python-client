Official Couchbase Python Client
================================

This is the new and improved Couchbase Python SDK. It is still in stages
of development but is making rapid progress. It is based on the common
C-based library [libcouchbase][1].


Prerequisites
-------------

[libcouchbase][1] version 2.0.5 or greater.


Building
--------

In order to build this client, you need to have `libcouchbase` installed. Once
this is done, you can now build the extension.

    python setup.py build_ext --inplace


If your libcouchbase install is in an alternate location (for example,
`/opt/local/libcouchbase'), you may add extra directives, like so:

    python setup.py build_ext --inplace \
        --library-dir /opt/local/libcouchbase/lib \
        --include-dir /opt/local/libcouchbase/include

Or you can modify the environment `CFLAGS` and `LDFLAGS` variables.


Running sample application
--------------------------

To run the small sample application that inserts one million documents into
a local Couchbase at the default port 8091 and a bucket called "default",
just execute:

    python examples/basic.py

If you do not with to install the package just yet, remember to set the
`$PYTHONPATH` environment variable so the example scripts can load the
module:

    PYTHONPATH=$PWD python examples/basic.py


Building documentaion
---------------------

The documentation is using Sphinx and also needs the numpydoc Sphinx extension.
To build the documentation, go into the `docs` directory and run:

    make html

The HTML output can be found in `docs/build/html/`.


Running tests
-------------

The tests need a running Couchbase instance. For this, a `tests/tests.ini`
file must be present, containing various connection parameters.
 An example of this file may be found in `tests/tests.ini.sample`.
You may copy this file to `tests/tests.ini` and modify the values as needed.

The test suite need several buckets which need to be created before the tests
are run. They will all have the common prefix as specified in the test
configuration file. To create them, run:

    python tests/setup_tests.py

To run the tests:

    nosetests

Tested platforms
----------------

So far the code has been tested on the following platforms/environments.

Linux 64-bit (with GCC):
 - Python 2.7.3
 - Python 3.2.3
 - Python 2.6.6

Mac OS X 10.6.8
 - Python 2.6.1
 - Python 3.3.1

Microsoft Windows 2008 R2 (MSVC 2008/VC9)
 - Python 2.7.3 (x86)
 - Python 2.7.4 (x64)


If you ran it on a different platform and it worked, please let me know and
I'll add it to the list.


Support
-------

If you found an issue, please file it in our [JIRA][2]. You may also ask in the
`#libcouchbase` IRC channel at [freenode.net IRC servers][3] (which is where
the author(s) of this module may be found).

License
-------

The Couchbase Python SDK is licensed under the Apache License 2.0.



[1]: http://couchbase.com/develop/c/current
[2]: http://couchbase.com/issues/browse/PYCBC
[3]: http://freenode.net/irc_servers.shtml
