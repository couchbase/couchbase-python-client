pylibcouchbase
==============

This is an experimental repository for the next generation Couchbase Python
SDK which is based on [libcouchbase][1].

The lcb.c was automatically generate by a [cwrap branch that uses libclang][2].


Prerequisites
-------------

Install libcouchbase.


Building
--------

As the target audience is currently developers, you probably want to install
it locally. You can run:

    python setup.py build_ext --inplace

If you have compile libcouchbase yourself at a custom location, you can pass
it in via the `CFLAGS` and `LDFLAGS` environment variables.


Running sample application
--------------------------

To run the small sample application that inserts one million documents into
a local Couchbase at the default port 8091 and a bucket called "default",
just execute:

    python basic.py


Tested platforms
----------------

So far the code has been tested on the following platforms/environments.

Linux 64-bit (with GCC):

 - Python 2.7.3
 - Python 3.2.3
 - PyPy 1.9

OSX (with clang):

 - Python 2.7


If you ran it on a different platform and it worked, please let me know and
I'll add it to the list.


License
-------

pylibcouchbase is licensed under the Apache License 2.0.



[1]: https://github.com/couchbase/libcouchbase
[2]: https://github.com/geggo/cwrap
