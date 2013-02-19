COUCHBASE PYTHON LIBRARY
========================

This library provides methods to connect to both the couchbase
memcached interface and the couchbase rest api interface.

This version requires Python 2.6 or later.

You'll need to install the following Python library requirements via `pip`:

    pip install requests

Open Issues: http://www.couchbase.org/issues/browse/PYCBC

[![Build Status](https://secure.travis-ci.org/couchbase/couchbase-python-client.png?branch=master)](http://travis-ci.org/couchbase/couchbase-python-client)

USAGE
=====

Two simple use cases to set and get a key in the default bucket
and then create a new bucket using the memcached and rest clients::

    #!/usr/bin/env python

    from couchbase.couchbaseclient import CouchbaseClient
    from couchbase.rest_client import RestConnection

    client = CouchbaseClient("http://localhost:8091/pools/default",
                             "default", "", False)
    client.set("key1", 0, 0, "value1")
    client.get("key1")

    server_info = {"ip": "localhost",
                   "port": 8091,
                   "username": "Administrator",
                   "password": "password"}
    rest = RestConnection(server_info)
    rest.create_bucket(bucket='newbucket',
                       ramQuotaMB=100,
                       authType='none',
                       saslPassword='',
                       replicaNumber=1,
                       proxyPort=11215,
                       bucketType='membase')

Example code that creates buckets and then does sets, gets and views using
the unified client::

    from couchbase import Couchbase

    # connect to a couchbase server
    cb = Couchbase('localhost:8091',
                   username='Administrator',
                   password='password')

    # fetch a Bucket with subscript
    default_bucket = cb['default']
    # set a value with subscript (nearly equivalent to .set)
    default_bucket['key1'] = 'value1'

    # fetch a bucket with a function
    default_bucket2 = cb.bucket('default')
    # set a json value with subscript (nearly equivalent to .set)
    default_bucket2['key2'] = {'value': 'value2', 'expiration': 0}

    # set a value with a function
    default_bucket.set('key3', 0, 0, 'value3')

    # fetch a key with a function
    print 'key1 ' + str(default_bucket.get('key1'))
    print 'key2 ' + str(default_bucket2.get('key2'))
    # fetch a key with subscript
    print 'key3 ' + str(default_bucket2['key3'])

    # create a new bucket
    try:
        newbucket = cb.create('newbucket', ram_quota_mb=100, replica=1)
    except:
        newbucket = cb['newbucket']

    # set a JSON document using the more "pythonic" interface
    newbucket['json_test'] = {'type': 'item', 'value': 'json test'}
    print 'json_test ' + str(newbucket[doc_id])
    # use the more verbose API which allows for setting expiration & flags
    newbucket.set('key4', 0, 0, {'type': 'item', 'value': 'json test'})
    print 'key4 ' + str(newbucket[doc_id])

    design_doc = {"views":
                  {"all_by_types":
                   {"map":
                    '''function (doc, meta) {
                         emit([meta.type, doc.type, meta.id], doc.value);
                         // row output: ['json', 'item', 'key4'], 'json test'
                       }'''
                    },
                   },
                  }
    # save a design document
    newbucket['_design/testing'] = design_doc

    all_by_types_view = newbucket['_design/testing'].views()[0]
    rows = all_by_types_view.results({'stale': False})
    for row in rows:
        print row

    # delete the 'newbucket' bucket
    cb.delete('newbucket')


RUNNING TESTS
=============

Requirements:

  * easy_install nose
  * pip install nose-testconfig

Thanks to nose's setup.py integration, test running is as simple as

    python setup.py nosetests

If you want to customize the nose settings which are stored in setup.cfg. The
default will generate coverage reports (placed in './cover'), and stop on the
first error found.

Additionally, to run these tests on a version of Couchbase Server greater than
1.8, you'll need to enable the `flush_all` setting.

In 1.8.1 use `cbflushctl`:

    cbflushctl localhost:11210 set flushall_enabled true

In 2.0.0 use `cbepctl`:

    cbepctl localhost:11210 set flush_param flushall_enabled true


BASIC BENCHMARKING
==================

We like things to go fast, and we can't know how fast they're going
without measuring them. To check the various Python SDK pieces against
python-memcached and pylibc, we've created a simple cProfile-based
performance reporting tool.

To run this (on a *testing* cluster, *not* on dev or production), do:

    python couchbase/benchmarks/benchmark.py

To read the profile output do:

    python couchbase/benchmarks/profiles/{name_of_profile_output_file}

It's early stage stuff as yet, but it should be helpful for quick
progress comparison, and to help track down places the SDK can improve.

