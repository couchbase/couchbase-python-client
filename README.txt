========================
COUCHBASE PYTHON LIBRARY
========================

This library provides methods to connect to both the couchbase
memcached interface and the couchbase rest api interface.

Two simple use cases to set and get a key in the default bucket
and then create a new bucket using the memcached and rest clients::

    #!/usr/bin/env python

    from couchbase.couchbaseclient import VBucketAwareCouchbaseClient
    from couchbase.couchbaseclient import MemcachedTimeoutException
    from couchbase.rest_client import RestConnection

    client = VBucketAwareCouchbaseClient("http://localhost:8091/pools/default",
                                         "default","",False)
    client.set("key1", 0, 0, "value1")
    client.get("key1")

    server_info = {"ip":"localhost",
                   "port":8091,
                   "username":"Administrator",
                   "password":"password"}
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

    import couchbase

    # connect to a couchbase server
    cb = couchbase.Server('localhost:8091',
                          username='Administrator',
                          password='password')

    # create default bucket if it doesn't exist
    try:
        cb.create('default')
    except:
        pass

    # fetch a Bucket with subscript
    default_bucket = cb['default']
    # set a value with subscript (equivilent to .set)
    default_bucket['key1'] = 'value1'

    # fetch a bucket with a function
    default_bucket2 = cb.bucket('default')
    # set a json value with subscript (equivilent to .set)
    default_bucket2['key2'] = {'value':'value2','expiration':0,'flags':10}

    # set a value with a function
    default_bucket.set('key3', 0, 0, 'value3')

    # fetch a key with a function
    print 'key1 ' + str(default_bucket.get('key1'))
    print 'key2 ' + str(default_bucket2.get('key2'))
    # fetch a key with subscript
    print 'key3 ' + str(default_bucket2['key3'])

    # delete a bucket
    cb.delete('default')
    try:
        cb['default']
    except Exception as ex:
        print ex

    # create a new bucket
    try:
        newbucket = cb.create('newbucket', ram_quota_mb=100, replica=1)
    except:
        newbucket = cb['newbucket']

    # set a json document with a function
    # this will translate $flags and $expiration to memcached protocol
    # automatically generate the _id
    doc_id = newbucket.save({'type':'item',
                             'value':'json test',
                             '$flags':25})
    print doc_id + ' ' + str(newbucket[doc_id])
    # use a provided _id
    doc_id = newbucket.save({'_id':'key4',
                             'type':'item',
                             'value':'json test',
                             '$flags':25})
    print doc_id + ' ' + str(newbucket[doc_id])

    design = {
        "_id" : "_design/testing",
        "language" : "javascript",
        "views" : {
            "all" : {
                "map" : '''function (doc) {\n    emit(doc, null);\n}'''
                },
            },
        }
    # save a design document
    # right now with no _rev, we can only create, we can't update
    try:
        doc_id = newbucket.save(design)
    except:
        doc_id = "_design/testing"

    rows = newbucket.view("_design/testing/_view/all")
    for row in rows:
        print row


This version requires Python 2.6 or later

Open Issues : http://www.couchbase.org/issues/browse/PYCBC

=============
RUNNING TESTS
=============

Requirements:
* easy_install nose
* pip install nose-testconfig

We're now using nose to run our tests. There's a supplied
test.ini.template that you can customize to match your installed
environment. Copy test.ini.template to test.ini, customize, and
then run the following command:

    nosetests --tc-file=test.ini
