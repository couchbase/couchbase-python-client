========================
COUCHBASE PYTHON LIBRARY
========================

This library provides mothods to connect to both the couchbase
memcached interface and the couchbase rest api interface.

Two simple use cases to set and get a key in the default bucket
and then create a new bucket::

    #!/usr/bin/env python

    from couchbase.couchbaseclient import VBucketAwareCouchbaseClient
    from couchbase.couchbaseclient import MemcachedTimeoutException
    from couchbase.rest_client import RestConnection

    client = VBucketAwareCouchbaseClient("http://localhost:8091/pools/default","default","",False)
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

This version requires Python 2.6 or later
