
import txcouchbase  # noqa: F401

from datetime import timedelta

from twisted.internet import defer, reactor

from couchbase.auth import PasswordAuthenticator
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import CouchbaseException, UnAmbiguousTimeoutException
from couchbase.options import (ClusterOptions,
                               QueryOptions,
                               WaitUntilReadyOptions)
from txcouchbase.cluster import Cluster


def after_upsert(res, key, d):
    print('Set key.  Result CAS: ', res.cas)
    # trigger get_document callback
    d.callback(key)


def upsert_document(key, doc):
    d = defer.Deferred()
    res = cb.upsert(key, doc)
    res.addCallback(after_upsert, key, d)
    return d


def handle_query_results(result):
    for r in result.rows():
        print("query row: {}".format(r))
    metadata = result.metadata()
    print(metadata.metrics())
    reactor.stop()


def on_get(res, _type=str):
    print('Got res: \n', res.content_as[_type])
    # reactor.stop()
    n1ql = 'SELECT * FROM `beer-sample` LIMIT 10;'
    d = cluster.query(n1ql, QueryOptions(metrics=True))
    d.addCallback(handle_query_results)


def get_document(key):
    res = cb.get(key)
    res.addCallback(on_get)


def cluster_ready(_):
    print('cluster ready!')
    d = upsert_document('testDoc_1', {'id': 1, 'type': 'testDoc', 'info': 'fake document'})
    d.addCallback(get_document)


def cluster_not_ready(exc):
    print('cluster NOT ready')
    # trapping the failure is the Twisted equivalent of an 'except:' block
    # what comes after trap() is exception handling
    #   i.e. except (CouchbaseException, UnAmbiguousTimeoutException):
    exc.trap(CouchbaseException, UnAmbiguousTimeoutException)
    # this returns the type, not the instance
    exc_type = exc.check(UnAmbiguousTimeoutException)
    if exc_type is None:
        print('Unexpected exception raised.')

    if issubclass(exc_type, UnAmbiguousTimeoutException):
        print(exc.getErrorMessage())

    print('Stopping reactor.')
    reactor.stop()


# create a cluster object
cluster = Cluster('couchbase://localhost',
                  ClusterOptions(PasswordAuthenticator('Administrator', 'password')))

# create a bucket object
bucket = cluster.bucket('default')
# create a collection object
cb = bucket.default_collection()

d = cluster.wait_until_ready(timedelta(seconds=3), WaitUntilReadyOptions(
    service_types=[ServiceType.KeyValue, ServiceType.Query]))
d.addCallback(cluster_ready)
d.addErrback(cluster_not_ready)

reactor.run()
