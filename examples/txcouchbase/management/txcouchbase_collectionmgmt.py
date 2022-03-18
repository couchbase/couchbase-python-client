# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
from twisted.internet import (defer,
                              reactor,
                              task)

import txcouchbase
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec
from txcouchbase.cluster import Cluster


@defer.inlineCallbacks
def retry(func, *args, back_off=0.5, limit=5, **kwargs):
    num_retries = kwargs.pop('num_retries', 0)
    if num_retries > limit:
        raise Exception(
            "Unable to successfully receive result from {}".format(func))

    try:
        result = yield func(*args, **kwargs)
        return result
    except Exception:
        print("Retry in {} seconds...".format((num_retries + 1) * back_off))
        kwargs['back_off'] = back_off
        kwargs['limit'] = limit
        kwargs['num_retries'] = num_retries + 1
        yield task.deferLater(reactor, (num_retries + 1) * back_off, retry, func, *args, **kwargs)


@defer.inlineCallbacks
def get_scope(collection_mgr, scope_name):
    scopes = yield collection_mgr.get_all_scopes()
    return next((s for s in scopes if s.name == scope_name), None)


@defer.inlineCallbacks
def get_collection(collection_mgr, scope_name, coll_name):
    scope = yield get_scope(collection_mgr, scope_name)
    if scope:
        return next(
            (c for c in scope.collections if c.name == coll_name),
            None)

    return None


@defer.inlineCallbacks
def main():
    cluster = Cluster(
        "couchbase://ec2-54-213-122-253.us-west-2.compute.amazonaws.com",
        authenticator=PasswordAuthenticator(
            "Administrator",
            "password"))

    yield cluster.on_connect()
    bucket = cluster.bucket("default")
    yield bucket.on_connect()
    coll_manager = bucket.collections()

    try:
        yield coll_manager.create_scope("example-scope")
    except ScopeAlreadyExistsException as ex:
        print(ex)

    scope = yield retry(get_scope, coll_manager, "example-scope")
    print("Found scope: {}".format(scope.name))

    collection_spec = CollectionSpec(
        "example-collection",
        scope_name="example-scope")

    try:
        collection = yield coll_manager.create_collection(collection_spec)
    except CollectionAlreadyExistsException as ex:
        print(ex)

    collection = yield retry(
        get_collection,
        coll_manager,
        "example-scope",
        "example-collection")
    print("Found collection: {}".format(collection.name))

    try:
        yield coll_manager.drop_collection(collection_spec)
    except CollectionNotFoundException as ex:
        print(ex)

    try:
        yield coll_manager.drop_scope("example-scope")
    except ScopeNotFoundException as ex:
        print(ex)

    yield cluster.close()
    reactor.stop()


if __name__ == "__main__":
    main()
    reactor.run()
