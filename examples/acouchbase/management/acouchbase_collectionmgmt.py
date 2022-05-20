import asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec


async def retry(func, *args, back_off=0.5, limit=5, **kwargs):
    for i in range(limit):
        try:
            return await func(*args, **kwargs)
        except Exception:
            print("Retry in {} seconds...".format((i + 1) * back_off))
            await asyncio.sleep((i + 1) * back_off)

    raise Exception(
        "Unable to successfully receive result from {}".format(func))


async def get_scope(collection_mgr, scope_name):
    scopes = await collection_mgr.get_all_scopes()
    return next((s for s in scopes if s.name == scope_name), None)


async def get_collection(collection_mgr, scope_name, coll_name):
    scope = await get_scope(collection_mgr, scope_name)
    if scope:
        return next(
            (c for c in scope.collections if c.name == coll_name),
            None)

    return None


async def main():
    cluster = Cluster(
        "couchbase://ec2-54-213-122-253.us-west-2.compute.amazonaws.com",
        authenticator=PasswordAuthenticator(
            "Administrator",
            "password"))

    await cluster.on_connect()
    bucket = cluster.bucket("default")
    await bucket.on_connect()
    coll_manager = bucket.collections()

    try:
        await coll_manager.create_scope("example-scope")
    except ScopeAlreadyExistsException as ex:
        print(ex)

    scope = await retry(get_scope, coll_manager, "example-scope")
    print("Found scope: {}".format(scope.name))

    collection_spec = CollectionSpec(
        "example-collection",
        scope_name="example-scope")

    try:
        collection = await coll_manager.create_collection(collection_spec)
    except CollectionAlreadyExistsException as ex:
        print(ex)

    collection = await retry(
        get_collection,
        coll_manager,
        "example-scope",
        "example-collection")
    print("Found collection: {}".format(collection.name))

    try:
        await coll_manager.drop_collection(collection_spec)
    except CollectionNotFoundException as ex:
        print(ex)

    try:
        await coll_manager.drop_scope("example-scope")
    except ScopeNotFoundException as ex:
        print(ex)


if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
