from datetime import timedelta

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator

# **DEPRECATED**, use: from couchbase.options import DeltaValue, SignedInt64
# **DEPRECATED**, import ALL options from `couchbase.options`
from couchbase.collection import (DecrementOptions,
                                  DeltaValue,
                                  GetOptions,
                                  IncrementOptions,
                                  InsertOptions,
                                  RemoveOptions,
                                  ReplaceOptions,
                                  SignedInt64,
                                  UpsertOptions)
from couchbase.durability import (ClientDurability,
                                  Durability,
                                  PersistTo,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import CASMismatchException, CouchbaseException


async def main():
    cluster = Cluster(
        "couchbase://localhost",
        authenticator=PasswordAuthenticator(
            "Administrator",
            "password"))
    await cluster.on_connect()
    bucket = cluster.bucket("default")
    await bucket.on_connect()
    collection = bucket.default_collection()

    # setup
    try:
        result = await collection.remove("document-key")
        result = await collection.remove("document-key-opts")
    except CouchbaseException as ex:
        pass  # may not exist in this example

    # Insert document
    document = {"foo": "bar", "bar": "foo"}
    result = await collection.insert("document-key", document)
    print("Result: {}; CAS: {}".format(result, result.cas))

    # Insert document with options
    document = {"foo": "bar", "bar": "foo"}
    opts = InsertOptions(timeout=timedelta(seconds=5))
    result = await collection.insert("document-key-opts",
                                     document,
                                     opts,
                                     expiry=timedelta(seconds=30))

    try:
        # Replace document with CAS
        document = {"foo": "bar", "bar": "foo"}
        result = await collection.replace(
            "document-key",
            document,
            cas=12345,
            timeout=timedelta(
                minutes=1))
    except CASMismatchException as ex:
        # we expect an exception here as the CAS value is chosen
        # for example purposes
        print('Caught CAS mismatch: {}'.format(ex))

    try:
        # Replace document with CAS
        result = await collection.get("document-key")
        doc = result.content_as[dict]
        doc["bar"] = "baz"
        opts = ReplaceOptions(cas=result.cas)
        result = await collection.replace("document-key", doc, opts)
    except CouchbaseException as ex:
        print('Caught Couchbase exception: {}'.format(ex))

    try:
        # Upsert with Durability (Couchbase Server >= 6.5) level Majority
        document = dict(foo="bar", bar="foo")
        opts = UpsertOptions(durability=ServerDurability(Durability.MAJORITY))
        result = await collection.upsert("document-key", document, opts)
    except CouchbaseException as ex:
        print('Caught Couchbase exception: {}'.format(ex))

    # @TODO: couchbase++ doesn't implement observe based durability
    # try:
    #     # Upsert with observe based durability (Couchbase Server < 6.5)
    #     document = {"foo": "bar", "bar": "foo"}
    #     opts = UpsertOptions(
    #         durability=ClientDurability(
    #             ReplicateTo.ONE,
    #             PersistTo.ONE))
    #     result = await collection.upsert("document-key", document, opts)
    # except CouchbaseException as ex:
    #     print(ex)

    result = await collection.get("document-key")
    print(result.content_as[dict])

    opts = GetOptions(timeout=timedelta(seconds=5))
    result = await collection.get("document-key", opts)
    print(result.content_as[dict])

    try:
        # remove document with options
        result = await collection.remove(
            "document-key",
            RemoveOptions(
                cas=12345,
                durability=ServerDurability(
                    Durability.MAJORITY)))
    except CouchbaseException as ex:
        # we expect an exception here as the CAS value is chosen
        # for example purposes
        print('Caught Couchbase exception: {}'.format(ex))

    result = await collection.touch("document-key", timedelta(seconds=10))

    result = await collection.get("document-key", GetOptions(with_expiry=True))
    print("Expiry of result: {}".format(result.expiryTime))

    result = await collection.get_and_touch("document-key", timedelta(seconds=10))

    # Increment binary value by 1
    await collection.binary().increment(
        "counter-key",
        IncrementOptions(
            delta=DeltaValue(1)))

    # Increment binary value by 5, if key doesn't exist, seed it at 1000
    await collection.binary().increment(
        "counter-key",
        IncrementOptions(
            delta=DeltaValue(5),
            initial=SignedInt64(1000)))

    # Decrement binary value by 1
    await collection.binary().decrement(
        "counter-key",
        DecrementOptions(
            delta=DeltaValue(1)))

    # Decrement binary value by 2, if key doesn't exist, seed it at 1000
    await collection.binary().decrement(
        "counter-key",
        DecrementOptions(
            delta=DeltaValue(2),
            initial=SignedInt64(1000)))

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
