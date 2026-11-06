#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from datetime import timedelta

# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
import txcouchbase  # nopep8 # isort:skip # noqa: E402, F401

from twisted.internet import defer, reactor

from txcouchbase.auth import PasswordAuthenticator
from txcouchbase.cluster import Cluster
from txcouchbase.durability import (ClientDurability,
                                    DurabilityLevel,
                                    PersistTo,
                                    ReplicateTo,
                                    ServerDurability)
from txcouchbase.exceptions import CASMismatchException, CouchbaseException
from txcouchbase.options import (DecrementOptions,
                                 DeltaValue,
                                 GetOptions,
                                 IncrementOptions,
                                 InsertOptions,
                                 RemoveOptions,
                                 ReplaceOptions,
                                 SignedInt64,
                                 UpsertOptions)


@defer.inlineCallbacks
def main():
    # create a cluster object
    cluster = Cluster('couchbase://localhost',
                      authenticator=PasswordAuthenticator('Administrator', 'password'))

    # @TODO: if connection fails, this will hang
    yield cluster.on_connect()
    # create a bucket object
    bucket = cluster.bucket('default')
    yield bucket.on_connect()

    collection = bucket.default_collection()

    try:
        yield collection.remove("document-key")
    except CouchbaseException as ex:
        pass  # may not exist in this example

    try:
        yield collection.remove("document-key-opts")
    except CouchbaseException as ex:
        pass  # may not exist in this example

    # Insert document
    document = {"foo": "bar", "bar": "foo"}
    result = yield collection.insert("document-key", document)
    print("Result: {}; CAS: {}".format(result, result.cas))

    # Insert document with options
    document = {"foo": "bar", "bar": "foo"}
    opts = InsertOptions(timeout=timedelta(seconds=5))
    result = yield collection.insert("document-key-opts",
                                     document,
                                     opts,
                                     expiry=timedelta(seconds=30))

    try:
        # Replace document with CAS
        document = {"foo": "bar", "bar": "foo"}
        result = yield collection.replace(
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
        get_result = yield collection.get("document-key")
        doc = get_result.content_as[dict]
        doc["bar"] = "baz"
        result = yield collection.replace("document-key", doc, ReplaceOptions(cas=get_result.cas))
    except CouchbaseException as ex:
        print('Caught Couchbase exception: {}'.format(ex))

    try:
        # Upsert with Durability (Couchbase Server >= 6.5) level Majority
        document = dict(foo="bar", bar="foo")
        result = yield collection.upsert("document-key",
                                         document,
                                         UpsertOptions(durability=ServerDurability(DurabilityLevel.MAJORITY)))
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
    #     result = yield collection.upsert("document-key", document, opts)
    # except CouchbaseException as ex:
    #     print(ex)

    get_result = yield collection.get("document-key")
    print(get_result.content_as[dict])

    get_result = yield collection.get("document-key", GetOptions(timeout=timedelta(seconds=5)))
    print(get_result.content_as[dict])

    try:
        # remove document with options
        result = yield collection.remove(
            "document-key",
            RemoveOptions(
                cas=12345,
                durability=ServerDurability(
                    DurabilityLevel.MAJORITY)))
    except CouchbaseException as ex:
        # we expect an exception here as the CAS value is chosen
        # for example purposes
        print('Caught Couchbase exception: {}'.format(ex))

    result = yield collection.touch("document-key", timedelta(seconds=10))

    get_result = yield collection.get("document-key", GetOptions(with_expiry=True))
    print("Expiry of result: {}".format(get_result.expiry_time))

    get_result = yield collection.get_and_touch("document-key", timedelta(seconds=10))

    # Increment binary value by 1
    yield collection.binary().increment(
        "counter-key",
        IncrementOptions(
            delta=DeltaValue(1)))

    # Increment binary value by 5, if key doesn't exist, seed it at 1000
    yield collection.binary().increment(
        "counter-key",
        IncrementOptions(
            delta=DeltaValue(5),
            initial=SignedInt64(1000)))

    # Decrement binary value by 1
    yield collection.binary().decrement(
        "counter-key",
        DecrementOptions(
            delta=DeltaValue(1)))

    # Decrement binary value by 2, if key doesn't exist, seed it at 1000
    yield collection.binary().decrement(
        "counter-key",
        DecrementOptions(
            delta=DeltaValue(2),
            initial=SignedInt64(1000)))

    reactor.stop()  # type: ignore[attr-defined]


if __name__ == "__main__":
    main()
    reactor.run()  # type: ignore[attr-defined]
