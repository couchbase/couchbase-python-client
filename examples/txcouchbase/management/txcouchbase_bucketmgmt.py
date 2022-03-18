# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
from twisted.internet import (defer,
                              reactor,
                              task)

import txcouchbase
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import BucketAlreadyExistsException, BucketDoesNotExistException
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          ConflictResolutionType,
                                          CreateBucketSettings)
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
def main():
    cluster = Cluster(
        "couchbase://localhost",
        authenticator=PasswordAuthenticator(
            "Administrator",
            "password"))
    # @TODO: if connection fails, this will hang
    yield cluster.on_connect()
    # create a bucket object
    bucket = cluster.bucket('default')
    yield bucket.on_connect()

    bucket_manager = cluster.buckets()

    yield bucket_manager.create_bucket(
        CreateBucketSettings(
            name="hello",
            flush_enabled=False,
            ram_quota_mb=100,
            num_replicas=0,
            bucket_type=BucketType.COUCHBASE,
            conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER))

    # Another approach to catch if bucket already exists
    try:
        yield bucket_manager.create_bucket(
            CreateBucketSettings(
                name="hello",
                flush_enabled=False,
                ram_quota_mb=100,
                num_replicas=0,
                bucket_type=BucketType.COUCHBASE,
                conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER))
    except BucketAlreadyExistsException:
        print("{} bucket previously created.".format("hello"))

    # Creating a bucket can take some time depending on what
    # the cluster is doing, good to use retries
    try:
        bucket = yield retry(bucket_manager.get_bucket, "hello")
    except Exception as ex:
        print(ex)

    bucket = yield bucket_manager.get_bucket("hello")
    print("Found bucket: {}".format(bucket.name))

    yield bucket_manager.update_bucket(BucketSettings(name="hello", flush_enabled=True))

    yield bucket_manager.flush_bucket("hello")

    yield bucket_manager.drop_bucket("hello")

    # verify bucket dropped
    try:
        yield bucket_manager.get_bucket("hello")
    except BucketDoesNotExistException:
        print("{} bucket dropped.".format("hello"))

    yield cluster.close()
    reactor.stop()


if __name__ == "__main__":
    main()
    reactor.run()
