import asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import BucketAlreadyExistsException, BucketDoesNotExistException
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          ConflictResolutionType,
                                          CreateBucketSettings)


async def retry(func, *args, back_off=0.5, limit=5, **kwargs):
    for i in range(limit):
        try:
            return await func(*args, **kwargs)
        except Exception:
            print("Retry in {} seconds...".format((i + 1) * back_off))
            await asyncio.sleep((i + 1) * back_off)

    raise Exception(
        "Unable to successfully receive result from {}".format(func))


async def main():
    cluster = Cluster(
        "couchbase://localhost",
        authenticator=PasswordAuthenticator(
            "Administrator",
            "password"))
    await cluster.on_connect()
    bucket = cluster.bucket("default")
    await bucket.on_connect()

    bucket_manager = cluster.buckets()

    await bucket_manager.create_bucket(
        CreateBucketSettings(
            name="hello",
            flush_enabled=False,
            ram_quota_mb=100,
            num_replicas=0,
            bucket_type=BucketType.COUCHBASE,
            conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER))

    # Another approach to catch if bucket already exists
    try:
        await bucket_manager.create_bucket(
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
    bucket = await retry(bucket_manager.get_bucket, "hello")

    bucket = await bucket_manager.get_bucket("hello")
    print("Found bucket: {}".format(bucket.name))

    await bucket_manager.update_bucket(BucketSettings(name="hello", flush_enabled=True))

    await bucket_manager.flush_bucket("hello")

    await bucket_manager.drop_bucket("hello")

    # verify bucket dropped
    try:
        await bucket_manager.get_bucket("hello")
    except BucketDoesNotExistException:
        print("{} bucket dropped.".format("hello"))

    await cluster.close()

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
