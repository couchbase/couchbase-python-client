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

import asyncio

from acouchbase.auth import PasswordAuthenticator
from acouchbase.cluster import Cluster, get_event_loop
from acouchbase.exceptions import BucketAlreadyExistsException, BucketDoesNotExistException
from acouchbase.management.buckets import (BucketSettings,
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
    bucket_settings = await retry(bucket_manager.get_bucket, "hello")

    bucket_settings = await bucket_manager.get_bucket("hello")
    print("Found bucket: {}".format(bucket_settings.name))

    await bucket_manager.update_bucket(BucketSettings(name="hello", flush_enabled=True))

    await bucket_manager.flush_bucket("hello")

    await bucket_manager.drop_bucket("hello")

    # verify bucket dropped
    try:
        await bucket_manager.get_bucket("hello")
    except BucketDoesNotExistException:
        print("{} bucket dropped.".format("hello"))


if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
