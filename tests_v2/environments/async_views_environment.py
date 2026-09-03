#  Copyright 2016-2024. Couchbase, Inc.
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

"""View bucket setup, teardown and row validation for the acouchbase views suites.

The async counterpart of views_environment.py, and the same shape.  Four things differ from the
sync module, all forced by the SDK rather than chosen:

  * every bucket and view-index manager call is awaited, since the async wrappers return
    awaitables where the sync ones return values.  Dropping the one on upsert_design_document
    leaves the view uncreated while setup carries on waiting for rows from it.
  * opening the bucket takes an `await bucket.on_connect()` between cluster.bucket() and
    default_collection().  That is the reason set_view_bucket() takes both handles rather than
    deriving the collection itself.
  * `bucket.view_query()` still returns its result object synchronously; it is the *rows* that
    are an async iterator.  So the drain is `async for`, and the assertion helper becomes a
    coroutine with it, since the count it asserts on is only known once the stream is consumed.
    `metadata()` stays synchronous in all three api styles.
  * the drain cannot be `for row in result.rows()`.  That does not raise over an async iterator,
    it iterates nothing, so every count would be zero and the readiness wait in particular would
    never see a single row while reporting a clean, ordinary timeout.

Everything else, including why there is no loose assertion helper, is settled in the sync
module's docstring and not restated here.
"""

from __future__ import annotations

import warnings
from typing import Any

from couchbase.exceptions import BucketDoesNotExistException, DesignDocumentNotFoundException
from couchbase.management.buckets import CreateBucketSettings, StorageBackend
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from couchbase.result import ViewResult
from couchbase.views import (ViewMetaData,
                             ViewRow,
                             ViewScanConsistency)
from tests_v2.assert_helpers import (cb_assert_eq,
                                     cb_assert_gte,
                                     cb_assert_isinstance)
from tests_v2.environments.async_environment import AsyncTestEnvironment
from tests_v2.environments.managers import AsyncTestEnvironmentBucketManager, AsyncTestEnvironmentViewIndexManager
from tests_v2.skips import CouchbaseTestEnvironmentException
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures
from tests_v2.view_defs import (REBUILT_VIEW_MAP_FUNCTION,
                                VIEW_DOCS,
                                VIEW_MAP_FUNCTION,
                                VIEW_NAME,
                                design_doc_name,
                                view_bucket_name)


class AsyncViewsEnvironment:
    """Asyncio view bucket setup, teardown and row validation."""

    VIEW_BUCKET_RAM_MB = 100
    VIEW_BUCKET_BACKEND = StorageBackend.COUCHSTORE
    VIEW_WAIT_ROUNDS = 12
    VIEW_WAIT_DELAY = 5

    # ------------------------------------------------------------------
    # Row and metadata validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_row(row: ViewRow) -> None:
        cb_assert_isinstance(row, ViewRow, what='view row')
        cb_assert_isinstance(row.id, str, what='view row id')
        cb_assert_isinstance(row.key, str, what='view row key')
        # Bound and asserted with a plain `assert` because the three lines below index it, and
        # ViewRow.value is annotated `object`; only the assert form narrows it for the gate.
        value = row.value
        assert isinstance(value, dict), f'view row value: expected a dict, got {type(value).__name__!r}'
        cb_assert_eq(value['id'], row.id, what='view row value id')
        cb_assert_eq(value['type'], 'vehicle', what='view row value type')
        cb_assert_isinstance(value['batch'], str, what='view row value batch')

    @staticmethod
    def _validate_metadata(result: ViewResult, expected_count: int) -> None:
        meta = result.metadata()
        cb_assert_isinstance(meta, ViewMetaData, what='view result metadata')
        total_rows = meta.total_rows()
        assert total_rows is not None, 'view result metadata carried no total_rows'
        # int() rather than comparing directly, and not for tidiness.  total_rows() is annotated
        # Optional[UnsignedInt64] while it returns the plain int the server sent, and
        # UnsignedInt64 defines no comparison operators at all, so the natural spelling of this
        # line is rejected by a type checker and would be a TypeError if the annotation were
        # ever made true.  int() is correct either way.  SearchMetrics.total_rows() is annotated
        # int and needs none of this.
        cb_assert_gte(int(total_rows), expected_count, what='view metadata total_rows')

    @staticmethod
    async def _drain(result: ViewResult, expected_count: int) -> list[ViewRow]:
        """Validate every row and the metadata in one pass, and return the rows."""
        cb_assert_isinstance(result, ViewResult, what='result passed to _drain')
        rows: list[ViewRow] = []
        async for row in result.rows():
            assert row is not None, f'view result yielded a None row at index {len(rows)}'
            AsyncViewsEnvironment._validate_row(row)
            rows.append(row)
        AsyncViewsEnvironment._validate_metadata(result, expected_count)
        return rows

    @staticmethod
    async def assert_rows_exact(result: ViewResult, expected_count: int) -> list[ViewRow]:
        """Assert the result yields exactly expected_count validated rows, and return them."""
        rows = await AsyncViewsEnvironment._drain(result, expected_count)
        cb_assert_eq(len(rows), expected_count, what='view rows returned')
        return rows

    @staticmethod
    def row_value(row: ViewRow) -> dict[str, Any]:
        """The emitted value as the dict it always is; see the sync module."""
        value = row.value
        assert isinstance(value, dict), f'view row value: expected a dict, got {type(value).__name__!r}'
        return value

    @staticmethod
    async def drain(result: ViewResult) -> None:
        """Consume the result without asserting anything about it; see the sync module."""
        async for _ in result.rows():
            pass

    # ------------------------------------------------------------------
    # Bucket and design document lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def design_document(rebuilt: bool = False) -> DesignDocument:
        """The one design document the suites upsert; see the sync environment for `rebuilt`."""
        map_function = REBUILT_VIEW_MAP_FUNCTION if rebuilt else VIEW_MAP_FUNCTION
        return DesignDocument(name=design_doc_name(), views={VIEW_NAME: View(map=map_function)})

    @staticmethod
    async def _drop_stranded_bucket(cb_env: AsyncTestEnvironment) -> None:
        """Remove a view bucket an earlier run left behind, if there is one.

        Nothing else will drop it.  A yield fixture only runs its teardown half if it got as far
        as yielding, so a setup that raises leaves the bucket in place, and a process that is
        killed outright runs neither half.  setup_views cleans up the failures it can see; what
        is left for this is the ones it cannot, a SIGINT or a crash between the create and the
        yield.  Without it the next run fails at create_bucket with BucketAlreadyExistsException,
        which says nothing at all about why the run before it stopped.
        """
        bm = cb_env.get_manager(AsyncTestEnvironmentBucketManager)
        bucket_name = view_bucket_name()
        try:
            await bm.get_bucket(bucket_name)
        except BucketDoesNotExistException:
            return
        print(f'Bucket {bucket_name} was left behind by an earlier run; dropping it.')
        await bm.drop_bucket(bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    @staticmethod
    async def _create_view_bucket(cb_env: AsyncTestEnvironment) -> None:
        await AsyncViewsEnvironment._drop_stranded_bucket(cb_env)
        bm = cb_env.get_manager(AsyncTestEnvironmentBucketManager)
        bucket_name = view_bucket_name()
        await bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                                    ram_quota_mb=AsyncViewsEnvironment.VIEW_BUCKET_RAM_MB,
                                                    flush_enabled=False,
                                                    storage_backend=AsyncViewsEnvironment.VIEW_BUCKET_BACKEND))
        cb_env.consistency.wait_until_bucket_healthy(bucket_name)
        bucket = cb_env.cluster.bucket(bucket_name)
        await bucket.on_connect()
        cb_env.set_view_bucket(bucket, bucket.default_collection())

    @staticmethod
    async def _load_view_data(cb_env: AsyncTestEnvironment) -> None:
        """Seed the view bucket with the corpus the map function partitions."""
        coll = cb_env.view_collection
        for doc in cb_env.data_provider.get_vehicles()[:VIEW_DOCS]:
            await AsyncTestEnvironment.try_n_times(3, 3, coll.upsert, f'{doc["id"]}', doc)

    @staticmethod
    async def _wait_for_view(cb_env: AsyncTestEnvironment) -> int:
        """Poll until the view returns the whole corpus; return the last count it gave.

        See the sync version for why the poll asks for REQUEST_PLUS.
        """
        count = 0
        for _ in range(AsyncViewsEnvironment.VIEW_WAIT_ROUNDS):
            try:
                result = cb_env.view_bucket.view_query(design_doc_name(),
                                                       VIEW_NAME,
                                                       namespace=DesignDocumentNamespace.DEVELOPMENT,
                                                       scan_consistency=ViewScanConsistency.REQUEST_PLUS)
                count = 0
                async for _row in result.rows():
                    count += 1
            except DesignDocumentNotFoundException:
                # A successful upsert does not mean every node can serve the design document
                # yet, and the 404 that comes back until it propagates says the same thing a
                # short row count does.  It belongs in this loop rather than ending the run:
                # otherwise the first query can arrive at a node the document has not reached,
                # and the whole suite errors out in the one situation the poll exists for.
                count = 0
                reason = f'design document {design_doc_name()} is not on every node yet'
            else:
                if count >= VIEW_DOCS:
                    break
                reason = f'the view returned {count} of {VIEW_DOCS} rows'
            print(f'Waiting for the view: {reason}...')
            await AsyncTestEnvironment.sleep(AsyncViewsEnvironment.VIEW_WAIT_DELAY)
        return count

    @staticmethod
    async def _full_set_count(cb_env: AsyncTestEnvironment) -> str:
        """The view's full_set count, for the failure message; see the sync environment."""
        try:
            result = cb_env.view_bucket.view_query(design_doc_name(),
                                                   VIEW_NAME,
                                                   namespace=DesignDocumentNamespace.DEVELOPMENT,
                                                   full_set=True)
            count = 0
            async for _row in result.rows():
                count += 1
            return str(count)
        except Exception as exc:
            return f'unavailable, {type(exc).__name__}'

    @staticmethod
    async def _stored_count(cb_env: AsyncTestEnvironment) -> str:
        """How many corpus documents a key lookup finds, for the failure message; see the sync environment."""
        try:
            coll = cb_env.view_collection
            found = 0
            for doc in cb_env.data_provider.get_vehicles()[:VIEW_DOCS]:
                if (await coll.exists(f'{doc["id"]}')).exists:
                    found += 1
            return str(found)
        except Exception as exc:
            return f'unavailable, {type(exc).__name__}'

    @staticmethod
    async def setup_views(cb_env: AsyncTestEnvironment) -> None:
        """Create the bucket, seed it, build the design document and wait for the view."""
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.BucketManagement, ServerFeatures.ViewIndexManagement], cb_env.server_context)

        await AsyncViewsEnvironment._create_view_bucket(cb_env)
        # The bucket exists from here on; see the sync environment for why every failure below
        # has to take it with it.
        try:
            await AsyncViewsEnvironment._load_view_data(cb_env)

            vixm = cb_env.get_manager(AsyncTestEnvironmentViewIndexManager)
            vixm.use_bucket(cb_env.view_bucket)
            await AsyncTestEnvironment.try_n_times(5, 3, vixm.upsert_design_document,
                                                   AsyncViewsEnvironment.design_document(),
                                                   DesignDocumentNamespace.DEVELOPMENT)

            count = await AsyncViewsEnvironment._wait_for_view(cb_env)
            if count < VIEW_DOCS:
                # One new index before giving up; see the sync environment.
                warnings.warn(f'Rebuilding the view after {count} of {VIEW_DOCS} rows')
                await AsyncTestEnvironment.try_n_times(5, 3, vixm.upsert_design_document,
                                                       AsyncViewsEnvironment.design_document(rebuilt=True),
                                                       DesignDocumentNamespace.DEVELOPMENT)
                count = await AsyncViewsEnvironment._wait_for_view(cb_env)
            if count < VIEW_DOCS:
                full_set = await AsyncViewsEnvironment._full_set_count(cb_env)
                stored = await AsyncViewsEnvironment._stored_count(cb_env)
                raise CouchbaseTestEnvironmentException(
                    f'View {design_doc_name()}/{VIEW_NAME} returned {count} of {VIEW_DOCS} documents '
                    f'after one rebuild ({full_set} with full_set, {stored} of {VIEW_DOCS} found by key); '
                    'the suite asserts exact row counts and cannot run against a partial index')
        except Exception:
            try:
                await AsyncViewsEnvironment.teardown_views(cb_env)
            except Exception as drop_exc:
                print(f'Could not drop {view_bucket_name()} after a failed setup: {drop_exc}')
            raise

    @staticmethod
    async def teardown_views(cb_env: AsyncTestEnvironment) -> None:
        """Drop the bucket, which takes the design document and the corpus with it."""
        bucket_name = view_bucket_name()
        bm = cb_env.get_manager(AsyncTestEnvironmentBucketManager)
        await AsyncTestEnvironment.try_n_times(5, 3, bm.drop_bucket, bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)
