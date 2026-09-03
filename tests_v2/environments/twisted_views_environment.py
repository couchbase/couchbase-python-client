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

"""View bucket setup, teardown and row validation for the txcouchbase views suites.

The third of the trio.  The Twisted view surface is complete, checked rather than assumed:
TxBucket.view_query, and, since PYCBC-1901, TxBucket.view_indexes with the same five
design-document methods the other two api styles carry.  The suites this serves are that
ticket's regression test.

The asynchrony sits in a different place from acouchbase, and that is the whole of the
difference between this module and the async one:

  * `bucket.view_query()` returns a Deferred, so the *call* goes through the reactor and this
    module bridges it with run_in_reactor_thread.  `result.rows()` is then a plain iterator, so
    the drain is an ordinary `for` and the assertion helper stays synchronous.  acouchbase is
    the mirror image: the call returns immediately and the rows are async.
  * the manager wrappers already bridge their own calls, so they are passed to try_n_times with
    `is_deferred=False`.  `collection.upsert` does not, so the seed loop deliberately leaves the
    default in place; the two calls are not the same and should not be made to look alike.
  * `bucket.on_connect()` returns a Deferred and is bridged the same way.

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
from tests_v2.environments.managers import TwistedTestEnvironmentBucketManager, TwistedTestEnvironmentViewIndexManager
from tests_v2.environments.twisted_environment import TwistedTestEnvironment, run_in_reactor_thread
from tests_v2.skips import CouchbaseTestEnvironmentException
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures
from tests_v2.view_defs import (REBUILT_VIEW_MAP_FUNCTION,
                                VIEW_DOCS,
                                VIEW_MAP_FUNCTION,
                                VIEW_NAME,
                                design_doc_name,
                                view_bucket_name)


class TwistedViewsEnvironment:
    """Twisted view bucket setup, teardown and row validation."""

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
    def _drain(result: ViewResult, expected_count: int) -> list[ViewRow]:
        """Validate every row and the metadata in one pass, and return the rows."""
        cb_assert_isinstance(result, ViewResult, what='result passed to _drain')
        rows: list[ViewRow] = []
        for row in result.rows():
            assert row is not None, f'view result yielded a None row at index {len(rows)}'
            TwistedViewsEnvironment._validate_row(row)
            rows.append(row)
        TwistedViewsEnvironment._validate_metadata(result, expected_count)
        return rows

    @staticmethod
    def assert_rows_exact(result: ViewResult, expected_count: int) -> list[ViewRow]:
        """Assert the result yields exactly expected_count validated rows, and return them."""
        rows = TwistedViewsEnvironment._drain(result, expected_count)
        cb_assert_eq(len(rows), expected_count, what='view rows returned')
        return rows

    @staticmethod
    def row_value(row: ViewRow) -> dict[str, Any]:
        """The emitted value as the dict it always is; see the sync module."""
        value = row.value
        assert isinstance(value, dict), f'view row value: expected a dict, got {type(value).__name__!r}'
        return value

    @staticmethod
    def drain(result: ViewResult) -> None:
        """Consume the result without asserting anything about it.

        Reached only by the two tests whose subject is an exception.  view_query is bridged here
        rather than lazy, so the call can errback on its own; a missing design document does not
        take that path, though, arriving as an error row and surfacing from this iteration just
        as it does under the other two apis.  Keeping the call inside the pytest.raises block
        alongside the drain is what makes one pair of statements correct whichever of them fails.
        """
        for _ in result.rows():
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
    def _drop_stranded_bucket(cb_env: TwistedTestEnvironment) -> None:
        """Remove a view bucket an earlier run left behind, if there is one.

        Nothing else will drop it.  A yield fixture only runs its teardown half if it got as far
        as yielding, so a setup that raises leaves the bucket in place, and a process that is
        killed outright runs neither half.  setup_views cleans up the failures it can see; what
        is left for this is the ones it cannot, a SIGINT or a crash between the create and the
        yield.  Without it the next run fails at create_bucket with BucketAlreadyExistsException,
        which says nothing at all about why the run before it stopped.
        """
        bm = cb_env.get_manager(TwistedTestEnvironmentBucketManager)
        bucket_name = view_bucket_name()
        try:
            bm.get_bucket(bucket_name)
        except BucketDoesNotExistException:
            return
        print(f'Bucket {bucket_name} was left behind by an earlier run; dropping it.')
        bm.drop_bucket(bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    @staticmethod
    def _create_view_bucket(cb_env: TwistedTestEnvironment) -> None:
        TwistedViewsEnvironment._drop_stranded_bucket(cb_env)
        bm = cb_env.get_manager(TwistedTestEnvironmentBucketManager)
        bucket_name = view_bucket_name()
        bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                              ram_quota_mb=TwistedViewsEnvironment.VIEW_BUCKET_RAM_MB,
                                              flush_enabled=False,
                                              storage_backend=TwistedViewsEnvironment.VIEW_BUCKET_BACKEND))
        cb_env.consistency.wait_until_bucket_healthy(bucket_name)
        bucket = cb_env.cluster.bucket(bucket_name)
        run_in_reactor_thread(bucket.on_connect)
        cb_env.set_view_bucket(bucket, bucket.default_collection())

    @staticmethod
    def _load_view_data(cb_env: TwistedTestEnvironment) -> None:
        """Seed the view bucket with the corpus the map function partitions.

        `coll.upsert` returns a Deferred, so this leaves try_n_times' default in place; contrast
        the manager calls in setup_views, which bridge their own and take is_deferred=False.
        """
        coll = cb_env.view_collection
        for doc in cb_env.data_provider.get_vehicles()[:VIEW_DOCS]:
            TwistedTestEnvironment.try_n_times(3, 3, coll.upsert, f'{doc["id"]}', doc)

    @staticmethod
    def _wait_for_view(cb_env: TwistedTestEnvironment) -> int:
        """Poll until the view returns the whole corpus; return the last count it gave.

        See the sync version for why the poll asks for REQUEST_PLUS.
        """
        count = 0
        for _ in range(TwistedViewsEnvironment.VIEW_WAIT_ROUNDS):
            try:
                result = run_in_reactor_thread(cb_env.view_bucket.view_query,
                                               design_doc_name(),
                                               VIEW_NAME,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT,
                                               scan_consistency=ViewScanConsistency.REQUEST_PLUS)
                count = sum(1 for _ in result.rows())
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
            TwistedTestEnvironment.sleep(TwistedViewsEnvironment.VIEW_WAIT_DELAY)
        return count

    @staticmethod
    def _full_set_count(cb_env: TwistedTestEnvironment) -> str:
        """The view's full_set count, for the failure message; see the sync environment."""
        try:
            result = run_in_reactor_thread(cb_env.view_bucket.view_query,
                                           design_doc_name(),
                                           VIEW_NAME,
                                           namespace=DesignDocumentNamespace.DEVELOPMENT,
                                           full_set=True)
            return str(sum(1 for _ in result.rows()))
        except Exception as exc:
            return f'unavailable, {type(exc).__name__}'

    @staticmethod
    def _stored_count(cb_env: TwistedTestEnvironment) -> str:
        """How many corpus documents a key lookup finds, for the failure message; see the sync environment."""
        try:
            coll = cb_env.view_collection
            ids = [f'{doc["id"]}' for doc in cb_env.data_provider.get_vehicles()[:VIEW_DOCS]]
            return str(sum(1 for key in ids if run_in_reactor_thread(coll.exists, key).exists))
        except Exception as exc:
            return f'unavailable, {type(exc).__name__}'

    @staticmethod
    def setup_views(cb_env: TwistedTestEnvironment) -> None:
        """Create the bucket, seed it, build the design document and wait for the view."""
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.BucketManagement, ServerFeatures.ViewIndexManagement], cb_env.server_context)

        TwistedViewsEnvironment._create_view_bucket(cb_env)
        # The bucket exists from here on; see the sync environment for why every failure below
        # has to take it with it.
        try:
            TwistedViewsEnvironment._load_view_data(cb_env)

            vixm = cb_env.get_manager(TwistedTestEnvironmentViewIndexManager)
            vixm.use_bucket(cb_env.view_bucket)
            TwistedTestEnvironment.try_n_times(5, 3, vixm.upsert_design_document,
                                               TwistedViewsEnvironment.design_document(),
                                               DesignDocumentNamespace.DEVELOPMENT,
                                               is_deferred=False)

            count = TwistedViewsEnvironment._wait_for_view(cb_env)
            if count < VIEW_DOCS:
                # One new index before giving up; see the sync environment.
                warnings.warn(f'Rebuilding the view after {count} of {VIEW_DOCS} rows')
                TwistedTestEnvironment.try_n_times(5, 3, vixm.upsert_design_document,
                                                   TwistedViewsEnvironment.design_document(rebuilt=True),
                                                   DesignDocumentNamespace.DEVELOPMENT,
                                                   is_deferred=False)
                count = TwistedViewsEnvironment._wait_for_view(cb_env)
            if count < VIEW_DOCS:
                full_set = TwistedViewsEnvironment._full_set_count(cb_env)
                stored = TwistedViewsEnvironment._stored_count(cb_env)
                raise CouchbaseTestEnvironmentException(
                    f'View {design_doc_name()}/{VIEW_NAME} returned {count} of {VIEW_DOCS} documents '
                    f'after one rebuild ({full_set} with full_set, {stored} of {VIEW_DOCS} found by key); '
                    'the suite asserts exact row counts and cannot run against a partial index')
        except Exception:
            try:
                TwistedViewsEnvironment.teardown_views(cb_env)
            except Exception as drop_exc:
                print(f'Could not drop {view_bucket_name()} after a failed setup: {drop_exc}')
            raise

    @staticmethod
    def teardown_views(cb_env: TwistedTestEnvironment) -> None:
        """Drop the bucket, which takes the design document and the corpus with it."""
        bucket_name = view_bucket_name()
        bm = cb_env.get_manager(TwistedTestEnvironmentBucketManager)
        TwistedTestEnvironment.try_n_times(5, 3, bm.drop_bucket, bucket_name, is_deferred=False)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)
