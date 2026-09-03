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

"""View bucket setup, teardown and row validation for the classic/sync views suites.

One module per api style, one class of static helpers, no TestEnvironment subclass.

**The suite owns a bucket, not a collection, and that is what makes this environment different
from every other one in the package.**  A view is a property of a bucket: a design document
upserted into the shared test bucket is visible to every other suite in the run and is dropped
from underneath them at teardown.  So setup creates `test-view-bucket`, seeds it, builds the
design document on it and drops the whole bucket afterwards, which is also why the suite is
gated on BucketManagement as well as ViewIndexManagement.  Both are in FEATURES_NOT_IN_MOCK, so
the gate takes the whole class on GoCAVES rather than failing inside a manager.

**Nothing is loaded into the shared bucket at all.**  The suite declares NUM_DOCS = 0, so
build_environment seeds nothing, and _load_view_data() puts the corpus where the view can see
it.  A suite that seeded both would pay for fifty writes it never reads.

**Setup establishes the row count the suite then asserts exactly.**  _wait_for_view() polls the
view with the same options the tests use until it returns the whole corpus, and raises naming
the shortfall if it never does.  That is what lets every assertion here be assert_rows_exact
with no loose form anywhere: the suite creates and drops its own bucket, so nothing outside it
can add a row, and the one remaining unknown (has the view finished indexing?) is answered once,
in setup, instead of being absorbed by a `>=` in fifty places.  A loose form costs real
coverage here in two distinct ways: asking for 10 rows out of a corpus of 50 cannot notice 40
missing, and an expectation computed from the smallest view key is only right when that key
falls in one of the three ten-document groups, which is three runs in four.

The design document, the map function and the key arithmetic the suite asserts on are all in
tests_v2/view_defs.py; that module's docstring has the reason they are one file.
"""

from __future__ import annotations

from typing import Any

from couchbase.exceptions import BucketDoesNotExistException, DesignDocumentNotFoundException
from couchbase.management.buckets import CreateBucketSettings, StorageBackend
from couchbase.management.views import (DesignDocument,
                                        DesignDocumentNamespace,
                                        View)
from couchbase.result import ViewResult
from couchbase.views import ViewMetaData, ViewRow
from tests_v2.assert_helpers import (cb_assert_eq,
                                     cb_assert_gte,
                                     cb_assert_isinstance)
from tests_v2.environments.managers import TestEnvironmentBucketManager, TestEnvironmentViewIndexManager
from tests_v2.environments.test_environment import SyncTestEnvironment, TestEnvironment
from tests_v2.skips import CouchbaseTestEnvironmentException
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures
from tests_v2.view_defs import (VIEW_DOCS,
                                VIEW_MAP_FUNCTION,
                                VIEW_NAME,
                                design_doc_name,
                                view_bucket_name)


class ViewsEnvironment:
    """Classic/sync view bucket setup, teardown and row validation."""

    # The bucket the suite creates.  100MB is the smallest quota the server accepts;
    # COUCHSTORE is not a preference but a requirement, since views are unsupported on Magma.
    VIEW_BUCKET_RAM_MB = 100
    VIEW_BUCKET_BACKEND = StorageBackend.COUCHSTORE

    # Rounds and seconds between them while waiting for the view to return the whole corpus.
    # Indexing starts on the first query, so the first round is expected to come back short.
    VIEW_WAIT_ROUNDS = 12
    VIEW_WAIT_DELAY = 5

    # ------------------------------------------------------------------
    # Row and metadata validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_row(row: ViewRow) -> None:
        """Everything true of every row this design document emits.

        The value is the trimmed copy the map function builds, so its three keys are as much a
        part of the view's contract as the key is, and checking them here is what lets the
        per-test assertions be about the *query* rather than about the fixture.
        """
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
        # total_rows() counts what the view holds, not what this query returned, so it stays
        # `>=` even where the row count itself is exact: most queries here carry a key filter.
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
            ViewsEnvironment._validate_row(row)
            rows.append(row)
        ViewsEnvironment._validate_metadata(result, expected_count)
        return rows

    @staticmethod
    def assert_rows_exact(result: ViewResult, expected_count: int) -> list[ViewRow]:
        """Assert the result yields exactly expected_count validated rows, and return them.

        The only assertion helper here, and the absence of a loose counterpart is deliberate.
        SearchEnvironment carries both because a bucket-wide FTS index sees documents another
        suite seeded; a view over a bucket this suite created and will drop sees nothing it did
        not write, so every count in the suite is knowable and `at_least` would only ever be
        hiding one that had gone wrong.
        """
        rows = ViewsEnvironment._drain(result, expected_count)
        cb_assert_eq(len(rows), expected_count, what='view rows returned')
        return rows

    @staticmethod
    def row_value(row: ViewRow) -> dict[str, Any]:
        """The emitted value as the dict this design document always makes it.

        `ViewRow.value` is annotated `object`, which is honest about a view emitting arbitrary
        JSON and useless to a caller that knows its own map function: indexing it is a type
        error.  Narrowing once, here, beats an isinstance line at every call site, and it puts
        the claim next to _validate_row, which has already checked the same thing for every row
        the suite sees.

        The rest of the dataclass is annotated the same way and worse: `key` and `id` are
        declared `str` with a default of None, so the annotations say a field cannot be None
        while the class hands one out.  All four belong to one fix in one file.
        """
        value = row.value
        assert isinstance(value, dict), f'view row value: expected a dict, got {type(value).__name__!r}'
        return value

    @staticmethod
    def drain(result: ViewResult) -> None:
        """Consume the result without asserting anything about it.

        For the two tests whose subject is an exception.  Which statement raises is api-dependent:
        a view query is lazy under sync and acouchbase, so nothing reaches the server until the
        rows are pulled, while the Twisted call is bridged and can errback on its own.  A missing
        design document in fact surfaces from the iteration under all three, arriving as an error
        row in the stream, but the suite keeps the call and the drain together inside the
        pytest.raises block so that one pair of statements is correct whichever of them fails.
        """
        for _ in result.rows():
            pass

    # ------------------------------------------------------------------
    # Bucket and design document lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def design_document() -> DesignDocument:
        """The one design document the suites upsert, holding the one view they query."""
        return DesignDocument(name=design_doc_name(), views={VIEW_NAME: View(map=VIEW_MAP_FUNCTION)})

    @staticmethod
    def _drop_stranded_bucket(cb_env: SyncTestEnvironment) -> None:
        """Remove a view bucket an earlier run left behind, if there is one.

        Nothing else will drop it.  A yield fixture only runs its teardown half if it got as far
        as yielding, so a setup that raises leaves the bucket in place, and a process that is
        killed outright runs neither half.  setup_views cleans up the failures it can see; what
        is left for this is the ones it cannot, a SIGINT or a crash between the create and the
        yield.  Without it the next run fails at create_bucket with BucketAlreadyExistsException,
        which says nothing at all about why the run before it stopped.
        """
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = view_bucket_name()
        try:
            bm.get_bucket(bucket_name)
        except BucketDoesNotExistException:
            return
        print(f'Bucket {bucket_name} was left behind by an earlier run; dropping it.')
        bm.drop_bucket(bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    @staticmethod
    def _create_view_bucket(cb_env: SyncTestEnvironment) -> None:
        ViewsEnvironment._drop_stranded_bucket(cb_env)
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = view_bucket_name()
        bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                              ram_quota_mb=ViewsEnvironment.VIEW_BUCKET_RAM_MB,
                                              flush_enabled=False,
                                              storage_backend=ViewsEnvironment.VIEW_BUCKET_BACKEND))
        cb_env.consistency.wait_until_bucket_healthy(bucket_name)
        bucket = cb_env.cluster.bucket(bucket_name)
        cb_env.set_view_bucket(bucket, bucket.default_collection())

    @staticmethod
    def _load_view_data(cb_env: SyncTestEnvironment) -> None:
        """Seed the view bucket with the corpus the map function partitions.

        The documents are the seeded vehicles, taken in order, so their ids run `<batch>::0`
        through `<batch>::49` and the four view keys hold 10, 10, 10 and 20 of them.  Order is
        what makes that true, which is why this iterates get_vehicles() rather than drawing.
        """
        coll = cb_env.view_collection
        for doc in cb_env.data_provider.get_vehicles()[:VIEW_DOCS]:
            TestEnvironment.try_n_times(3, 3, coll.upsert, f'{doc["id"]}', doc)

    @staticmethod
    def _wait_for_view(cb_env: SyncTestEnvironment) -> int:
        """Poll until the view returns the whole corpus; return the last count it gave.

        Queried with exactly the options the suite uses, so what this establishes is what the
        suite goes on to assert.  A count that is merely *behind* keeps polling; the caller
        treats anything short of the corpus at the end as a failure, because every expected
        count in the suite is derived from the corpus being complete.  A design document the
        server cannot find yet counts as behind too, for the reason the except clause gives.
        """
        count = 0
        for _ in range(ViewsEnvironment.VIEW_WAIT_ROUNDS):
            try:
                result = cb_env.view_bucket.view_query(design_doc_name(),
                                                       VIEW_NAME,
                                                       namespace=DesignDocumentNamespace.DEVELOPMENT)
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
            TestEnvironment.sleep(ViewsEnvironment.VIEW_WAIT_DELAY)
        return count

    @staticmethod
    def setup_views(cb_env: SyncTestEnvironment) -> None:
        """Create the bucket, seed it, build the design document and wait for the view.

        The feature check runs first and runs here rather than in the suite, so it takes the
        whole class with it on a mock or a server without bucket management, the same way
        setup_search and setup_query_indexes gate theirs.
        """
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.BucketManagement, ServerFeatures.ViewIndexManagement], cb_env.server_context)

        ViewsEnvironment._create_view_bucket(cb_env)
        # The bucket exists from here on, and every remaining step can fail.  Anything that does
        # has to take the bucket with it: this is a fixture setup, so a raise from here never
        # reaches the teardown half that would otherwise drop it.
        try:
            ViewsEnvironment._load_view_data(cb_env)

            vixm = cb_env.get_manager(TestEnvironmentViewIndexManager)
            vixm.use_bucket(cb_env.view_bucket)
            TestEnvironment.try_n_times(5, 3, vixm.upsert_design_document,
                                        ViewsEnvironment.design_document(),
                                        DesignDocumentNamespace.DEVELOPMENT)

            count = ViewsEnvironment._wait_for_view(cb_env)
            if count < VIEW_DOCS:
                raise CouchbaseTestEnvironmentException(
                    f'View {design_doc_name()}/{VIEW_NAME} returned {count} of {VIEW_DOCS} documents; '
                    'the suite asserts exact row counts and cannot run against a partial index')
        except Exception:
            # A failure to drop is printed rather than raised, so the exception that reaches the
            # run is the one worth debugging.
            try:
                ViewsEnvironment.teardown_views(cb_env)
            except Exception as drop_exc:
                print(f'Could not drop {view_bucket_name()} after a failed setup: {drop_exc}')
            raise

    @staticmethod
    def teardown_views(cb_env: SyncTestEnvironment) -> None:
        """Drop the bucket, which takes the design document and the corpus with it.

        The design document is deliberately *not* dropped first.  It lives on the bucket being
        dropped, so a separate drop is either redundant or, if it runs after, addressed at a
        bucket that no longer exists.
        """
        bucket_name = view_bucket_name()
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        TestEnvironment.try_n_times(5, 3, bm.drop_bucket, bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)
