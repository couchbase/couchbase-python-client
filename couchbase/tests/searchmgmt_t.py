import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import InvalidArgumentException, SearchIndexNotFoundException
from couchbase.management.search import SearchIndex
from couchbase.options import ClusterOptions

from ._test_utils import TestEnvironment


class SearchIndexManagementTests:

    IDX_NAME = 'test-fts-index'

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster(
            conn_string, opts)
        cluster.cluster_info()
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")

        coll = bucket.default_collection()
        cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config,
                                 manage_buckets=True, manage_search_indexes=True)

        yield cb_env
        cluster.close()

    @pytest.fixture(scope="class")
    def check_search_index_mgmt_supported(self, cb_env):
        cb_env.check_if_feature_supported('search_index_mgmt')

    @pytest.fixture(scope="class", name='test_idx')
    def get_test_index(self):
        return SearchIndex(name=self.IDX_NAME, source_name='default')

    @pytest.fixture()
    def create_test_index(self, cb_env, test_idx):
        cb_env.sixm.upsert_index(test_idx)

    @pytest.fixture()
    def drop_test_index(self, cb_env, test_idx):
        yield
        try:
            cb_env.sixm.drop_index(test_idx.name)
        except SearchIndexNotFoundException:
            pass
        except Exception as ex:
            raise ex

    @pytest.mark.usefixtures("drop_test_index")
    def test_upsert_index(self, cb_env, test_idx):
        res = cb_env.sixm.upsert_index(test_idx)
        assert res is None
        res = cb_env.try_n_times(10, 3, cb_env.sixm.get_index, test_idx.name)
        assert isinstance(res, SearchIndex)

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_drop_index(self, cb_env, test_idx):
        res = cb_env.try_n_times(3, 3, cb_env.sixm.get_index, test_idx.name)
        assert isinstance(res, SearchIndex)
        cb_env.sixm.drop_index(test_idx.name)
        with pytest.raises(SearchIndexNotFoundException):
            cb_env.sixm.drop_index(test_idx.name)

    def test_drop_index_fail(self, cb_env, test_idx):
        with pytest.raises(SearchIndexNotFoundException):
            cb_env.sixm.drop_index(test_idx.name)

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_get_index(self, cb_env, test_idx):
        res = cb_env.try_n_times(3, 3, cb_env.sixm.get_index, test_idx.name)
        assert isinstance(res, SearchIndex)
        assert res.name == test_idx.name

    def test_get_index_fail_no_index_name(self, cb_env):
        with pytest.raises(InvalidArgumentException):
            cb_env.sixm.get_index('')
        with pytest.raises(InvalidArgumentException):
            cb_env.sixm.get_index(None)

    def test_get_index_fail(self, cb_env):
        with pytest.raises(SearchIndexNotFoundException):
            cb_env.sixm.get_index('not-an-index')

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_get_all_indexes(self, cb_env, test_idx):
        # lets add one more
        new_idx = SearchIndex(name='new-search-idx', source_name='default')
        res = cb_env.sixm.upsert_index(new_idx)
        assert res is None
        res = cb_env.try_n_times(10, 3, cb_env.sixm.get_index, new_idx.name)
        assert isinstance(res, SearchIndex)

        indexes = cb_env.sixm.get_all_indexes()
        assert isinstance(indexes, list)
        assert len(indexes) >= 2
        assert next((idx for idx in indexes if idx.name == test_idx.name), None) is not None
        assert next((idx for idx in indexes if idx.name == new_idx.name), None) is not None

        cb_env.sixm.drop_index(new_idx.name)
        cb_env.try_n_times_till_exception(10,
                                          3,
                                          cb_env.sixm.get_index,
                                          new_idx.name,
                                          expected_exceptions=(SearchIndexNotFoundException,))

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_analyze_doc(self, cb_env, test_idx):
        # like getting the doc count, this can fail immediately after index
        # creation
        doc = {"field": "I got text in here"}
        analysis = cb_env.try_n_times(
            5, 2, cb_env.sixm.analyze_document, test_idx.name, doc)
        # analysis = cb_env.sixm.analyze_document(test_idx.name, doc)

        assert analysis.get('analysis', None) is not None
        assert isinstance(analysis.get('analysis'), (list, dict))
        assert analysis.get('status', None) == 'ok'

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_ingestion_control(self, cb_env, test_idx):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        res = cb_env.try_n_times(
            10,
            3,
            cb_env.sixm.pause_ingest,
            test_idx.name)
        assert res is None

        res = cb_env.try_n_times(
            10,
            3,
            cb_env.sixm.resume_ingest,
            test_idx.name)
        assert res is None

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_query_control(self, cb_env, test_idx):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        res = cb_env.try_n_times(
            10,
            3,
            cb_env.sixm.disallow_querying,
            test_idx.name)
        assert res is None

        res = cb_env.try_n_times(
            10,
            3,
            cb_env.sixm.allow_querying,
            test_idx.name)
        assert res is None

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_plan_freeze_control(self, cb_env, test_idx):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        res = cb_env.try_n_times(
            10,
            3,
            cb_env.sixm.freeze_plan,
            test_idx.name)
        assert res is None

        res = cb_env.try_n_times(
            10,
            3,
            cb_env.sixm.unfreeze_plan,
            test_idx.name)
        assert res is None

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_get_index_stats(self, cb_env, test_idx):
        # like getting the doc count, this can fail immediately after index
        # creation
        stats = cb_env.try_n_times(
            5, 2, cb_env.sixm.get_index_stats, test_idx.name)

        assert stats is not None
        assert isinstance(stats, dict)

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_get_index_doc_count(self, cb_env, test_idx):
        # like getting the doc count, this can fail immediately after index
        # creation
        doc_count = cb_env.try_n_times(
            5, 2, cb_env.sixm.get_indexed_documents_count, test_idx.name)

        assert doc_count is not None
        assert isinstance(doc_count, int)

    @pytest.mark.usefixtures("create_test_index")
    @pytest.mark.usefixtures("drop_test_index")
    def test_get_all_index_stats(self, cb_env, test_idx):
        # like getting the doc count, this can fail immediately after index
        # creation
        stats = cb_env.try_n_times(
            5, 2, cb_env.sixm.get_all_index_stats)

        assert stats is not None
        assert isinstance(stats, dict)
