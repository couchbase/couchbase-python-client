from datetime import datetime, timedelta

import pytest

import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (CouchbaseException,
                                  ParsingFailedException,
                                  QueryErrorContext)
from couchbase.n1ql import (QueryMetaData,
                            QueryMetrics,
                            QueryProfile,
                            QueryStatus,
                            QueryWarning)
from couchbase.options import (ClusterOptions,
                               QueryOptions,
                               UnsignedInt64,
                               UpsertOptions)
from couchbase.result import QueryResult

from ._test_utils import TestEnvironment


class QueryTests:

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
        cb_env = TestEnvironment(cluster,
                                 bucket,
                                 coll,
                                 couchbase_config,
                                 manage_buckets=True,
                                 manage_query_indexes=True)

        cb_env.ixm.create_primary_index(
            bucket.name,
            timeout=timedelta(seconds=60),
            ignore_if_exists=True)
        cb_env.load_data()
        # let it load a bit...
        for _ in range(5):
            row_count_good = self._check_row_count(cb_env, 5)
            if not row_count_good:
                print('Waiting for index to load, sleeping a bit...')
                cb_env.sleep(5)
        yield cb_env
        cb_env.purge_data()
        cb_env.ixm.drop_primary_index(bucket.name,
                                      ignore_if_not_exists=True)
        cluster.close()

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('preserve_expiry')

    def _check_row_count(self, cb_env,
                         min_count  # type: int
                         ) -> bool:

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE 'United%' LIMIT 5")
        count = 0
        for _ in result.rows():
            count += 1
        return count >= min_count

    def assert_rows(self,
                    result,  # type: QueryResult
                    expected_count):
        count = 0
        assert isinstance(result, QueryResult)
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def test_simple_query(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        self.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    def test_simple_query_prepared(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        self.assert_rows(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._request.params.get('adhoc', None) is False

    def test_simple_query_with_positional_params(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 2", 'United%')
        self.assert_rows(result, 2)

    def test_simple_query_with_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 2",
                                      country='United%')
        self.assert_rows(result, 2)

    def test_simple_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 2",
                                      QueryOptions(positional_parameters=['United%']))
        self.assert_rows(result, 2)

    def test_simple_query_with_named_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 1",
                                      QueryOptions(named_parameters={'country': 'United%'}))
        self.assert_rows(result, 1)

    # # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # # now, kwargs override the corresponding value in the options, only.
    def test_simple_query_without_options_with_kwargs_positional_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 1",
                                      positional_parameters=['United%'])
        self.assert_rows(result, 1)

    def test_simple_query_without_options_with_kwargs_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 1",
                                      named_parameters={'country': 'United%'})
        self.assert_rows(result, 1)

    def test_query_with_profile(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(profile=QueryProfile.TIMINGS))
        self.assert_rows(result, 1)
        assert result.metadata().profile() is not None

    def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(metrics=True))
        self.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        assert isinstance(metadata, QueryMetaData)
        metrics = metadata.metrics()
        assert isinstance(metrics, QueryMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert UnsignedInt64(expected) == count_result, fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert metrics.error_count() == UnsignedInt64(0)
        assert metadata.profile() is None

    def test_query_metadata(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        self.assert_rows(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    def test_mixed_positional_parameters(self, cb_env):
        # we assume that positional overrides one in the Options
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 1",
                                      QueryOptions(positional_parameters=['xgfflq']), 'United%')
        self.assert_rows(result, 1)

    def test_mixed_named_parameters(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 1",
                                      QueryOptions(named_parameters={'country': 'xxffqqlx'}), country='United%')
        self.assert_rows(result, 1)

    def test_query_raw_options(self, cb_env):
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT $1",
                                      QueryOptions(raw={"$country": "United%", "args": [2]}))
        self.assert_rows(result, 2)

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 1",
                                      QueryOptions(raw={"args": ['United%']}))
        self.assert_rows(result, 1)

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    def test_preserve_expiry(self, cb_env):
        key = "imakey"
        content = {"a": "aaa", "b": "bbb"}

        cb_env.collection.upsert(key, content, UpsertOptions(
            expiry=timedelta(days=1)))

        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb_env.collection.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb_env.cluster.query(f"UPDATE `{cb_env.bucket.name}` AS content USE KEYS '{key}' SET content.a = 'aaaa'",
                             QueryOptions(preserve_expiry=True)).execute()

        res = cb_env.try_n_times(10, 3, cb_env.collection.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2

    def test_query_error_context(self, cb_env):
        try:
            cb_env.cluster.query("SELECT * FROM no_such_bucket").execute()
        except CouchbaseException as ex:
            assert isinstance(ex.context, QueryErrorContext)
            assert ex.context.statement is not None
            assert ex.context.first_error_code is not None
            assert ex.context.first_error_message is not None
            assert ex.context.client_context_id is not None
            assert ex.context.response_body is not None
            # @TODO:  these are diff from 3.x -> 4.x
            # self.assertIsNotNone(ex.context.endpoint)
            # self.assertIsNotNone(ex.context.error_response_body)


class QueryCollectionTests:

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
        cb_env = TestEnvironment(cluster,
                                 bucket,
                                 coll,
                                 couchbase_config,
                                 manage_buckets=True,
                                 manage_collections=True,
                                 manage_query_indexes=True)
        cb_env.setup_named_collections()

        cb_env.cluster.query(f"CREATE PRIMARY INDEX ON `default`:{cb_env.fqdn}").execute()

        cb_env.load_data()
        yield cb_env
        cb_env.purge_data()
        cb_env.teardown_named_collections()
        cluster.close()

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('preserve_expiry')

    def assert_rows(self,
                    result,  # type: QueryResult
                    expected_count):
        count = 0
        assert isinstance(result, QueryResult)
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def test_query_fully_qualified(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")
        self.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    def test_cluster_query_context(self, cb_env):
        q_context = f'{cb_env.bucket.name}.{cb_env.scope.name}'
        # test with QueryOptions
        q_opts = QueryOptions(query_context=q_context, adhoc=True)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", q_opts)
        self.assert_rows(result, 2)

        # test with kwargs
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", query_context=q_context)
        self.assert_rows(result, 2)

    def test_bad_query_context(self, cb_env):
        # test w/ no context
        # @TODO:  what about KeyspaceNotFoundException?
        with pytest.raises(ParsingFailedException):
            cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2").execute()

        # test w/ bad scope
        # @TODO:  what about ScopeNotFoundException?
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ParsingFailedException):
            cb_env.cluster.query(
                f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", QueryOptions(query_context=q_context)).execute()

    def test_scope_query(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        self.assert_rows(result, 2)
        result = cb_env.scope.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")

    def test_bad_scope_query(self, cb_env):
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        # @TODO:  ScopeNotFoundException
        with pytest.raises(ParsingFailedException):
            cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                               QueryOptions(query_context=q_context)).execute()

        # @TODO:  KeyspaceNotFoundException
        q_context = f'`fake-bucket`.`{cb_env.scope.name}`'
        with pytest.raises(ParsingFailedException):
            cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                               query_context=q_context).execute()

    def test_scope_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE country LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=['United%']))
        self.assert_rows(result, 1)

    def test_scope_query_with_named_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE country LIKE $country LIMIT 1",
                                    QueryOptions(named_parameters={'country': 'United%'}))
        self.assert_rows(result, 1)

    def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.scope.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 1", QueryOptions(metrics=True))
        self.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        assert isinstance(metadata, QueryMetaData)
        metrics = metadata.metrics()
        assert isinstance(metrics, QueryMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert UnsignedInt64(expected) == count_result, fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert metrics.error_count() == UnsignedInt64(0)
        assert metadata.profile() is None

    def test_query_metadata(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        self.assert_rows(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)
