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

from __future__ import annotations

from datetime import timedelta

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.collections import UpdateCollectionSettings
from couchbase.management.options import CreateQueryIndexOptions, WatchQueryIndexOptions
from couchbase.management.search import SearchIndex
from couchbase.management.users import Role, User
from couchbase.management.views import DesignDocumentNamespace
from tests.environments.metrics import ManagementMetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType


class ManagementMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_analytics_mgmt',
        'test_bucket_mgmt',
        'test_bucket_mgmt_op_no_dispatch_failure',
        'test_collection_mgmt',
        'test_collection_mgmt_op_no_dispatch_failure',
        'test_eventing_function_mgmt',
        'test_query_index_mgmt',
        'test_query_index_mgmt_op_no_dispatch_failure',
        'test_search_index_mgmt',
        'test_user_mgmt',
        'test_view_index_mgmt',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: ManagementMetricsEnvironment):
        yield
        cb_env.http_meter_validator.reset()

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env):
        if cb_env.cluster._impl._cluster_info is None:
            cb_env.cluster.cluster_info()
        return len(cb_env.cluster._impl._cluster_info.nodes)

    @pytest.fixture(scope='function')
    def enable_bucket_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_tracing_bucket_mgmt()
        yield
        cb_env.disable_tracing_bucket_mgmt()

    @pytest.fixture(scope='function')
    def enable_collection_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_tracing_collection_mgmt()
        yield
        cb_env.disable_tracing_collection_mgmt()

    @pytest.fixture(scope='function')
    def enable_query_index_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_tracing_query_index_mgmt()
        yield
        cb_env.disable_tracing_query_index_mgmt()

    @pytest.fixture(scope='function')
    def enable_analytics_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_analytics_mgmt()
        yield
        cb_env.disable_analytics_mgmt()

    @pytest.fixture(scope='function')
    def enable_eventing_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_eventing_mgmt()
        yield
        cb_env.disable_eventing_mgmt()

    @pytest.fixture(scope='function')
    def enable_search_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_search_mgmt()
        yield
        cb_env.disable_search_mgmt()

    @pytest.fixture(scope='function')
    def enable_user_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_user_mgmt()
        yield
        cb_env.disable_user_mgmt()

    @pytest.fixture(scope='function')
    def enable_views_mgmt(self, cb_env: ManagementMetricsEnvironment):
        cb_env.enable_views_mgmt()
        yield
        cb_env.disable_views_mgmt()

    @pytest.mark.usefixtures('enable_bucket_mgmt')
    def test_bucket_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        bucket_name = cb_env.get_bucket_name()
        validator = cb_env.http_meter_validator

        validator.reset(op_name=OpName.BucketCreate, bucket_name=bucket_name)
        settings = CreateBucketSettings(name=bucket_name, ram_quota_mb=100, flush_enabled=False)
        cb_env.bm.create_bucket(settings)
        validator.validate_http_op()

        # lets make sure the bucket propagates to all nodes
        cb_env.consistency.wait_until_bucket_present(bucket_name)

        validator.reset(op_name=OpName.BucketGet, bucket_name=bucket_name)
        cb_env.bm.get_bucket(bucket_name)
        validator.validate_http_op()

        validator.reset(op_name=OpName.BucketGetAll)
        cb_env.bm.get_all_buckets()
        validator.validate_http_op()

        validator.reset(op_name=OpName.BucketCreate, validate_error=True, bucket_name=bucket_name)
        try:
            cb_env.bm.create_bucket(settings)
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.BucketFlush, validate_error=True, bucket_name=bucket_name)
        try:
            cb_env.bm.flush_bucket(bucket_name)
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.BucketDrop, bucket_name=bucket_name)
        cb_env.bm.drop_bucket(bucket_name)
        validator.validate_http_op()

        # lets make sure the bucket is removed from all nodes
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

        validator.reset(op_name=OpName.BucketDrop, validate_error=True, bucket_name=bucket_name)
        try:
            cb_env.bm.drop_bucket(bucket_name)
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.BucketGet, validate_error=True, bucket_name=bucket_name)
        try:
            cb_env.bm.get_bucket(bucket_name)
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_bucket_mgmt')
    def test_bucket_mgmt_op_no_dispatch_failure(self, cb_env: ManagementMetricsEnvironment) -> None:
        # so we don't need to setup/teardown; these all fail fast
        bm_ops = [
            (OpName.BucketCreate, True),
            (OpName.BucketDescribe, False),
            (OpName.BucketDrop, False),
            (OpName.BucketFlush, False),
            (OpName.BucketGet, False),
            (OpName.BucketUpdate, True)
        ]

        for op_name, has_settings in bm_ops:
            validator = cb_env.http_meter_validator
            validator.reset(op_name=op_name,
                            validate_error=True,
                            error_before_dispatch=True)

            if op_name == OpName.BucketDescribe:
                bm_op_name = 'bucket_describe'
            else:
                bm_op_name = op_name.value.replace('manager_buckets_', '')
            operation = getattr(cb_env.bm, bm_op_name)
            try:
                if has_settings:
                    settings = CreateBucketSettings(name=cb_env.get_bucket_name(),
                                                    ram_quota_mb=100,
                                                    flush_enabled=False)
                    settings.pop('name')  # will trigger a KeyError
                    operation(settings)
                else:
                    operation('')
            except (InvalidArgumentException, KeyError):
                pass

            validator.validate_http_op()

    @pytest.mark.usefixtures('enable_collection_mgmt')
    def test_collection_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator
        bucket_name = cb_env.test_bucket.name
        scope_name = cb_env.get_scope_name()
        collection_name = cb_env.get_collection_name()

        validator.reset(op_name=OpName.ScopeCreate, bucket_name=bucket_name, scope_name=scope_name)
        cb_env.test_bucket_cm.create_scope(scope_name)
        validator.validate_http_op()
        cb_env.consistency.wait_until_scope_present(bucket_name, scope_name)

        validator.reset(op_name=OpName.ScopeGetAll, bucket_name=bucket_name)
        cb_env.test_bucket_cm.get_all_scopes()
        validator.validate_http_op()

        validator.reset(op_name=OpName.ScopeCreate,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name)
        try:
            cb_env.test_bucket_cm.create_scope(scope_name)
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.CollectionCreate,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        cb_env.test_bucket_cm.create_collection(scope_name, collection_name)
        validator.validate_http_op()
        cb_env.consistency.wait_until_collection_present(cb_env.test_bucket.name, scope_name, collection_name)

        validator.reset(op_name=OpName.CollectionCreate,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        try:
            cb_env.test_bucket_cm.create_collection(scope_name, collection_name)
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.CollectionDrop,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        cb_env.test_bucket_cm.drop_collection(scope_name, collection_name)
        validator.validate_http_op()
        cb_env.consistency.wait_until_collection_dropped(cb_env.test_bucket.name, scope_name, collection_name)

        validator.reset(op_name=OpName.ScopeDrop, bucket_name=bucket_name, scope_name=scope_name)
        cb_env.test_bucket_cm.drop_scope(scope_name)
        validator.validate_http_op()
        cb_env.consistency.wait_until_scope_dropped(cb_env.test_bucket.name, scope_name)

        validator.reset(op_name=OpName.CollectionDrop,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        try:
            cb_env.test_bucket_cm.drop_collection(scope_name, collection_name)
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.ScopeDrop,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name)
        try:
            cb_env.test_bucket_cm.drop_scope(scope_name)
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_collection_mgmt')
    def test_collection_mgmt_op_no_dispatch_failure(self, cb_env: ManagementMetricsEnvironment) -> None:
        # so we don't need to setup/teardown; these all fail fast
        cm_ops = [(OpName.ScopeCreate, False),
                  (OpName.ScopeDrop, False),
                  (OpName.CollectionCreate, True),
                  (OpName.CollectionDrop, True),
                  (OpName.CollectionUpdate, True)]

        for op_name, has_collection_name in cm_ops:
            validator = cb_env.http_meter_validator
            # the bucket name is set internally and will be applied to the span
            validator.reset(op_name=op_name,
                            validate_error=True,
                            error_before_dispatch=True,
                            bucket_name=cb_env.test_bucket.name)

            cm_op_name = op_name.value.replace('manager_collections_', '')
            operation = getattr(cb_env.test_bucket_cm, cm_op_name)
            try:
                if op_name == OpName.CollectionUpdate:
                    settings = UpdateCollectionSettings()
                    operation('', '', settings)
                elif has_collection_name:
                    operation('', '')
                else:
                    operation('')
            except InvalidArgumentException:
                pass

            validator.validate_http_op()

    @pytest.mark.usefixtures('enable_query_index_mgmt')
    def test_query_index_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator

        validator.reset(op_name=OpName.QueryIndexCreate)
        # depending on how tests are ordered, we may already have an index here, so we ignore failure and
        # just validate the span
        try:
            cb_env.qixm.create_primary_index(cb_env.bucket.name, deferred=True)
        except Exception:
            validator.reset(op_name=OpName.QueryIndexCreate, validate_error=True, do_not_clear_meter=True)
        validator.validate_http_op()

        validator.reset(op_name=OpName.QueryIndexGetAll)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        validator.validate_http_op()
        assert len(ixs) == 1

        validator.reset(op_name=OpName.QueryIndexCreate, validate_error=True)
        try:
            cb_env.qixm.create_primary_index(cb_env.bucket.name, deferred=True)
        except Exception:
            pass
        validator.validate_http_op()

        # Create a bunch of other indexes
        for n in range(5):
            validator.reset(op_name=OpName.QueryIndexCreate)
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'ix{0}'.format(n),
                                     ['fld{0}'.format(n)],
                                     CreateQueryIndexOptions(deferred=True))
            validator.validate_http_op()

        validator.reset(op_name=OpName.QueryIndexGetAll)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 6
        validator.validate_http_op()

        ix_names = list(map(lambda i: i.name, ixs))

        validator.reset(op_name=OpName.QueryIndexCreate, validate_error=True)
        try:
            cb_env.qixm.create_index(cb_env.bucket.name, ix_names[0], ['fld0'], CreateQueryIndexOptions(deferred=True))
        except Exception:
            pass
        validator.validate_http_op()

        validator.reset(op_name=OpName.QueryIndexBuildDeferred)
        cb_env.qixm.build_deferred_indexes(cb_env.bucket.name)
        # TODO: this would be complicated to validate, it is deeply nested
        # manager_query_build_deferred_indexes (python)
        # └── manager_query_get_all_deferred_indexes (C++ core)
        #     ├── dispatch_to_server (C++ core)
        #     └── manager_query_build_indexes (C++ core)
        #         └── dispatch_to_server (C++ core)
        # validator.validate_http_op()

        validator.reset(op_name=OpName.QueryIndexWatchIndexes, nested_ops=[OpName.QueryIndexGetAll])
        cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                  ix_names,
                                  WatchQueryIndexOptions(timeout=timedelta(seconds=30)))  # Should be OK
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_query_index_mgmt')
    def test_query_index_mgmt_op_no_dispatch_failure(self, cb_env: ManagementMetricsEnvironment) -> None:
        # so we don't need to setup/teardown; these all fail fast
        qixm_ops = [('create_index', OpName.QueryIndexCreate),
                    ('create_primary_index', OpName.QueryIndexCreate),
                    ('drop_index', OpName.QueryIndexDrop),
                    ('drop_primary_index', OpName.QueryIndexDrop),
                    ('get_all_indexes', OpName.QueryIndexGetAll),
                    ('build_deferred_indexes', OpName.QueryIndexBuildDeferred),
                    ('watch_indexes', OpName.QueryIndexWatchIndexes)]

        for qixm_op_name, op_name in qixm_ops:
            validator = cb_env.http_meter_validator
            validator.reset(op_name=op_name,
                            validate_error=True,
                            error_before_dispatch=True)

            operation = getattr(cb_env.qixm, qixm_op_name)
            try:
                if op_name == OpName.QueryIndexCreate:
                    if qixm_op_name == 'create_index':
                        operation('', '', ['key1'])
                    else:
                        operation('')
                elif op_name == OpName.QueryIndexDrop:
                    if qixm_op_name == 'drop_index':
                        operation('', '')
                    else:
                        operation('')
                elif op_name == OpName.QueryIndexWatchIndexes:
                    operation('', [])
                else:
                    operation(None)
            except InvalidArgumentException:
                pass

            validator.validate_http_op()

    @pytest.mark.usefixtures('enable_analytics_mgmt')
    def test_analytics_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator
        validator.reset(op_name=OpName.AnalyticsDatasetGetAll)
        cb_env.aixm.get_all_datasets()
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_eventing_mgmt')
    def test_eventing_function_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator
        validator.reset(op_name=OpName.EventingGetAllFunctions)
        cb_env.efm.get_all_functions()
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_search_mgmt')
    def test_search_index_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator

        validator.reset(op_name=OpName.SearchIndexUpsert)
        idx = SearchIndex(name='tracing-test-index', source_name='default')
        cb_env.sixm.upsert_index(idx)
        validator.validate_http_op()

        validator.reset(op_name=OpName.SearchIndexGet)
        cb_env.sixm.get_index('tracing-test-index')
        validator.validate_http_op()

        validator.reset(op_name=OpName.SearchIndexGetAll)
        cb_env.sixm.get_all_indexes()
        validator.validate_http_op()

        validator.reset(op_name=OpName.SearchIndexDrop)
        cb_env.sixm.drop_index('tracing-test-index')
        validator.validate_http_op()

        validator.reset(op_name=OpName.SearchIndexDrop, validate_error=True)
        try:
            cb_env.sixm.drop_index('tracing-test-index')
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_user_mgmt')
    def test_user_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator

        username = 'tracing-test-user'
        validator.reset(op_name=OpName.UserUpsert)
        test_user = User(username=username,
                         password='password123!',
                         roles=[Role(name='admin')])
        cb_env.um.upsert_user(test_user)
        validator.validate_http_op()

        cb_env.consistency.wait_until_user_present(username)

        validator.reset(op_name=OpName.UserGet)
        cb_env.um.get_user(username)
        validator.validate_http_op()

        validator.reset(op_name=OpName.UserGetAll)
        cb_env.um.get_all_users()
        validator.validate_http_op()

        validator.reset(op_name=OpName.UserDrop)
        cb_env.um.drop_user(username)
        validator.validate_http_op()

        cb_env.consistency.wait_until_user_dropped(username)

        validator.reset(op_name=OpName.UserDrop, validate_error=True)
        try:
            cb_env.um.drop_user(username)
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('enable_views_mgmt')
    def test_view_index_mgmt(self, cb_env: ManagementMetricsEnvironment) -> None:
        validator = cb_env.http_meter_validator
        validator.reset(op_name=OpName.ViewIndexGetAll, bucket_name=cb_env.bucket.name)
        cb_env.vixm.get_all_design_documents(DesignDocumentNamespace.PRODUCTION)
        validator.validate_http_op()


class ClassicManagementMetricsTests(ManagementMetricsTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicManagementMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicManagementMetricsTests) if valid_test_method(meth)]
        test_list = set(ManagementMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic, MeterType.NoOp])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = ManagementMetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        cb_env.setup(num_docs=50)
        yield cb_env
        cb_env.teardown()
