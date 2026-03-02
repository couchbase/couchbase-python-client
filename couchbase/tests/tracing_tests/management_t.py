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
from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                          CreateBucketOptions,
                                          CreateCollectionOptions,
                                          CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          CreateScopeOptions,
                                          DropBucketOptions,
                                          DropCollectionOptions,
                                          DropScopeOptions,
                                          FlushBucketOptions,
                                          GetAllBucketOptions,
                                          GetAllQueryIndexOptions,
                                          GetAllScopesOptions,
                                          GetBucketOptions,
                                          WatchQueryIndexOptions)
from tests.environments.tracing import ManagementTracingEnvironment
from tests.environments.tracing.base_tracing_environment import TracingType


class ManagementTracingTestsSuite:

    TEST_MANIFEST = [
        # 'test_analytics_mgmt',
        'test_bucket_mgmt',
        'test_bucket_mgmt_op_no_dispatch_failure',
        'test_bucket_mgmt_with_parent',
        'test_collection_mgmt',
        'test_collection_mgmt_op_no_dispatch_failure',
        'test_collection_mgmt_with_parent',
        # 'test_eventing_function_mgmt',
        'test_query_index_mgmt',
        'test_query_index_mgmt_op_no_dispatch_failure',
        'test_query_index_mgmt_with_parent',
        # 'test_search_index_mgmt',
        # 'test_user_mgmt',
        # 'test_view_index_mgmt',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: ManagementTracingEnvironment):
        yield
        cb_env.http_span_validator.reset()

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env):
        if cb_env.cluster._impl._cluster_info is None:
            cb_env.cluster.cluster_info()
        return len(cb_env.cluster._impl._cluster_info.nodes)

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip('Test only for clusters with more than a single node.')

    @pytest.fixture(scope='function')
    def enable_bucket_mgmt(self, cb_env: ManagementTracingEnvironment):
        cb_env.enable_tracing_bucket_mgmt()
        yield
        cb_env.disable_tracing_bucket_mgmt()

    @pytest.fixture(scope='function')
    def enable_collection_mgmt(self, cb_env: ManagementTracingEnvironment):
        cb_env.enable_tracing_collection_mgmt()
        yield
        cb_env.disable_tracing_collection_mgmt()

    @pytest.fixture(scope='function')
    def enable_query_index_mgmt(self, cb_env: ManagementTracingEnvironment):
        cb_env.enable_tracing_query_index_mgmt()
        yield
        cb_env.disable_tracing_query_index_mgmt()

    @pytest.mark.usefixtures('enable_bucket_mgmt')
    def test_bucket_mgmt(self, cb_env: ManagementTracingEnvironment) -> None:
        bucket_name = cb_env.get_bucket_name()
        validator = cb_env.http_span_validator

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
    def test_bucket_mgmt_op_no_dispatch_failure(self, cb_env: ManagementTracingEnvironment) -> None:
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
            validator = cb_env.http_span_validator
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

    @pytest.mark.usefixtures('enable_bucket_mgmt')
    def test_bucket_mgmt_with_parent(self, cb_env: ManagementTracingEnvironment) -> None:
        bucket_name = cb_env.get_bucket_name()
        validator = cb_env.http_span_validator

        op_name = OpName.BucketCreate
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, bucket_name=bucket_name)
        settings = CreateBucketSettings(name=bucket_name, ram_quota_mb=100, flush_enabled=False)
        cb_env.bm.create_bucket(settings, CreateBucketOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)

        # lets make sure the bucket propagates to all nodes
        cb_env.consistency.wait_until_bucket_present(bucket_name)

        op_name = OpName.BucketGet
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, bucket_name=bucket_name)
        cb_env.bm.get_bucket(bucket_name, GetBucketOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)

        op_name = OpName.BucketGetAll
        cb_env.tracer.clear_spans()
        validator.reset()  # clear out the bucket_name which we don't use for get_all_buckets
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span)
        cb_env.bm.get_all_buckets(GetAllBucketOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)

        op_name = OpName.BucketCreate
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name)
        try:
            cb_env.bm.create_bucket(settings, CreateBucketOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.BucketFlush
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name)
        try:
            cb_env.bm.flush_bucket(bucket_name, FlushBucketOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.BucketDrop
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        # sense we cannot clear the spans, we need to make sure we are not validating the error
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=False,
                        bucket_name=bucket_name)
        cb_env.bm.drop_bucket(bucket_name, DropBucketOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)

        # lets make sure the bucket is removed from all nodes
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

        op_name = OpName.BucketDrop
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name)
        try:
            cb_env.bm.drop_bucket(bucket_name, DropBucketOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.BucketGet
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name)
        try:
            cb_env.bm.get_bucket(bucket_name, GetBucketOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

    @pytest.mark.usefixtures('enable_collection_mgmt')
    def test_collection_mgmt(self, cb_env: ManagementTracingEnvironment) -> None:
        validator = cb_env.http_span_validator
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
    def test_collection_mgmt_op_no_dispatch_failure(self, cb_env: ManagementTracingEnvironment) -> None:
        # so we don't need to setup/teardown; these all fail fast
        cm_ops = [(OpName.ScopeCreate, False),
                  (OpName.ScopeDrop, False),
                  (OpName.CollectionCreate, True),
                  (OpName.CollectionDrop, True),
                  (OpName.CollectionUpdate, True)]

        for op_name, has_collection_name in cm_ops:
            validator = cb_env.http_span_validator
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

    @pytest.mark.usefixtures('enable_collection_mgmt')
    def test_collection_mgmt_with_parent(self, cb_env: ManagementTracingEnvironment) -> None:
        validator = cb_env.http_span_validator
        bucket_name = cb_env.test_bucket.name
        scope_name = cb_env.get_scope_name()
        collection_name = cb_env.get_collection_name()

        op_name = OpName.ScopeCreate
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        bucket_name=bucket_name,
                        scope_name=scope_name)
        cb_env.test_bucket_cm.create_scope(scope_name, CreateScopeOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)
        cb_env.consistency.wait_until_scope_present(bucket_name, scope_name)

        op_name = OpName.ScopeGetAll
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, bucket_name=bucket_name)
        cb_env.test_bucket_cm.get_all_scopes(GetAllScopesOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)

        op_name = OpName.ScopeCreate
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name)
        try:
            cb_env.test_bucket_cm.create_scope(scope_name, CreateScopeOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.CollectionCreate
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        cb_env.test_bucket_cm.create_collection(scope_name, collection_name,
                                                CreateCollectionOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)
        cb_env.consistency.wait_until_collection_present(cb_env.test_bucket.name, scope_name, collection_name)

        op_name = OpName.CollectionCreate
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        try:
            cb_env.test_bucket_cm.create_collection(scope_name,
                                                    collection_name,
                                                    CreateCollectionOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.CollectionDrop
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        cb_env.test_bucket_cm.drop_collection(scope_name, collection_name,
                                              DropCollectionOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)
        cb_env.consistency.wait_until_collection_dropped(cb_env.test_bucket.name, scope_name, collection_name)

        op_name = OpName.ScopeDrop
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        bucket_name=bucket_name,
                        scope_name=scope_name)
        cb_env.test_bucket_cm.drop_scope(scope_name, DropScopeOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)
        cb_env.consistency.wait_until_scope_dropped(cb_env.test_bucket.name, scope_name)

        op_name = OpName.CollectionDrop
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name,
                        collection_name=collection_name)
        try:
            cb_env.test_bucket_cm.drop_collection(scope_name, collection_name,
                                                  DropCollectionOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.ScopeDrop
        validator.reset()  # full reset to clear spans, bucket, scope & collection names
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        validate_error=True,
                        bucket_name=bucket_name,
                        scope_name=scope_name)
        try:
            cb_env.test_bucket_cm.drop_scope(scope_name, DropScopeOptions(parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

    @pytest.mark.usefixtures('enable_query_index_mgmt')
    def test_query_index_mgmt(self, cb_env: ManagementTracingEnvironment) -> None:
        validator = cb_env.http_span_validator

        validator.reset(op_name=OpName.QueryIndexCreate)
        # depending on how tests are ordered, we may already have an index here, so we ignore failure and
        # just validate the span
        try:
            cb_env.qixm.create_primary_index(cb_env.bucket.name, deferred=True)
        except Exception:
            pass
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
    def test_query_index_mgmt_op_no_dispatch_failure(self, cb_env: ManagementTracingEnvironment) -> None:
        # so we don't need to setup/teardown; these all fail fast
        qixm_ops = [('create_index', OpName.QueryIndexCreate),
                    ('create_primary_index', OpName.QueryIndexCreate),
                    ('drop_index', OpName.QueryIndexDrop),
                    ('drop_primary_index', OpName.QueryIndexDrop),
                    ('get_all_indexes', OpName.QueryIndexGetAll),
                    ('build_deferred_indexes', OpName.QueryIndexBuildDeferred),
                    ('watch_indexes', OpName.QueryIndexWatchIndexes)]

        for qixm_op_name, op_name in qixm_ops:
            validator = cb_env.http_span_validator
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

    @pytest.mark.usefixtures('enable_query_index_mgmt')
    def test_query_index_mgmt_with_parent(self, cb_env: ManagementTracingEnvironment) -> None:
        validator = cb_env.http_span_validator

        op_name = OpName.QueryIndexCreate
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span)
        # depending on how tests are ordered, we may already have an index here, so we ignore failure and
        # just validate the span
        try:
            cb_env.qixm.create_primary_index(cb_env.bucket.name, deferred=True, parent_span=parent_span)
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.QueryIndexGetAll
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name, GetAllQueryIndexOptions(parent_span=parent_span))
        validator.validate_http_op(end_parent=True)
        assert len(ixs) == 1

        op_name = OpName.QueryIndexCreate
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, validate_error=True)
        try:
            cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                             CreatePrimaryQueryIndexOptions(parent_span=parent_span),
                                             deferred=True)
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        # Create a bunch of other indexes
        op_name = OpName.QueryIndexCreate
        for n in range(5):
            cb_env.tracer.clear_spans()
            parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
            validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, validate_error=False)
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'ix{0}'.format(n),
                                     ['fld{0}'.format(n)],
                                     CreateQueryIndexOptions(deferred=True, parent_span=parent_span))
            validator.validate_http_op(end_parent=True)

        op_name = OpName.QueryIndexGetAll
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name, GetAllQueryIndexOptions(parent_span=parent_span))
        assert len(ixs) == 6
        validator.validate_http_op(end_parent=True)

        ix_names = list(map(lambda i: i.name, ixs))

        op_name = OpName.QueryIndexCreate
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, validate_error=True)
        try:
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     ix_names[0],
                                     ['fld0'],
                                     CreateQueryIndexOptions(deferred=True, parent_span=parent_span))
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        op_name = OpName.QueryIndexBuildDeferred
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span, validate_error=False)
        cb_env.qixm.build_deferred_indexes(cb_env.bucket.name, BuildDeferredQueryIndexOptions(parent_span=parent_span))
        # TODO: this would be complicated to validate, it is deeply nested
        # manager_query_build_deferred_indexes (python)
        # └── manager_query_get_all_deferred_indexes (C++ core)
        #     ├── dispatch_to_server (C++ core)
        #     └── manager_query_build_indexes (C++ core)
        #         └── dispatch_to_server (C++ core)
        # validator.validate_http_op()

        op_name = OpName.QueryIndexWatchIndexes
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name,
                        do_not_clear_spans=True,
                        parent_span=parent_span,
                        nested_ops=[OpName.QueryIndexGetAll],)
        cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                  ix_names,
                                  WatchQueryIndexOptions(parent_span=parent_span,
                                                         timeout=timedelta(seconds=30)))  # Should be OK
        validator.validate_http_op(end_parent=True)


class ClassicManagementTracingTests(ManagementTracingTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicManagementTracingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicManagementTracingTests) if valid_test_method(meth)]
        test_list = set(ManagementTracingTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.ThresholdLogging,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.Modern
                                                          ])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = ManagementTracingEnvironment.from_environment(cb_base_env, tracing_type=request.param)
        cb_env.setup(num_docs=50)
        yield cb_env
        cb_env.teardown()
        # TODO(PYCBC-1746): Update once legacy tracing logic is removed.
        #                   See PYCBC-1748 for details on issue, this seems to mainly impact the error scenarios.
        if request.param == TracingType.Legacy:
            cb_env.cluster.close()
