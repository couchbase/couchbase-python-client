#  Copyright 2016-2022. Couchbase, Inc.
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

from datetime import timedelta

import pytest

from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  EventingFunctionAlreadyDeployedException,
                                  EventingFunctionCollectionNotFoundException,
                                  EventingFunctionNotBootstrappedException,
                                  EventingFunctionNotDeployedException,
                                  EventingFunctionNotFoundException,
                                  EventingFunctionNotUnDeployedException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionBucketAccess,
                                           EventingFunctionBucketBinding,
                                           EventingFunctionConstantBinding,
                                           EventingFunctionDcpBoundary,
                                           EventingFunctionDeploymentStatus,
                                           EventingFunctionKeyspace,
                                           EventingFunctionLanguageCompatibility,
                                           EventingFunctionProcessingStatus,
                                           EventingFunctionSettings,
                                           EventingFunctionsStatus,
                                           EventingFunctionState,
                                           EventingFunctionStatus,
                                           EventingFunctionUrlAuthBasic,
                                           EventingFunctionUrlAuthBearer,
                                           EventingFunctionUrlAuthDigest,
                                           EventingFunctionUrlBinding,
                                           EventingFunctionUrlNoAuth)
from couchbase.management.options import GetFunctionOptions, UpsertFunctionOptions
from tests.environments.eventing_mgmt_environment import EventingManagementTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class EventingManagementTestSuite:
    TEST_MANIFEST = [
        'test_constant_bindings',
        'test_deploy_function',
        'test_deploy_function_fail',
        'test_drop_function',
        'test_drop_function_fail',
        'test_functions_status',
        'test_get_all_functions',
        'test_get_function',
        'test_get_function_fail',
        'test_options_simple',
        'test_pause_function',
        'test_pause_function_fail',
        'test_resume_function',
        'test_resume_function_fail',
        'test_undeploy_function',
        'test_undeploy_function_fail',
        'test_upsert_function',
        'test_upsert_function_fail',
        'test_url_bindings',
        'test_with_scope_and_collection',
    ]

    @pytest.fixture()
    def create_and_drop_eventing_function(self, cb_env):
        cb_env.efm.upsert_function(cb_env.BASIC_FUNC)
        yield
        cb_env.efm.drop_function(cb_env.BASIC_FUNC.name)

    @pytest.fixture()
    def create_eventing_function(self, cb_env):
        cb_env.efm.upsert_function(cb_env.BASIC_FUNC)

    @pytest.fixture()
    def drop_eventing_function(self, cb_env):
        yield
        cb_env.efm.drop_function(cb_env.TEST_EVT_NAME)

    @pytest.fixture()
    def undeploy_and_drop_eventing_function(self, cb_env):
        yield
        cb_env.efm.undeploy_function(cb_env.TEST_EVT_NAME)
        cb_env.wait_until_status(15, 2, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.drop_function(cb_env.TEST_EVT_NAME)

    def test_constant_bindings(self, cb_env):
        # TODO:  look into why timeout occurs when providing > 1 constant
        # binding
        local_func = EventingFunction(
            'test-evt-const-func',
            cb_env.SIMPLE_EVT_CODE,
            cb_env.evt_version,
            metadata_keyspace=EventingFunctionKeyspace('default'),
            source_keyspace=EventingFunctionKeyspace(cb_env.EVT_SRC_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias='evtFunc',
                    name=EventingFunctionKeyspace(cb_env.EVT_SRC_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            constant_bindings=[
                EventingFunctionConstantBinding(
                    alias='testConstant', literal='1234'),
                EventingFunctionConstantBinding(
                    alias='testConstant1', literal="\"another test value\"")
            ]
        )

        cb_env.efm.upsert_function(local_func)
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, local_func.name)
        func = TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    def test_deploy_function(self, cb_env):
        # deploy function -- but first verify in undeployed state
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(20, 3, EventingFunctionState.Deployed, cb_env.BASIC_FUNC.name)
        func = TestEnvironment.try_n_times(5, 1, cb_env.efm.get_function, cb_env.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status has changed
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Deployed

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    def test_deploy_function_fail(self, cb_env):
        with pytest.raises(EventingFunctionNotFoundException):
            cb_env.efm.deploy_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(20, 3, EventingFunctionState.Deployed, cb_env.BASIC_FUNC.name)
        with pytest.raises(EventingFunctionAlreadyDeployedException):
            cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_eventing_function')
    def test_drop_function(self, cb_env):
        cb_env.efm.drop_function(cb_env.BASIC_FUNC.name)
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.efm.get_function,
                                                   cb_env.BASIC_FUNC.name,
                                                   EventingFunctionNotFoundException)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    def test_drop_function_fail(self, cb_env):
        with pytest.raises(
            (EventingFunctionNotDeployedException,
             EventingFunctionNotFoundException)
        ):
            cb_env.efm.drop_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        cb_env.wait_until_status(15, 2, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)
        # now, wait for it to be deployed
        cb_env.wait_until_status(20, 3, EventingFunctionState.Deployed, cb_env.BASIC_FUNC.name)

        with pytest.raises(EventingFunctionNotUnDeployedException):
            cb_env.efm.drop_function(cb_env.BASIC_FUNC.name)

    def test_functions_status(self, cb_env):
        new_funcs = [
            EventingFunction(
                'test-evt-func-1',
                cb_env.SIMPLE_EVT_CODE,
                cb_env.evt_version,
                metadata_keyspace=EventingFunctionKeyspace('default'),
                source_keyspace=EventingFunctionKeyspace('beer-sample'),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias='evtFunc1',
                        name=EventingFunctionKeyspace('beer-sample'),
                        access=EventingFunctionBucketAccess.ReadWrite
                    )
                ]
            ),
            EventingFunction(
                'test-evt-func-2',
                cb_env.SIMPLE_EVT_CODE,
                cb_env.evt_version,
                metadata_keyspace=EventingFunctionKeyspace('default'),
                source_keyspace=EventingFunctionKeyspace('beer-sample'),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_5_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias='evtFunc2',
                        name=EventingFunctionKeyspace('beer-sample'),
                        access=EventingFunctionBucketAccess.ReadOnly
                    )
                ]
            )
        ]
        for func in new_funcs:
            cb_env.efm.upsert_function(func)
            TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, func.name)

        funcs = cb_env.efm.functions_status()
        assert isinstance(funcs, EventingFunctionsStatus)
        for func in funcs.functions:
            assert isinstance(func, EventingFunctionStatus)

    def test_get_all_functions(self, cb_env):
        new_funcs = [
            EventingFunction(
                'test-evt-func-1',
                cb_env.SIMPLE_EVT_CODE,
                cb_env.evt_version,
                metadata_keyspace=EventingFunctionKeyspace('default'),
                source_keyspace=EventingFunctionKeyspace('beer-sample'),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias='evtFunc1',
                        name=EventingFunctionKeyspace('beer-sample'),
                        access=EventingFunctionBucketAccess.ReadWrite
                    )
                ]
            ),
            EventingFunction(
                'test-evt-func-2',
                cb_env.SIMPLE_EVT_CODE,
                cb_env.evt_version,
                metadata_keyspace=EventingFunctionKeyspace('default'),
                source_keyspace=EventingFunctionKeyspace('beer-sample'),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_5_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias='evtFunc2',
                        name=EventingFunctionKeyspace('beer-sample'),
                        access=EventingFunctionBucketAccess.ReadOnly
                    )
                ]
            )
        ]
        for func in new_funcs:
            cb_env.efm.upsert_function(func)
            TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, func.name)

        funcs = cb_env.efm.get_all_functions()
        for func in funcs:
            cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    def test_get_function(self, cb_env):
        func = TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, cb_env.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func)

    def test_get_function_fail(self, cb_env):
        with pytest.raises(EventingFunctionNotFoundException):
            cb_env.efm.get_function('not-a-function')

    def test_options_simple(self, cb_env):
        local_func = EventingFunction(
            'test-evt-func-1',
            cb_env.SIMPLE_EVT_CODE,
            cb_env.evt_version,
            metadata_keyspace=EventingFunctionKeyspace('default'),
            source_keyspace=EventingFunctionKeyspace('beer-sample'),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias='evtFunc1',
                    name=EventingFunctionKeyspace('beer-sample'),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        cb_env.efm.upsert_function(local_func, UpsertFunctionOptions(timeout=timedelta(seconds=20)))
        func = TestEnvironment.try_n_times(5,
                                           3,
                                           cb_env.efm.get_function,
                                           local_func.name,
                                           GetFunctionOptions(timeout=timedelta(seconds=15)))
        cb_env.validate_eventing_function(func, shallow=True)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    def test_pause_function(self, cb_env):
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(20, 3, EventingFunctionState.Deployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.pause_function(cb_env.BASIC_FUNC.name)
        func = TestEnvironment.try_n_times(5, 1, cb_env.efm.get_function, cb_env.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function processing status
        assert func.settings.processing_status == EventingFunctionProcessingStatus.Paused

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    def test_pause_function_fail(self, cb_env):
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)

        with pytest.raises(EventingFunctionNotFoundException):
            cb_env.efm.pause_function('not-a-function')

        with pytest.raises(EventingFunctionNotBootstrappedException):
            cb_env.efm.pause_function(cb_env.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    def test_resume_function(self, cb_env):
        # make sure function has been deployed
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(20, 3, EventingFunctionState.Deployed, cb_env.BASIC_FUNC.name)
        # pause function - verify status is paused
        cb_env.efm.pause_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(15, 2, EventingFunctionState.Paused, cb_env.BASIC_FUNC.name)
        # resume function
        cb_env.efm.resume_function(cb_env.BASIC_FUNC.name)
        func = TestEnvironment.try_n_times(5, 1, cb_env.efm.get_function, cb_env.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function processing status
        assert func.settings.processing_status == EventingFunctionProcessingStatus.Running

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    def test_resume_function_fail(self, cb_env):
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)

        with pytest.raises(EventingFunctionNotFoundException):
            cb_env.efm.pause_function('not-a-function')

        with pytest.raises(EventingFunctionNotBootstrappedException):
            cb_env.efm.pause_function(cb_env.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    def test_undeploy_function(self, cb_env):
        # deploy function -- but first verify in undeployed state
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        cb_env.efm.deploy_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(20, 3, EventingFunctionState.Deployed, cb_env.BASIC_FUNC.name)
        func = TestEnvironment.try_n_times(5, 1, cb_env.efm.get_function, cb_env.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Deployed
        # now, undeploy function
        cb_env.efm.undeploy_function(cb_env.BASIC_FUNC.name)
        cb_env.wait_until_status(15, 2, EventingFunctionState.Undeployed, cb_env.BASIC_FUNC.name)
        func = TestEnvironment.try_n_times(5, 1, cb_env.efm.get_function, cb_env.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status has changed
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Undeployed

    def test_undeploy_function_fail(self, cb_env):
        with pytest.raises(
            (EventingFunctionNotDeployedException,
             EventingFunctionNotFoundException)
        ):
            cb_env.efm.undeploy_function('not-a-function')

    def test_upsert_function(self, cb_env):
        local_func = EventingFunction(
            'test-evt-func-1',
            cb_env.SIMPLE_EVT_CODE,
            cb_env.evt_version,
            metadata_keyspace=EventingFunctionKeyspace('default'),
            source_keyspace=EventingFunctionKeyspace('beer-sample'),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias='evtFunc1',
                    name=EventingFunctionKeyspace('beer-sample'),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        cb_env.efm.upsert_function(local_func)
        func = TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func, shallow=True)

    def test_upsert_function_fail(self, cb_env):
        # bad appcode
        local_func = EventingFunction(
            'test-evt-func-1',
            'func OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}\n\n',
            cb_env.evt_version,
            metadata_keyspace=EventingFunctionKeyspace('default'),
            source_keyspace=EventingFunctionKeyspace('beer-sample'),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias='evtFunc1',
                    name=EventingFunctionKeyspace('beer-sample'),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )

        # @TODO:  couchbase++ seg faults on this...
        # with pytest.raises(EventingFunctionCompilationFailureException):
        #     cb_env.efm.upsert_function(local_func)

        local_func.code = cb_env.SIMPLE_EVT_CODE
        local_func.source_keyspace = EventingFunctionKeyspace(
            'beer-sample', "test-scope", "test-collection"
        )
        with pytest.raises(EventingFunctionCollectionNotFoundException):
            cb_env.efm.upsert_function(local_func)

    def test_url_bindings(self, cb_env):
        local_func = EventingFunction(
            "test-evt-url-func",
            cb_env.SIMPLE_EVT_CODE,
            cb_env.evt_version,
            metadata_keyspace=EventingFunctionKeyspace('default'),
            source_keyspace=EventingFunctionKeyspace(cb_env.EVT_SRC_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias='evtFunc',
                    name=EventingFunctionKeyspace(cb_env.EVT_SRC_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            url_bindings=[
                EventingFunctionUrlBinding(
                    hostname='http://localhost:5000',
                    alias='urlBinding1',
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlNoAuth()
                ),
                EventingFunctionUrlBinding(
                    hostname='http://localhost:5001',
                    alias='urlBinding2',
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthBasic('username', 'password')
                ),
                EventingFunctionUrlBinding(
                    hostname='http://localhost:5002',
                    alias='urlBinding3',
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthBearer(
                        'IThinkTheBearerTokenIsSupposedToGoHere'
                    )
                ),
                EventingFunctionUrlBinding(
                    hostname='http://localhost:5003',
                    alias='urlBinding4',
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthDigest('username', 'password')
                )
            ]
        )

        cb_env.efm.upsert_function(local_func)
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, local_func.name)
        func = TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

    def test_with_scope_and_collection(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('collections',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

        coll_name = 'test-collection-1'
        collection_spec = CollectionSpec(coll_name, cb_env.TEST_SCOPE)

        # Make sure the collection is there...
        TestEnvironment.try_n_times_till_exception(5,
                                                   3,
                                                   cb_env.cm.create_collection,
                                                   collection_spec,
                                                   expected_exceptions=(CollectionAlreadyExistsException,))

        local_func = EventingFunction(
            'test-evt-func-coll',
            cb_env.SIMPLE_EVT_CODE,
            cb_env.evt_version,
            metadata_keyspace=EventingFunctionKeyspace(
                cb_env.bucket.name,
                cb_env.TEST_SCOPE,
                cb_env.TEST_COLLECTION
            ),
            source_keyspace=EventingFunctionKeyspace(
                cb_env.bucket.name,
                cb_env.TEST_SCOPE,
                coll_name
            ),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias='evtFunc',
                    name=EventingFunctionKeyspace(cb_env.bucket.name),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        cb_env.efm.upsert_function(local_func)
        cb_env.wait_until_status(10, 1, EventingFunctionState.Undeployed, local_func.name)
        func = TestEnvironment.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

        cb_env.efm.drop_function(local_func.name)


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicEventingManagementTests(EventingManagementTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicEventingManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicEventingManagementTests) if valid_test_method(meth)]
        compare = set(EventingManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = EventingManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.setup()
        yield cb_env
        cb_env.teardown()
