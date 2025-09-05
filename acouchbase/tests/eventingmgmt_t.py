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

import asyncio
from datetime import timedelta
from typing import List

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  EventingFunctionAlreadyDeployedException,
                                  EventingFunctionCollectionNotFoundException,
                                  EventingFunctionNotBootstrappedException,
                                  EventingFunctionNotDeployedException,
                                  EventingFunctionNotFoundException,
                                  EventingFunctionNotUnDeployedException,
                                  InternalServerFailureException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionBucketAccess,
                                           EventingFunctionBucketBinding,
                                           EventingFunctionConstantBinding,
                                           EventingFunctionDcpBoundary,
                                           EventingFunctionDeploymentStatus,
                                           EventingFunctionKeyspace,
                                           EventingFunctionLanguageCompatibility,
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

from ._test_utils import (CollectionType,
                          EventingFunctionManagementTestStatusException,
                          TestEnvironment)


@pytest.mark.flaky(reruns=5)
class EventingManagementTests:

    EVT_SRC_BUCKET_NAME = "beer-sample"
    EVT_META_BUCKET_NAME = "default"
    TEST_EVT_NAME = 'test-evt-func'
    SIMPLE_EVT_CODE = ('function OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}'
                       '\n\nfunction OnDelete(meta, options) {\n    log("Doc deleted/expired", meta.id);\n}')
    EVT_VERSION = None
    BASIC_FUNC = EventingFunction(
        TEST_EVT_NAME,
        SIMPLE_EVT_CODE,
        "evt-7.0.0-5302-ee",
        metadata_keyspace=EventingFunctionKeyspace(EVT_META_BUCKET_NAME),
        source_keyspace=EventingFunctionKeyspace(EVT_SRC_BUCKET_NAME),
        settings=EventingFunctionSettings.new_settings(
            dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
            language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
        ),
        bucket_bindings=[
            EventingFunctionBucketBinding(
                alias="evtFunc",
                name=EventingFunctionKeyspace(EVT_SRC_BUCKET_NAME),
                access=EventingFunctionBucketAccess.ReadWrite
            )
        ]
    )

    @property
    def function_names(self) -> List[str]:
        return self._function_names

    @function_names.setter
    def function_names(self, names: List[str]) -> None:
        self._function_names = names

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        self._function_names = []
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_eventing_functions=True)

        if cb_env.is_feature_supported('collections'):
            cb_env._cm = cb_env.bucket.collections()
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if cb_env.is_feature_supported('collections'):
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest.fixture(scope="class", name='evt_version')
    def get_eventing_function_version(self, cb_env):
        version = "evt-{}".format(
            cb_env.server_version.replace("enterprise", "ee").replace("community", "ce")
        )
        return version

    @pytest_asyncio.fixture()
    async def create_eventing_function(self, cb_env):
        await cb_env.efm.upsert_function(self.BASIC_FUNC)

    @pytest_asyncio.fixture()
    async def drop_eventing_function(self, cb_env):
        yield
        await cb_env.efm.drop_function(self.TEST_EVT_NAME)

    @pytest_asyncio.fixture()
    async def drop_eventing_functions(self, cb_env):
        yield
        for fn_name in self.function_names:
            await cb_env.efm.drop_function(fn_name)
            await cb_env.try_n_times_till_exception(10,
                                                    1,
                                                    cb_env.efm.get_function,
                                                    fn_name,
                                                    EventingFunctionNotFoundException)
        self.function_names = []

    @pytest_asyncio.fixture()
    async def undeploy_and_drop_eventing_function(self, cb_env):
        yield
        await cb_env.efm.undeploy_function(self.TEST_EVT_NAME)
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Undeployed, self.TEST_EVT_NAME
        )
        await cb_env.efm.drop_function(self.TEST_EVT_NAME)

    @pytest_asyncio.fixture()
    async def create_and_drop_eventing_function(self, cb_env):
        await cb_env.efm.upsert_function(self.BASIC_FUNC)
        yield
        await cb_env.efm.drop_function(self.BASIC_FUNC.name)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.efm.get_function,
                                                self.BASIC_FUNC.name,
                                                EventingFunctionNotFoundException)

    async def _wait_until_status(self,
                                 cb_env,  # type: TestEnvironment
                                 num_times,  # type: int
                                 seconds_between,  # type: int
                                 state,  # type: EventingFunctionState
                                 name  # type: str
                                 ) -> None:

        func_status = None
        for _ in range(num_times):
            func_status = await cb_env.efm._get_status(name)
            if func_status is None or func_status.state != state:
                await asyncio.sleep(seconds_between)
            else:
                break

        if func_status is None:
            raise EventingFunctionManagementTestStatusException(
                "Unable to obtain function status for {}".format(name)
            )
        if func_status.state != state:
            raise EventingFunctionManagementTestStatusException(
                "Function {} status is {} which does not match desired status of {}.".format(
                    name, func_status.state.value, state.value
                )
            )

    @pytest.mark.usefixtures("drop_eventing_functions")
    @pytest.mark.asyncio
    async def test_upsert_function(self, cb_env, evt_version):
        self.function_names = [self.TEST_EVT_NAME]
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            evt_version,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace("beer-sample"),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc1",
                    name=EventingFunctionKeyspace("beer-sample"),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        await cb_env.efm.upsert_function(local_func)
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func, shallow=True)

    @pytest.mark.usefixtures("drop_eventing_functions")
    @pytest.mark.asyncio
    async def test_upsert_function_fail(self, cb_env, evt_version):
        self.function_names = [self.TEST_EVT_NAME]
        # bad appcode
        local_func = EventingFunction(
            self.TEST_EVT_NAME,
            'func OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}\n\n',
            evt_version,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace("beer-sample"),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc1",
                    name=EventingFunctionKeyspace("beer-sample"),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )

        local_func.code = self.SIMPLE_EVT_CODE
        local_func.source_keyspace = EventingFunctionKeyspace(
            "beer-sample", "test-scope", "test-collection"
        )
        if cb_env.server_version_short >= 8.0:
            with pytest.raises((InternalServerFailureException, EventingFunctionCollectionNotFoundException)):
                await cb_env.efm.upsert_function(local_func)
                await cb_env.efm.deploy_function(local_func.name)
        else:
            with pytest.raises(EventingFunctionCollectionNotFoundException):
                await cb_env.efm.upsert_function(local_func)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.asyncio
    async def test_drop_function(self, cb_env):
        await cb_env.efm.drop_function(self.BASIC_FUNC.name)
        await cb_env.try_n_times_till_exception(
            10,
            1,
            cb_env.efm.get_function,
            self.BASIC_FUNC.name,
            EventingFunctionNotFoundException
        )

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_drop_function_fail(self, cb_env):
        with pytest.raises(
            (EventingFunctionNotDeployedException,
             EventingFunctionNotFoundException)
        ):
            await cb_env.efm.drop_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        # now, wait for it to be deployed
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )

        with pytest.raises(EventingFunctionNotUnDeployedException):
            await cb_env.efm.drop_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures("create_and_drop_eventing_function")
    @pytest.mark.asyncio
    async def test_get_function(self, cb_env):
        func = await cb_env.try_n_times(
            5, 3, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.asyncio
    async def test_get_function_fail(self, cb_env):
        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.get_function("not-a-function")

    @pytest.mark.usefixtures("drop_eventing_functions")
    @pytest.mark.asyncio
    async def test_get_all_functions(self, cb_env):
        self.function_names = [
            'test-evt-func-1',
            'test-evt-func-2',
        ]
        new_funcs = [
            EventingFunction(
                self.function_names[0],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc1",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadWrite
                    )
                ]
            ),
            EventingFunction(
                self.function_names[1],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_5_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc2",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadOnly
                    )
                ]
            )
        ]
        for func in new_funcs:
            await cb_env.efm.upsert_function(func)
            await cb_env.try_n_times(5, 3, cb_env.efm.get_function, func.name)

        funcs = await cb_env.efm.get_all_functions()
        for func in funcs:
            cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_deploy_function(self, cb_env):
        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status has changed
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Deployed

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_deploy_function_fail(self, cb_env):
        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.deploy_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        if cb_env.server_version_short >= 8.0:
            # no longer a failure in 8.0+
            await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
            await self._wait_until_status(
                cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
            )
        else:
            with pytest.raises(EventingFunctionAlreadyDeployedException):
                await cb_env.efm.deploy_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_undeploy_function(self, cb_env):
        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Deployed
        # now, undeploy function
        await cb_env.efm.undeploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status has changed
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Undeployed

    @pytest.mark.asyncio
    async def test_undeploy_function_fail(self, cb_env):
        with pytest.raises(
            (EventingFunctionNotDeployedException,
             EventingFunctionNotFoundException)
        ):
            await cb_env.efm.undeploy_function("not-a-function")

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_pause_function_fail(self, cb_env):
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )

        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.pause_function("not-a-function")

        if cb_env.server_version_short >= 8.0:
            with pytest.raises(InternalServerFailureException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)
        else:
            with pytest.raises(EventingFunctionNotBootstrappedException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_pause_and_resume_function(self, cb_env):
        # make sure function has been deployed
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        # pause function - verify status is paused
        await cb_env.efm.pause_function(self.BASIC_FUNC.name)
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Paused, self.BASIC_FUNC.name
        )
        # resume function
        await cb_env.efm.resume_function(self.BASIC_FUNC.name)
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function state
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_resume_function_fail(self, cb_env):
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )

        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.pause_function("not-a-function")

        if cb_env.server_version_short >= 8.0:
            with pytest.raises(InternalServerFailureException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)
        else:
            with pytest.raises(EventingFunctionNotBootstrappedException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_constant_bindings(self, cb_env):
        # TODO:  look into why timeout occurs when providing > 1 constant binding
        self.function_names = ['test-evt-const-func']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            constant_bindings=[
                EventingFunctionConstantBinding(
                    alias="testConstant", literal="1234"),
                EventingFunctionConstantBinding(
                    alias="testConstant1", literal="\"another test value\"")
            ]
        )

        await cb_env.efm.upsert_function(local_func)
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_url_bindings(self, cb_env):
        self.function_names = ['test-evt-url-func']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            url_bindings=[
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5000",
                    alias="urlBinding1",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlNoAuth()
                ),
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5001",
                    alias="urlBinding2",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthBasic("username", "password")
                ),
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5002",
                    alias="urlBinding3",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthBearer(
                        "IThinkTheBearerTokenIsSupposedToGoHere"
                    )
                ),
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5003",
                    alias="urlBinding4",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthDigest("username", "password")
                )
            ]
        )

        await cb_env.efm.upsert_function(local_func)
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_functions_status(self, cb_env):
        self.function_names = ['test-evt-func-1', 'test-evt-func-2']
        new_funcs = [
            EventingFunction(
                self.function_names[0],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc1",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadWrite
                    )
                ]
            ),
            EventingFunction(
                self.function_names[1],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_5_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc2",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadOnly
                    )
                ]
            )
        ]
        for func in new_funcs:
            await cb_env.efm.upsert_function(func)
            await cb_env.try_n_times(5, 3, cb_env.efm.get_function, func.name)

        funcs = await cb_env.efm.functions_status()
        assert isinstance(funcs, EventingFunctionsStatus)
        for func in funcs.functions:
            assert isinstance(func, EventingFunctionStatus)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_options_simple(self, cb_env):
        self.function_names = ['test-evt-func-1']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace("beer-sample"),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc1",
                    name=EventingFunctionKeyspace("beer-sample"),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        await cb_env.efm.upsert_function(
            local_func, UpsertFunctionOptions(timeout=timedelta(seconds=20))
        )
        func = await cb_env.try_n_times(
            5,
            3,
            cb_env.efm.get_function,
            local_func.name,
            GetFunctionOptions(timeout=timedelta(seconds=15))
        )
        cb_env.validate_eventing_function(func, shallow=True)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_with_scope_and_collection(self, cb_env, evt_version):
        if not cb_env.is_feature_supported('collections'):
            pytest.skip('Server does not support scopes/collections.')

        coll_name = 'test-collection-1'
        collection_spec = CollectionSpec(coll_name, cb_env.TEST_SCOPE)
        # Make sure the collection is there...
        await cb_env.try_n_times_till_exception(5,
                                                3,
                                                cb_env.cm.create_collection,
                                                collection_spec,
                                                expected_exceptions=(CollectionAlreadyExistsException,))

        self.function_names = ['test-evt-func-coll']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            evt_version,
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
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(cb_env.bucket.name),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        await cb_env.efm.upsert_function(local_func)
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)


@pytest.mark.flaky(reruns=3)
class ScopeEventingManagementTests:

    EVT_SRC_BUCKET_NAME = "beer-sample"
    EVT_META_BUCKET_NAME = "default"
    TEST_EVT_NAME = 'test-evt-func'
    SIMPLE_EVT_CODE = ('function OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}'
                       '\n\nfunction OnDelete(meta, options) {\n    log("Doc deleted/expired", meta.id);\n}')
    EVT_VERSION = None
    BASIC_FUNC = EventingFunction(
        TEST_EVT_NAME,
        SIMPLE_EVT_CODE,
        "evt-7.0.0-5302-ee",
        metadata_keyspace=EventingFunctionKeyspace(EVT_META_BUCKET_NAME),
        source_keyspace=EventingFunctionKeyspace(EVT_SRC_BUCKET_NAME),
        settings=EventingFunctionSettings.new_settings(
            dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
            language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
        ),
        bucket_bindings=[
            EventingFunctionBucketBinding(
                alias="evtFunc",
                name=EventingFunctionKeyspace(EVT_SRC_BUCKET_NAME),
                access=EventingFunctionBucketAccess.ReadWrite
            )
        ]
    )

    @property
    def function_names(self) -> List[str]:
        return self._function_names

    @function_names.setter
    def function_names(self, names: List[str]) -> None:
        self._function_names = names

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        self._function_names = []
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       CollectionType.NAMED)

        await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)
        cb_env.enable_scope_eventing_mgmt().enable_eventing_mgmt()
        yield cb_env
        cb_env.disable_eventing_mgmt().disable_scope_eventing_mgmt()
        await cb_env.try_n_times_till_exception(5, 3,
                                                cb_env.teardown_named_collections,
                                                raise_if_no_exception=False)

    @pytest.fixture(scope="class", name='evt_version')
    def get_eventing_function_version(self, cb_env):
        version = "evt-{}".format(
            cb_env.server_version.replace("enterprise", "ee").replace("community", "ce")
        )
        return version

    @pytest_asyncio.fixture()
    async def create_eventing_function(self, cb_env):
        await cb_env.efm.upsert_function(self.BASIC_FUNC)

    @pytest_asyncio.fixture()
    async def drop_eventing_functions(self, cb_env):
        yield
        for fn_name in self.function_names:
            try:
                await cb_env.efm.drop_function(fn_name)
            except EventingFunctionNotFoundException:
                continue
            await cb_env.try_n_times_till_exception(10,
                                                    1,
                                                    cb_env.efm.get_function,
                                                    fn_name,
                                                    EventingFunctionNotFoundException)
        self.function_names = []

    @pytest_asyncio.fixture()
    async def undeploy_and_drop_eventing_function(self, cb_env):
        yield
        await cb_env.efm.undeploy_function(self.TEST_EVT_NAME)
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Undeployed, self.TEST_EVT_NAME
        )
        try:
            await cb_env.efm.drop_function(self.TEST_EVT_NAME)
        except EventingFunctionNotFoundException:
            pass

    @pytest_asyncio.fixture()
    async def create_and_drop_eventing_function(self, cb_env):
        await cb_env.efm.upsert_function(self.BASIC_FUNC)
        yield
        try:
            await cb_env.efm.drop_function(self.BASIC_FUNC.name)
        except EventingFunctionNotFoundException:
            pass

    async def _wait_until_status(self,
                                 cb_env,  # type: TestEnvironment
                                 num_times,  # type: int
                                 seconds_between,  # type: int
                                 state,  # type: EventingFunctionState
                                 name  # type: str
                                 ) -> None:

        func_status = None
        for _ in range(num_times):
            func_status = await cb_env.efm._get_status(name)
            if func_status is None or func_status.state != state:
                await asyncio.sleep(seconds_between)
            else:
                break

        if func_status is None:
            raise EventingFunctionManagementTestStatusException(
                "Unable to obtain function status for {}".format(name)
            )
        if func_status.state != state:
            raise EventingFunctionManagementTestStatusException(
                "Function {} status is {} which does not match desired status of {}.".format(
                    name, func_status.state.value, state.value
                )
            )

    @pytest.mark.usefixtures("drop_eventing_functions")
    @pytest.mark.asyncio
    async def test_upsert_function(self, cb_env, evt_version):
        self.function_names = [self.TEST_EVT_NAME]
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            evt_version,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace("beer-sample"),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc1",
                    name=EventingFunctionKeyspace("beer-sample"),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        await cb_env.efm.upsert_function(local_func)
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func, shallow=True)

    @pytest.mark.usefixtures("drop_eventing_functions")
    @pytest.mark.asyncio
    async def test_upsert_function_fail(self, cb_env, evt_version):
        self.function_names = [self.TEST_EVT_NAME]
        # bad appcode
        local_func = EventingFunction(
            self.TEST_EVT_NAME,
            'func OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}\n\n',
            evt_version,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace("beer-sample"),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc1",
                    name=EventingFunctionKeyspace("beer-sample"),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )

        local_func.code = self.SIMPLE_EVT_CODE
        local_func.source_keyspace = EventingFunctionKeyspace(
            "beer-sample", "test-scope", "test-collection"
        )
        if cb_env.server_version_short >= 8.0:
            with pytest.raises((InternalServerFailureException, EventingFunctionCollectionNotFoundException)):
                await cb_env.efm.upsert_function(local_func)
                await cb_env.efm.deploy_function(local_func.name)
        else:
            with pytest.raises(EventingFunctionCollectionNotFoundException):
                await cb_env.efm.upsert_function(local_func)

    @pytest.mark.usefixtures("create_eventing_function")
    @pytest.mark.asyncio
    async def test_drop_function(self, cb_env):
        await cb_env.efm.drop_function(self.BASIC_FUNC.name)
        await cb_env.try_n_times_till_exception(
            10,
            1,
            cb_env.efm.get_function,
            self.BASIC_FUNC.name,
            EventingFunctionNotFoundException
        )

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_drop_function_fail(self, cb_env):
        with pytest.raises(
            (EventingFunctionNotDeployedException,
             EventingFunctionNotFoundException)
        ):
            await cb_env.efm.drop_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        # now, wait for it to be deployed
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )

        with pytest.raises(EventingFunctionNotUnDeployedException):
            await cb_env.efm.drop_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures("create_and_drop_eventing_function")
    @pytest.mark.asyncio
    async def test_get_function(self, cb_env):
        func = await cb_env.try_n_times(
            5, 3, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.asyncio
    async def test_get_function_fail(self, cb_env):
        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.get_function("not-a-function")

    @pytest.mark.asyncio
    async def test_get_all_functions(self, cb_env):
        self.function_names = [
            'test-evt-func-1',
            'test-evt-func-2',
        ]
        new_funcs = [
            EventingFunction(
                self.function_names[0],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc1",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadWrite
                    )
                ]
            ),
            EventingFunction(
                self.function_names[1],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_5_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc2",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadOnly
                    )
                ]
            )
        ]
        for func in new_funcs:
            await cb_env.efm.upsert_function(func)
            await cb_env.try_n_times(5, 3, cb_env.efm.get_function, func.name)

        funcs = await cb_env.efm.get_all_functions()
        for func in funcs:
            cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_deploy_function(self, cb_env):
        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status has changed
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Deployed

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_deploy_function_fail(self, cb_env):
        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.deploy_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        if cb_env.server_version_short >= 8.0:
            # no longer a failure in 8.0+
            await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
            await self._wait_until_status(
                cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
            )
        else:
            with pytest.raises(EventingFunctionAlreadyDeployedException):
                await cb_env.efm.deploy_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_undeploy_function(self, cb_env):
        # deploy function -- but first verify in undeployed state
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Deployed
        # now, undeploy function
        await cb_env.efm.undeploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function deployement status has changed
        assert func.settings.deployment_status == EventingFunctionDeploymentStatus.Undeployed

    @pytest.mark.asyncio
    async def test_undeploy_function_fail(self, cb_env):
        with pytest.raises(
            (EventingFunctionNotDeployedException,
             EventingFunctionNotFoundException)
        ):
            await cb_env.efm.undeploy_function("not-a-function")

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_pause_function_fail(self, cb_env):
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )

        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.pause_function("not-a-function")

        if cb_env.server_version_short >= 8.0:
            with pytest.raises(InternalServerFailureException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)
        else:
            with pytest.raises(EventingFunctionNotBootstrappedException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures('create_eventing_function')
    @pytest.mark.usefixtures('undeploy_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_pause_and_resume_function(self, cb_env):
        # make sure function has been deployed
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        await cb_env.efm.deploy_function(self.BASIC_FUNC.name)
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        # pause function - verify status is paused
        await cb_env.efm.pause_function(self.BASIC_FUNC.name)
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        await self._wait_until_status(
            cb_env, 15, 2, EventingFunctionState.Paused, self.BASIC_FUNC.name
        )
        # resume function
        await cb_env.efm.resume_function(self.BASIC_FUNC.name)
        func = await cb_env.try_n_times(
            5, 1, cb_env.efm.get_function, self.BASIC_FUNC.name)
        cb_env.validate_eventing_function(func, shallow=True)
        # verify function state
        await self._wait_until_status(
            cb_env, 20, 3, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )

    @pytest.mark.usefixtures('create_and_drop_eventing_function')
    @pytest.mark.asyncio
    async def test_resume_function_fail(self, cb_env):
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )

        with pytest.raises(EventingFunctionNotFoundException):
            await cb_env.efm.pause_function("not-a-function")

        if cb_env.server_version_short >= 8.0:
            with pytest.raises(InternalServerFailureException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)
        else:
            with pytest.raises(EventingFunctionNotBootstrappedException):
                await cb_env.efm.pause_function(self.BASIC_FUNC.name)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_constant_bindings(self, cb_env):
        # TODO:  look into why timeout occurs when providing > 1 constant binding
        self.function_names = ['test-evt-const-func']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            constant_bindings=[
                EventingFunctionConstantBinding(
                    alias="testConstant", literal="1234"),
                EventingFunctionConstantBinding(
                    alias="testConstant1", literal="\"another test value\"")
            ]
        )

        await cb_env.efm.upsert_function(local_func)
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_url_bindings(self, cb_env):
        self.function_names = ['test-evt-url-func']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_SRC_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            url_bindings=[
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5000",
                    alias="urlBinding1",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlNoAuth()
                ),
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5001",
                    alias="urlBinding2",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthBasic("username", "password")
                ),
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5002",
                    alias="urlBinding3",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthBearer(
                        "IThinkTheBearerTokenIsSupposedToGoHere"
                    )
                ),
                EventingFunctionUrlBinding(
                    hostname="http://localhost:5003",
                    alias="urlBinding4",
                    allow_cookies=True,
                    validate_ssl_certificate=False,
                    auth=EventingFunctionUrlAuthDigest("username", "password")
                )
            ]
        )

        await cb_env.efm.upsert_function(local_func)
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_functions_status(self, cb_env):
        self.function_names = ['test-evt-func-1', 'test-evt-func-2']
        new_funcs = [
            EventingFunction(
                self.function_names[0],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc1",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadWrite
                    )
                ]
            ),
            EventingFunction(
                self.function_names[1],
                self.SIMPLE_EVT_CODE,
                self.EVT_VERSION,
                metadata_keyspace=EventingFunctionKeyspace("default"),
                source_keyspace=EventingFunctionKeyspace("beer-sample"),
                settings=EventingFunctionSettings.new_settings(
                    language_compatibility=EventingFunctionLanguageCompatibility.Version_6_5_0
                ),
                bucket_bindings=[
                    EventingFunctionBucketBinding(
                        alias="evtFunc2",
                        name=EventingFunctionKeyspace("beer-sample"),
                        access=EventingFunctionBucketAccess.ReadOnly
                    )
                ]
            )
        ]
        for func in new_funcs:
            await cb_env.efm.upsert_function(func)
            await cb_env.try_n_times(5, 3, cb_env.efm.get_function, func.name)

        funcs = await cb_env.efm.functions_status()
        assert isinstance(funcs, EventingFunctionsStatus)
        for func in funcs.functions:
            assert isinstance(func, EventingFunctionStatus)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_options_simple(self, cb_env):
        self.function_names = ['test-evt-func-1']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace("beer-sample"),
            settings=EventingFunctionSettings.new_settings(
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_0_0
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc1",
                    name=EventingFunctionKeyspace("beer-sample"),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        await cb_env.efm.upsert_function(
            local_func, UpsertFunctionOptions(timeout=timedelta(seconds=20))
        )
        func = await cb_env.try_n_times(
            5,
            3,
            cb_env.efm.get_function,
            local_func.name,
            GetFunctionOptions(timeout=timedelta(seconds=15))
        )
        cb_env.validate_eventing_function(func, shallow=True)

    @pytest.mark.usefixtures('drop_eventing_functions')
    @pytest.mark.asyncio
    async def test_with_scope_and_collection(self, cb_env, evt_version):
        if not cb_env.is_feature_supported('collections'):
            pytest.skip('Server does not support scopes/collections.')

        coll_name = 'test-collection-1'
        collection_spec = CollectionSpec(coll_name, cb_env.TEST_SCOPE)
        await cb_env.try_n_times_till_exception(5,
                                                3,
                                                cb_env.cm.create_collection,
                                                collection_spec,
                                                expected_exceptions=(CollectionAlreadyExistsException,))

        self.function_names = ['test-evt-func-coll']
        local_func = EventingFunction(
            self.function_names[0],
            self.SIMPLE_EVT_CODE,
            evt_version,
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
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(cb_env.bucket.name),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        await cb_env.efm.upsert_function(local_func)
        await self._wait_until_status(
            cb_env, 10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = await cb_env.try_n_times(5, 3, cb_env.efm.get_function, local_func.name)
        cb_env.validate_eventing_function(func)
