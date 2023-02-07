#  Copyright 2016-2023. Couchbase, Inc.
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

from dataclasses import fields
from typing import (List,
                    Optional,
                    get_type_hints)

from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionBucketAccess,
                                           EventingFunctionBucketBinding,
                                           EventingFunctionConstantBinding,
                                           EventingFunctionDcpBoundary,
                                           EventingFunctionDeploymentStatus,
                                           EventingFunctionKeyspace,
                                           EventingFunctionLanguageCompatibility,
                                           EventingFunctionLogLevel,
                                           EventingFunctionProcessingStatus,
                                           EventingFunctionSettings,
                                           EventingFunctionState,
                                           EventingFunctionUrlAuthBasic,
                                           EventingFunctionUrlAuthBearer,
                                           EventingFunctionUrlAuthDigest,
                                           EventingFunctionUrlBinding,
                                           EventingFunctionUrlNoAuth)
from couchbase.management.logic.eventing_logic import QueryScanConsistency
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class EventingFunctionManagementTestStatusException(Exception):
    """Raised when waiting for a certained status does not happen within a specified timeframe"""


class EventingManagementTestEnvironment(TestEnvironment):
    EVT_SRC_BUCKET_NAME = 'beer-sample'
    EVT_META_BUCKET_NAME = 'default'
    TEST_EVT_NAME = 'test-evt-func'
    SIMPLE_EVT_CODE = ('function OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}'
                       '\n\nfunction OnDelete(meta, options) {\n    log("Doc deleted/expired", meta.id);\n}')

    BASIC_FUNC = EventingFunction(
        TEST_EVT_NAME,
        SIMPLE_EVT_CODE,
        'evt-7.0.0-5302-ee',
        metadata_keyspace=EventingFunctionKeyspace(EVT_META_BUCKET_NAME),
        source_keyspace=EventingFunctionKeyspace(EVT_SRC_BUCKET_NAME),
        settings=EventingFunctionSettings.new_settings(
            dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
            language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
        ),
        bucket_bindings=[
            EventingFunctionBucketBinding(
                alias='evtFunc',
                name=EventingFunctionKeyspace(EVT_SRC_BUCKET_NAME),
                access=EventingFunctionBucketAccess.ReadWrite
            )
        ]
    )

    @property
    def evt_version(self):
        return self._version

    def setup(self):
        self._version = 'evt-{}'.format(
            self.server_version_full.replace('enterprise', 'ee').replace('community', 'ce')
        )
        self.enable_eventing_mgmt()
        if EnvironmentFeatures.is_feature_supported('collections', self.server_version_short):
            self.enable_collection_mgmt()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)

    def teardown(self):
        self.disable_eventing_mgmt()
        if EnvironmentFeatures.is_feature_supported('collections', self.server_version_short):
            TestEnvironment.try_n_times_till_exception(5,
                                                       3,
                                                       self.teardown_named_collections,
                                                       raise_if_no_exception=False)
            self.disable_collection_mgmt()

    def validate_bucket_bindings(self, bindings  # type: List[EventingFunctionBucketBinding]
                                 ) -> None:
        binding_fields = fields(EventingFunctionBucketBinding)
        type_hints = get_type_hints(EventingFunctionBucketBinding)
        for binding in bindings:
            assert isinstance(binding, EventingFunctionBucketBinding)  # nosec
            for field in binding_fields:
                value = getattr(binding, field.name)
                if value is not None:
                    if field.name == 'name':
                        assert isinstance(value, EventingFunctionKeyspace)  # nosec
                    elif field.name == 'access':
                        assert isinstance(value, EventingFunctionBucketAccess)  # nosec
                    else:
                        assert isinstance(value, type_hints[field.name])  # nosec

    def validate_constant_bindings(self, bindings  # type: List[EventingFunctionConstantBinding]
                                   ) -> None:
        binding_fields = fields(EventingFunctionConstantBinding)
        type_hints = get_type_hints(EventingFunctionConstantBinding)
        for binding in bindings:
            assert isinstance(binding, EventingFunctionConstantBinding)  # nosec
            for field in binding_fields:
                value = getattr(binding, field.name)
                if value is not None:
                    assert isinstance(value, type_hints[field.name])  # nosec

    def validate_eventing_function(self, func,  # type: EventingFunction
                                   shallow=False  # type: Optional[bool]
                                   ) -> None:
        assert isinstance(func, EventingFunction)  # nosec
        if shallow is False:
            func_fields = fields(EventingFunction)
            type_hints = get_type_hints(EventingFunction)
            for field in func_fields:
                value = getattr(func, field.name)
                if value is not None:
                    if field.name == 'bucket_bindings':
                        self.validate_bucket_bindings(value)
                    elif field.name == 'url_bindings':
                        self.validate_url_bindings(value)
                    elif field.name == 'constant_bindings':
                        self.validate_constant_bindings(value)
                    elif field.name == 'settings':
                        self.validate_settings(value)
                    else:
                        assert isinstance(value, type_hints[field.name])  # nosec

    def validate_settings(self, settings  # type: EventingFunctionSettings  # noqa: C901
                          ) -> None:  # noqa: C901
        assert isinstance(settings, EventingFunctionSettings)  # nosec
        settings_fields = fields(EventingFunctionSettings)
        type_hints = get_type_hints(EventingFunctionSettings)
        for field in settings_fields:
            value = getattr(settings, field.name)
            if value is not None:
                if field.name == 'dcp_stream_boundary':
                    assert isinstance(value, EventingFunctionDcpBoundary)  # nosec
                elif field.name == 'deployment_status':
                    assert isinstance(value, EventingFunctionDeploymentStatus)  # nosec
                elif field.name == 'processing_status':
                    assert isinstance(value, EventingFunctionProcessingStatus)  # nosec
                elif field.name == 'language_compatibility':
                    assert isinstance(value, EventingFunctionLanguageCompatibility)  # nosec
                elif field.name == 'log_level':
                    assert isinstance(value, EventingFunctionLogLevel)  # nosec
                elif field.name == 'query_consistency':
                    assert isinstance(value, QueryScanConsistency)  # nosec
                elif field.name == 'handler_headers':
                    assert isinstance(value, list)  # nosec
                elif field.name == 'handler_footers':
                    assert isinstance(value, list)  # nosec
                else:
                    assert isinstance(value, type_hints[field.name])  # nosec

    def validate_url_bindings(self, bindings  # type: List[EventingFunctionUrlBinding]
                              ) -> None:
        binding_fields = fields(EventingFunctionUrlBinding)
        type_hints = get_type_hints(EventingFunctionUrlBinding)
        for binding in bindings:
            assert isinstance(binding, EventingFunctionUrlBinding)  # nosec
            for field in binding_fields:
                value = getattr(binding, field.name)
                if value is not None:
                    if field.name == 'auth':
                        if isinstance(value, EventingFunctionUrlAuthBasic):
                            assert isinstance(value.username, str)  # nosec
                            assert value.password is None  # nosec
                        elif isinstance(value, EventingFunctionUrlAuthDigest):
                            assert isinstance(value.username, str)  # nosec
                            assert value.password is None  # nosec
                        elif isinstance(value, EventingFunctionUrlAuthBearer):
                            assert value.key is None  # nosec
                        else:
                            assert isinstance(value, EventingFunctionUrlNoAuth)  # nosec
                    else:
                        assert isinstance(value, type_hints[field.name])  # nosec

    def wait_until_status(self,
                          num_times,  # type: int
                          seconds_between,  # type: int
                          state,  # type: EventingFunctionState
                          name  # type: str
                          ) -> None:

        func_status = None
        for _ in range(num_times):
            func_status = self.efm._get_status(name)
            if func_status is None or func_status.state != state:
                TestEnvironment.sleep(seconds_between)
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

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> EventingManagementTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
