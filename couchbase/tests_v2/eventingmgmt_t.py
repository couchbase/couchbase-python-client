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

# Two suites, because the SDK offers two managers over the same nine operations:
# cluster.eventing_functions() addresses any function in the cluster by bare name, and
# scope.eventing_functions() only the functions created through its own scope.  The bodies
# are duplicated rather than factored together because which manager a test asks cb_env for
# is the difference the suites exist to exercise, and it has to be named at the call site.
# The suites create the bucket they stream rather than naming a sample bucket, which would
# confine them to clusters where somebody had loaded one by hand.

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta
from typing import Any

import pytest

from couchbase.exceptions import (EventingFunctionAlreadyDeployedException,
                                  EventingFunctionCollectionNotFoundException,
                                  EventingFunctionCompilationFailureException,
                                  EventingFunctionNotBootstrappedException,
                                  EventingFunctionNotDeployedException,
                                  EventingFunctionNotFoundException,
                                  EventingFunctionNotUnDeployedException,
                                  InternalServerFailureException)
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
                                           EventingFunctionsStatus,
                                           EventingFunctionState,
                                           EventingFunctionStatus,
                                           EventingFunctionUrlAuthBasic,
                                           EventingFunctionUrlAuthBearer,
                                           EventingFunctionUrlAuthDigest,
                                           EventingFunctionUrlBinding,
                                           EventingFunctionUrlNoAuth)
from couchbase.management.options import GetFunctionOptions, UpsertFunctionOptions
from couchbase.n1ql import QueryScanConsistency
from tests_v2.assert_helpers import cb_assert_eq, cb_assert_isinstance
from tests_v2.environments.eventing_environment import EventingEnvironment
from tests_v2.environments.managers import (TestEnvironmentBucketManager,
                                            TestEnvironmentEventingFunctionManager,
                                            TestEnvironmentScopeEventingFunctionManager)
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SuiteRequirements,
                                                    SyncTestEnvironment,
                                                    TestEnvironment,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.eventing_defs import (BUCKET_BINDING_ALIAS,
                                    CONSTANT_BINDING_ALIASES,
                                    CONSTANT_BINDING_LITERALS,
                                    FUNCTION_SCOPE_NAME,
                                    INVALID_EVT_CODE,
                                    META_COLLECTION_NAME,
                                    MISSING_FUNCTION_NAME,
                                    SIMPLE_EVT_CODE,
                                    SOURCE_COLLECTION_NAME,
                                    URL_BINDING_ALIASES,
                                    URL_BINDING_BEARER_KEY,
                                    URL_BINDING_HOSTNAMES,
                                    URL_BINDING_PASSWORD,
                                    URL_BINDING_USERNAME,
                                    eventing_bucket_name,
                                    function_name,
                                    function_version,
                                    scope_eventing_bucket_name)
from tests_v2.manifest import validate_test_manifest
from tests_v2.test_features import ServerFeatures


class EventingFunctionManagementSuiteBase(SuiteRequirements):
    # Declares no test methods of its own, so pytest does not collect it as a suite.

    COLLECTION_STRATEGY = CollectionStrategy.DEFAULT

    # An eventing function streams its source keyspace, and every test here asserts on the
    # definition rather than on anything the handler did, so OnUpdate never fires.
    NUM_DOCS = 0

    # What deploy, pause, resume and undeploy raise for a name the manager cannot see.
    # Before 8.0 the server reports a missing function; from 8.0.1 an internal failure for
    # the same request (MB-68692), with 8.0.0 between the two depending on the operation.
    MISSING_FUNCTION_ERRORS = (EventingFunctionNotFoundException, InternalServerFailureException)

    @staticmethod
    def _source_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        raise NotImplementedError

    @staticmethod
    def _metadata_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        raise NotImplementedError

    @staticmethod
    def _binding_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        raise NotImplementedError

    @staticmethod
    def _function_version(cb_env: SyncTestEnvironment) -> str:
        full = cb_env.server_version_full
        assert full is not None
        return function_version(full)

    def _function(self,
                  cb_env: SyncTestEnvironment,
                  name: str,
                  *,
                  source_keyspace: EventingFunctionKeyspace | None = None) -> EventingFunction:
        # The keyspaces are the only place the two suites' functions differ.  A test that
        # varies one of the other parts assigns to the field after the fact.
        if source_keyspace is None:
            source_keyspace = self._source_keyspace(cb_env)
        return EventingFunction(
            name,
            SIMPLE_EVT_CODE,
            self._function_version(cb_env),
            metadata_keyspace=self._metadata_keyspace(cb_env),
            source_keyspace=source_keyspace,
            # FromNow, so a deploy has no backlog even if something later seeds the source
            # keyspace.  new_settings takes `**kwargs: Dict[str, Any]`, which says each
            # keyword's value is a dict rather than that the collected kwargs are one, so the
            # checker rejects every call it has; the suppressions come off with PYCBC-1882.
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,  # type: ignore[arg-type]  # PYCBC-1882
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2),  # type: ignore[arg-type]  # PYCBC-1882 # noqa: E501
            bucket_bindings=[
                EventingFunctionBucketBinding(alias=BUCKET_BINDING_ALIAS,
                                              name=self._binding_keyspace(cb_env),
                                              access=EventingFunctionBucketAccess.ReadWrite)])

    @staticmethod
    def _constant_bindings() -> list[EventingFunctionConstantBinding]:
        return [EventingFunctionConstantBinding(alias=alias, literal=literal)
                for alias, literal in zip(CONSTANT_BINDING_ALIASES, CONSTANT_BINDING_LITERALS)]

    @staticmethod
    def _url_bindings() -> list[EventingFunctionUrlBinding]:
        auths = (EventingFunctionUrlNoAuth(),
                 EventingFunctionUrlAuthBasic(URL_BINDING_USERNAME, URL_BINDING_PASSWORD),
                 EventingFunctionUrlAuthBearer(URL_BINDING_BEARER_KEY),
                 EventingFunctionUrlAuthDigest(URL_BINDING_USERNAME, URL_BINDING_PASSWORD))
        return [EventingFunctionUrlBinding(hostname=hostname,
                                           alias=alias,
                                           allow_cookies=True,
                                           validate_ssl_certificate=False,
                                           auth=auth)
                for hostname, alias, auth in zip(URL_BINDING_HOSTNAMES, URL_BINDING_ALIASES, auths)]

    # --- Assertions ---

    @staticmethod
    def _redeploy_succeeds(cb_env: SyncTestEnvironment) -> bool:
        # 8.0.0 alone.  Every other version refuses it with
        # EventingFunctionAlreadyDeployedException, and 8.0.1 went back to refusing it
        # (MB-68692), so this is a one-patch-release window rather than a version floor.
        return cb_env.server_version_short == 8.0 and (cb_env.server_version_patch or 0) < 1

    @staticmethod
    def _upsert_checks_source_keyspace(cb_env: SyncTestEnvironment) -> bool:
        # From 8.0 the server stores a function whose source collection does not exist and
        # refuses it only at deploy, reporting either the missing collection or an internal
        # failure.
        return (cb_env.server_version_short or 0) < 8.0

    @staticmethod
    def _keyspace_parts(keyspace: EventingFunctionKeyspace) -> dict[str, Any]:
        # @FINDING(PYCBC-1912): a keyspace read back from the server carries the entire
        # server dict in `bucket` and leaves `scope` and `collection` None, because
        # `EventingFunction.from_server` passes that dict to
        # `EventingFunctionKeyspace.from_server` positionally, where the bucket name belongs.
        # Bindings deserialize correctly because the next block splats the same shape with
        # `**name`.  Reading through this rather than pinning it keeps every assertion below
        # stating what the caller asked for, and none of them changes when the fix lands.
        if isinstance(keyspace.bucket, dict):
            return dict(keyspace.bucket)
        return {'bucket': keyspace.bucket, 'scope': keyspace.scope, 'collection': keyspace.collection}

    @classmethod
    def _assert_keyspace(cls,
                         found: EventingFunctionKeyspace,
                         sent: EventingFunctionKeyspace,
                         *,
                         what: str) -> None:
        # The scope and collection are compared only when the sent keyspace named them: the
        # server completes a bucket-only keyspace to the default scope and collection, so
        # requiring equality would fail on a keyspace that got exactly what it asked for.
        cb_assert_isinstance(found, EventingFunctionKeyspace, what=what)
        parts = cls._keyspace_parts(found)
        cb_assert_eq(parts['bucket'], sent.bucket, what=f'{what} bucket')
        if sent.scope is not None:
            cb_assert_eq(parts.get('scope'), sent.scope, what=f'{what} scope')
        if sent.collection is not None:
            cb_assert_eq(parts.get('collection'), sent.collection, what=f'{what} collection')

    @classmethod
    def _assert_round_trip(cls, found: EventingFunction, sent: EventingFunction) -> None:
        # Values, not types: a read that checks only types cannot notice a manager that
        # returned somebody else's function.
        cb_assert_isinstance(found, EventingFunction, what='eventing function')
        cb_assert_eq(found.name, sent.name, what='function name')
        cb_assert_eq(found.code, sent.code, what='function code')
        cls._assert_keyspace(found.source_keyspace, sent.source_keyspace, what='source keyspace')
        cls._assert_keyspace(found.metadata_keyspace, sent.metadata_keyspace, what='metadata keyspace')

        found_bindings = {b.alias: b for b in found.bucket_bindings or []}
        for binding in sent.bucket_bindings or []:
            assert binding.alias in found_bindings, \
                f'bucket binding {binding.alias} missing, got {sorted(found_bindings)}'
            cls._assert_keyspace(found_bindings[binding.alias].name, binding.name,
                                 what=f'bucket binding {binding.alias}')
            cb_assert_eq(found_bindings[binding.alias].access, binding.access,
                         what=f'bucket binding {binding.alias} access')

        settings = found.settings
        cb_assert_isinstance(settings, EventingFunctionSettings, what='function settings')
        cb_assert_eq(settings.dcp_stream_boundary, sent.settings.dcp_stream_boundary,
                     what='dcp stream boundary')
        cb_assert_eq(settings.language_compatibility, sent.settings.language_compatibility,
                     what='language compatibility')

    @staticmethod
    def _assert_deserialized(func: EventingFunction) -> None:
        # Kept apart from the value comparison because the two fail for different reasons.  A
        # field is checked only when the server sent one, and a raw string arriving where an
        # enum belongs is not None, which is what this is here to catch.
        cb_assert_isinstance(func, EventingFunction, what='eventing function')
        cb_assert_isinstance(func.name, str, what='function name')
        cb_assert_isinstance(func.code, str, what='function code')
        cb_assert_isinstance(func.source_keyspace, EventingFunctionKeyspace, what='source keyspace')
        cb_assert_isinstance(func.metadata_keyspace, EventingFunctionKeyspace, what='metadata keyspace')

        settings = func.settings
        cb_assert_isinstance(settings, EventingFunctionSettings, what='function settings')
        for attr, expected in (('dcp_stream_boundary', EventingFunctionDcpBoundary),
                               ('deployment_status', EventingFunctionDeploymentStatus),
                               ('processing_status', EventingFunctionProcessingStatus),
                               ('language_compatibility', EventingFunctionLanguageCompatibility),
                               ('log_level', EventingFunctionLogLevel),
                               ('query_consistency', QueryScanConsistency)):
            value = getattr(settings, attr)
            if value is not None:
                cb_assert_isinstance(value, expected, what=f'settings.{attr}')

        for binding in func.bucket_bindings or []:
            cb_assert_isinstance(binding, EventingFunctionBucketBinding, what='bucket binding')
            cb_assert_isinstance(binding.name, EventingFunctionKeyspace, what='bucket binding keyspace')
            cb_assert_isinstance(binding.access, EventingFunctionBucketAccess, what='bucket binding access')

    @staticmethod
    def _assert_constant_bindings(func: EventingFunction) -> None:
        found = {b.alias: b for b in func.constant_bindings or []}
        cb_assert_eq(sorted(found), sorted(CONSTANT_BINDING_ALIASES), what='constant binding aliases')
        for alias, literal in zip(CONSTANT_BINDING_ALIASES, CONSTANT_BINDING_LITERALS):
            cb_assert_isinstance(found[alias], EventingFunctionConstantBinding, what='constant binding')
            cb_assert_eq(found[alias].literal, literal, what=f'constant binding {alias} literal')

    @staticmethod
    def _assert_url_bindings(func: EventingFunction) -> None:
        # The redaction is the claim this test exists for: a password or bearer key echoed by
        # the server is readable by anyone who can call get_function, a weaker permission
        # than the one needed to set it.
        found = {b.alias: b for b in func.url_bindings or []}
        cb_assert_eq(sorted(found), sorted(URL_BINDING_ALIASES), what='url binding aliases')
        for hostname, alias in zip(URL_BINDING_HOSTNAMES, URL_BINDING_ALIASES):
            binding = found[alias]
            cb_assert_isinstance(binding, EventingFunctionUrlBinding, what='url binding')
            cb_assert_eq(binding.hostname, hostname, what=f'url binding {alias} hostname')
            auth = binding.auth
            # The server withholds the secret but does not agree with itself on how: basic
            # and digest come back with the password absent, bearer with the key masked.
            # Both hold the claim, so what is asserted is that the secret sent is not the
            # secret returned, rather than the shape whatever stands in its place takes.
            if isinstance(auth, (EventingFunctionUrlAuthBasic, EventingFunctionUrlAuthDigest)):
                cb_assert_eq(auth.username, URL_BINDING_USERNAME, what=f'url binding {alias} username')
                assert auth.password != URL_BINDING_PASSWORD, f'url binding {alias} echoed its password'
            elif isinstance(auth, EventingFunctionUrlAuthBearer):
                assert auth.key != URL_BINDING_BEARER_KEY, f'url binding {alias} echoed its bearer key'
            else:
                cb_assert_isinstance(auth, EventingFunctionUrlNoAuth, what=f'url binding {alias} auth')

    @staticmethod
    def _assert_status_lists(statuses: EventingFunctionsStatus, name: str) -> None:
        cb_assert_isinstance(statuses, EventingFunctionsStatus, what='functions status')
        assert statuses.num_eventing_nodes is not None and statuses.num_eventing_nodes >= 1
        for status in statuses.functions:
            cb_assert_isinstance(status, EventingFunctionStatus, what='function status')
            cb_assert_isinstance(status.state, EventingFunctionState, what='function state')
        assert name in [s.name for s in statuses.functions], \
            f'{name} not among {[s.name for s in statuses.functions]}'


class EventingFunctionManagementTestSuite(EventingFunctionManagementSuiteBase):
    MANAGERS = [TestEnvironmentBucketManager, TestEnvironmentEventingFunctionManager]
    REQUIRED_FEATURES = [ServerFeatures.BucketManagement, ServerFeatures.EventingFunctionManagement]

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
        'test_missing_source_collection',
        'test_options_simple',
        'test_pause_and_resume_function',
        'test_pause_function_fail',
        'test_resume_function_fail',
        'test_undeploy_function',
        'test_undeploy_function_fail',
        'test_upsert_function',
        'test_upsert_function_fail',
        'test_url_bindings',
        'test_with_scope_and_collection',
    ]

    @staticmethod
    def _source_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        # The whole bucket, which is what a cluster-level function streams unless it is told
        # otherwise; test_with_scope_and_collection is the one that tells it otherwise.
        return EventingFunctionKeyspace(eventing_bucket_name())

    @staticmethod
    def _metadata_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        return EventingFunctionKeyspace(eventing_bucket_name(),
                                        FUNCTION_SCOPE_NAME,
                                        META_COLLECTION_NAME)

    @staticmethod
    def _binding_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        return EventingFunctionKeyspace(eventing_bucket_name())

    @pytest.fixture(name='test_func')
    def get_test_function(self, cb_env: SyncTestEnvironment) -> EventingFunction:
        return self._function(cb_env, function_name())

    @pytest.fixture()
    def create_test_function(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)

    @pytest.fixture()
    def drop_test_function(self,
                           cb_env: SyncTestEnvironment,
                           test_func: EventingFunction) -> Generator[None, None, None]:
        yield
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        EventingEnvironment.ensure_function_dropped(efm, test_func.name)

    @pytest.fixture(name='created')
    def track_created_functions(self,
                                cb_env: SyncTestEnvironment) -> Generator[list[str], None, None]:
        # A fixture rather than a list on the environment: a shared list each test reassigns
        # before filling cleans up the previous test's names when a test raises early.
        names: list[str] = []
        yield names
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        for name in names:
            EventingEnvironment.ensure_function_dropped(efm, name)

    @pytest.mark.usefixtures('drop_test_function')
    def test_upsert_function(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.upsert_function(test_func)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_deserialized(found)

    def test_missing_source_collection(self,
                                       cb_env: SyncTestEnvironment,
                                       created: list[str]) -> None:
        # A source keyspace naming a collection that does not exist.  The scope is the suite's
        # own, so what is missing is the collection alone, which is what the exception names.
        bad = self._function(cb_env,
                             function_name('bad'),
                             source_keyspace=EventingFunctionKeyspace(eventing_bucket_name(),
                                                                      FUNCTION_SCOPE_NAME,
                                                                      'not-a-collection'))
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        created.append(bad.name)
        if self._upsert_checks_source_keyspace(cb_env):
            with pytest.raises(EventingFunctionCollectionNotFoundException):
                efm.upsert_function(bad)
        else:
            efm.upsert_function(bad)
            with pytest.raises((EventingFunctionCollectionNotFoundException,
                                InternalServerFailureException)):
                efm.deploy_function(bad.name)

    def test_upsert_function_fail(self,
                                  cb_env: SyncTestEnvironment,
                                  created: list[str]) -> None:
        # A handler that does not compile.  The service compiles on upsert on every version,
        # so this is the failure upsert_function itself still reports from 8.0, where the
        # missing source collection moved to deploy.
        bad = self._function(cb_env, function_name('bad-code'))
        bad.code = INVALID_EVT_CODE
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        created.append(bad.name)
        with pytest.raises(EventingFunctionCompilationFailureException):
            efm.upsert_function(bad)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_get_function(self,
                          cb_env: SyncTestEnvironment,
                          test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_deserialized(found)

    def test_get_function_fail(self, cb_env: SyncTestEnvironment) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises(EventingFunctionNotFoundException):
            efm.get_function(MISSING_FUNCTION_NAME)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_get_all_functions(self,
                               cb_env: SyncTestEnvironment,
                               test_func: EventingFunction,
                               created: list[str]) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        second = self._function(cb_env, function_name('second'))
        created.append(second.name)
        efm.upsert_function(second)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, second.name)

        functions = efm.get_all_functions()
        by_name = {f.name: f for f in functions}
        # At least, not exactly: this manager sees every function in the cluster, so another
        # worker's functions can legitimately be in the listing alongside these two.
        assert test_func.name in by_name, f'{test_func.name} not among {sorted(by_name)}'
        assert second.name in by_name, f'{second.name} not among {sorted(by_name)}'
        for func in functions:
            self._assert_deserialized(func)
        self._assert_round_trip(by_name[test_func.name], test_func)
        self._assert_round_trip(by_name[second.name], second)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_functions_status(self,
                              cb_env: SyncTestEnvironment,
                              test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        statuses = efm.functions_status()
        self._assert_status_lists(statuses, test_func.name)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_deploy_function(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(found.settings.deployment_status, EventingFunctionDeploymentStatus.Deployed,
                     what='deployment status')

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_deploy_function_fail(self,
                                  cb_env: SyncTestEnvironment,
                                  test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS):
            efm.deploy_function(MISSING_FUNCTION_NAME)

        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        if self._redeploy_succeeds(cb_env):
            efm.deploy_function(test_func.name)
            EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        else:
            with pytest.raises(EventingFunctionAlreadyDeployedException):
                efm.deploy_function(test_func.name)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_drop_function(self,
                           cb_env: SyncTestEnvironment,
                           test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.drop_function(test_func.name)
        TestEnvironment.try_n_times_till_exception(
            10, 1, efm.get_function, test_func.name,
            expected_exceptions=(EventingFunctionNotFoundException,))

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_drop_function_fail(self,
                                cb_env: SyncTestEnvironment,
                                test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises((EventingFunctionNotDeployedException, EventingFunctionNotFoundException)):
            efm.drop_function(MISSING_FUNCTION_NAME)

        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        with pytest.raises(EventingFunctionNotUnDeployedException):
            efm.drop_function(test_func.name)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_undeploy_function(self,
                               cb_env: SyncTestEnvironment,
                               test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        # Both halves are asserted through the same field, because state and deployment_status
        # are two different things the server reports and a transition can move one alone.
        deployed = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(deployed.settings.deployment_status, EventingFunctionDeploymentStatus.Deployed,
                     what='deployment status after deploy')

        efm.undeploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(found.settings.deployment_status, EventingFunctionDeploymentStatus.Undeployed,
                     what='deployment status')

    def test_undeploy_function_fail(self, cb_env: SyncTestEnvironment) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS + (EventingFunctionNotDeployedException,)):
            efm.undeploy_function(MISSING_FUNCTION_NAME)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_pause_and_resume_function(self,
                                       cb_env: SyncTestEnvironment,
                                       test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)

        efm.pause_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Paused, test_func.name)
        paused = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(paused.settings.processing_status, EventingFunctionProcessingStatus.Paused,
                     what='processing status while paused')

        efm.resume_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        resumed = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(resumed.settings.processing_status, EventingFunctionProcessingStatus.Running,
                     what='processing status after resume')

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_pause_function_fail(self,
                                 cb_env: SyncTestEnvironment,
                                 test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS):
            efm.pause_function(MISSING_FUNCTION_NAME)

        # A function that exists but has never been deployed cannot be paused either, and the
        # exception is a different one: it has no bootstrapped nodes to pause.
        with pytest.raises((EventingFunctionNotBootstrappedException, InternalServerFailureException)):
            efm.pause_function(test_func.name)

    def test_resume_function_fail(self, cb_env: SyncTestEnvironment) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS):
            efm.resume_function(MISSING_FUNCTION_NAME)

    @pytest.mark.usefixtures('drop_test_function')
    def test_options_simple(self,
                            cb_env: SyncTestEnvironment,
                            test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.upsert_function(test_func, UpsertFunctionOptions(timeout=timedelta(seconds=20)))
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name,
                                            GetFunctionOptions(timeout=timedelta(seconds=15)))
        self._assert_round_trip(found, test_func)
        # The keyword form of the same option.  Both reach the same request builder, and only
        # sending the options object leaves that half of every signature unexercised.
        by_kwarg = efm.get_function(test_func.name, timeout=timedelta(seconds=15))
        self._assert_round_trip(by_kwarg, test_func)

    @pytest.mark.usefixtures('drop_test_function')
    def test_constant_bindings(self,
                               cb_env: SyncTestEnvironment,
                               test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        test_func.constant_bindings = self._constant_bindings()
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_constant_bindings(found)

    @pytest.mark.usefixtures('drop_test_function')
    def test_url_bindings(self,
                          cb_env: SyncTestEnvironment,
                          test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        test_func.url_bindings = self._url_bindings()
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_url_bindings(found)

    @pytest.mark.usefixtures('drop_test_function')
    def test_with_scope_and_collection(self,
                                       cb_env: SyncTestEnvironment,
                                       test_func: EventingFunction) -> None:
        # The one function here whose source keyspace is narrowed below the bucket, so what
        # this adds is that a cluster-level manager streams a single collection when asked.
        test_func.source_keyspace = EventingFunctionKeyspace(eventing_bucket_name(),
                                                             FUNCTION_SCOPE_NAME,
                                                             SOURCE_COLLECTION_NAME)
        efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_deserialized(found)


class ScopeEventingFunctionManagementTestSuite(EventingFunctionManagementSuiteBase):
    # The cluster-level manager is here for test_scope_isolation alone, which is the one
    # test whose claim is about what a manager cannot see.
    MANAGERS = [TestEnvironmentBucketManager,
                TestEnvironmentScopeEventingFunctionManager,
                TestEnvironmentEventingFunctionManager]
    REQUIRED_FEATURES = [ServerFeatures.BucketManagement,
                         ServerFeatures.ScopeEventingFunctionManagement]

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
        'test_missing_source_collection',
        'test_options_simple',
        'test_pause_and_resume_function',
        'test_pause_function_fail',
        'test_resume_function_fail',
        'test_scope_isolation',
        'test_undeploy_function',
        'test_undeploy_function_fail',
        'test_upsert_function',
        'test_upsert_function_fail',
        'test_url_bindings',
        'test_with_scope_and_collection',
    ]

    @staticmethod
    def _source_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        # Both keyspaces live inside the scope the manager hangs off, which is what makes
        # these functions addressable through that scope and nowhere else.
        return EventingFunctionKeyspace(scope_eventing_bucket_name(),
                                        FUNCTION_SCOPE_NAME,
                                        SOURCE_COLLECTION_NAME)

    @staticmethod
    def _metadata_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        return EventingFunctionKeyspace(scope_eventing_bucket_name(),
                                        FUNCTION_SCOPE_NAME,
                                        META_COLLECTION_NAME)

    @staticmethod
    def _binding_keyspace(cb_env: SyncTestEnvironment) -> EventingFunctionKeyspace:
        # Bucket-level, as the cluster-level suite sends.  A function managed through a scope
        # is confined to it for addressing, not for reads and writes.
        return EventingFunctionKeyspace(scope_eventing_bucket_name())

    @pytest.fixture(name='test_func')
    def get_test_function(self, cb_env: SyncTestEnvironment) -> EventingFunction:
        return self._function(cb_env, function_name('scoped'))

    @pytest.fixture()
    def create_test_function(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)

    @pytest.fixture()
    def drop_test_function(self,
                           cb_env: SyncTestEnvironment,
                           test_func: EventingFunction) -> Generator[None, None, None]:
        yield
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        EventingEnvironment.ensure_function_dropped(efm, test_func.name)

    @pytest.fixture(name='created')
    def track_created_functions(self,
                                cb_env: SyncTestEnvironment) -> Generator[list[str], None, None]:
        # A fixture rather than a list on the environment: a shared list each test reassigns
        # before filling cleans up the previous test's names when a test raises early.
        names: list[str] = []
        yield names
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        for name in names:
            EventingEnvironment.ensure_function_dropped(efm, name)

    @pytest.mark.usefixtures('drop_test_function')
    def test_upsert_function(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.upsert_function(test_func)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_deserialized(found)

    def test_missing_source_collection(self,
                                       cb_env: SyncTestEnvironment,
                                       created: list[str]) -> None:
        # A source keyspace naming a collection that does not exist.  The scope is the suite's
        # own, so what is missing is the collection alone, which is what the exception names.
        bad = self._function(cb_env,
                             function_name('scoped', 'bad'),
                             source_keyspace=EventingFunctionKeyspace(scope_eventing_bucket_name(),
                                                                      FUNCTION_SCOPE_NAME,
                                                                      'not-a-collection'))
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        created.append(bad.name)
        if self._upsert_checks_source_keyspace(cb_env):
            with pytest.raises(EventingFunctionCollectionNotFoundException):
                efm.upsert_function(bad)
        else:
            efm.upsert_function(bad)
            with pytest.raises((EventingFunctionCollectionNotFoundException,
                                InternalServerFailureException)):
                efm.deploy_function(bad.name)

    def test_upsert_function_fail(self,
                                  cb_env: SyncTestEnvironment,
                                  created: list[str]) -> None:
        # A handler that does not compile.  The service compiles on upsert on every version,
        # so this is the failure upsert_function itself still reports from 8.0, where the
        # missing source collection moved to deploy.
        bad = self._function(cb_env, function_name('scoped', 'bad-code'))
        bad.code = INVALID_EVT_CODE
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        created.append(bad.name)
        with pytest.raises(EventingFunctionCompilationFailureException):
            efm.upsert_function(bad)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_get_function(self,
                          cb_env: SyncTestEnvironment,
                          test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_deserialized(found)

    def test_get_function_fail(self, cb_env: SyncTestEnvironment) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        with pytest.raises(EventingFunctionNotFoundException):
            efm.get_function(MISSING_FUNCTION_NAME)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_get_all_functions(self,
                               cb_env: SyncTestEnvironment,
                               test_func: EventingFunction,
                               created: list[str]) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        second = self._function(cb_env, function_name('scoped', 'second'))
        created.append(second.name)
        efm.upsert_function(second)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, second.name)

        functions = efm.get_all_functions()
        by_name = {f.name: f for f in functions}
        # At least, not exactly: this manager sees every function in the cluster, so another
        # worker's functions can legitimately be in the listing alongside these two.
        assert test_func.name in by_name, f'{test_func.name} not among {sorted(by_name)}'
        assert second.name in by_name, f'{second.name} not among {sorted(by_name)}'
        for func in functions:
            self._assert_deserialized(func)
        self._assert_round_trip(by_name[test_func.name], test_func)
        self._assert_round_trip(by_name[second.name], second)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_functions_status(self,
                              cb_env: SyncTestEnvironment,
                              test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        statuses = efm.functions_status()
        self._assert_status_lists(statuses, test_func.name)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_deploy_function(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(found.settings.deployment_status, EventingFunctionDeploymentStatus.Deployed,
                     what='deployment status')

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_deploy_function_fail(self,
                                  cb_env: SyncTestEnvironment,
                                  test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS):
            efm.deploy_function(MISSING_FUNCTION_NAME)

        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        if self._redeploy_succeeds(cb_env):
            efm.deploy_function(test_func.name)
            EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        else:
            with pytest.raises(EventingFunctionAlreadyDeployedException):
                efm.deploy_function(test_func.name)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_drop_function(self,
                           cb_env: SyncTestEnvironment,
                           test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.drop_function(test_func.name)
        TestEnvironment.try_n_times_till_exception(
            10, 1, efm.get_function, test_func.name,
            expected_exceptions=(EventingFunctionNotFoundException,))

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_drop_function_fail(self,
                                cb_env: SyncTestEnvironment,
                                test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        with pytest.raises((EventingFunctionNotDeployedException, EventingFunctionNotFoundException)):
            efm.drop_function(MISSING_FUNCTION_NAME)

        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        with pytest.raises(EventingFunctionNotUnDeployedException):
            efm.drop_function(test_func.name)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_undeploy_function(self,
                               cb_env: SyncTestEnvironment,
                               test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        # Both halves are asserted through the same field, because state and deployment_status
        # are two different things the server reports and a transition can move one alone.
        deployed = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(deployed.settings.deployment_status, EventingFunctionDeploymentStatus.Deployed,
                     what='deployment status after deploy')

        efm.undeploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(found.settings.deployment_status, EventingFunctionDeploymentStatus.Undeployed,
                     what='deployment status')

    def test_undeploy_function_fail(self, cb_env: SyncTestEnvironment) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS + (EventingFunctionNotDeployedException,)):
            efm.undeploy_function(MISSING_FUNCTION_NAME)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_pause_and_resume_function(self,
                                       cb_env: SyncTestEnvironment,
                                       test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.deploy_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)

        efm.pause_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Paused, test_func.name)
        paused = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(paused.settings.processing_status, EventingFunctionProcessingStatus.Paused,
                     what='processing status while paused')

        efm.resume_function(test_func.name)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Deployed, test_func.name)
        resumed = TestEnvironment.try_n_times(5, 1, efm.get_function, test_func.name)
        cb_assert_eq(resumed.settings.processing_status, EventingFunctionProcessingStatus.Running,
                     what='processing status after resume')

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_pause_function_fail(self,
                                 cb_env: SyncTestEnvironment,
                                 test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS):
            efm.pause_function(MISSING_FUNCTION_NAME)

        # A function that exists but has never been deployed cannot be paused either, and the
        # exception is a different one: it has no bootstrapped nodes to pause.
        with pytest.raises((EventingFunctionNotBootstrappedException, InternalServerFailureException)):
            efm.pause_function(test_func.name)

    def test_resume_function_fail(self, cb_env: SyncTestEnvironment) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        with pytest.raises(self.MISSING_FUNCTION_ERRORS):
            efm.resume_function(MISSING_FUNCTION_NAME)

    @pytest.mark.usefixtures('drop_test_function')
    def test_options_simple(self,
                            cb_env: SyncTestEnvironment,
                            test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.upsert_function(test_func, UpsertFunctionOptions(timeout=timedelta(seconds=20)))
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name,
                                            GetFunctionOptions(timeout=timedelta(seconds=15)))
        self._assert_round_trip(found, test_func)
        # The keyword form of the same option.  Both reach the same request builder, and only
        # sending the options object leaves that half of every signature unexercised.
        by_kwarg = efm.get_function(test_func.name, timeout=timedelta(seconds=15))
        self._assert_round_trip(by_kwarg, test_func)

    @pytest.mark.usefixtures('drop_test_function')
    def test_constant_bindings(self,
                               cb_env: SyncTestEnvironment,
                               test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        test_func.constant_bindings = self._constant_bindings()
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_constant_bindings(found)

    @pytest.mark.usefixtures('drop_test_function')
    def test_url_bindings(self,
                          cb_env: SyncTestEnvironment,
                          test_func: EventingFunction) -> None:
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        test_func.url_bindings = self._url_bindings()
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_url_bindings(found)

    @pytest.mark.usefixtures('drop_test_function')
    def test_with_scope_and_collection(self,
                                       cb_env: SyncTestEnvironment,
                                       test_func: EventingFunction) -> None:
        # Every function this suite sends already names both keyspaces down to a collection,
        # so what is left to qualify is the bucket binding, which the baseline leaves whole.
        test_func.bucket_bindings = [
            EventingFunctionBucketBinding(alias=BUCKET_BINDING_ALIAS,
                                          name=EventingFunctionKeyspace(scope_eventing_bucket_name(),
                                                                        FUNCTION_SCOPE_NAME,
                                                                        SOURCE_COLLECTION_NAME),
                                          access=EventingFunctionBucketAccess.ReadWrite)]
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        efm.upsert_function(test_func)
        EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, test_func.name)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)
        self._assert_deserialized(found)

    @pytest.mark.usefixtures('create_test_function')
    @pytest.mark.usefixtures('drop_test_function')
    def test_scope_isolation(self,
                             cb_env: SyncTestEnvironment,
                             test_func: EventingFunction) -> None:
        # Running the same bodies through both managers leaves what a scope-level manager is
        # for untested: without this, a scope manager that quietly created cluster-level
        # functions satisfies every other test in this file.
        efm = cb_env.get_manager(TestEnvironmentScopeEventingFunctionManager)
        found = TestEnvironment.try_n_times(5, 3, efm.get_function, test_func.name)
        self._assert_round_trip(found, test_func)

        cluster_efm = cb_env.get_manager(TestEnvironmentEventingFunctionManager)
        with pytest.raises(EventingFunctionNotFoundException):
            cluster_efm.get_function(test_func.name)
        # Absent from the cluster-level listing too, not merely unreachable by name.
        names = [f.name for f in cluster_efm.get_all_functions()]
        assert test_func.name not in names, f'{test_func.name} is visible at cluster level'


@pytest.mark.xdist_group('bucket_lifecycle')
class EventingFunctionManagementTests(EventingFunctionManagementTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(EventingFunctionManagementTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, EventingFunctionManagementTestSuite)
        EventingEnvironment.setup_eventing(env,
                                           eventing_bucket_name(),
                                           ServerFeatures.EventingFunctionManagement,
                                           TestEnvironmentEventingFunctionManager)
        yield env
        EventingEnvironment.teardown_eventing(env, eventing_bucket_name())
        teardown_environment(env, EventingFunctionManagementTestSuite)


@pytest.mark.xdist_group('bucket_lifecycle')
class ScopeEventingFunctionManagementTests(ScopeEventingFunctionManagementTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(ScopeEventingFunctionManagementTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, ScopeEventingFunctionManagementTestSuite)
        EventingEnvironment.setup_eventing(env,
                                           scope_eventing_bucket_name(),
                                           ServerFeatures.ScopeEventingFunctionManagement,
                                           TestEnvironmentScopeEventingFunctionManager,
                                           scoped=True)
        yield env
        EventingEnvironment.teardown_eventing(env, scope_eventing_bucket_name())
        teardown_environment(env, ScopeEventingFunctionManagementTestSuite)
