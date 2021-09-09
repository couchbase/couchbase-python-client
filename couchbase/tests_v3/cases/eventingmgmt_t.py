import time
from unittest import SkipTest
import attr
from datetime import timedelta

from couchbase_tests.base import CollectionTestCase
from couchbase.cluster import QueryScanConsistency
from couchbase.management.eventing import (
    EventingFunction,
    EventingFunctionDcpBoundary,
    EventingFunctionDeploymentStatus,
    EventingFunctionProcessingStatus,
    EventingFunctionSettings,
    EventingFunctionKeyspace,
    EventingFunctionBucketBinding,
    EventingFunctionUrlAuthBasic,
    EventingFunctionUrlAuthBearer,
    EventingFunctionUrlAuthDigest,
    EventingFunctionUrlBinding,
    EventingFunctionConstantBinding,
    EventingFunctionBucketAccess,
    EventingFunctionLanguageCompatibility,
    EventingFunctionLogLevel,
    EventingFunctionUrlNoAuth,
    EventingFunctionsStatus,
    EventingFunctionStatus,
    EventingFunctionState,
    GetFunctionOptions,
    UpsertFunctionOptions
)
from couchbase.exceptions import (
    EventingFunctionNotFoundException,
    EventingFunctionCompilationFailureException,
    EventingFunctionNotBootstrappedException,
    EventingFunctionNotDeployedException,
    EventingFunctionNotUnDeployedException,
    EventingFunctionCollectionNotFoundException,
    EventingFunctionAlreadyDeployedException
)


class EventingFunctionManagementTestStatusException(Exception):
    """Raised when waiting for a certained status does not happen within a specified timeframe"""

    pass


class EventingFunctionManagementTests(CollectionTestCase):
    EVT_BUCKET_NAME = "beer-sample"
    SIMPLE_EVT_CODE = 'function OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}\n\nfunction OnDelete(meta, options) {\n    log("Doc deleted/expired", meta.id);\n}'
    EVT_VERSION = None
    BASIC_FUNC = EventingFunction(
        "test-evt-func",
        SIMPLE_EVT_CODE,
        "evt-7.0.0-5302-ee",
        metadata_keyspace=EventingFunctionKeyspace("default"),
        source_keyspace=EventingFunctionKeyspace(EVT_BUCKET_NAME),
        settings=EventingFunctionSettings.new_settings(
            dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
            language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
        ),
        bucket_bindings=[
            EventingFunctionBucketBinding(
                alias="evtFunc",
                name=EventingFunctionKeyspace(EVT_BUCKET_NAME),
                access=EventingFunctionBucketAccess.ReadWrite
            )
        ]
    )

    def setUp(self, *args, **kwargs):
        super(EventingFunctionManagementTests, self).setUp(*args, **kwargs)

        if not self.is_realserver:
            raise SkipTest("Real server must be used for admin tests")

        version = self.get_cluster_version(full=True)

        if int(version.split(".")[0]) < 7:
            raise SkipTest("No eventing function management in {}".format(version))

        self.EVT_VERSION = "evt-{}".format(
            version.replace("enterprise", "ee").replace("community", "ce")
        )

        self.BASIC_FUNC.version = self.EVT_VERSION

        self.efm = self.cluster.eventing_functions()

        self.efm.upsert_function(self.BASIC_FUNC)
        self._wait_until_status(
            20, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        func = self.efm.get_function(self.BASIC_FUNC.name)
        self._validate_function(func, shallow=True)
        # TODO:  For scope/collection testing requires multiple buckets + scopes/collections
        #       a lot more work to put this into the testing framework in an automated fashion...
        # self.cm = self.bucket.collections()
        # self.create_eventing_collections()

    def tearDown(self):
        funcs = self.efm.get_all_functions()

        for func in funcs:
            if (
                func.settings.deployment_status
                == EventingFunctionDeploymentStatus.Deployed
            ):
                self.efm.undeploy_function(func.name)
                self._wait_until_status(
                    20, 2, EventingFunctionState.Undeployed, func.name
                )

            self.try_n_times(
                10,
                1,
                self.efm.drop_function,
                func.name
            )
            self.try_n_times_till_exception(
                10,
                1,
                self.efm.get_function,
                func.name,
                (Exception, EventingFunctionNotFoundException)
            )

        super(EventingFunctionManagementTests, self).tearDown()

    def _validate_bucket_bindings(self, bindings):
        fields = attr.fields_dict(EventingFunctionBucketBinding)
        for binding in bindings:
            self.assertIsInstance(binding, EventingFunctionBucketBinding)
            for field in fields:
                value = getattr(binding, field)
                if value is not None:
                    if field == "name":
                        self.assertIsInstance(value, EventingFunctionKeyspace)
                    elif field == "access":
                        self.assertIsInstance(value, EventingFunctionBucketAccess)
                    else:
                        self.assertIsInstance(value, fields[field].type)

    def _validate_url_bindings(self, bindings):
        fields = attr.fields_dict(EventingFunctionUrlBinding)
        for binding in bindings:
            self.assertIsInstance(binding, EventingFunctionUrlBinding)
            for field in fields:
                value = getattr(binding, field)
                if value is not None:
                    if field == "auth":
                        if isinstance(value, EventingFunctionUrlAuthBasic):
                            self.assertIsInstance(value.username, str)
                            self.assertIsNone(value.password)
                        elif isinstance(value, EventingFunctionUrlAuthDigest):
                            self.assertIsInstance(value.username, str)
                            self.assertIsNone(value.password)
                        elif isinstance(value, EventingFunctionUrlAuthBearer):
                            self.assertIsNone(value.key)
                        else:
                            self.assertIsInstance(value, EventingFunctionUrlNoAuth)
                    else:
                        self.assertIsInstance(value, fields[field].type)

    def _validate_constant_bindings(self, bindings):
        fields = attr.fields_dict(EventingFunctionConstantBinding)
        for binding in bindings:
            self.assertIsInstance(binding, EventingFunctionConstantBinding)
            for field in fields:
                value = getattr(binding, field)
                if value is not None:
                    self.assertIsInstance(value, fields[field].type)

    def _validate_settings(self, settings):
        self.assertIsInstance(settings, EventingFunctionSettings)
        fields = attr.fields_dict(EventingFunctionSettings)
        for field in fields.keys():
            value = getattr(settings, field)
            if value is not None:
                if field == "dcp_stream_boundary":
                    self.assertIsInstance(value, EventingFunctionDcpBoundary)
                elif field == "deployment_status":
                    self.assertIsInstance(value, EventingFunctionDeploymentStatus)
                elif field == "processing_status":
                    self.assertIsInstance(value, EventingFunctionProcessingStatus)
                elif field == "language_compatibility":
                    self.assertIsInstance(value, EventingFunctionLanguageCompatibility)
                elif field == "log_level":
                    self.assertIsInstance(value, EventingFunctionLogLevel)
                elif field == "query_consistency":
                    self.assertIsInstance(value, QueryScanConsistency)
                elif field == "handler_headers":
                    self.assertIsInstance(value, list)
                elif field == "handler_footers":
                    self.assertIsInstance(value, list)
                else:
                    self.assertIsInstance(value, fields[field].type)

    def _validate_function(self, func, shallow=False):
        self.assertIsInstance(func, EventingFunction)
        if shallow is False:
            fields = attr.fields_dict(EventingFunction)
            for field in fields.keys():
                value = getattr(func, field)
                if value is not None:
                    if field == "bucket_bindings":
                        self._validate_bucket_bindings(value)
                    elif field == "url_bindings":
                        self._validate_url_bindings(value)
                    elif field == "constant_bindings":
                        self._validate_constant_bindings(value)
                    elif field == "settings":
                        self._validate_settings(value)
                    else:
                        self.assertIsInstance(value, fields[field].type)

    def _wait_until_status(
        self,  # type: "EventingFunctionManagementTests"
        num_times,  # type: int
        seconds_between,  # type: int
        state,  # type: EventingFunctionState
        name  # type: str
    ) -> None:

        func_status = None
        for _ in range(num_times):
            func_status = self.efm._get_status(name)
            if func_status.state != state:
                time.sleep(seconds_between)
            else:
                break

        if func_status.state != state:
            raise EventingFunctionManagementTestStatusException(
                "Function {} status is {} which does not match desired status of {}.".format(
                    name, func_status.state.value, state.value
                )
            )

    def test_upsert_function(self):
        local_func = EventingFunction(
            "test-evt-func-1",
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
        self.efm.upsert_function(local_func)
        func = self.try_n_times(5, 3, self.efm.get_function, local_func.name)
        self._validate_function(func, shallow=True)

    def test_upsert_function_fail(self):
        # bad appcode
        local_func = EventingFunction(
            "test-evt-func-1",
            'func OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}\n\n',
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
        with self.assertRaises(EventingFunctionCompilationFailureException):
            self.efm.upsert_function(local_func)

        local_func.code = self.SIMPLE_EVT_CODE
        local_func.source_keyspace = EventingFunctionKeyspace(
            "beer-sample", "test-scope", "test-collection"
        )
        with self.assertRaises(EventingFunctionCollectionNotFoundException):
            self.efm.upsert_function(local_func)

    def test_drop_function(self):
        self.efm.drop_function(self.BASIC_FUNC.name)
        self.try_n_times_till_exception(
            10,
            1,
            self.efm.get_function,
            self.BASIC_FUNC.name,
            EventingFunctionNotFoundException
        )

    def test_drop_function_fail(self):
        with self.assertRaises(
            (EventingFunctionNotDeployedException, EventingFunctionNotFoundException)
        ):
            self.efm.drop_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        self._wait_until_status(
            15, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        self.efm.deploy_function(self.BASIC_FUNC.name)
        # now, wait for it to be deployed
        self._wait_until_status(
            15, 2, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )

        with self.assertRaises(EventingFunctionNotUnDeployedException):
            self.efm.drop_function(self.BASIC_FUNC.name)

    def test_get_function(self):
        func = self.try_n_times(5, 3, self.efm.get_function, self.BASIC_FUNC.name)
        self._validate_function(func)

    def test_get_function_fail(self):
        with self.assertRaises(EventingFunctionNotFoundException):
            self.efm.get_function("not-a-function")

    def test_get_all_functions(self):
        new_funcs = [
            EventingFunction(
                "test-evt-func-1",
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
                "test-evt-func-2",
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
            self.efm.upsert_function(func)
            self.try_n_times(5, 3, self.efm.get_function, func.name)

        funcs = self.efm.get_all_functions()
        for func in funcs:
            self._validate_function(func)

    def test_deploy_function(self):
        # deploy function -- but first verify in undeployed state
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        self.efm.deploy_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        func = self.try_n_times(5, 1, self.efm.get_function, self.BASIC_FUNC.name)
        self._validate_function(func, shallow=True)
        # verify function deployement status has changed
        self.assertEqual(
            func.settings.deployment_status, EventingFunctionDeploymentStatus.Deployed
        )

    def test_deploy_function_fail(self):
        with self.assertRaises(EventingFunctionNotFoundException):
            self.efm.deploy_function("not-a-function")

        # deploy function -- but first verify in undeployed state
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        self.efm.deploy_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        with self.assertRaises(EventingFunctionAlreadyDeployedException):
            self.efm.deploy_function(self.BASIC_FUNC.name)

    def test_undeploy_function(self):
        # deploy function -- but first verify in undeployed state
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        self.efm.deploy_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        func = self.try_n_times(5, 1, self.efm.get_function, self.BASIC_FUNC.name)
        self._validate_function(func, shallow=True)
        # verify function deployement status
        self.assertEqual(
            func.settings.deployment_status, EventingFunctionDeploymentStatus.Deployed
        )
        # now, undeploy function
        self.efm.undeploy_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        func = self.try_n_times(5, 1, self.efm.get_function, self.BASIC_FUNC.name)
        self._validate_function(func, shallow=True)
        # verify function deployement status has changed
        self.assertEqual(
            func.settings.deployment_status, EventingFunctionDeploymentStatus.Undeployed
        )

    def test_undeploy_function_fail(self):
        with self.assertRaises(
            (EventingFunctionNotDeployedException, EventingFunctionNotFoundException)
        ):
            self.efm.undeploy_function("not-a-function")

    def test_pause_function(self):
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        self.efm.deploy_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        self.efm.pause_function(self.BASIC_FUNC.name)
        func = self.try_n_times(5, 1, self.efm.get_function, self.BASIC_FUNC.name)
        self._validate_function(func, shallow=True)
        # verify function processing status
        self.assertEqual(
            func.settings.processing_status, EventingFunctionProcessingStatus.Paused
        )

    def test_pause_function_fail(self):
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )

        with self.assertRaises(EventingFunctionNotFoundException):
            self.efm.pause_function("not-a-function")

        with self.assertRaises(EventingFunctionNotBootstrappedException):
            self.efm.pause_function(self.BASIC_FUNC.name)

    def test_resume_function(self):
        # make sure function has been deployed
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )
        self.efm.deploy_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Deployed, self.BASIC_FUNC.name
        )
        # pause function - verify status is paused
        self.efm.pause_function(self.BASIC_FUNC.name)
        self._wait_until_status(
            15, 2, EventingFunctionState.Paused, self.BASIC_FUNC.name
        )
        # resume function
        self.efm.resume_function(self.BASIC_FUNC.name)
        func = self.try_n_times(5, 1, self.efm.get_function, self.BASIC_FUNC.name)
        self._validate_function(func, shallow=True)
        # verify function processing status
        self.assertEqual(
            func.settings.processing_status, EventingFunctionProcessingStatus.Running
        )

    def test_resume_function_fail(self):
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, self.BASIC_FUNC.name
        )

        with self.assertRaises(EventingFunctionNotFoundException):
            self.efm.pause_function("not-a-function")

        with self.assertRaises(EventingFunctionNotBootstrappedException):
            self.efm.pause_function(self.BASIC_FUNC.name)

    def test_with_scope_and_collection(self):
        raise SkipTest("Currently only for local testing.")
        local_func = EventingFunction(
            "test-evt-func-coll",
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace(
                "beer-sample",
                "evt-scope",
                "metadata"
            ),
            source_keyspace=EventingFunctionKeyspace(
                "default",
                "evt-scope",
                "source"
            ),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ]
        )
        self.efm.upsert_function(local_func)
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = self.try_n_times(5, 3, self.efm.get_function, local_func.name)
        self._validate_function(func)

    def test_constant_bindings(self):
        # TODO:  look into why timeout occurs when providing > 1 constant binding
        local_func = EventingFunction(
            "test-evt-const-func",
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace(self.EVT_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_BUCKET_NAME),
                    access=EventingFunctionBucketAccess.ReadWrite
                )
            ],
            constant_bindings=[
                EventingFunctionConstantBinding(alias="testConstant", literal="1234"),
                EventingFunctionConstantBinding(alias="testConstant1", literal="\"another test value\"")
            ]
        )

        self.efm.upsert_function(local_func)
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = self.try_n_times(5, 3, self.efm.get_function, local_func.name)
        self._validate_function(func)

    def test_url_bindings(self):
        local_func = EventingFunction(
            "test-evt-url-func",
            self.SIMPLE_EVT_CODE,
            self.EVT_VERSION,
            metadata_keyspace=EventingFunctionKeyspace("default"),
            source_keyspace=EventingFunctionKeyspace(self.EVT_BUCKET_NAME),
            settings=EventingFunctionSettings.new_settings(
                dcp_stream_boundary=EventingFunctionDcpBoundary.FromNow,
                language_compatibility=EventingFunctionLanguageCompatibility.Version_6_6_2
            ),
            bucket_bindings=[
                EventingFunctionBucketBinding(
                    alias="evtFunc",
                    name=EventingFunctionKeyspace(self.EVT_BUCKET_NAME),
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

        self.efm.upsert_function(local_func)
        self._wait_until_status(
            10, 1, EventingFunctionState.Undeployed, local_func.name
        )
        func = self.try_n_times(5, 3, self.efm.get_function, local_func.name)
        self._validate_function(func)

    def test_functions_status(self):
        new_funcs = [
            EventingFunction(
                "test-evt-func-1",
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
                "test-evt-func-2",
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
            self.efm.upsert_function(func)
            self.try_n_times(5, 3, self.efm.get_function, func.name)

        funcs = self.efm.functions_status()
        self.assertIsInstance(funcs, EventingFunctionsStatus)
        for func in funcs.functions:
            self.assertIsInstance(func, EventingFunctionStatus)

    def test_options_simple(self):
        local_func = EventingFunction(
            "test-evt-func-1",
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
        self.efm.upsert_function(
            local_func, UpsertFunctionOptions(timeout=timedelta(seconds=20))
        )
        func = self.try_n_times(
            5,
            3,
            self.efm.get_function,
            local_func.name,
            GetFunctionOptions(timeout=timedelta(seconds=15))
        )
        self._validate_function(func, shallow=True)
