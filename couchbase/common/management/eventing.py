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

__all__ = [
    'EventingFunction',
    'EventingFunctionBucketAccess',
    'EventingFunctionBucketBinding',
    'EventingFunctionConstantBinding',
    'EventingFunctionDcpBoundary',
    'EventingFunctionDeploymentStatus',
    'EventingFunctionKeyspace',
    'EventingFunctionLanguageCompatibility',
    'EventingFunctionLogLevel',
    'EventingFunctionProcessingStatus',
    'EventingFunctionSettings',
    'EventingFunctionState',
    'EventingFunctionStatus',
    'EventingFunctionUrlAuth',
    'EventingFunctionUrlAuthBasic',
    'EventingFunctionUrlAuthBearer',
    'EventingFunctionUrlAuthDigest',
    'EventingFunctionUrlBinding',
    'EventingFunctionUrlNoAuth',
    'EventingFunctionsStatus',
]

from dataclasses import dataclass, fields
from datetime import timedelta
from enum import Enum
from typing import Any

from couchbase.common._transforms import timedelta_as_milliseconds
from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.n1ql import QueryScanConsistency


class EventingFunctionBucketAccess(Enum):
    """Indicates the bucket access of the eventing function.

    Values:

    ReadOnly
        The eventing function has read-only bucket access
    ReadWrite (default)
        The eventing function has read and write bucket access.
    Unknown
        Couchbase Server returned an access the SDK does not recognize.
    """

    ReadOnly = "r"
    ReadWrite = "rw"
    Unknown = "unknown"

    def to_str(self) -> str:
        if self is EventingFunctionBucketAccess.ReadOnly:
            return 'read_only'
        if self is EventingFunctionBucketAccess.Unknown:
            return 'unknown'
        return 'read_write'

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionBucketAccess:
        if value in ['r', 'read_only']:
            return cls.ReadOnly
        elif value in ['rw', 'read_write']:
            return cls.ReadWrite
        else:
            return cls.Unknown


class EventingFunctionDcpBoundary(Enum):
    """Indicates where to start DCP stream from.

    Values:

    Everything (default)
        Start DCP stream from the beginning.
    FromNow
        Start DCP stream from present point.
    Unknown
        Couchbase Server returned a boundary the SDK does not recognize.
    """

    Everything = "everything"
    FromNow = "from_now"
    Unknown = "unknown"

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionDcpBoundary:
        if value == "everything":
            return cls.Everything
        elif value == "from_now":
            return cls.FromNow
        else:
            return cls.Unknown


class EventingFunctionDeploymentStatus(Enum):
    """Indicates the eventing function's deployment status.

    Values:

    Undeployed (default)
        Indicates the function is not deployed.
    Deployed
        Indicates the function is deployed.
    Unknown
        Couchbase Server returned a status the SDK does not recognize.
    """

    Undeployed = False
    Deployed = True
    Unknown = "unknown"

    def to_str(self) -> str:
        if self is EventingFunctionDeploymentStatus.Unknown:
            return 'unknown'
        if self.value:
            return 'deployed'

        return 'undeployed'

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionDeploymentStatus:
        if value == 'undeployed':
            return cls.Undeployed
        elif value == 'deployed':
            return cls.Deployed
        else:
            return cls.Unknown


class EventingFunctionProcessingStatus(Enum):
    """Indicates if the eventing function is running

    Allowed values:

    Paused (default)
        Indicates the eventing function is not running.
    Running
        Indicates the eventing function is running.
    Unknown
        Couchbase Server returned a status the SDK does not recognize.
    """

    Paused = False
    Running = True
    Unknown = "unknown"

    def to_str(self) -> str:
        if self is EventingFunctionProcessingStatus.Unknown:
            return 'unknown'
        if self.value:
            return 'running'

        return 'paused'

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionProcessingStatus:
        if value == 'paused':
            return cls.Paused
        elif value == 'running':
            return cls.Running
        else:
            return cls.Unknown


class EventingFunctionLogLevel(Enum):
    """Indicates the system logs level of detail.

    Allowed values:

    Info (default)
    Error
    Warning
    Debug
    Trace
    Unknown
        Couchbase Server returned a level the SDK does not recognize.
    """

    Info = "INFO"
    Error = "ERROR"
    Warning = "WARNING"
    Debug = "DEBUG"
    Trace = "TRACE"
    Unknown = "UNKNOWN"

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionLogLevel:
        if not isinstance(value, str):
            return cls.Unknown
        if value.upper() == "INFO":
            return cls.Info
        elif value.upper() == "ERROR":
            return cls.Error
        elif value.upper() == "WARNING":
            return cls.Warning
        elif value.upper() == "DEBUG":
            return cls.Debug
        elif value.upper() == "TRACE":
            return cls.Trace
        else:
            return cls.Unknown


class EventingFunctionLanguageCompatibility(Enum):
    """Eventing language version the eventing function assumes with respect to syntax and behavior.

    Allowed values:

    Version_6_0_0
    Version_6_5_0
    Version_6_6_2
    Version_7_2_0
    Unknown
        Couchbase Server returned a version the SDK does not recognize.
    """

    Version_6_0_0 = "6.0.0"
    Version_6_5_0 = "6.5.0"
    Version_6_6_2 = "6.6.2"
    Version_7_2_0 = "7.2.0"
    Unknown = "unknown"

    def to_str(self) -> str:
        if self is EventingFunctionLanguageCompatibility.Unknown:
            return 'unknown'
        return f"version_{self.value.replace('.', '_')}"

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionLanguageCompatibility:
        if value == "version_6_0_0":
            return cls.Version_6_0_0
        elif value == "version_6_5_0":
            return cls.Version_6_5_0
        elif value == "version_6_6_2":
            return cls.Version_6_6_2
        elif value == "version_7_2_0":
            return cls.Version_7_2_0
        else:
            return cls.Unknown


class EventingFunctionState(Enum):
    """Indicates the eventing function's composite status.

    Values:

    Undeployed (default)
        Indicates the function is not deployed.
    Deploying
        Indicates the function is deploying.
    Deployed
        Indicates the function is deployed.
    Undeploying
        Indicates the function is undeploying.
    Paused
        Indicates the function is paused.
    Pausing
        Indicates the function is pausing.
    Resuming
        Indicates the function is resuming.
    Unknown
        Indicates the function is in an unknown state.
    """

    Undeployed = "undeployed"
    Deployed = "deployed"
    Deploying = "deploying"
    Undeploying = "undeploying"
    Paused = "paused"
    Pausing = "pausing"
    Resuming = "resuming"
    Unknown = "unknown"

    @classmethod
    def from_server(cls, value: str) -> EventingFunctionState:
        if value == "undeployed":
            return cls.Undeployed
        elif value == "deployed":
            return cls.Deployed
        elif value == "deploying":
            return cls.Deploying
        elif value == "undeploying":
            return cls.Undeploying
        elif value == "paused":
            return cls.Paused
        elif value == "pausing":
            return cls.Pausing
        elif value == "resuming":
            return cls.Resuming
        else:
            return cls.Unknown


@dataclass
class EventingFunctionKeyspace:
    """Object representation for a Couchbase Server eventing keyspace

    A keyspace is a triple consisting of the following
    components: bucket (required), scope (optional) and
    collection (optional)

    Args:
        bucket (str): bucket name
        scope (str, optional): scope name
        collection (str, optional): collection name
    """
    bucket: str | None = None
    scope: str | None = None
    collection: str | None = None

    def as_dict(self) -> dict[str, Any]:
        output: dict[str, Any] = {
            'bucket': self.bucket,
        }
        if self.scope:
            output['scope'] = self.scope

        if self.collection:
            output['collection'] = self.collection

        return output

    @classmethod
    def from_server(cls,
                    bucket: str,
                    scope: str | None = None,
                    collection: str | None = None) -> EventingFunctionKeyspace:
        """Returns a new `EventingFunctionKeyspace` object based
        on the JSON response received from Couchbase Server

        Args:
            bucket (str): bucket name
            scope (str, optional): scope name
            collection (str, optional): collection name

        Returns:
            EventingFunctionKeyspace: new `EventingFunctionKeyspace` object
        """
        keyspace = cls(bucket)
        if not is_null_or_empty(scope) and scope != "_default":
            keyspace.scope = scope

        if not is_null_or_empty(collection) and collection != "_default":
            keyspace.collection = collection

        return keyspace


@dataclass
class EventingFunctionBucketBinding:
    """Object representation for an eventing function bucket binding

    Args:
        alias (str): binding's alias
        name (EventingFunctionKeyspace): binding's keyspace which consists of the bucket name, scope (optional) and
            collection (optional)
        access (EventingFunctionBucketAccess): binding's bucket access
    """

    alias: str | None = None
    name: EventingFunctionKeyspace | None = None
    access: EventingFunctionBucketAccess | None = None

    def as_dict(self) -> dict[str, Any]:
        """Returns a representation of the `EventingFunctionBucketBinding` that
        aligns with what Couchbase Server eventing spec.

        Returns:
            dict[str, Any]: dict representation of the `EventingFunctionBucketBinding`
        """
        if self.name is None:
            raise InvalidArgumentException('An EventingFunctionBucketBinding must have a name.')
        output: dict[str, Any] = {'alias': self.alias, 'name': self.name.as_dict()}
        if self.access is not None:
            output['access'] = self.access.to_str()

        return output

    @classmethod
    def from_server(cls, bucket_binding: dict[str, Any]) -> EventingFunctionBucketBinding:
        alias = bucket_binding.get('alias', None)
        name = bucket_binding.get('name', {})
        keyspace = EventingFunctionKeyspace.from_server(**name)
        access = EventingFunctionBucketAccess.from_server(bucket_binding['access'])
        return cls(alias=alias, name=keyspace, access=access)


@dataclass
class EventingFunctionUrlAuth:
    """Base class for all object representation of eventing function URL binding
    authorization types
    """
    pass


@dataclass
class EventingFunctionUrlNoAuth(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    with no authorization
    """
    pass


@dataclass
class EventingFunctionUrlAuthBasic(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    basic authorization

    Args:
        username (str): auth username
        password (str): auth password
    """

    username: str | None = None
    password: str | None = None


@dataclass
class EventingFunctionUrlAuthDigest(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    digest authorization

    Args:
        username (str): auth digest username
        password (str): auth digest password
    """

    username: str | None = None
    password: str | None = None


@dataclass
class EventingFunctionUrlAuthBearer(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    bearer authorization

    Args:
        key (str): bearer key
    """

    key: str | None = None


@dataclass
class EventingFunctionUrlBinding:
    """Object representation for an eventing function URL binding

    Args:
        hostname (str): binding's hostname
        alias (str): binding's alias
        allow_cookies (bool): If the binding should allow cookies
        validate_ssl_certificate (bool): If the binding should validate SSL cert
        auth (EventingFunctionUrlAuth): binding's authorization type
    """

    hostname: str | None = None
    alias: str | None = None
    allow_cookies: bool | None = None
    validate_ssl_certificate: bool | None = None
    auth: EventingFunctionUrlAuth | None = None

    def as_dict(self) -> dict[str, Any]:
        """Returns a representation of the `EventingFunctionUrlBinding` that
        aligns with what Couchbase Server eventing spec.

        Returns:
            dict[str, Any]: dict representation of the `EventingFunctionUrlBinding`
        """
        output: dict[str, Any] = {
            "hostname": self.hostname,
            "alias": self.alias,
            "allow_cookies": self.allow_cookies,
            "validate_ssl_certificate": self.validate_ssl_certificate,
        }

        # The binding layer reads auth as a tagged dict; the tag key matches the discriminator
        # configured for the variant in tools/autogen/config/bindings.yaml.
        if isinstance(self.auth, EventingFunctionUrlNoAuth):
            output["auth"] = {"auth_type": "no-auth"}
        elif isinstance(self.auth, EventingFunctionUrlAuthBasic):
            output["auth"] = {"auth_type": "basic",
                              "username": self.auth.username,
                              "password": self.auth.password}
        elif isinstance(self.auth, EventingFunctionUrlAuthDigest):
            output["auth"] = {"auth_type": "digest",
                              "username": self.auth.username,
                              "password": self.auth.password}
        elif isinstance(self.auth, EventingFunctionUrlAuthBearer):
            output["auth"] = {"auth_type": "bearer", "key": self.auth.key}

        return output

    @classmethod
    def from_server(cls, url_binding: dict[str, Any]) -> EventingFunctionUrlBinding:
        input: dict[str, Any] = {
            'alias': url_binding.get('alias', None),
            'hostname': url_binding.get('hostname', None),
            'allow_cookies': url_binding.get('allow_cookies', None),
            'validate_ssl_certificate': url_binding.get('validate_ssl_certificate', None)
        }
        auth = url_binding.get('auth', None) or {}
        auth_type = auth.get('auth_type', 'no-auth')
        if auth_type == 'no-auth':
            input['auth'] = EventingFunctionUrlNoAuth()
        elif auth_type == 'basic':
            username = auth.get('username', None)
            input['auth'] = EventingFunctionUrlAuthBasic(username=username)
        elif auth_type == 'digest':
            username = auth.get('username', None)
            input['auth'] = EventingFunctionUrlAuthDigest(username=username)
        elif auth_type == 'bearer':
            key = auth.get('key', None)
            input['auth'] = EventingFunctionUrlAuthBearer(key=key)
        return cls(**input)


@dataclass
class EventingFunctionConstantBinding:
    """Object representation for an eventing function constant binding

    Args:
        alias (str): binding's alias
        literal (str): binding's value
    """

    alias: str | None = None
    literal: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Returns a representation of the `EventingFunctionConstantBinding` that
        aligns with what Couchbase Server eventing spec.

        Returns:
            dict[str, Any]: dict representation of the `EventingFunctionConstantBinding`
        """

        return {"alias": self.alias, "literal": self.literal}

    @classmethod
    def from_server(cls, constant_binding: dict[str, Any]) -> EventingFunctionConstantBinding:
        input = {
            'alias': constant_binding.get('alias', None),
            'literal': constant_binding.get('literal', None)
        }
        return cls(**input)


@dataclass
class EventingFunctionSettings:
    """Object representation for an settings relevant to an eventing function

    Args:
        cpp_worker_thread_count (int): Number of threads each worker utilizes
        description (str): Free form text for user to describe the eventing function
        execution_timeout (timedelta): Maximum time the eventing function can run before it is forcefully terminated (in
            seconds)
        lcb_inst_capacity (int): Maximum number of libcouchbase connections that may be opened and pooled
        lcb_retry_count (int): Number of retries of retriable libcouchbase failures, 0 keeps trying till
            execution_timeout
        lcb_timeout (timedelta): Maximum time the lcb command is waited until completion before we terminate the request
            (in seconds)
        num_timer_partitions (int): Number of timer shards, defaults to number of vbuckets
        sock_batch_size (int): Batch size for messages from producer to consumer, normally not specified
        tick_duration (timedelta): Duration to log stats from this eventing function, in milliseconds
        timer_context_size (int): Size limit of timer context object
        user_prefix (str): Key prefix for all data stored in metadata by this eventing function
        bucket_cache_size (int): Maximum size in bytes the bucket cache can grow to
        bucket_cache_age (timedelta): Time in milliseconds after which a cached bucket object is considered stale
        curl_max_allowed_resp_size (int): maximum allowable curl call response in MegaBytes
        worker_count (int): Number of worker processes eventing function utilizes on each eventing node
        query_prepare_all (bool): Automatically prepare all N1QL statements in the eventing function
        enable_applog_rotation (bool): Enable rotating this eventing function's log() message files
        app_log_dir (str): Directory to write content of log() message files
        app_log_max_size (int): Rotate logs when file grows to this size in bytes approximately
        app_log_max_files (int): Number of log() message files to retain when rotating
        checkpoint_interval (timedelta): Number of seconds before writing a progress checkpoint
        dcp_stream_boundary (EventingFunctionDcpBoundary): indicates where to start dcp stream from (beginning of time
            or present point)
        deployment_status (EventingFunctionDeploymentStatus): Indicates if the function is deployed
        processing_status (EventingFunctionProcessingStatus): Indicates if the function is running
        language_compatibility (EventingFunctionLanguageCompatibility): Eventing language version this eventing function
            assumes in terms of syntax and behavior
        log_level (EventingFunctionLogLevel): Level of detail in system logging
        query_consistency (QueryScanConsistency): Consistency level used by n1ql statements in the
            eventing function
        handler_headers (list[str]): Code to automatically prepend to top of eventing function code
        handler_footers (list[str]): Code to automatically prepend to top of eventing function code
    """

    cpp_worker_thread_count: int | None = None
    description: str | None = None
    execution_timeout: timedelta | None = None
    lcb_inst_capacity: int | None = None
    lcb_retry_count: int | None = None
    lcb_timeout: timedelta | None = None
    num_timer_partitions: int | None = None
    sock_batch_size: int | None = None
    tick_duration: timedelta | None = None
    timer_context_size: int | None = None
    user_prefix: str | None = None
    bucket_cache_size: int | None = None
    bucket_cache_age: timedelta | None = None
    curl_max_allowed_resp_size: int | None = None
    query_prepare_all: bool | None = None
    worker_count: int | None = None
    enable_applog_rotation: bool | None = None
    app_log_dir: str | None = None
    app_log_max_size: int | None = None
    app_log_max_files: int | None = None
    checkpoint_interval: timedelta | None = None
    dcp_stream_boundary: EventingFunctionDcpBoundary | None = None
    deployment_status: EventingFunctionDeploymentStatus | None = None
    processing_status: EventingFunctionProcessingStatus | None = None
    language_compatibility: EventingFunctionLanguageCompatibility | None = None
    log_level: EventingFunctionLogLevel | None = None
    query_consistency: QueryScanConsistency | None = None
    handler_headers: list[str] | None = None
    handler_footers: list[str] | None = None

    def as_dict(self) -> dict[str, Any]:  # noqa: C901
        """Returns a representation of the `EventingFunctionSettings` that
        aligns with what Couchbase Server eventing spec.

        Returns:
            dict[str, Any]: dict representation of the `EventingFunctionSettings`
        """

        output: dict[str, Any] = {}

        if self.cpp_worker_thread_count:
            output['cpp_worker_count'] = self.cpp_worker_thread_count

        if self.dcp_stream_boundary:
            output['dcp_stream_boundary'] = self.dcp_stream_boundary.value

        if self.description:
            output['description'] = self.description

        if self.deployment_status:
            output['deployment_status'] = self.deployment_status.to_str()

        if self.processing_status:
            output['processing_status'] = self.processing_status.to_str()

        if self.log_level:
            output['log_level'] = self.log_level.value.lower()

        if self.language_compatibility:
            output['language_compatibility'] = self.language_compatibility.to_str()

        if self.execution_timeout:
            if not isinstance(self.execution_timeout, timedelta):
                raise InvalidArgumentException(
                    'EventingFunctionSettings execution timeout should be a timedelta'
                )
            output['execution_timeout'] = int(self.execution_timeout.total_seconds())

        if self.lcb_inst_capacity:
            output['lcb_inst_capacity'] = self.lcb_inst_capacity

        if self.lcb_retry_count:
            output['lcb_retry_count'] = self.lcb_retry_count

        if self.lcb_timeout:
            if not isinstance(self.lcb_timeout, timedelta):
                raise InvalidArgumentException(
                    'EventingFunctionSettings lcb timeout should be a timedelta'
                )
            output['lcb_timeout'] = int(self.lcb_timeout.total_seconds())

        if self.query_consistency:
            if self.query_consistency.value in ['not_bounded', 'request_plus']:
                output['query_consistency'] = self.query_consistency.value
            else:
                raise InvalidArgumentException('Only not_bounded and request_plus allowed for query consistency.')

        if self.num_timer_partitions:
            output['num_timer_partitions'] = self.num_timer_partitions

        if self.sock_batch_size:
            output['sock_batch_size'] = self.sock_batch_size

        if self.tick_duration:
            output['tick_duration'] = timedelta_as_milliseconds(self.tick_duration)

        if self.timer_context_size:
            output['timer_context_size'] = self.timer_context_size

        if self.user_prefix:
            output['user_prefix'] = self.user_prefix

        if self.bucket_cache_size:
            output['bucket_cache_size'] = self.bucket_cache_size

        if self.bucket_cache_age:
            output['bucket_cache_age'] = timedelta_as_milliseconds(self.bucket_cache_age)

        if self.curl_max_allowed_resp_size:
            output['curl_max_allowed_resp_size'] = self.curl_max_allowed_resp_size

        if self.query_prepare_all:
            output['query_prepare_all'] = self.query_prepare_all

        if self.worker_count:
            output['worker_count'] = self.worker_count

        if self.handler_headers:
            output['handler_headers'] = self.handler_headers

        if self.handler_footers:
            output['handler_footers'] = self.handler_footers

        if self.enable_applog_rotation:
            output['enable_app_log_rotation'] = self.enable_applog_rotation

        if self.app_log_dir:
            output['app_log_dir'] = self.app_log_dir

        if self.app_log_max_size:
            output['app_log_max_size'] = self.app_log_max_size

        if self.app_log_max_files:
            output['app_log_max_files'] = self.app_log_max_files

        if self.checkpoint_interval:
            if not isinstance(self.checkpoint_interval, timedelta):
                raise InvalidArgumentException(
                    'EventingFunctionSettings checkpoint interval should be a timedelta'
                )
            output['checkpoint_interval'] = int(self.checkpoint_interval.total_seconds())

        return output

    @classmethod
    def new_settings(cls, **kwargs: Any) -> EventingFunctionSettings:
        """Returns a new `EventingFunctionSettings` object

        Args:
            kwargs (dict): Keyword arguments to populate `EventingFunctionSettings` object

        Returns:
            EventingFunctionSettings: new `EventingFunctionSettings` object
        """
        if "dcp_stream_boundary" not in kwargs:
            kwargs["dcp_stream_boundary"] = None

        if "deployment_status" not in kwargs:
            kwargs["deployment_status"] = EventingFunctionDeploymentStatus.Undeployed

        if "processing_status" not in kwargs:
            kwargs["processing_status"] = EventingFunctionProcessingStatus.Paused

        if "language_compatibility" not in kwargs:
            kwargs["language_compatibility"] = None

        if "log_level" not in kwargs:
            kwargs["log_level"] = None

        if "query_consistency" not in kwargs:
            kwargs["query_consistency"] = None

        settings_fields = [f.name for f in fields(cls)]
        final_json = {k: v for k, v in kwargs.items() if k in settings_fields}
        return cls(**final_json)

    @classmethod
    def from_server(cls, settings: dict[str, Any]) -> EventingFunctionSettings:  # noqa: C901
        input: dict[str, Any] = {
            'cpp_worker_thread_count': settings.get('cpp_worker_count', None),
            'description': settings.get('description', None),
            'lcb_inst_capacity': settings.get('lcb_inst_capacity', None),
            'lcb_retry_count': settings.get('lcb_retry_count', None),
            'num_timer_partitions': settings.get('num_timer_partitions', None),
            'sock_batch_size': settings.get('sock_batch_size', None),
            'timer_context_size': settings.get('timer_context_size', None),
            'user_prefix': settings.get('user_prefix', None),
            'bucket_cache_size': settings.get('bucket_cache_size', None),
            'curl_max_allowed_resp_size': settings.get('curl_max_allowed_resp_size', None),
            'query_prepare_all': settings.get('query_prepare_all', None),
            'worker_count': settings.get('worker_count', None),
            'handler_headers': settings.get('handler_headers', None),
            'handler_footers': settings.get('handler_footers', None),
            'enable_applog_rotation': settings.get('enable_app_log_rotation', None),
            'app_log_dir': settings.get('app_log_dir', None),
            'app_log_max_size': settings.get('app_log_max_size', None),
            'app_log_max_files': settings.get('app_log_max_files', None)
        }

        dcp_stream_boundary = settings.get('dcp_stream_boundary', None)
        if dcp_stream_boundary:
            input['dcp_stream_boundary'] = EventingFunctionDcpBoundary.from_server(dcp_stream_boundary)

        deployment_status = settings.get('deployment_status', None)
        if deployment_status:
            input['deployment_status'] = EventingFunctionDeploymentStatus.from_server(deployment_status)

        processing_status = settings.get('processing_status', None)
        if processing_status:
            input['processing_status'] = EventingFunctionProcessingStatus.from_server(processing_status)

        log_level = settings.get('log_level', None)
        if log_level:
            input['log_level'] = EventingFunctionLogLevel.from_server(log_level)

        language_compatibility = settings.get('language_compatibility', None)
        if language_compatibility:
            input['language_compatibility'] = EventingFunctionLanguageCompatibility.from_server(language_compatibility)

        query_consistency = settings.get('query_consistency', None)
        if query_consistency:
            if query_consistency == 'not_bounded':
                input['query_consistency'] = QueryScanConsistency.NOT_BOUNDED
            elif query_consistency == 'request_plus':
                input['query_consistency'] = QueryScanConsistency.REQUEST_PLUS

        execution_timeout = settings.get('execution_timeout', None)
        if execution_timeout:
            input['execution_timeout'] = timedelta(seconds=execution_timeout)

        lcb_timeout = settings.get('lcb_timeout', None)
        if lcb_timeout:
            input['lcb_timeout'] = timedelta(seconds=lcb_timeout)

        tick_duration = settings.get('tick_duration', None)
        if tick_duration:
            input['tick_duration'] = timedelta(milliseconds=tick_duration)

        bucket_cache_age = settings.get('bucket_cache_age', None)
        if bucket_cache_age:
            input['bucket_cache_age'] = timedelta(milliseconds=bucket_cache_age)

        checkpoint_interval = settings.get('checkpoint_interval', None)
        if checkpoint_interval:
            input['checkpoint_interval'] = timedelta(seconds=checkpoint_interval)

        return cls(**input)


@dataclass
class EventingFunction:
    """Object representation for settings relevant to an eventing function

    Args:
        name (str):
        code (str):
        version (str):
        enforce_schema (bool):
        handler_uuid (int):
        function_instance_id (str):
        metadata_keyspace (EventingFunctionKeyspace):
        source_keyspace (EventingFunctionKeyspace):
        bucket_bindings (list[EventingFunctionBucketBinding]):
        url_bindings (list[EventingFunctionUrlBinding]):
        constant_bindings (list[EventingFunctionConstantBinding]):
        settings (EventingFunctionSettings):
    """

    name: str | None = None
    code: str | None = None
    version: str | None = None
    enforce_schema: bool | None = None
    handler_uuid: int | None = None
    function_instance_id: str | None = None
    metadata_keyspace: EventingFunctionKeyspace | None = None
    source_keyspace: EventingFunctionKeyspace | None = None
    bucket_bindings: list[EventingFunctionBucketBinding] | None = None
    url_bindings: list[EventingFunctionUrlBinding] | None = None
    constant_bindings: list[EventingFunctionConstantBinding] | None = None
    settings: EventingFunctionSettings | None = None

    def as_dict(self) -> dict[str, Any]:  # noqa: C901
        """Returns a representation of the `EventingFunction` that
        aligns with what Couchbase Server eventing spec.

        Returns:
            dict[str, Any]: dict representation of the `EventingFunction`
        """

        output: dict[str, Any] = {}

        if self.name:
            output['name'] = self.name

        if self.code:
            output['code'] = self.code

        if self.version:
            output['version'] = self.version

        if self.enforce_schema:
            output['enforce_schema'] = self.enforce_schema

        if self.handler_uuid:
            output['handler_uuid'] = self.handler_uuid

        if self.function_instance_id:
            output['function_instance_id'] = self.function_instance_id

        if self.metadata_keyspace:
            output['metadata_keyspace'] = self.metadata_keyspace.as_dict()

        if self.source_keyspace:
            output['source_keyspace'] = self.source_keyspace.as_dict()

        if self.bucket_bindings:
            output['bucket_bindings'] = [b.as_dict() for b in self.bucket_bindings]

        if self.url_bindings:
            output['url_bindings'] = [b.as_dict() for b in self.url_bindings]

        if self.constant_bindings:
            output['constant_bindings'] = [b.as_dict() for b in self.constant_bindings]

        if self.settings:
            output['settings'] = self.settings.as_dict()

        return output

    @classmethod
    def from_server(cls, eventing_function: dict[str, Any]) -> EventingFunction:
        input: dict[str, Any] = {
            'name': eventing_function['name'],
            'code': eventing_function['code'],
            'version': eventing_function.get('version', None),
            'enforce_schema': eventing_function.get('enforce_schema', None),
            'handler_uuid': eventing_function.get('handler_uuid', None),
            'function_instance_id': eventing_function.get('function_instance_id', None)
        }

        metadata_keyspace = eventing_function.get('metadata_keyspace', None)
        if metadata_keyspace:
            input['metadata_keyspace'] = EventingFunctionKeyspace.from_server(**metadata_keyspace)

        source_keyspace = eventing_function.get('source_keyspace', None)
        if source_keyspace:
            input['source_keyspace'] = EventingFunctionKeyspace.from_server(**source_keyspace)

        bucket_bindings = eventing_function.get('bucket_bindings', None)
        if bucket_bindings:
            input['bucket_bindings'] = [EventingFunctionBucketBinding.from_server(b) for b in bucket_bindings]

        url_bindings = eventing_function.get('url_bindings', None)
        if url_bindings:
            input['url_bindings'] = [EventingFunctionUrlBinding.from_server(b) for b in url_bindings]

        constant_bindings = eventing_function.get('constant_bindings', None)
        if constant_bindings:
            input['constant_bindings'] = [EventingFunctionConstantBinding.from_server(b) for b in constant_bindings]

        settings = eventing_function.get('settings', None)
        if settings:
            input['settings'] = EventingFunctionSettings.from_server(settings)

        return cls(**input)


@dataclass
class EventingFunctionStatus:
    """Object representation for the status of an eventing function

    Particularly useful when determining an interim state of an evening function (i.e,
    deploying, pausing, undeploying).

    Args:
        name (str): eventing function's name
        num_bootstrapping_nodes (int): number of nodes for bootstrapping
        num_deployed_nodes (int): number of nodes eventing function is deployed on
        state (EventingFunctionState): composite status of eventing function
        deployment_status (EventingFunctionDeploymentStatus): Indicates if the function is deployed
        processing_status (EventingFunctionProcessingStatus): Indicates if the function is running
        redeploy_required (bool): Indicates if function needs to be redeployed
        function_scope (dict[str, Any]): Indicates the eventing function's scope
    """

    name: str | None = None
    num_bootstrapping_nodes: int | None = None
    num_deployed_nodes: int | None = None
    state: EventingFunctionState | None = None
    deployment_status: EventingFunctionDeploymentStatus | None = None
    processing_status: EventingFunctionProcessingStatus | None = None
    redeploy_required: bool | None = None
    function_scope: dict[str, Any] | None = None

    @classmethod
    def from_server(cls, function_status: dict[str, Any]) -> EventingFunctionStatus:
        input: dict[str, Any] = {
            'name': function_status.get('name', None),
            'num_bootstrapping_nodes': function_status.get('num_bootstrapping_nodes', None),
            'num_deployed_nodes': function_status.get('num_deployed_nodes', None),
            'redeploy_required': function_status.get('redeploy_required', None),
            'function_scope': function_status.get('function_scope', None)
        }

        state = function_status.get('status', None)
        if state:
            input['state'] = EventingFunctionState.from_server(state)

        deployment_status = function_status.get('deployment_status', None)
        if deployment_status:
            input['deployment_status'] = EventingFunctionDeploymentStatus.from_server(deployment_status)

        processing_status = function_status.get('processing_status', None)
        if processing_status:
            input['processing_status'] = EventingFunctionProcessingStatus.from_server(processing_status)

        return cls(**input)


@dataclass
class EventingFunctionsStatus:
    """Object representation for statuses for all eventing functions

    Args:
        num_eventing_nodes (int):
        functions (list[EventingFunctionStatus]):
    """

    num_eventing_nodes: int | None = None
    functions: list[EventingFunctionStatus] | None = None

    @classmethod
    def from_server(cls, function_status: dict[str, Any]) -> EventingFunctionsStatus:
        """Returns a new `EventingFunctionsStatus` object based
        on the dict[str, Any] response received from Couchbase Server

        Args:
            function_status (dict): dict[str, Any]

        Returns:
            EventingFunctionsStatus: new `EventingFunctionsStatus` object
        """
        input: dict[str, Any] = {
            'num_eventing_nodes': function_status.get('num_eventing_nodes', None)
        }
        functions = function_status.get('functions', None)
        if functions:
            input['functions'] = [EventingFunctionStatus.from_server(f) for f in functions]

        return cls(**input)
