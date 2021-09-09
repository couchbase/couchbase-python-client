from abc import ABC
import json
from datetime import timedelta
from typing import Any, List, Dict, Optional
import attr
from enum import Enum

from .generic import GenericManager
from ..options import OptionBlockTimeOut, forward_args
from ..cluster import QueryScanConsistency
from couchbase.management.admin import METHMAP
from couchbase.exceptions import (
    ErrorMapper,
    HTTPException,
    InvalidArgumentException,
    InvalidException,
    EventingFunctionNotFoundException,
    EventingFunctionCompilationFailureException,
    EventingFunctionNotBootstrappedException,
    EventingFunctionNotDeployedException,
    EventingFunctionNotUnDeployedException,
    EventingFunctionCollectionNotFoundException,
    EventingFunctionAlreadyDeployedException,
)
import couchbase_core._libcouchbase as LCB


class EventingFunctionErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...)->Mapping[str, CBErrorType]
        return {
            HTTPException: {
                ".*ERR_APP_NOT_FOUND_TS": EventingFunctionNotFoundException,
                ".*ERR_HANDLER_COMPILATION": EventingFunctionCompilationFailureException,
                ".*ERR_APP_NOT_BOOTSTRAPPED": EventingFunctionNotBootstrappedException,
                ".*ERR_APP_NOT_DEPLOYED": EventingFunctionNotDeployedException,
                ".*ERR_APP_NOT_UNDEPLOYED": EventingFunctionNotUnDeployedException,
                ".*ERR_COLLECTION_MISSING": EventingFunctionCollectionNotFoundException,
                ".*ERR_APP_ALREADY_DEPLOYED": EventingFunctionAlreadyDeployedException,
            }
        }


@EventingFunctionErrorHandler.wrap
class EventingFunctionManager(GenericManager):
    """Eventing Function Manager

    **UNCOMMITTED**
    The EventingFunctionManager is an uncommitted API that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

    Programmatic access to the user management REST API:
    https://docs.couchbase.com/server/current/eventing/eventing-api.html

    Methods:

    upsert_function()
        Upserts an eventing function
    drop_function()
        Drops an existing eventing function
    deploy_function()
        Deploys an existing eventing function
    get_all_functions()
        Returns a list of all eventing functions
    get_function()
        Returns specified eventing function
    pause_function()
        Pauses an existing eventing function
    resume_function()
        Resumes an existing eventing function
    undeploy_function()
        Undeploys an existing eventing function
    """

    def __init__(self, admin_bucket):
        super(EventingFunctionManager, self).__init__(admin_bucket)

    def _http_request(self, **kwargs):
        # the kwargs can override the defaults
        imeth = None
        method = kwargs.get("method", "GET")
        if not method in METHMAP:
            raise InvalidArgumentException("Unknown HTTP Method", method)

        imeth = METHMAP[method]
        return self._admin_bucket._http_request(
            type=LCB.LCB_HTTP_TYPE_EVENTING,
            path=kwargs["path"],
            method=imeth,
            content_type=kwargs.get("content_type", "application/json"),
            post_data=kwargs.get("content", None),
            response_format=LCB.FMT_JSON,
            timeout=kwargs.get("timeout", None),
        )

    def _get_status(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
    ) -> "EventingFunctionStatus":

        statuses = self.functions_status()

        return next((f for f in statuses.functions if f.name == name), None)

    def upsert_function(
        self,  # type: "EventingFunctionManager"
        function,  # type: "EventingFunction"
        *options,  # type: "UpsertFunctionOptions"
        **kwargs  # type: Any
    ) -> None:
        """
        Upserts an eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param function: the EventingFunction to upsert
        :param options: UpsertFunctionOptions to upsert an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionCompilationFailureException
        :raises: EventingFunctionCollectionNotFoundException
        :raises: InvalidArgumentException
        Any exceptions raised by the underlying platform
        """
        func_json = function.to_server_dict(to_json=True)
        self._http_request(
            path="api/v1/functions/{}".format(function.name),
            method="POST",
            content=func_json,
            **forward_args(kwargs, *options)
        )

    def drop_function(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
        *options,  # type: "DropFunctionOptions"
        **kwargs  # type: Any
    ) -> None:
        """
        Drops an existing eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to drop
        :param options: DropFunctionOptions to drop an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        :raises: EventingFunctionNotUndeployedException
        Any exceptions raised by the underlying platform
        """
        self._http_request(
            path="api/v1/functions/{}".format(name),
            method="DELETE",
            **forward_args(kwargs, *options)
        )

    def deploy_function(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
        *options,  # type: "DeployFunctionOptions"
        **kwargs  # type: Any
    ) -> None:
        """
        Deploys an existing eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to drop
        :param options: DeployFunctionOptions to deploy an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotDeployedException
        :raises: EventingFunctionAlreadyDeployedException
        Any exceptions raised by the underlying platform
        """
        self._http_request(
            path="api/v1/functions/{}/deploy".format(name),
            method="POST",
            **forward_args(kwargs, *options)
        )

    def get_all_functions(
        self,  # type: "EventingFunctionManager"
        *options,  # type: "GetAllFunctionOptions"
        **kwargs  # type: Any
    ) -> List["EventingFunction"]:
        """
        Returns a list of all eventing functions

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param options: GetAllFunctionOptions to get all eventing functions
        :param kwargs: Override corresponding value in options

        :raises:
        Any exceptions raised by the underlying platform

        :return: A list :class:`EventingFunction` objects
        :rtype: list
        """
        functions = self._http_request(
            path="api/v1/functions/", method="GET", **forward_args(kwargs, *options)
        ).value

        return [EventingFunction.from_server(func) for func in functions]

    def get_function(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
        *options,  # type: "GetFunctionOptions"
        **kwargs  # type: Any
    ) -> "EventingFunction":
        """
        Returns specified eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to retreive
        :param options: GetFunctionOptions to get an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        Any exceptions raised by the underlying platform

        :return: class:`EventingFunction` object
        :rtype: class:`EventingFunction`
        """
        response = self._http_request(
            path="api/v1/functions/{}".format(name),
            method="GET",
            **forward_args(kwargs, *options)
        ).value

        return EventingFunction.from_server(response)

    def pause_function(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
        *options,  # type: "PauseFunctionOptions"
        **kwargs  # type: Any
    ) -> None:
        """
        Pauses an existing eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to pause
        :param options: PauseFunctionOptions to pause an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        :raises: EventingFunctionNotBootstrappedException
        Any exceptions raised by the underlying platform
        """
        self._http_request(
            path="api/v1/functions/{}/pause".format(name),
            method="POST",
            **forward_args(kwargs, *options)
        )

    def resume_function(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
        *options,  # type: "ResumeFunctionOptions"
        **kwargs  # type: Any
    ) -> None:
        """
        Resumes an existing eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to resume
        :param options: ResumeFunctionOptions to resume an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        :raises: EventingFunctionNotDeployedException
        Any exceptions raised by the underlying platform
        """
        self._http_request(
            path="api/v1/functions/{}/resume".format(name),
            method="POST",
            **forward_args(kwargs, *options)
        )

    def undeploy_function(
        self,  # type: "EventingFunctionManager"
        name,  # type: str
        *options,  # type: "UndeployFunctionOptions"
        **kwargs  # type: Any
    ) -> None:
        """
        Undeploys an existing eventing function

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to drop
        :param options: UndeployFunctionOptions to undeploy an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotDeployedException
        Any exceptions raised by the underlying platform
        """
        self._http_request(
            path="api/v1/functions/{}/undeploy".format(name),
            method="POST",
            **forward_args(kwargs, *options)
        )

    def functions_status(
        self,  # type: "EventingFunctionManager"
        *options,  # type: "FunctionsStatusOptions"
        **kwargs  # type: Any
    ) -> "EventingFunctionsStatus":
        """
        Returns the `EventingFunctionStatus` of all eventing functions

        **UNCOMMITTED** 
        This is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.

        :param options: UndeployFunctionOptions to undeploy an eventing function
        :param kwargs: Override corresponding value in options

        :raises:
            Any exceptions raised by the underlying platform

        :return: class:`EventingFunctionsStatus` object
        :rtype: class:`EventingFunctionsStatus`
        """
        response = self._http_request(
            path="api/v1/status/", method="GET", **forward_args(kwargs, *options)
        ).value
        return EventingFunctionsStatus.from_server(response)


class EventingFunctionBucketAccess(Enum):
    """Indicates the bucket access of the eventing function.

    Values:

    ReadOnly
        The eventing function has read-only bucket access
    ReadWrite (default)
        The eventing function has read and write bucket access.
    """

    ReadOnly = "r"
    ReadWrite = "rw"

    @classmethod
    def from_server(cls, value):
        if value == "r":
            return cls.ReadOnly
        elif value == "rw":
            return cls.ReadWrite
        else:
            raise InvalidArgumentException(
                "Invalid value for bucket access: {}".format(value)
            )


class EventingFunctionDcpBoundary(Enum):
    """Indicates where to start DCP stream from.

    Values:

    Everything (default)
        Start DCP stream from the beginning.
    FromNow
        Start DCP stream from present point.
    """

    Everything = "everything"
    FromNow = "from_now"

    @classmethod
    def from_server(cls, value):
        if value == "everything":
            return cls.Everything
        elif value == "from_now":
            return cls.FromNow
        else:
            raise InvalidArgumentException(
                "Invalid value for DCP boundary: {}".format(value)
            )


class EventingFunctionDeploymentStatus(Enum):
    """Indicates the eventing function's deployment status.

    Values:

    Undeployed (default)
        Indicates the function is not deployed.
    Deployed
        Indicates the function is deployed.
    """

    Undeployed = False
    Deployed = True

    @classmethod
    def from_server(cls, value):
        if value is False:
            return cls.Undeployed
        elif value is True:
            return cls.Deployed
        else:
            raise InvalidArgumentException(
                "Invalid value for deployment status: {}".format(value)
            )


class EventingFunctionProcessingStatus(Enum):
    """Indicates if the eventing function is running

    Allowed values:

    Paused (default)
        Indicates the eventing function is not running.
    Running
        Indicates the eventing function is running.
    """

    Paused = False
    Running = True

    @classmethod
    def from_server(cls, value):
        if value is False:
            return cls.Paused
        elif value is True:
            return cls.Running
        else:
            raise InvalidArgumentException(
                "Invalid value for processing status: {}".format(value)
            )


class EventingFunctionLogLevel(Enum):
    """Indicates the system logs level of detail.

    Allowed values:

    Info (default)
    Error
    Warning
    Debug
    Trace
    """

    Info = "INFO"
    Error = "ERROR"
    Warning = "WARNING"
    Debug = "DEBUG"
    Trace = "TRACE"

    @classmethod
    def from_server(cls, value):
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
            raise InvalidArgumentException(
                "Invalid value for log level: {}".format(value)
            )


class EventingFunctionLanguageCompatibility(Enum):
    """Eventing language version the eventing function assumes with respect to syntax and behavior.

    Allowed values:

    Version_6_0_0
    Version_6_5_0
    Version_6_6_2 (default)
    """

    Version_6_0_0 = "6.0.0"
    Version_6_5_0 = "6.5.0"
    Version_6_6_2 = "6.6.2"

    @classmethod
    def from_server(cls, value):
        if value == "6.0.0":
            return cls.Version_6_0_0
        elif value == "6.5.0":
            return cls.Version_6_5_0
        elif value == "6.6.2":
            return cls.Version_6_6_2
        else:
            raise InvalidArgumentException(
                "Invalid value for language compatibility: {}".format(value)
            )


@attr.s
class EventingFunctionKeyspace(object):
    """Object representation for a Couchbase Server eventing keyspace

    A keyspace is a triple consisting of the following
    components: bucket (required), scope (optional) and
    collection (optional)

    :param bucket: bucket name
    :type bucket: str
    :param scope: scope name
    :type scope: str, optional
    :param collection: collection name
    :type collection: str, optional
    """

    bucket = attr.ib(type=str, validator=attr.validators.instance_of(str))
    scope = attr.ib(
        type=str,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    collection = attr.ib(
        type=str,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )

    @classmethod
    def from_server(
        cls,  # type: "EventingFunctionKeyspace"
        bucket,  # type: str
        scope=None,  # type: Optional[str]
        collection=None,  # type: Optional[str]
    ) -> "EventingFunctionKeyspace":
        """Returns a new `EventingFunctionKeyspace` object based
        on the JSON response received from Couchbase Server

        :param bucket: bucket name
        :type bucket: str
        :param scope: scope name
        :type scope: str, optional
        :param collection: collection name
        :type collection: str, optional

        :return: new `EventingFunctionKeyspace` object
        :rtype: `EventingFunctionKeyspace`
        """
        keyspace = cls(bucket)
        if scope and scope.split() and scope != "_default":
            keyspace.scope = scope

        if collection and collection.split() and collection != "_default":
            keyspace.collection = collection

        return keyspace


@attr.s
class EventingFunctionBucketBinding(object):
    """Object representation for an eventing function bucket binding

    :param alias: binding's alias
    :type alias: str
    :param name: binding's keyspace which consists of the bucket name, scope (optional) and collection (optional)
    :type name: class:`couchbase.management.EventingFunctionKeyspace`
    :param access: binding's bucket access
    :type access: class:`EventingFunctionBucketAccess`
    """

    alias = attr.ib(type=str)
    name = attr.ib(factory=EventingFunctionKeyspace, type=EventingFunctionKeyspace)
    access = attr.ib(
        factory=EventingFunctionBucketAccess, type=EventingFunctionBucketAccess
    )

    def to_server_dict(
        self,  # type: "EventingFunctionBucketBinding"
        to_json=False,  # type: bool
    ) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionBucketBinding` that
        aligns with what Couchbase Server eventing spec.

        :param to_json: If True, convert dict representation to JSON, otherwise return dict
        :type to_json: bool

        :return: dict or JSON str representation of the `EventingFunctionBucketBinding`
        :rtype: Dict[str, Any]
        """
        server_dict = {"bucket_name": self.name.bucket}

        if self.name.scope and self.name.scope.split():
            server_dict["scope_name"] = self.name.scope
        if self.name.collection and self.name.collection.split():
            server_dict["collection_name"] = self.name.collection

        server_dict["alias"] = self.alias
        server_dict["access"] = self.access.value

        return json.dumps(server_dict) if to_json is True else server_dict


@attr.s
class EventingFunctionUrlAuth(object):
    """Base class for all object representation of eventing function URL binding
    authorization types
    """

    pass


@attr.s
class EventingFunctionUrlNoAuth(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    with no authorization
    """


@attr.s
class EventingFunctionUrlAuthBasic(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    basic authorization

    :param username: auth username
    :type username: str
    :param password: auth password
    :type password: str
    """

    username = attr.ib(type=str)
    password = attr.ib(type=str)


@attr.s
class EventingFunctionUrlAuthDigest(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    digest authorization

    :param username: auth digest username
    :type username: str
    :param password: auth digest password
    :type password: str
    """

    username = attr.ib(type=str)
    password = attr.ib(type=str)


@attr.s
class EventingFunctionUrlAuthBearer(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    bearer authorization

    :param key: bearer key
    :type key: str
    """

    key = attr.ib(type=str)


@attr.s
class EventingFunctionUrlBinding(object):
    """Object representation for an eventing function URL binding

    :param hostname: binding's hostname
    :type hostname: str
    :param alias: binding's alias
    :type alias: str
    :param allow_cookies: If the binding should allow cookies
    :type allow_cookies: bool
    :param validate_ssl_certificate: If the binding should validate SSL cert
    :type validate_ssl_certificate: bool
    :param auth: binding's authorization type
    :type auth: class:`EventingFunctionUrlAuth`
    """

    hostname = attr.ib(type=str)
    alias = attr.ib(type=str)
    allow_cookies = attr.ib(type=bool)
    validate_ssl_certificate = attr.ib(type=bool)
    auth = attr.ib(factory=EventingFunctionUrlAuth, type=EventingFunctionUrlAuth)

    def to_server_dict(
        self,  # type: "EventingFunctionUrlBinding"
        to_json=False,  # type: bool
    ) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionUrlBinding` that
        aligns with what Couchbase Server eventing spec.

        :param to_json: If True, convert dict representation to JSON, otherwise return dict
        :type to_json: bool

        :return: dict or JSON str representation of the `EventingFunctionUrlBinding`
        :rtype: Dict[str, Any]
        """
        server_dict = {
            "hostname": self.hostname,
            "value": self.alias,
            "allow_cookies": self.allow_cookies,
            "validate_ssl_certificate": self.validate_ssl_certificate,
        }

        if isinstance(self.auth, EventingFunctionUrlNoAuth):
            server_dict["auth_type"] = "no-auth"
        elif isinstance(self.auth, EventingFunctionUrlAuthBasic):
            server_dict["auth_type"] = "basic"
            server_dict["username"] = self.auth.username
            server_dict["password"] = self.auth.password
        elif isinstance(self.auth, EventingFunctionUrlAuthDigest):
            server_dict["auth_type"] = "digest"
            server_dict["username"] = self.auth.username
            server_dict["password"] = self.auth.password
        elif isinstance(self.auth, EventingFunctionUrlAuthBearer):
            server_dict["auth_type"] = "bearer"
            server_dict["bearer_key"] = self.auth.key

        return json.dumps(server_dict) if to_json is True else server_dict


@attr.s
class EventingFunctionConstantBinding(object):
    """Object representation for an eventing function constant binding

    :param alias: binding's alias
    :type alias: str
    :param literal: binding's value
    :type literal: str
    """

    alias = attr.ib(type=str)
    literal = attr.ib(type=str)

    def to_server_dict(
        self,  # type: "EventingFunctionConstantBinding"
        to_json=False,  # type: bool
    ) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionConstantBinding` that
        aligns with what Couchbase Server eventing spec.

        :param to_json: If True, convert dict representation to JSON, otherwise return dict
        :type to_json: bool

        :return: dict or JSON str representation of the `EventingFunctionConstantBinding`
        :rtype: Dict[str, Any]
        """
        server_dict = {"value": self.alias, "literal": self.literal}

        return json.dumps(server_dict) if to_json is True else server_dict


@attr.s(kw_only=True)
class EventingFunctionSettings(object):
    """Object representation for an settings relevant to an eventing function

    :param cpp_worker_thread_count: Number of threads each worker utilizes
    :type cpp_worker_thread_count: int
    :param description: Free form text for user to describe the eventing function
    :type description: str
    :param execution_timeout: Maximum time the eventing function can run before it is forcefully terminated (in seconds)
    :type execution_timeout: timedelta
    :param lcb_inst_capacity: Maximum number of libcouchbase connections that may be opened and pooled
    :type lcb_inst_capacity: int
    :param lcb_retry_count: Number of retries of retriable libcouchbase failures, 0 keeps trying till execution_timeout
    :type lcb_retry_count: int
    :param lcb_timeout: Maximum time the lcb command is waited until completion before we terminate the request (in seconds)
    :type lcb_timeout: timedelta
    :param num_timer_partitions: Number of timer shards, defaults to number of vbuckets
    :type num_timer_partitions: int
    :param sock_batch_size: Batch size for messages from producer to consumer, normally not specified
    :type sock_batch_size: int
    :param tick_duration: Duration to log stats from this eventing function, in milliseconds
    :type tick_duration: int
    :param timer_context_size: Size limit of timer context object
    :type timer_context_size: int
    :param user_prefix: Key prefix for all data stored in metadata by this eventing function
    :type user_prefix: str
    :param bucket_cache_size: Maximum size in bytes the bucket cache can grow to
    :type bucket_cache_size: int
    :param bucket_cache_age: Time in milliseconds after which a cached bucket object is considered stale
    :type bucket_cache_age: int
    :param curl_max_allowed_resp_size: maximum allowable curl call response in MegaBytes
    :type curl_max_allowed_resp_size: int
    :param worker_count: Number of worker processes eventing function utilizes on each eventing node
    :type worker_count: int
    :param query_prepare_all: Automatically prepare all N1QL statements in the eventing function
    :type query_prepare_all: bool
    :param enable_applog_rotation: Enable rotating this eventing function's log() message files
    :type enable_applog_rotation: bool
    :param app_log_dir: Directory to write content of log() message files
    :type app_log_dir: str
    :param app_log_max_size: Rotate logs when file grows to this size in bytes approximately
    :type app_log_max_size: int
    :param app_log_max_files: Number of log() message files to retain when rotating
    :type app_log_max_files: int
    :param checkpoint_interval: Number of seconds before writing a progress checkpoint
    :type checkpoint_interval: int
    :param dcp_stream_boundary: indicates where to start dcp stream from (beginning of time or present point)
    :type dcp_stream_boundary: `EventingFunctionDcpBoundary`
    :param deployment_status: Indicates if the function is deployed
    :type deployment_status: `EventingFunctionDeploymentStatus`
    :param processing_status: Indicates if the function is running
    :type processing_status: `EventingFunctionProcessingStatus`
    :param language_compatibility: Eventing language version this eventing function assumes in terms of syntax and behavior
    :type language_compatibility: `EventingFunctionLanguageCompatibility`
    :param log_level: Level of detail in system logging
    :type log_level: `EventingFunctionLogLevel`
    :param query_consistency: Consistency level used by n1ql statements in the eventing function
    :type query_consistency: `couchbase.cluster.QueryScanConsistency`
    :param handler_headers: Code to automatically prepend to top of eventing function code
    :type handler_headers: List[str]
    :param handler_footers: Code to automatically prepend to top of eventing function code
    :type handler_footers: List[str]
    """

    cpp_worker_thread_count = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    description = attr.ib(
        type=str,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    execution_timeout = attr.ib(
        type=timedelta,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(timedelta)),
    )
    lcb_inst_capacity = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    lcb_retry_count = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    lcb_timeout = attr.ib(
        type=timedelta,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(timedelta)),
    )
    num_timer_partitions = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    sock_batch_size = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    tick_duration = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    timer_context_size = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    user_prefix = attr.ib(
        type=str,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    bucket_cache_size = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    bucket_cache_age = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    curl_max_allowed_resp_size = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    query_prepare_all = attr.ib(
        type=bool,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(bool)),
    )
    worker_count = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    enable_applog_rotation = attr.ib(
        type=bool,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(bool)),
    )
    app_log_dir = attr.ib(
        type=str,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    app_log_max_size = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    app_log_max_files = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    checkpoint_interval = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    dcp_stream_boundary = attr.ib(
        factory=EventingFunctionDcpBoundary, type=EventingFunctionDcpBoundary
    )
    deployment_status = attr.ib(
        factory=EventingFunctionDeploymentStatus, type=EventingFunctionDeploymentStatus
    )
    processing_status = attr.ib(
        factory=EventingFunctionProcessingStatus, type=EventingFunctionProcessingStatus
    )
    language_compatibility = attr.ib(
        factory=EventingFunctionLanguageCompatibility,
        type=EventingFunctionLanguageCompatibility,
    )
    log_level = attr.ib(factory=EventingFunctionLogLevel, type=EventingFunctionLogLevel)
    query_consistency = attr.ib(factory=QueryScanConsistency, type=QueryScanConsistency)
    handler_headers = attr.ib(factory=list, type=List[str])
    handler_footers = attr.ib(factory=list, type=List[str])

    def _to_server_value(self, setting):
        if not setting:
            return None
        elif isinstance(setting, str) and setting.split():
            return setting
        elif isinstance(setting, Enum):
            if isinstance(setting, QueryScanConsistency):
                return QueryScanConsistency.to_eventing_server(setting)
            return setting.value
        elif isinstance(setting, list) and len(setting) > 0:
            return setting
        else:
            return setting

    def to_server_dict(
        self,  # type: "EventingFunctionSettings"
        to_json=False,  # type: bool
    ) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionSettings` that
        aligns with what Couchbase Server eventing spec

        :param to_json: If True, convert dict representation to JSON, otherwise return dict
        :type to_json: bool

        :return: dict or JSON str representation of the `EventingFunctionSettings`
        :rtype: Dict[str, Any]
        """
        server_dict = {}
        for _attr in attr.fields_dict(EventingFunctionSettings).keys():
            value = self._to_server_value(getattr(self, _attr))
            if value is not None:
                if _attr == "query_consistency":
                    server_dict["n1ql_consistency"] = value
                elif _attr == "query_prepare_all":
                    server_dict["n1ql_prepare_all"] = value
                else:
                    server_dict[_attr] = value

        return json.dumps(server_dict) if to_json is True else server_dict

    @classmethod
    def new_settings(cls, **kwargs):
        """Returns a new `EventingFunctionSettings` object

        :param kwargs: Keyword arguments to populate `EventingFunctionSettings` object
        :type kwargs: dict

        :return: new `EventingFunctionSettings` object
        :rtype: `EventingFunctionSettings`
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

        return cls(**kwargs)

    @classmethod
    def from_server(
        cls,  # type: "EventingFunctionSettings"
        server_dict,  # type: Dict[str, Any]
    ) -> "EventingFunctionSettings":
        """Returns a new `EventingFunctionSettings` object based
        on the JSON response received from Couchbase Server

        :param server_dict: Keyword arguments to populate `EventingFunctionSettings` object
        :type server_dict: dict

        :return: new `EventingFunctionSettings` object
        :rtype: `EventingFunctionSettings`
        """
        if not server_dict:
            raise InvalidArgumentException("No server content provided.")

        value = server_dict.get("dcp_stream_boundary", None)
        if value is not None and value.split():
            server_dict[
                "dcp_stream_boundary"
            ] = EventingFunctionDcpBoundary.from_server(value)
        else:
            server_dict["dcp_stream_boundary"] = None

        value = server_dict.get("deployment_status", None)
        if value is not None:
            server_dict[
                "deployment_status"
            ] = EventingFunctionDeploymentStatus.from_server(value)

        value = server_dict.get("processing_status", None)
        if value is not None:
            server_dict[
                "processing_status"
            ] = EventingFunctionProcessingStatus.from_server(value)

        value = server_dict.get("language_compatibility", None)
        if value is not None and value.split():
            server_dict[
                "language_compatibility"
            ] = EventingFunctionLanguageCompatibility.from_server(value)
        else:
            server_dict["language_compatibility"] = None

        value = server_dict.get("log_level", None)
        if value is not None and value.split():
            server_dict["log_level"] = EventingFunctionLogLevel.from_server(value)
        else:
            server_dict["log_level"] = None

        value = server_dict.pop("n1ql_consistency", None)
        if value is not None and value.split():
            server_dict[
                "query_consistency"
            ] = QueryScanConsistency.from_eventing_server(value)
        else:
            server_dict["query_consistency"] = None

        value = server_dict.pop("n1ql_prepare_all", None)
        if value is not None:
            server_dict["query_prepare_all"] = value

        value = server_dict.pop("n1ql_prepare_all", None)
        if value is not None:
            server_dict["query_prepare_all"] = value

        # handle timedeltas:
        for key in ["execution_timeout", "lcb_timeout"]:
            if key in server_dict:
                server_dict[key] = timedelta(seconds=int(server_dict[key]))

        return cls(**server_dict)


@attr.s
class EventingFunction(object):
    """Object representation for settings relevant to an eventing function

    :param name:
    :type name: str
    :param code:
    :type code: str
    :param version:
    :type version: str
    :param enforce_schema:
    :type enforce_schema: bool
    :param handler_uuid:
    :type handler_uuid: int
    :param function_instance_id:
    :type function_instance_id: str
    :param metadata_keyspace:
    :type metadata_keyspace: `.generic.EventingFunctionKeyspace`
    :param source_keyspace:
    :type source_keyspace: `.generic.EventingFunctionKeyspace`
    :param bucket_bindings:
    :type bucket_bindings: List[`EventingFunctionBucketBinding`]
    :param url_bindings:
    :type url_bindings: List[`EventingFunctionUrlBinding`]
    :param constant_bindings:
    :type constant_bindings: List[`EventingFunctionConstantBinding`]
    :param settings:
    :type settings: `EventingFunctionSettings`
    """

    name = attr.ib(type=str, validator=attr.validators.instance_of(str))
    code = attr.ib(type=str, validator=attr.validators.instance_of(str))
    version = attr.ib(type=str, validator=attr.validators.instance_of(str))
    enforce_schema = attr.ib(
        type=bool,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(bool)),
    )
    handler_uuid = attr.ib(
        type=int,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(int)),
    )
    function_instance_id = attr.ib(
        type=str,
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    metadata_keyspace = attr.ib(
        factory=EventingFunctionKeyspace,
        type=EventingFunctionKeyspace,
        validator=attr.validators.instance_of(EventingFunctionKeyspace),
    )
    source_keyspace = attr.ib(
        factory=EventingFunctionKeyspace,
        type=EventingFunctionKeyspace,
        validator=attr.validators.instance_of(EventingFunctionKeyspace),
    )
    bucket_bindings = attr.ib(factory=list, type=List[EventingFunctionBucketBinding])
    url_bindings = attr.ib(factory=list, type=List[EventingFunctionUrlBinding])
    constant_bindings = attr.ib(
        factory=list, type=List[EventingFunctionConstantBinding]
    )
    settings = attr.ib(
        factory=EventingFunctionSettings,
        type=EventingFunctionSettings,
        validator=attr.validators.instance_of(EventingFunctionSettings),
    )

    def to_server_dict(
        self,  # type: "EventingFunction"
        to_json=False,  # type: bool
    ) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunction` that
        aligns with what Couchbase Server eventing spec

        :param to_json: If True, convert dict representation to JSON, otherwise return dict
        :type to_json: bool

        :return: dict or JSON str representation of the `EventingFunction`
        :rtype: Dict[str, Any]
        """

        if not (self.name and self.name.split()):
            raise InvalidException("Eventing function must include a name.")
        if not (self.code and self.code.split()):
            raise InvalidException("Eventing function must include app code.")
        if not (self.version and self.version.split()):
            raise InvalidException("Eventing function must include a version.")

        server_dict = {
            "appname": self.name,
            "appcode": self.code,
            "version": self.version,
        }

        if self.enforce_schema is not None:
            server_dict["enforce_schema"] = self.enforce_schema

        if self.handler_uuid is not None:
            server_dict["handleruuid"] = self.handler_uuid

        if self.function_instance_id and self.function_instance_id.split():
            server_dict["function_instance_id"] = self.function_instance_id

        server_dict["settings"] = self.settings.to_server_dict()
        server_dict["depcfg"] = {}
        server_dict["depcfg"]["metadata_bucket"] = self.metadata_keyspace.bucket
        if self.metadata_keyspace.scope and self.metadata_keyspace.scope.split():
            server_dict["depcfg"]["metadata_scope"] = self.metadata_keyspace.scope
        if (
            self.metadata_keyspace.collection
            and self.metadata_keyspace.collection.split()
        ):
            server_dict["depcfg"][
                "metadata_collection"
            ] = self.metadata_keyspace.collection

        server_dict["depcfg"]["source_bucket"] = self.source_keyspace.bucket
        if self.source_keyspace.scope and self.source_keyspace.scope.split():
            server_dict["depcfg"]["source_scope"] = self.source_keyspace.scope
        if self.source_keyspace.collection and self.source_keyspace.collection.split():
            server_dict["depcfg"]["source_collection"] = self.source_keyspace.collection

        if self.bucket_bindings is not None and len(self.bucket_bindings) > 0:
            bindings = []
            for binding in self.bucket_bindings:
                bindings.append(binding.to_server_dict())

            if len(bindings) > 0:
                server_dict["depcfg"]["buckets"] = bindings

        if self.url_bindings is not None and len(self.url_bindings) > 0:
            urls = []
            for binding in self.url_bindings:
                urls.append(binding.to_server_dict())

            if len(urls) > 0:
                server_dict["depcfg"]["curl"] = urls

        if self.constant_bindings is not None and len(self.constant_bindings) > 0:
            constants = []
            for binding in self.constant_bindings:
                constants.append(binding.to_server_dict())

            if len(constants) > 0:
                server_dict["depcfg"]["constants"] = constants

        return json.dumps(server_dict) if to_json is True else server_dict

    @classmethod
    def from_server(
        cls,  # type: "EventingFunction"
        server_json,  # type: Dict[str, Any]
    ) -> "EventingFunction":
        """Returns a new `EventingFunction` object based
        on the JSON response received from Couchbase Server

        :param server_json: JSON response
        :type server_dict: dict

        :return: new `EventingFunction` object
        :rtype: `EventingFunction`
        """
        depcfg = server_json["depcfg"]
        func = cls(
            server_json["appname"],
            server_json["appcode"],
            server_json["version"],
            enforce_schema=server_json.get("enforce_schema", None),
            handler_uuid=server_json.get("handleruuid", None),
            function_instance_id=server_json.get("function_instance_id", None),
            metadata_keyspace=EventingFunctionKeyspace.from_server(
                depcfg["metadata_bucket"],
                depcfg["metadata_scope"],
                depcfg["metadata_collection"],
            ),
            source_keyspace=EventingFunctionKeyspace.from_server(
                depcfg["source_bucket"],
                depcfg["source_scope"],
                depcfg["source_collection"],
            ),
            settings=EventingFunctionSettings.from_server(server_json["settings"]),
        )
        if "buckets" in depcfg:
            buckets = []
            for bucket in depcfg["buckets"]:
                buckets.append(
                    EventingFunctionBucketBinding(
                        alias=bucket["alias"],
                        name=EventingFunctionKeyspace.from_server(
                            bucket["bucket_name"],
                            bucket["scope_name"],
                            bucket["collection_name"],
                        ),
                        access=EventingFunctionBucketAccess.from_server(
                            bucket["access"]
                        ),
                    )
                )

            func.bucket_bindings = buckets

        if "curl" in depcfg:
            urls = []
            for url in depcfg["curl"]:
                url_binding = EventingFunctionUrlBinding(
                    hostname=url.get("hostname"),
                    alias=url.get("value"),
                    allow_cookies=url.get("allow_cookies"),
                    validate_ssl_certificate=url.get("validate_ssl_certificate"),
                )
                auth_type = url.get("auth_type")
                if auth_type == "no-auth":
                    url_binding.auth = EventingFunctionUrlNoAuth()
                elif auth_type == "basic":
                    url_binding.auth = EventingFunctionUrlAuthBasic(
                        username=url.get("username"), password=None
                    )
                elif auth_type == "digest":
                    url_binding.auth = EventingFunctionUrlAuthBasic(
                        username=url.get("username"), password=None
                    )
                elif auth_type == "bearer":
                    url_binding.auth = EventingFunctionUrlAuthBearer(key=None)
                urls.append(url_binding)

            func.url_bindings = urls

        if "constants" in depcfg:
            constants = []
            for constant in depcfg["constants"]:
                constants.append(
                    EventingFunctionConstantBinding(
                        alias=constant["value"], literal=constant["literal"]
                    )
                )

            func.constant_bindings = constants

        return func


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
        Indicates the function is pausedd.
    Pausing
        Indicates the function is pausing.
    """

    Undeployed = "undeployed"
    Deployed = "deployed"
    Deploying = "deploying"
    Undeploying = "undeploying"
    Paused = "paused"
    Pausing = "pausing"

    @classmethod
    def from_server(cls, value):
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
        else:
            raise InvalidArgumentException("Invalid value for state: {}".format(value))


@attr.s(kw_only=True)
class EventingFunctionStatus(object):
    """Object representation for the status of an eventing function

    Particularly useful when determining an interim state of an evening function (i.e,
    deploying, pausing, undeploying).

    :param name: eventing function's name
    :type name: str
    :param num_bootstrapping_nodes: number of nodes for bootstrapping
    :type num_bootstrapping_nodes: int
    :param num_deployed_nodes: number of nodes eventing function is deployed on
    :type num_deployed_nodes: int
    :param state: composite status of eventing function
    :type state: `EventingFunctionState`
    :param deployment_status: Indicates if the function is deployed
    :type deployment_status: `EventingFunctionDeploymentStatus`
    :param processing_status: Indicates if the function is running
    :type processing_status: `EventingFunctionProcessingStatus`
    """

    name = attr.ib(type=str, validator=attr.validators.instance_of(str))
    num_bootstrapping_nodes = attr.ib(
        type=int, validator=attr.validators.instance_of(int)
    )
    num_deployed_nodes = attr.ib(type=int, validator=attr.validators.instance_of(int))
    state = attr.ib(factory=EventingFunctionState, type=EventingFunctionState)
    deployment_status = attr.ib(
        factory=EventingFunctionDeploymentStatus, type=EventingFunctionDeploymentStatus
    )
    processing_status = attr.ib(
        factory=EventingFunctionProcessingStatus, type=EventingFunctionProcessingStatus
    )

    @classmethod
    def from_server(
        cls,  # type: "EventingFunction"
        server_json,  # type: Dict[str, Any]
    ) -> "EventingFunction":
        """Returns a new `EventingFunctionStatus` object based
        on the JSON response received from Couchbase Server

        :param server_json: JSON response
        :type server_dict: dict

        :return: new `EventingFunctionStatus` object
        :rtype: `EventingFunctionStatus`
        """
        status = server_json.pop("composite_status", None)
        if status is not None:
            server_json["state"] = EventingFunctionState.from_server(status)
        else:
            server_json["state"] = EventingFunctionState.Undeployed
        return cls(**server_json)


@attr.s
class EventingFunctionsStatus(object):
    """Object representation for statuses for all eventing functions

    :param num_eventing_nodes:
    :type num_eventing_nodes: int
    :param functions:
    :type functions: List[`EventingFunctionStatus`]
    """

    num_eventing_nodes = attr.ib(type=int, validator=attr.validators.instance_of(int))
    functions = attr.ib(factory=list, type=List[EventingFunctionStatus])

    @classmethod
    def from_server(
        cls,  # type: "EventingFunctionsStatus"
        server_json,  # type: Dict[str, Any]
    ) -> "EventingFunctionsStatus":
        """Returns a new `EventingFunctionsStatus` object based
        on the JSON response received from Couchbase Server

        :param server_json: JSON response
        :type server_dict: dict

        :return: new `EventingFunctionsStatus` object
        :rtype: `EventingFunctionsStatus`
        """
        functions = []
        if "apps" in server_json:
            for func in server_json["apps"]:
                functions.append(EventingFunctionStatus.from_server(func))
        return cls(
            num_eventing_nodes=server_json["num_eventing_nodes"], functions=functions
        )


class EventingFunctionOptions(OptionBlockTimeOut):
    def __init__(self, **kwargs):
        """
        EventingFunctionOptions
        Various options for eventing function management API

        :param timeout:
            Uses this timeout value, rather than the default for the cluster.
        :type timeout: timedelta
        """
        super(EventingFunctionOptions, self).__init__(**kwargs)


class UpsertFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "UpsertFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(UpsertFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class DropFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "DropFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(DropFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class DeployFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "DeployFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(DeployFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class GetAllFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "GetAllFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(GetAllFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class GetFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "GetFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(GetFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class PauseFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "PauseFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(PauseFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class ResumeFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "ResumeFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(ResumeFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class UndeployFunctionOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "UndeployFunctionOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(UndeployFunctionOptions, self).__init__(timeout=timeout, **kwargs)


class FunctionsStatusOptions(EventingFunctionOptions):
    def __init__(
        self,  # type: "FunctionsStatusOptions"
        timeout=None,  # type: timedelta
        **kwargs  # type: Any
    ) -> None:
        super(FunctionsStatusOptions, self).__init__(timeout=timeout, **kwargs)
