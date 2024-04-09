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

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional)

from couchbase._utils import is_null_or_empty, timedelta_as_microseconds
from couchbase.exceptions import (EventingFunctionAlreadyDeployedException,
                                  EventingFunctionCollectionNotFoundException,
                                  EventingFunctionCompilationFailureException,
                                  EventingFunctionIdenticalKeyspaceException,
                                  EventingFunctionNotBootstrappedException,
                                  EventingFunctionNotDeployedException,
                                  EventingFunctionNotFoundException,
                                  EventingFunctionNotUnDeployedException,
                                  InvalidArgumentException)
from couchbase.logic.n1ql import QueryScanConsistency
from couchbase.options import forward_args
from couchbase.pycbc_core import (eventing_function_mgmt_operations,
                                  management_operation,
                                  mgmt_operations)

if TYPE_CHECKING:
    from couchbase.management.options import (DeployFunctionOptions,
                                              DropFunctionOptions,
                                              FunctionsStatusOptions,
                                              GetAllFunctionOptions,
                                              GetFunctionOptions,
                                              PauseFunctionOptions,
                                              ResumeFunctionOptions,
                                              UndeployFunctionOptions,
                                              UpsertFunctionOptions)


class EventingFunctionManagerLogic:

    _ERROR_MAPPING = {r'.*ERR_APP_NOT_FOUND_TS': EventingFunctionNotFoundException,
                      r'.*ERR_HANDLER_COMPILATION': EventingFunctionCompilationFailureException,
                      r'.*ERR_APP_NOT_BOOTSTRAPPED': EventingFunctionNotBootstrappedException,
                      r'.*ERR_APP_NOT_DEPLOYED': EventingFunctionNotDeployedException,
                      r'.*ERR_APP_NOT_UNDEPLOYED': EventingFunctionNotUnDeployedException,
                      r'.*ERR_COLLECTION_MISSING': EventingFunctionCollectionNotFoundException,
                      r'.*ERR_APP_ALREADY_DEPLOYED': EventingFunctionAlreadyDeployedException,
                      r'.*ERR_SRC_MB_SAME': EventingFunctionIdenticalKeyspaceException}

    def __init__(self,
                 connection,
                 bucket_name=None,  # type: Optional[str]
                 scope_name=None  # type: Optional[str]
                 ):
        self._connection = connection
        self._bucket_name = bucket_name
        self._scope_name = scope_name

    def upsert_function(
        self,
        function,  # type: EventingFunction
        *options,  # type: UpsertFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        """
        Upserts an eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param function: the EventingFunction to upsert
        :param options: UpsertFunctionOptions to upsert an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionCompilationFailureException
        :raises: EventingFunctionCollectionNotFoundException
        :raises: InvalidArgumentException
        Any exceptions raised by the underlying platform
        """
        eventing_function = function.as_dict()

        op_args = {
            'eventing_function': eventing_function,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.UPSERT_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def drop_function(
        self,
        name,  # type: str
        *options,  # type: DropFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        """
        Drops an existing eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to drop
        :param options: DropFunctionOptions to drop an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        :raises: EventingFunctionNotUndeployedException
        Any exceptions raised by the underlying platform
        """
        op_args = {
            'name': name,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.DROP_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def deploy_function(
        self,
        name,  # type: str
        *options,  # type: DeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        """
        Deploys an existing eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to drop
        :param options: DeployFunctionOptions to deploy an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotDeployedException
        :raises: EventingFunctionAlreadyDeployedException
        Any exceptions raised by the underlying platform
        """
        op_args = {
            'name': name,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.DEPLOY_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def get_all_functions(
        self,
        *options,  # type: GetAllFunctionOptions
        **kwargs  # type: Any
    ) -> List[EventingFunction]:
        """
        Returns a list of all eventing functions

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param options: GetAllFunctionOptions to get all eventing functions
        :param kwargs: Override corresponding value in options

        :raises:
        Any exceptions raised by the underlying platform

        :return: A list :class:`EventingFunction` objects
        :rtype: list
        """
        op_args = {}

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.GET_ALL_FUNCTIONS.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def get_function(
        self,
        name,  # type: str
        *options,  # type: GetFunctionOptions
        **kwargs  # type: Any
    ) -> EventingFunction:
        """
        Returns specified eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to retreive
        :param options: GetFunctionOptions to get an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        Any exceptions raised by the underlying platform

        :return: class:`EventingFunction` object
        :rtype: class:`EventingFunction`
        """
        op_args = {
            'name': name,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.GET_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def pause_function(
        self,
        name,  # type: str
        *options,  # type: PauseFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        """
        Pauses an existing eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to pause
        :param options: PauseFunctionOptions to pause an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        :raises: EventingFunctionNotBootstrappedException
        Any exceptions raised by the underlying platform
        """
        op_args = {
            'name': name,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.PAUSE_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def resume_function(
        self,
        name,  # type: str
        *options,  # type: ResumeFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        """
        Resumes an existing eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to resume
        :param options: ResumeFunctionOptions to resume an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotFoundException
        :raises: EventingFunctionNotDeployedException
        Any exceptions raised by the underlying platform
        """
        op_args = {
            'name': name,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.RESUME_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def undeploy_function(
        self,
        name,  # type: str
        *options,  # type: UndeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        """
        Undeploys an existing eventing function

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param name: the name of the eventing function to drop
        :param options: UndeployFunctionOptions to undeploy an eventing function
        :param kwargs: Override corresponding value in options

        :raises: EventingFunctionNotDeployedException
        Any exceptions raised by the underlying platform
        """
        op_args = {
            'name': name,
        }

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.UNDEPLOY_FUNCTION.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)

    def functions_status(
        self,
        *options,  # type: FunctionsStatusOptions
        **kwargs  # type: Any
    ) -> EventingFunctionsStatus:
        """
        Returns the `EventingFunctionStatus` of all eventing functions

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        :param options: UndeployFunctionOptions to undeploy an eventing function
        :param kwargs: Override corresponding value in options

        :raises:
            Any exceptions raised by the underlying platform

        :return: class:`EventingFunctionsStatus` object
        :rtype: class:`EventingFunctionsStatus`
        """
        op_args = {}

        if self._bucket_name is not None:
            op_args['bucket_name'] = self._bucket_name

        if self._scope_name is not None:
            op_args['scope_name'] = self._scope_name

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.EVENTING_FUNCTION.value,
            'op_type': eventing_function_mgmt_operations.GET_STATUS.value,
            'op_args': op_args,
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)


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

    def to_str(self):
        if self.value == 'r':
            return 'read_only'
        elif self.value == 'rw':
            return 'read_write'

    @classmethod
    def from_server(cls, value  # type: str
                    ) -> EventingFunctionBucketAccess:
        if value in ['r', 'read_only']:
            return cls.ReadOnly
        elif value in ['rw', 'read_write']:
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

    def to_str(self):
        if self.value:
            return 'deployed'

        return 'undeployed'

    @classmethod
    def from_server(cls, value):
        if value == 'undeployed':
            return cls.Undeployed
        elif value == 'deployed':
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

    def to_str(self):
        if self.value:
            return 'running'

        return 'paused'

    @classmethod
    def from_server(cls, value):
        if value == 'paused':
            return cls.Paused
        elif value == 'running':
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
    Version_6_6_2
    Version_7_2_0
    """

    Version_6_0_0 = "6.0.0"
    Version_6_5_0 = "6.5.0"
    Version_6_6_2 = "6.6.2"
    Version_7_2_0 = "7.2.0"

    def to_str(self):
        if self.value == '6.0.0':
            return 'version_6_0_0'
        if self.value == '6.5.0':
            return 'version_6_5_0'
        if self.value == '6.6.2':
            return 'version_6_6_2'
        if self.value == '7.2.0':
            return 'version_7_2_0'

    @classmethod
    def from_server(cls, value):
        if value == "version_6_0_0":
            return cls.Version_6_0_0
        elif value == "version_6_5_0":
            return cls.Version_6_5_0
        elif value == "version_6_6_2":
            return cls.Version_6_6_2
        elif value == "version_7_2_0":
            return cls.Version_7_2_0
        else:
            raise InvalidArgumentException(
                "Invalid value for language compatibility: {}".format(value)
            )


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
            raise InvalidArgumentException(
                "Invalid value for state: {}".format(value))


@dataclass
class EventingFunctionKeyspace:
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
    bucket: str = None
    scope: str = None
    collection: str = None

    def as_dict(self) -> Dict[str, Any]:
        output = {
            'bucket': self.bucket,
        }
        if self.scope:
            output['scope'] = self.scope

        if self.collection:
            output['collection'] = self.collection

        return output

    @classmethod
    def from_server(
        cls,
        bucket,  # type: str
        scope=None,  # type: Optional[str]
        collection=None,  # type: Optional[str]
    ) -> EventingFunctionKeyspace:
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
        if not is_null_or_empty(scope) and scope != "_default":
            keyspace.scope = scope

        if not is_null_or_empty(collection) and collection != "_default":
            keyspace.collection = collection

        return keyspace


@dataclass
class EventingFunctionBucketBinding:
    """Object representation for an eventing function bucket binding

    :param alias: binding's alias
    :type alias: str
    :param name: binding's keyspace which consists of the bucket name, scope (optional) and collection (optional)
    :type name: class:`couchbase.management.EventingFunctionKeyspace`
    :param access: binding's bucket access
    :type access: class:`EventingFunctionBucketAccess`
    """

    alias: str = None
    name: EventingFunctionKeyspace = None
    access: EventingFunctionBucketAccess = None

    def as_dict(self) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionBucketBinding` that
        aligns with what Couchbase Server eventing spec.

        :return: dict representation of the `EventingFunctionBucketBinding`
        :rtype: Dict[str, Any]
        """
        output = {'alias': self.alias,
                  'access': self.access.to_str(),
                  'name': self.name.as_dict()
                  }

        return output

    @classmethod
    def from_server(
        cls,
        bucket_binding  # type: Dict[str, Any]
    ) -> EventingFunctionBucketBinding:
        alias = bucket_binding.get('alias', None)
        name = bucket_binding.get('name', {})
        keyspace = EventingFunctionKeyspace.from_server(**name)
        access = EventingFunctionBucketAccess.from_server(bucket_binding.get('access', None))
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

    :param username: auth username
    :type username: str
    :param password: auth password
    :type password: str
    """

    username: str = None
    password: str = None


@dataclass
class EventingFunctionUrlAuthDigest(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    digest authorization

    :param username: auth digest username
    :type username: str
    :param password: auth digest password
    :type password: str
    """

    username: str = None
    password: str = None


@dataclass
class EventingFunctionUrlAuthBearer(EventingFunctionUrlAuth):
    """Object representation for an eventing function URL binding
    bearer authorization

    :param key: bearer key
    :type key: str
    """

    key: str = None


@dataclass
class EventingFunctionUrlBinding:
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

    hostname: str = None
    alias: str = None
    allow_cookies: bool = None
    validate_ssl_certificate: bool = None
    auth: EventingFunctionUrlAuth = None

    def as_dict(self) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionUrlBinding` that
        aligns with what Couchbase Server eventing spec.

        :return: dict representation of the `EventingFunctionUrlBinding`
        :rtype: Dict[str, Any]
        """
        output = {
            "hostname": self.hostname,
            "alias": self.alias,
            "allow_cookies": self.allow_cookies,
            "validate_ssl_certificate": self.validate_ssl_certificate,
        }

        if isinstance(self.auth, EventingFunctionUrlNoAuth):
            output["auth_type"] = "no-auth"
        elif isinstance(self.auth, EventingFunctionUrlAuthBasic):
            output["auth_type"] = "basic"
            output["username"] = self.auth.username
            output["password"] = self.auth.password
        elif isinstance(self.auth, EventingFunctionUrlAuthDigest):
            output["auth_type"] = "digest"
            output["username"] = self.auth.username
            output["password"] = self.auth.password
        elif isinstance(self.auth, EventingFunctionUrlAuthBearer):
            output["auth_type"] = "bearer"
            output["bearer_key"] = self.auth.key

        return output

    @classmethod
    def from_server(
        cls,
        url_binding  # type: Dict[str, Any]
    ) -> EventingFunctionUrlBinding:
        input = {
            'alias': url_binding.get('alias', None),
            'hostname': url_binding.get('hostname', None),
            'allow_cookies': url_binding.get('allow_cookies', None),
            'validate_ssl_certificate': url_binding.get('validate_ssl_certificate', None)
        }
        auth_type = url_binding.get('auth_type', 'no-auth')
        if auth_type == 'no-auth':
            input['auth'] = EventingFunctionUrlNoAuth()
        elif auth_type == 'basic':
            username = url_binding.get('username', None)
            input['auth'] = EventingFunctionUrlAuthBasic(username=username)
        elif auth_type == 'digest':
            username = url_binding.get('username', None)
            input['auth'] = EventingFunctionUrlAuthDigest(username=username)
        elif auth_type == 'bearer':
            key = url_binding.get('key', None)
            input['auth'] = EventingFunctionUrlAuthBearer(key=key)
        return cls(**input)


@dataclass
class EventingFunctionConstantBinding:
    """Object representation for an eventing function constant binding

    :param alias: binding's alias
    :type alias: str
    :param literal: binding's value
    :type literal: str
    """

    alias: str = None
    literal: str = None

    def as_dict(self) -> Dict[str, Any]:
        """Returns a representation of the `EventingFunctionConstantBinding` that
        aligns with what Couchbase Server eventing spec.

        :return: dict representation of the `EventingFunctionConstantBinding`
        :rtype: Dict[str, Any]
        """

        return {"alias": self.alias, "literal": self.literal}

    @classmethod
    def from_server(
        cls,
        constant_binding  # type: Dict[str, Any]
    ) -> EventingFunctionConstantBinding:
        input = {
            'alias': constant_binding.get('alias', None),
            'literal': constant_binding.get('literal', None)
        }
        return cls(**input)


@dataclass
class EventingFunctionSettings:
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
    :param lcb_timeout: Maximum time the lcb command is waited until completion before we terminate the request
        (in seconds)
    :type lcb_timeout: timedelta
    :param num_timer_partitions: Number of timer shards, defaults to number of vbuckets
    :type num_timer_partitions: int
    :param sock_batch_size: Batch size for messages from producer to consumer, normally not specified
    :type sock_batch_size: int
    :param tick_duration: Duration to log stats from this eventing function, in milliseconds
    :type tick_duration: timedelta
    :param timer_context_size: Size limit of timer context object
    :type timer_context_size: int
    :param user_prefix: Key prefix for all data stored in metadata by this eventing function
    :type user_prefix: str
    :param bucket_cache_size: Maximum size in bytes the bucket cache can grow to
    :type bucket_cache_size: int
    :param bucket_cache_age: Time in milliseconds after which a cached bucket object is considered stale
    :type bucket_cache_age: timedelta
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
    :type checkpoint_interval: timedelta
    :param dcp_stream_boundary: indicates where to start dcp stream from (beginning of time or present point)
    :type dcp_stream_boundary: `EventingFunctionDcpBoundary`
    :param deployment_status: Indicates if the function is deployed
    :type deployment_status: `EventingFunctionDeploymentStatus`
    :param processing_status: Indicates if the function is running
    :type processing_status: `EventingFunctionProcessingStatus`
    :param language_compatibility: Eventing language version this eventing function assumes in terms of syntax
        and behavior
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

    cpp_worker_thread_count: int = None
    description: str = None
    execution_timeout: timedelta = None
    lcb_inst_capacity: int = None
    lcb_retry_count: int = None
    lcb_timeout: timedelta = None
    num_timer_partitions: int = None
    sock_batch_size: int = None
    tick_duration: timedelta = None
    timer_context_size: int = None
    user_prefix: str = None
    bucket_cache_size: int = None
    bucket_cache_age: timedelta = None
    curl_max_allowed_resp_size: int = None
    query_prepare_all: bool = None
    worker_count: int = None
    enable_applog_rotation: bool = None
    app_log_dir: str = None
    app_log_max_size: int = None
    app_log_max_files: int = None
    checkpoint_interval: timedelta = None
    dcp_stream_boundary: EventingFunctionDcpBoundary = None
    deployment_status: EventingFunctionDeploymentStatus = None
    processing_status: EventingFunctionProcessingStatus = None
    language_compatibility: EventingFunctionLanguageCompatibility = None
    log_level: EventingFunctionLogLevel = None
    query_consistency: QueryScanConsistency = None
    handler_headers: List[str] = None
    handler_footers: List[str] = None

    def as_dict(self) -> Dict[str, Any]:  # noqa: C901
        """Returns a representation of the `EventingFunctionSettings` that
        aligns with what Couchbase Server eventing spec.

        :return: dict representation of the `EventingFunctionSettings`
        :rtype: Dict[str, Any]
        """

        output = {}

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
            output['tick_duration'] = timedelta_as_microseconds(self.tick_duration)

        if self.timer_context_size:
            output['timer_context_size'] = self.timer_context_size

        if self.user_prefix:
            output['user_prefix'] = self.user_prefix

        if self.bucket_cache_size:
            output['bucket_cache_size'] = self.bucket_cache_size

        if self.bucket_cache_age:
            output['bucket_cache_age'] = timedelta_as_microseconds(self.bucket_cache_age)

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

        settings_fields = [f.name for f in fields(cls)]
        final_json = {k: v for k, v in kwargs.items() if k in settings_fields}
        return cls(**final_json)

    @classmethod  # noqa: C901
    def from_server(  # noqa: C901
        cls,
        settings  # type: Dict[str, Any]
    ) -> EventingFunctionSettings:
        input = {
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

    name: str = None
    code: str = None
    version: str = None
    enforce_schema: bool = None
    handler_uuid: int = None
    function_instance_id: str = None
    metadata_keyspace: EventingFunctionKeyspace = None
    source_keyspace: EventingFunctionKeyspace = None
    bucket_bindings: List[EventingFunctionBucketBinding] = None
    url_bindings: List[EventingFunctionUrlBinding] = None
    constant_bindings: List[EventingFunctionConstantBinding] = None
    settings: EventingFunctionSettings = None

    def as_dict(self) -> Dict[str, Any]:  # noqa: C901
        """Returns a representation of the `EventingFunction` that
        aligns with what Couchbase Server eventing spec.

        :return: dict representation of the `EventingFunction`
        :rtype: Dict[str, Any]
        """

        output = {}

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
    def from_server(
        cls,
        eventing_function  # type: Dict[str, Any]
    ) -> EventingFunction:
        input = {
            'name': eventing_function.get('name', None),
            'code': eventing_function.get('code', None),
            'version': eventing_function.get('version', None),
            'enforce_schema': eventing_function.get('enforce_schema', None),
            'handler_uuid': eventing_function.get('handler_uuid', None)
        }

        metadata_keyspace = eventing_function.get('metadata_keyspace', None)
        if metadata_keyspace:
            input['metadata_keyspace'] = EventingFunctionKeyspace.from_server(metadata_keyspace)

        source_keyspace = eventing_function.get('source_keyspace', None)
        if source_keyspace:
            input['source_keyspace'] = EventingFunctionKeyspace.from_server(source_keyspace)

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
    :param redeploy_required: Indicates if function needs to be redeployed
    :type redeploy_required: bool
    :param function_scope: Indicates the eventing function's scope
    :type function_scope: Dict[str, Any]
    """

    name: str = None
    num_bootstrapping_nodes: int = None
    num_deployed_nodes: int = None
    state: EventingFunctionState = None
    deployment_status: EventingFunctionDeploymentStatus = None
    processing_status: EventingFunctionProcessingStatus = None
    redeploy_required: bool = None
    function_scope: Dict[str, Any] = None

    @classmethod
    def from_server(
        cls,
        function_status  # type: Dict[str, Any]
    ) -> EventingFunctionStatus:
        input = {
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

    :param num_eventing_nodes:
    :type num_eventing_nodes: int
    :param functions:
    :type functions: List[`EventingFunctionStatus`]
    """

    num_eventing_nodes: int = None
    functions: List[EventingFunctionStatus] = None

    @classmethod
    def from_server(
        cls,  # type: EventingFunctionsStatus
        function_status,  # type: Dict[str, Any]
    ) -> EventingFunctionsStatus:
        """Returns a new `EventingFunctionsStatus` object based
        on the Dict[str, Any] response received from Couchbase Server

        :param function_status: Dict[str, Any]
        :type function_status: dict

        :return: new `EventingFunctionsStatus` object
        :rtype: `EventingFunctionsStatus`
        """
        input = {
            'num_eventing_nodes': function_status.get('num_eventing_nodes', None)
        }
        functions = function_status.get('functions', None)
        if functions:
            input['functions'] = [EventingFunctionStatus.from_server(f) for f in functions]

        return cls(**input)
