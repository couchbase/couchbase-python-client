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

from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Union,
                    overload)

from couchbase._utils import (BidirectionalMapping,
                              BidirectionalTransform,
                              EnumToStr,
                              Identity,
                              ParamTransform,
                              SecondsToTimeDelta,
                              StrToEnum,
                              TimeDeltaToSeconds,
                              is_null_or_empty)
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  BucketNotFlushableException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  RateLimitedException)
from couchbase.options import forward_args
from couchbase.pycbc_core import (bucket_mgmt_operations,
                                  management_operation,
                                  mgmt_operations)

if TYPE_CHECKING:
    from couchbase.management.options import (CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManagerLogic:

    _ERROR_MAPPING = {'Bucket with given name (already|still) exists': BucketAlreadyExistsException,
                      'Requested resource not found': BucketDoesNotExistException,
                      r'.*Flush is disabled for the bucket.*': BucketNotFlushableException,
                      r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException,
                      r'.*is supported only with developer preview enabled.*': FeatureUnavailableException}

    def __init__(self, connection):
        self._connection = connection

    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Any
                      ) -> None:
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings: settings for the bucket.
        :param: CreateBucketOptions options: options for setting the bucket.
        :param: Any kwargs: override corresponding values in the options.

        :raises: BucketAlreadyExistsException
        :raises: InvalidArgumentsException
        """

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.CREATE_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        params = settings.transform_to_dest()

        mgmt_kwargs["op_args"] = {
            "bucket_settings": params
        }

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Any
                      ) -> None:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.UPDATE_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        params = settings.transform_to_dest()

        mgmt_kwargs["op_args"] = {
            "bucket_settings": params
        }

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Any
                    ) -> None:

        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException("Bucket name cannot be None or empty.")

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException("Bucket name must be a str.")

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.DROP_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        mgmt_kwargs["op_args"] = {
            "bucket_name": bucket_name
        }

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Any
                   ) -> BucketSettings:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.GET_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        mgmt_kwargs["op_args"] = {
            "bucket_name": bucket_name
        }

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Any
                        ) -> List[BucketSettings]:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.GET_ALL_BUCKETS.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Any
                     ) -> None:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.FLUSH_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        mgmt_kwargs["op_args"] = {
            "bucket_name": bucket_name
        }

        final_args = forward_args(kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)


class EvictionPolicyType(Enum):
    NOT_RECENTLY_USED = "nruEviction"
    NO_EVICTION = "noEviction"
    FULL = "fullEviction"
    VALUE_ONLY = "valueOnly"


class EjectionMethod(Enum):
    FULL_EVICTION = "fullEviction"
    VALUE_ONLY = "valueOnly"


class BucketType(Enum):
    COUCHBASE = "membase"
    MEMCACHED = "memcached"
    EPHEMERAL = "ephemeral"


class CompressionMode(Enum):
    OFF = "off"
    PASSIVE = "passive"
    ACTIVE = "active"


class ConflictResolutionType(Enum):
    """
    Specifies the conflict resolution type to use for the bucket.

    Members:
    TIMESTAMP: specifies to use timestamp conflict resolution
    SEQUENCE_NUMBER: specifies to use sequence number conflict resolution
    CUSTOM: **VOLATILE** This API is subject to change at any time. In Couchbase Server 7.1,
    this feature is only available in "developer-preview" mode. See the UI XDCR settings.

    """
    TIMESTAMP = "lww"
    SEQUENCE_NUMBER = "seqno"
    CUSTOM = "custom"


class StorageBackend(Enum):
    """
    **UNCOMMITTED**
    StorageBackend is part of an uncommitted API that is unlikely to change,
    but may still change as final consensus on its behavior has not yet been reached.

    Specifies the storage type to use for the bucket.
    """
    UNDEFINED = "undefined"
    COUCHSTORE = "couchstore"
    MAGMA = "magma"


class BucketSettings(dict):
    mapping = BidirectionalMapping([
        BidirectionalTransform("flush_enabled",
                               ParamTransform("flushEnabled", Identity(bool)),
                               ParamTransform("flushEnabled", Identity(bool)),
                               default=False),
        BidirectionalTransform("num_replicas",
                               ParamTransform("numReplicas", Identity(int)),
                               ParamTransform("numReplicas", Identity(int)),
                               default=0),
        BidirectionalTransform("ram_quota_mb",
                               ParamTransform("ramQuotaMB", Identity(int)),
                               ParamTransform("ramQuotaMB", Identity(int))),
        BidirectionalTransform("replica_index",
                               ParamTransform("replicaIndex", Identity(bool)),
                               ParamTransform("replicaIndex", Identity(bool)),
                               default=False),
        BidirectionalTransform("bucket_type",
                               ParamTransform(
                                   "bucketType", EnumToStr(BucketType)),
                               ParamTransform(
                                   "bucketType", StrToEnum(BucketType)),
                               default=BucketType.COUCHBASE),
        BidirectionalTransform("max_ttl",
                               ParamTransform("maxTTL", Identity(int)),
                               ParamTransform("maxTTL", Identity(int))),
        BidirectionalTransform("max_expiry",
                               ParamTransform(
                                   "maxExpiry", TimeDeltaToSeconds(int)),
                               ParamTransform("maxExpiry", SecondsToTimeDelta(timedelta))),
        BidirectionalTransform("compression_mode",
                               ParamTransform("compressionMode",
                                              EnumToStr(CompressionMode)),
                               ParamTransform("compressionMode", StrToEnum(CompressionMode))),
        BidirectionalTransform("conflict_resolution_type",
                               ParamTransform("conflictResolutionType", EnumToStr(
                                   ConflictResolutionType)),
                               ParamTransform("conflictResolutionType", StrToEnum(ConflictResolutionType))),
        BidirectionalTransform("eviction_policy",
                               ParamTransform("evictionPolicy",
                                              EnumToStr(EvictionPolicyType)),
                               ParamTransform("evictionPolicy", StrToEnum(EvictionPolicyType))),
        # BidirectionalTransform("ejection_method",
        #                        ParamTransform("ejectionMethod",
        #                                       EnumToStr(EjectionMethod)),
        #                        ParamTransform("ejectionMethod", StrToEnum(EjectionMethod))),
        BidirectionalTransform("name",
                               ParamTransform(transform=Identity(str)),
                               ParamTransform(transform=Identity(str))),
        BidirectionalTransform("minimum_durability_level",
                               ParamTransform("durabilityMinLevel", EnumToStr(
                                   DurabilityLevel, DurabilityLevel.to_server_str)),
                               ParamTransform("durabilityMinLevel", StrToEnum(
                                   DurabilityLevel, DurabilityLevel.from_server_str))),
        BidirectionalTransform("storage_backend",
                               ParamTransform("storageBackend",
                                              EnumToStr(StorageBackend)),
                               ParamTransform("storageBackend",
                                              StrToEnum(StorageBackend)),
                               default=StorageBackend.COUCHSTORE),
    ])

    @overload
    def __init__(self,
                 name=None,  # type: str
                 flush_enabled=False,  # type: bool
                 ram_quota_mb=None,  # type: int
                 num_replicas=0,  # type: int
                 replica_index=None,  # type: bool
                 bucket_type=None,  # type: BucketType
                 eviction_policy=None,  # type: EvictionPolicyType
                 max_ttl=None,  # type: Union[timedelta,float,int]
                 max_expiry=None,  # type: Union[timedelta,float,int]
                 compression_mode=None,  # type: CompressionMode
                 minimum_durability_level=None,  # type: DurabilityLevel
                 storage_backend=None,  # type: StorageBackend
                 ):
        # type: (...) -> None
        """BucketSettings provides a means of mapping bucket settings into an object.

        """
        pass

    def __init__(self, **kwargs):
        """BucketSettings provides a means of mapping bucket settings into an object.

        """
        if kwargs.get('bucket_type', None) == "couchbase":
            kwargs['bucket_type'] = BucketType.COUCHBASE
        super(BucketSettings, self).__init__(**kwargs)

    @property
    def name(self) -> str:
        """ Bucket name"""
        return self.get("name")

    @property
    def flush_enabled(self) -> bool:
        """True if flush enabled on bucket, False otherwise"""
        return self.get("flush_enabled")

    @property
    def num_replicas(self):
        # type: (...) -> int
        """NumReplicas (int) - The number of replicas for documents."""
        return self.get('replica_number')

    @property
    def replica_index(self):
        # type: (...) -> bool
        """ Whether replica indexes should be enabled for the bucket."""
        return self.get('replica_index')

    @property
    def bucket_type(self):
        # type: (...) -> BucketType
        """BucketType {couchbase (sent on wire as membase), memcached, ephemeral}
        The type of the bucket. Default to couchbase."""
        return self.get('bucket_type')

    @property
    def eviction_policy(self):
        # type: (...) -> EvictionPolicyType
        """{fullEviction | valueOnly}. The eviction policy to use."""
        return self.get('eviction_policy')

    @property
    def max_ttl(self) -> Optional[int]:
        """
         **DEPRECATED** use max_expiry
            Value for the maxTTL of new documents created without a ttl.
        """
        return self.get('max_ttl', None)

    @property
    def max_expiry(self) -> timedelta:
        """
           Value for the max expiry of new documents created without an expiry.
        """
        return self.get('max_expiry')

    @property
    def compression_mode(self):
        # type: (...) -> CompressionMode
        """{off | passive | active} - The compression mode to use."""
        return self.get('compression_mode')

    @property
    def conflict_resolution_type(self) -> Optional[ConflictResolutionType]:
        """
        {TIMESTAMP | SEQUENCE_NUMBER | CUSTOM} - The conflict resolution type to use.
        CUSTOM: **VOLATILE** This API is subject to change at any time. In Couchbase Server 7.1,
        this feature is only available in "developer-preview" mode. See the UI XDCR settings.
        """
        return self.get('conflict_resolution_type', None)

    @property
    def storage_backend(self):
        # type: (...) -> StorageBackend
        """
        {couchstore | magma | undefined} - The storage backend to use.
        """
        return self.get('storage_backend')

    def transform_to_dest(self) -> Dict[str, Any]:
        kwargs = {**self}
        # needed?
        kwargs["bucket_password"] = ""  # nosec
        params = self.mapping.transform_to_dest(kwargs)
        params.update({
            'authType': 'sasl',
            'saslPassword': kwargs['bucket_password']
        })
        return params

    @classmethod
    def transform_from_dest(cls, data  # type: Dict[str, Any]
                            ) -> BucketSettings:
        params = cls.mapping.transform_from_dest(data)
        return cls(**params)


class CreateBucketSettings(BucketSettings):
    @overload  # nosec
    def __init__(self,  # nosec
                 name=None,  # type: str
                 flush_enabled=False,  # type: bool
                 ram_quota_mb=None,  # type: int
                 num_replicas=0,  # type: int
                 replica_index=None,  # type: bool
                 bucket_type=None,  # type: BucketType
                 eviction_policy=None,  # type: EvictionPolicyType
                 max_ttl=None,  # type: Union[timedelta,float,int]
                 max_expiry=None,  # type: Union[timedelta,float,int]
                 compression_mode=None,  # type: CompressionMode
                 conflict_resolution_type=None,  # type: ConflictResolutionType
                 bucket_password="",  # type: str
                 ejection_method=None,  # type: EjectionMethod
                 storage_backend=None  # type: StorageBackend
                 ):
        """
        Bucket creation settings.

        :param name: name of the bucket
        :param flush_enabled: whether flush is enabled
        :param ram_quota_mb: raw quota in megabytes
        :param num_replicas: number of replicas
        :param replica_index: whether this is a replica index
        :param bucket_type: type of bucket
        :param eviction_policy: policy for eviction
        :param max_ttl: **DEPRECATED** max time to live for bucket
        :param max_expiry: max expiry time for bucket
        :param compression_mode: compression mode
        :param ejection_method: ejection method (deprecated, please use eviction_policy instead)
        :param storage_backend: **UNCOMMITTED** specifies the storage type to use for the bucket
        """

    def __init__(self, **kwargs):
        BucketSettings.__init__(self, **kwargs)

    @property
    def conflict_resolution_type(self):
        # type: (...) -> ConflictResolutionType
        return self.get('conflict_resolution_type')
