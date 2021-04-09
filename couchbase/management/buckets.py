from couchbase.management.admin import Admin
from couchbase_core.mapper import BijectiveMapping, \
    StringEnum, Identity, Timedelta, Bijection, StringEnumLoose
from ..options import OptionBlockTimeOut, forward_args
from couchbase.management.generic import GenericManager
from typing import *
from couchbase_core import abstractmethod, mk_formstr
from couchbase_core.durability import Durability
from couchbase.exceptions import HTTPException, ErrorMapper, \
    BucketAlreadyExistsException, BucketDoesNotExistException, \
    BucketNotFlushableException
import enum
import datetime


class BucketManagerErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...)->Mapping[str, CBErrorType]
        return {HTTPException: {'Bucket with given name (already|still) exists': BucketAlreadyExistsException,
                                'Requested resource not found': BucketDoesNotExistException,
                                'Flush is disabled for the bucket': BucketNotFlushableException}}


@BucketManagerErrorHandler.wrap
class BucketManager(GenericManager):
    def __init__(self,         # type: BucketManager
                 admin_bucket  # type: Admin
                 ):
        """Bucket Manager

        :param admin_bucket: Admin bucket
        """
        super(BucketManager, self).__init__(admin_bucket)

    def create_bucket(self,      # type: BucketManager
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Any
                      ):
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings: settings for the bucket.
        :param: CreateBucketOptions options: options for setting the bucket.
        :param: Any kwargs: override corresponding values in the options.

        :raises: BucketAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # prune the missing settings...
        params = settings.as_dict()

        # insure flushEnabled is an int
        params['flushEnabled'] = int(params.get('flushEnabled', 0))

        # ensure replicaIndex is an int, if specified
        if 'replicaIndex' in params:
            params['replicaIndex'] = 1 if params['replicaIndex'] else 0

        # send it
        return self._admin_bucket.http_request(
            path='/pools/default/buckets',
            method='POST',
            content=mk_formstr(params),
            content_type='application/x-www-form-urlencoded',
            **forward_args(kwargs, *options))

    def update_bucket(self,     # type: BucketManager
                      settings, # type: BucketSettings
                      *options, # type: UpdateBucketOptions
                      **kwargs  # type: Any
                      ):
        """
        Updates a bucket. Every setting must be set to what the user wants it to be after the update.
        Any settings that are not set to their desired values may be reverted to default values by the server.

        :param BucketSettings settings: settings for updating the bucket.
        :param UpdateBucketOptions options: options for updating the bucket.
        :param Any kwargs: override corresponding values in the options.

        :raises: InvalidArgumentsException
        :raises: BucketDoesNotExistException
        """

        # prune the missing settings...
        params = settings.as_dict ()#*options, **kwargs)

        # insure flushEnabled is an int
        params['flushEnabled'] = int(params.get('flushEnabled', 0))

        # send it
        return self._admin_bucket.http_request(
            path='/pools/default/buckets/' + settings.name,
            method='POST',
            content_type='application/x-www-form-urlencoded',
            content=mk_formstr(params),
            **forward_args(kwargs, *options))

    def drop_bucket(self,         # type: BucketManager
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Any
                    ):
        # type: (...) -> None
        """
        Removes a bucket.

        :param str bucket_name: the name of the bucket.
        :param DropBucketOptions options: options for dropping the bucket.
        :param Any kwargs: override corresponding value in the options.

        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        """
        return self._admin_bucket.http_request(
            path='/pools/default/buckets/' + bucket_name,
            method='DELETE',
            **forward_args(kwargs, *options))

    def get_bucket(self,          # type: BucketManager
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Any
                   ):
        # type: (...) -> BucketSettings
        """
        Gets a bucket's settings.

        :param str bucket_name: the name of the bucket.
        :param GetBucketOptions options: options for getting the bucket.
        :param Any kwargs: override corresponding values in options.

        :returns: settings for the bucket. Note: the ram quota returned is in bytes
          not mb so requires x  / 1024 twice. Also Note: FlushEnabled is not a setting returned by the server, if flush is enabled then the doFlush endpoint will be listed and should be used to populate the field.

        :rtype: BucketSettings
        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        """
        return BucketSettings.from_raw(
          self._admin_bucket.http_request(
              path='/pools/default/buckets/' + bucket_name,
              method='GET',
              **forward_args(kwargs, *options)
            ).value)

    def get_all_buckets(self,     # type: BucketManager
                        *options, # type: GetAllBucketOptions
                        **kwargs  # type: Any
                        ):
        # type: (...) -> Iterable[BucketSettings]

        """
        Gets all bucket settings. Note,  # type: the ram quota returned is in bytes
        not mb so requires x  / 1024 twice.

        :param GetAllBucketOptions options: options for getting all buckets.
        :param Any kwargs: override corresponding value in options.

        :returns: An iterable of settings for each bucket.
        :rtype: Iterable[BucketSettings]
        """
        return list(
            map(lambda x: BucketSettings(**x),
                self._admin_bucket.http_request(
                    path='/pools/default/buckets',
                    method='GET',
                    **forward_args(kwargs, *options)
                  ).value))

    def flush_bucket(self,          # type: BucketManager
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Any
                     ):
        # using the ns_server REST interface
        """
        Flushes a bucket (uses the ns_server REST interface).

        :param str bucket_name: the name of the bucket.
        :param FlushBucketOptions options: options for flushing the bucket.
        :param Any kwargs: override corresponding value in options.

        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        :raises: FlushDisabledException
        """
        self._admin_bucket.http_request(
            path="/pools/default/buckets/{bucket_name}/controller/doFlush".format(bucket_name=bucket_name),
            method='POST',
            **forward_args(kwargs, *options))


class EvictionPolicyType(enum.Enum):
    NOT_RECENTLY_USED = "nruEviction"
    NO_EVICTION = "noEviction"
    FULL = "fullEviction"
    VALUE_ONLY = "valueOnly"


class EjectionMethod(enum.Enum):
    FULL_EVICTION = "fullEviction"
    VALUE_ONLY = "valueOnly"


class BucketType(enum.Enum):
    COUCHBASE = "membase"
    MEMCACHED = "memcached"
    EPHEMERAL = "ephemeral"


class CompressionMode(enum.Enum):
    OFF = "off"
    PASSIVE = "passive"
    ACTIVE = "active"


class ConflictResolutionType(enum.Enum):
    TIMESTAMP = "lww"
    SEQUENCE_NUMBER = "seqno"


class BucketSettings(dict):
    mapping = BijectiveMapping({'flushEnabled': {'flush_enabled': Bijection(int.__bool__, bool.__int__)},
                                'numReplicas': {'num_replicas': Identity(int)},
                                'ramQuotaMB': {'ram_quota_mb': Identity(int)},
                                'replicaNumber': {'num_replicas': Identity(int)},
                                'replicaIndex': {'replica_index': Identity(bool)},
                                'bucketType': {'bucket_type': -StringEnumLoose(BucketType)},
                                'maxTTL': {'max_ttl': -Timedelta(int)},
                                'compressionMode': {'compression_mode': -StringEnum(CompressionMode)},
                                'conflictResolutionType': {
                                    'conflict_resolution_type': -StringEnumLoose(ConflictResolutionType)},
                                'evictionPolicy': {'eviction_policy': -StringEnumLoose(EvictionPolicyType)},
                                'ejectionMethod': {'ejection_method': -StringEnumLoose(EjectionMethod)},
                                'name': {'name': Identity(str)},
                                'durabilityMinLevel': {'minimum_durability_level': Identity(str)}})

    @overload
    def __init__(self,
                 name=None,  # type: str
                 flush_enabled=False,  # type: bool
                 ram_quota_mb=None,  # type: int
                 num_replicas=None,  # type: int
                 replica_index=None,  # type: bool
                 bucket_type=None,  # type: BucketType
                 eviction_policy=None,  # type: EvictionPolicyType
                 max_ttl=None,  # type: Union[datetime.timedelta,float,int]
                 compression_mode=None  # type: CompressionMode
                 ):
        # type: (...) -> None
        pass

    def __init__(self, **kwargs):
        """BucketSettings provides a means of mapping bucket settings into an object.

        """
        if kwargs.get('bucket_type',None) == "couchbase":
            kwargs['bucket_type'] = BucketType.COUCHBASE

        """
            PYCBC-956
            Bucket min durability setting is represented as string on the wire.
            See Durability enum for string representations
        """
        durability = kwargs.pop('minimum_durability_level', None)
        if durability:
            if isinstance(durability, Durability):
                kwargs['minimum_durability_level'] = durability.to_server_str()
            else:
                kwargs['minimum_durability_level'] = Durability.from_server_str(durability)

        super(BucketSettings, self).__init__(**self.mapping.sanitize_src(kwargs))

    def as_dict(self, *options, **kwargs):
        final_opts = dict(**Admin.bc_defaults)
        final_opts.update(**forward_args(kwargs,*options))
        params=self.mapping.to_src(self)
        params.update({
            'authType': 'sasl',
            'saslPassword': final_opts['bucket_password']
        })
        return params

    @classmethod
    def from_raw(cls,
                 raw_info  # type: Mapping[str, Any]
                 ):
        # type: (...) -> BucketSettings
        result = cls(**cls.mapping.to_dest(raw_info))

        quota = raw_info.get('quota', {})
        # convert rawRAM to MB
        if 'rawRAM' in quota:
            result['ram_quota_mb'] = quota.get('rawRAM') / 1024 / 1024
        else:
            result['ram_quota_mb'] = None
        controllers = raw_info.get('controllers', {})
        result['flush_enabled'] = ('flush' in controllers)
        return result

    @property
    def name(self):
        # type: (...) -> str
        """Name (string) - The name of the bucket."""
        return self.get('name')

    @property
    def flush_enabled(self):
        # type: (...) -> bool
        """Whether or not flush should be enabled on the bucket. Default to false."""
        return self.get('flush_enabled', False)

    @property
    def ram_quota_mb(self):
        # type: (...) -> int
        """Ram Quota in mb for the bucket. (rawRAM in the server payload)"""
        return self.get('ram_quota_mb')

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
        return self.get('bucketType')

    @property
    def eviction_policy(self):
        # type: (...) -> EvictionPolicyType
        """{fullEviction | valueOnly}. The eviction policy to use."""
        return self.get('eviction_policy')

    @property
    def max_ttl(self):
        # type: (...) -> datetime.timedelta
        """Value for the maxTTL of new documents created without a ttl."""
        return self.get('max_ttl')

    @property
    def compression_mode(self):
        # type: (...) -> CompressionMode
        """{off | passive | active} - The compression mode to use."""
        return self.get('compression_mode')


class CreateBucketSettings(BucketSettings):
    @overload
    def __init__(self,
                 name=None,  # type: str
                 flush_enabled=False,  # type: bool
                 ram_quota_mb=None,  # type: int
                 num_replicas=None,  # type: int
                 replica_index=None,  # type: bool
                 bucket_type=None,  # type: BucketType
                 eviction_policy=None,  # type: EvictionPolicyType
                 max_ttl=None,  # type: Union[datetime.timedelta,float,int]
                 compression_mode=None,  # type: CompressionMode
                 conflict_resolution_type=None,  # type: ConflictResolutionType
                 bucket_password=None,  # type: str
                 ejection_method=None  # type: EjectionMethod
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
        :param max_ttl: max time to live for bucket
        :param compression_mode: compression mode
        :param ejection_method: ejection method (deprecated, please use eviction_policy instead)
        """

    def __init__(self, **kwargs):
        BucketSettings.__init__(self, **kwargs)

    @property
    def conflict_resolution_type(self):
        # type: (...) -> ConflictResolutionType
        return self.get('conflict_resolution_type')


class CreateBucketOptions(OptionBlockTimeOut):
    pass


class UpdateBucketOptions(OptionBlockTimeOut):
    pass


class DropBucketOptions(OptionBlockTimeOut):
    pass


class GetAllBucketOptions(OptionBlockTimeOut):
    pass


class GetBucketOptions(OptionBlockTimeOut):
    pass


class FlushBucketOptions(OptionBlockTimeOut):
    pass
