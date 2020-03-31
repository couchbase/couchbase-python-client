from couchbase.management.admin import Admin
from ..options import OptionBlockTimeOut, forward_args
from couchbase.management.generic import GenericManager
from typing import *
from couchbase_core import abstractmethod, mk_formstr
from couchbase.exceptions import HTTPException, ErrorMapper, BucketAlreadyExistsException, BucketDoesNotExistException


class BucketManagerErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...)->Mapping[str, CBErrorType]
        return {HTTPException: {'Bucket with given name (already|still) exists': BucketAlreadyExistsException,
                                'Requested resource not found': BucketDoesNotExistException}}

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
        params = ({k:v for k,v in settings.items() if (v is not None)})

        # insure flushEnabled is an int
        params['flushEnabled'] = int(params.get('flushEnabled', None))

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
        params = ({k:v for k,v in settings.items() if (v is not None)})

        # insure flushEnabled is an int
        params['flushEnabled'] = int(params.get('flushEnabled', None))

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
        return BucketSettings(
          **self._admin_bucket.http_request(
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


class BucketSettings(dict):
    @overload
    def __init__(self, name=None, flush_enabled=None, ram_quota_mb=None, num_replicas=None, replica_index=None, bucket_type=None, eviction_policy=None, max_ttl=None, compression_mode=None):
        pass

    def __init__(self, **raw_info):
        """BucketSettings provides a means of mapping bucket settings into an object.
        :param info:
        :param raw_info:
        """

        # TODO: do we need this?
        # dict.__init__(self, Admin.bc_defaults)
        # if created from the bucket info coming back from get_bucket, we need to convert some things here...
        self.__convert_from_raw(raw_info)

        # if created by a call from the user, we need to convert the names to the camel-case versions...
        # we really could do this via a package, but I hate to add a dependency just for this
        key_tuple = [ ('flushEnabled', 'flush_enabled'),
                      ('numReplicas', 'num_replicas'),
                      ('ramQuotaMB', 'ram_quota_mb'),
                      ('replicaIndex', 'replica_index'),
                      ('bucketType', 'bucket_type'),
                      ('maxTTL', 'max_ttl'),
                      ('compressionMode', 'compression_mode'),
                      ('conflictResolutionType', 'conflict_resolution_type'),
                      ('evictionPolicy', 'eviction_policy'),
                      ('name', 'name') ]
        self.__pop_if_there(raw_info, key_tuple)

    def __pop_if_there(self, raw_info, keys):
      for k in keys:
        if isinstance(k, tuple):
          # default is the current value of self[k[0]]
          self[k[0]] = raw_info.get(k[1], self[k[0]])
        else:
          # you passed in a list of strings, so default is none and the keys are same
          self[k] = raw_info.get(k, None)

    def __convert_from_raw(self, raw_info):
      key_tuple = [ 'name', 'numReplicas', 'replicaIndex', 'replicaIndex', 'bucketType', 'maxTTL', 'compressionMode', 'conflictResolutionType', 'evictionPolicy']
      self.__pop_if_there(raw_info, key_tuple)
      quota = raw_info.get('quota', {})
      # convert rawRAM to MB
      if 'rawRAM' in quota:
        self['ramQuotaMB'] = quota.get('rawRAM')/1024/1024
      else:
        self['ramQuotaMB'] = None
      controllers = raw_info.get('controllers', {})
      self['flushEnabled'] = ('flush' in controllers)

    @property
    def name(self):
        # type: (...) -> str
        """Name (string) - The name of the bucket."""
        return self.get('name')

    @property
    def flush_enabled(self):
        # type: (...) -> bool
        """Whether or not flush should be enabled on the bucket. Default to false."""
        return self.get('flushEnabled', False)

    @property
    def ram_quota_mb(self):
        # type: (...) -> int
        """Ram Quota in mb for the bucket. (rawRAM in the server payload)"""
        return self.get('ramQuotaMB')

    @property
    def num_replicas(self):
        # type: (...) -> int
        """NumReplicas (int) - The number of replicas for documents."""
        return self.get('replicaNumber')

    @property
    def replica_index(self):
        # type: (...) -> bool
        """ Whether replica indexes should be enabled for the bucket."""
        return self.get('replicaIndex')

    @property
    def bucket_type(self):
        # type: (...) -> int
        """BucketType {couchbase (sent on wire as membase), memcached, ephemeral}
        The type of the bucket. Default to couchbase."""
        return self.get('bucketType')

    @property
    def eviction_policy(self):
        # type: (...) -> int
        """{fullEviction | valueOnly}. The eviction policy to use."""
        return self.get('evictionPolicy')

    @property
    def max_ttl(self):
        # type: (...) -> int
        """Value for the maxTTL of new documents created without a ttl."""
        return self.get('maxTTL')

    @property
    def compression_mode(self):
        # type: (...) -> int
        """""""{off | passive | active} - The compression mode to use."""
        return self.get('compressionMode')

    @property
    def as_dict(self):
        return self


class CreateBucketSettings(BucketSettings):
    @overload
    def __init__(self, name=None, flush_enabled=None, ram_quota_mb=None, num_replicas=None, replica_index=None, bucket_type=None, ejection_method=None, max_ttl=None, compression_mode=None, conflict_resolution_type=None, bucket_password=None):
        pass

    def __init__(self, **kwargs):
        BucketSettings.__init__(self, **kwargs)

    @property
    def conflict_resolution_type(self):
        return self.get('conflictResolutionType')


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
