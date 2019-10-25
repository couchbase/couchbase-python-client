from couchbase.management.admin import Admin
from ..options import OptionBlock
from couchbase.management.generic import GenericManager
from typing import *
from couchbase_core import abstractmethod, mk_formstr


class BucketManager(GenericManager):
    def __init__(self,  # type: BucketManager
                 admin_bucket  # type: Admin
                 ):
        """Bucket Manager

        :param admin_bucket: Admin bucket
        """
        super(BucketManager, self).__init__(admin_bucket)

    def create_bucket(self,  # type: BucketManager
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs
                      ):
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings - settings for the bucket.

        :raises: BucketAlreadyExistsException (http 400 and content contains "Bucket with given name already exists")
        :raises: InvalidArgumentsException
        """
        self._admin_bucket.bucket_create(settings.name, bucket_type=settings.bucket_type, replicas=settings.replica_indexes,
                                         ram_quota=settings.ram_quota_mb, flush_enabled=settings.flush_enabled)

    def bucket_update(self, current, bucket_password=None, replicas=None,
                      ram_quota=None, flush_enabled=None, **kwargs):
        """
        Update an existing bucket's settings.

        :param string name: The name of the bucket to update
        :param dict current: Current state of the bucket.
            This can be retrieve from :meth:`bucket_info`
        :param str bucket_password: Change the bucket's password
        :param int replicas: The number of replicas for the bucket
        :param int ram_quota: The memory available to the bucket
            on each node.
        :param bool flush_enabled: Whether the flush API should be allowed
            from normal clients
        :return: A :class:`~.HttpResult` object
        :raise: :exc:`~.HTTPError` if the request could not be
            completed


        .. note::

            The default value for all options in this method is
            ``None``. If a value is set to something else, it will
            modify the setting.


        Change the bucket password::

            adm.bucket_update('a_bucket', adm.bucket_info('a_bucket'),
                              bucket_password='n3wpassw0rd')

        Enable the flush API::

            adm.bucket_update('a_bucket', adm.bucket_info('a_bucket'),
                              flush_enabled=True)
        """
        params = {}
        current = current.value
        name=kwargs.pop('name')
        # Merge params
        params['authType'] = current['authType']
        if 'saslPassword' in current:
            params['saslPassword'] = current['saslPassword']

        if bucket_password is not None:
            params['authType'] = 'sasl'
            params['saslPassword'] = bucket_password

        params['replicaNumber'] = (
            replicas if replicas is not None else current['replicaNumber'])

        if ram_quota:
            params['ramQuotaMB'] = ram_quota
        else:
            params['ramQuotaMB'] = int(current['quota']['rawRAM'] / 1024 / 1024)

        if flush_enabled is not None:
            params['flushEnabled'] = int(flush_enabled)

        proxy_port=current.get('proxyPort')
        if proxy_port:
            params['proxyPort'] = proxy_port

        params.update({k:v for k,v in kwargs.items() if (v is not None)})
        return self._admin_bucket.http_request(path='/pools/default/buckets/' + name,
                                 method='POST',
                                 content_type='application/x-www-form-urlencoded',
                                 content=mk_formstr(params))

    def update_bucket(self,  # type: BucketManager
                      settings,  # type: IBucketSettings
                      *options  # type: UpdateBucketOptions
                      ):
        """
        Updates a bucket. Every setting must be set to what the user wants it to be after the update.
        Any settings that are not set to their desired values may be reverted to default values by the server.

        :param IBucketSettings settings: settings for the bucket.
        :raises: InvalidArgumentsException
        :raises: BucketDoesNotExistException
        """
        current = self._admin_bucket.bucket_info(settings.name)
        self.bucket_update(current, **settings.as_dict)

    def drop_bucket(self,  # type: BucketManager
                    bucket_name,  # type: str
                    *options  # type: DropBucketOptions
                    ):
        # type: (...) -> None
        """
        Removes a bucket.

        :param str bucket_name: the name of the bucket.
        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        """
        self._admin_bucket.bucket_remove(bucket_name)

    def get_bucket(self,  # type: BucketManager
                   bucket_name,  # type: str
                   *options  # type: GetBucketOptions
                   ):
        # type: (...) -> IBucketSettings
        """
        Gets a bucket's settings.

        :param str bucket_name: the name of the bucket.
        :returns: settings for the bucket. Note: the ram quota returned is in bytes
        not mb so requires x  / 1024 twice. Also Note: FlushEnabled is not a setting returned by the server, if flush is enabled then the doFlush endpoint will be listed and should be used to populate the field.

        :rtype: IBucketSettings
        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        """
        return BucketSettings(**self._admin_bucket.bucket_info(bucket_name).value)

    def get_all_buckets(self,  # type: BucketManager
                        *options  # type: GetAllBucketOptions
                        ):
        # type: (...) -> Iterable[IBucketSettings]

        """
        Gets all bucket settings. Note,  # type: the ram quota returned is in bytes
        not mb so requires x  / 1024 twice.

        :returns: An iterable of settings for each bucket.
        :rtype: Iterable[IBucketSettings]
        """
        return list(
            map(lambda x: BucketSettings(**x),
                self._admin_bucket.http_request(path='/pools/default/buckets', method='GET').value))

    def flush_bucket(self,  # type: BucketManager
                     bucket_name,  # type: str
                     *options  # type: FlushBucketOptions
                     ):
        # using the ns_server REST interface
        """
        Flushes a bucket (uses the ns_server REST interface).

        :param str bucket_name: the name of the bucket.
        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        :raises: FlushDisabledException
        """
        self._admin_bucket.http_request(
            "/pools/default/buckets/{bucket_name}/controller/doFlush".format(bucket_name=bucket_name), method='POST')


class IBucketSettings(object):
    @property
    @abstractmethod
    def name(self):
        # type: (...) -> str
        """Name (string) - The name of the bucket."""
        pass

    @property
    @abstractmethod
    def flush_enabled(self):
        # type: (...) -> bool
        """Whether or not flush should be enabled on the bucket. Default to false."""
        pass

    @property
    @abstractmethod
    def ram_quota_mb(self):
        # type: (...) -> int
        """Ram Quota in mb for the bucket. (rawRAM in the server payload)"""
        pass

    @property
    @abstractmethod
    def num_replicas(self):
        # type: (...) -> int
        """NumReplicas (int) - The number of replicas for documents."""
        pass

    @property
    @abstractmethod
    def replica_indexes(self):
        # type: (...) -> bool
        """ Whether replica indexes should be enabled for the bucket."""

    @property
    @abstractmethod
    def bucket_type(self):
        # type: (...) -> int
        """BucketType  - The type of the bucket. Default to couchbase.
        Sent on wire as {membase, memcached, ephemeral}"""

    @property
    @abstractmethod
    def ejection_method(self):
        # type: (...) -> int
        """{fullEviction | valueOnly}. The eviction policy to use."""

    @property
    @abstractmethod
    def max_ttl(self):
        # type: (...) -> int
        """Value for the maxTTL of new documents created without a ttl."""
        pass

    @property
    @abstractmethod
    def compression_mode(self):
        # type: (...) -> int
        """""""{off | passive | active} - The compression mode to use."""

    @property
    @abstractmethod
    def as_dict(self):
        pass


class BucketSettings(IBucketSettings, dict):
    @overload
    def __init__(self, name=None, flush_enabled=None, ram_quota_mb=None, num_replicas=None, replica_indexes=None, bucket_type=None, ejection_method=None, max_ttl=None, compression_mode=None):
        pass

    def __init__(self, **raw_info):
        """BucketSettings provides a means of mapping bucket settings into an object.
        :param info:
        :param raw_info:
        """
        dict.__init__(self, Admin.bc_defaults)
        self['rawRAM']=raw_info.pop('ram_quota_mb',None)
        self['maxTTL']=raw_info.pop('max_ttl',None)
        self.update(**{k: v for k, v in raw_info.items() if (v is not None)})

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
        return self.get('rawRAM')

    @property
    def num_replicas(self):
        # type: (...) -> int
        """NumReplicas (int) - The number of replicas for documents."""
        return self.get('num_replicas')

    @property
    def replica_indexes(self):
        # type: (...) -> bool
        """ Whether replica indexes should be enabled for the bucket."""
        return self.get('replica_indexes')

    @property
    def bucket_type(self):
        # type: (...) -> int
        """BucketType {couchbase (sent on wire as membase), memcached, ephemeral}
        The type of the bucket. Default to couchbase."""
        return self.get('bucket_type')

    @property
    def ejection_method(self):
        # type: (...) -> int
        """{fullEviction | valueOnly}. The eviction policy to use."""
        return self.get('ejection_method')

    @property
    def max_ttl(self):
        # type: (...) -> int
        """Value for the maxTTL of new documents created without a ttl."""
        return self.get('maxTTL')

    @property
    def compression_mode(self):
        # type: (...) -> int
        """""""{off | passive | active} - The compression mode to use."""
        return self.get('compression_mode')

    @property
    def as_dict(self):
        return self


class ICreateBucketSettings(IBucketSettings):
    """CreateBucketSettings is a superset of BucketSettings providing one extra property, ConflictResolutionType:
    The reasoning for this is that on Update ConflictResolutionType cannot be present in the JSON payload at all.
    """
    @property
    @abstractmethod
    def conflict_resolution_type(self):
        # type: (...) -> int
        """{Timestamp (sent as lww) | SequenceNumber (sent as seqno)}. The conflict resolution type to use."""
        pass


class CreateBucketSettings(ICreateBucketSettings, BucketSettings):
    @overload
    def __init__(self, name=None, flush_enabled=None, ram_quota_mb=None, num_replicas=None, replica_indexes=None, bucket_type=None, ejection_method=None, max_ttl=None, compression_mode=None, conflict_resolution_type=None, bucket_password=None):
        pass

    def __init__(self, **kwargs):
        BucketSettings.__init__(self, **kwargs)

    @property
    def conflict_resolution_type(self):
        return self.get('conflict_resolution_type')


class CreateBucketOptions(OptionBlock):
    pass


class UpdateBucketOptions(OptionBlock):
    pass


class DropBucketOptions(OptionBlock):
    pass


class GetAllBucketOptions(OptionBlock):
    pass


class GetBucketOptions(OptionBlock):
    pass


class FlushBucketOptions(OptionBlock):
    pass
