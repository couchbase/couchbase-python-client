from typing import (TYPE_CHECKING,
                    Any,
                    List)

from couchbase.management.logic.buckets_logic import BucketType  # noqa: F401
from couchbase.management.logic.buckets_logic import CompressionMode  # noqa: F401
from couchbase.management.logic.buckets_logic import ConflictResolutionType  # noqa: F401
from couchbase.management.logic.buckets_logic import EjectionMethod  # noqa: F401
from couchbase.management.logic.buckets_logic import EvictionPolicyType  # noqa: F401
from couchbase.management.logic.buckets_logic import StorageBackend  # noqa: F401
from couchbase.management.logic.buckets_logic import (BucketManagerLogic,
                                                      BucketSettings,
                                                      CreateBucketSettings)
from couchbase.management.logic.wrappers import BucketMgmtWrapper

if TYPE_CHECKING:
    from couchbase.management.options import (CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManager(BucketManagerLogic):
    def __init__(self, connection):
        super().__init__(connection)

    @BucketMgmtWrapper.block(None, BucketManagerLogic._ERROR_MAPPING)
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
        super().create_bucket(settings, *options, **kwargs)

    @BucketMgmtWrapper.block(None, BucketManagerLogic._ERROR_MAPPING)
    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Any
                      ) -> None:

        super().update_bucket(settings, *options, **kwargs)

    @BucketMgmtWrapper.block(None, BucketManagerLogic._ERROR_MAPPING)
    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Any
                    ) -> None:

        super().drop_bucket(bucket_name, *options, **kwargs)

    @BucketMgmtWrapper.block(BucketSettings, BucketManagerLogic._ERROR_MAPPING)
    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Any
                   ) -> BucketSettings:

        return super().get_bucket(bucket_name, *options, **kwargs)

    @BucketMgmtWrapper.block(BucketSettings, BucketManagerLogic._ERROR_MAPPING)
    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Any
                        ) -> List[BucketSettings]:

        return super().get_all_buckets(*options, **kwargs)

    @BucketMgmtWrapper.block(None, BucketManagerLogic._ERROR_MAPPING)
    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Any
                     ) -> None:

        super().flush_bucket(bucket_name, *options, **kwargs)
