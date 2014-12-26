from gcouchbase.bucket import Bucket
from couchbase.bucket import _depr
from couchbase.connstr import convert_1x_args

class GConnection(Bucket):
    def __init__(self, bucket, **kwargs):
        _depr('gcouchbase.connection.GConnection',
              'gcouchbase.bucket.Bucket')

        kwargs = convert_1x_args(bucket, **kwargs)
        super(GConnection, self).__init__(**kwargs)
