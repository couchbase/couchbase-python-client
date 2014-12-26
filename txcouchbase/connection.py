from txcouchbase.bucket import Bucket, RawBucket
from couchbase.bucket import _depr
from couchbase.connstr import convert_1x_args

class TxAsyncConnection(RawBucket):
    def __init__(self, bucket, **kwargs):
        _depr('txcouchbase.connection.TxAsyncConnection',
              'txcouchbase.bucket.RawBucket')
        kwargs = convert_1x_args(bucket, **kwargs)
        super(TxAsyncConnection, self).__init__(**kwargs)


class Connection(Bucket):
    def __init__(self, bucket, **kwargs):
        _depr('txcouchbase.connection.Connection',
              'txcouchbase.bucket.Bucket')
        kwargs = convert_1x_args(bucket, **kwargs)
        super(Connection, self).__init__(**kwargs)
