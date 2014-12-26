from couchbase.bucket import Bucket, _depr
from couchbase.user_constants import *
from couchbase.connstr import convert_1x_args

_depr('couchbase.connection', 'couchbase.bucket')

class Connection(Bucket):
    def __init__(self, bucket='default', **kwargs):
        kwargs = convert_1x_args(bucket, **kwargs)
        super(Connection, self).__init__(**kwargs)
