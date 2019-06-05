from couchbase_core.cluster import *
from .bucket import Bucket
from couchbase_core.cluster import Cluster as CoreCluster


class Cluster(CoreCluster):
    # list of all authentication types, keep up to date, used to identify connstr/kwargs auth styles

    def __init__(self, connection_string='couchbase://localhost',
                 bucket_class=Bucket):
        super(Cluster, self).__init__(connection_string, bucket_class)