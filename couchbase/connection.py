from warnings import warn

from couchbase.bucket import Bucket

warn("Use/import couchbase.bucket.Bucket instead", DeprecationWarning)

class Connection(Bucket): pass
