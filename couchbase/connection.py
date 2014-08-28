from warnings import warn

from couchbase.bucket import Bucket

warn("Use/import couchbase.bucket.Bucket instead", DeprecationWarning)

class Connection(Bucket):
    delete = Bucket.remove
    set = Bucket.upsert
    add = Bucket.insert
    delete_multi = Bucket.remove_multi
    set_multi = Bucket.upsert_multi
    add_multi = Bucket.insert_multi
