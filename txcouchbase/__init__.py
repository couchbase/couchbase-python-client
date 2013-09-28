from txcouchbase.connection import Connection
class TxCouchbase(object):
    @classmethod
    def connect(cls, *args, **kwargs):
        return Connection(*args, **kwargs)
