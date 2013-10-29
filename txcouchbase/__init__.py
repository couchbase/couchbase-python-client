__version__ = '0.1.0'
class TxCouchbase(object):
    @classmethod
    def connect(cls, *args, **kwargs):
        from txcouchbase.connection import Connection
        return Connection(*args, **kwargs)
