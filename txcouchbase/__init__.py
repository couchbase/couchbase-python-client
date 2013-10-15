from txcouchbase.connection import Connection
__version__ = '0.1.0'

class TxCouchbase(object):
    @classmethod
    def connect(cls, *args, **kwargs):
        return Connection(*args, **kwargs)
