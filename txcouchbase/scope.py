from txcouchbase.collection import Collection


class Scope:
    def __init__(self, bucket, scope_name):
        self._bucket = bucket
        self._set_connection()
        self._loop = bucket.loop
        self._scope_name = scope_name

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._connection

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @property
    def transcoder(self):
        """
        **INTERNAL**
        """
        return self._bucket.transcoder

    @property
    def name(self):
        return self._scope_name

    @property
    def bucket_name(self):
        return self._bucket.name

    def collection(self, name  # type: str
                   ) -> Collection:

        return Collection(self, name)

    def _connect_bucket(self):
        """
        **INTERNAL**
        """
        return self._bucket.on_connect()

    def _set_connection(self):
        """
        **INTERNAL**
        """
        self._connection = self._bucket.connection

    @staticmethod
    def default_name():
        return "_default"
