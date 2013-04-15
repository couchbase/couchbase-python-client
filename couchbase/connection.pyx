cdef void cb_store_callback(lcb.lcb_t instance, const void *cookie,
                            lcb.lcb_storage_t operation, lcb.lcb_error_t rc,
                            const lcb.lcb_store_resp_t *resp):
    ctx = <object>cookie
    cas = None
    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')

    if resp.v.v0.cas > 0:
        cas = resp.v.v0.cas
    ctx['operation'] = Const.store_names[operation]

    Utils.maybe_raise(rc, 'failed to store value', key=key, cas=cas,
                      operation=operation)
    ctx['rv'].append((key, cas))


cdef void cb_get_callback(lcb.lcb_t instance, const void *cookie,
                          lcb.lcb_error_t rc,
                          const lcb.lcb_get_resp_t *resp):
    # A lot of error handling is missing here, but you get at least your
    # values back
    ctx = <object>cookie

    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')

    if rc != lcb.LCB_KEY_ENOENT:
        try:
            Utils.maybe_raise(rc, 'failed to get value', key=key,
                              operation='GET')
        except CouchbaseError as e:
            ctx['exception'] = e

    flags = resp.v.v0.flags
    cas = resp.v.v0.cas
    val = None

    if resp.v.v0.nbytes != 0:
        raw = (<char *>resp.v.v0.bytes)[:resp.v.v0.nbytes]
        if ctx['force_format'] is not None:
            format = ctx['force_format']
        else:
            format = flags & FMT_MASK
        try:
            val = Connection._decode_value(raw, format)
        except Exception as e:
            ctx['exception'] = exceptions.ValueFormatError(
                "unable to convert value for key '{0}': {1}. ".format(
                    key, val))

    ctx['rv'].append((key, val))


cdef class Connection:
    cdef lcb.lcb_t _instance
    cdef lcb.lcb_create_st _create_options
    cdef public lcb.lcb_uint32_t default_format
    def __cinit__(self):
        self.default_format = FMT_JSON
        memset(&self._create_options, 0, sizeof(self._create_options))

    def __init__(self, host='localhost', port=8091, username=None,
                 password=None, bucket=None):
        """Connection to a bucket

        Normally it's initialized through
        :meth:`couchbase.Couchbase.connect`

        **Class attributes**

          **default_format** = `Couchbase.FMT_JSON`
            It uses the flags field to store the format. Possible values
            are:
             * `couchbase.FMT_JSON`: Converts the Python object to
               JSON and stores it as JSON in Couchbase
             * `couchbase.FMT_PICKLE`: Pickles the Python object and
               stores it as binary in Couchbase
             * `couchbase.FMT_PLAIN`: Stores the Python object as is
               in Couchbase. If it is a string containing valid JSON it
               will be stored as JSON, else binary.
            On a :meth:`couchbase.libcouchbase.Connection.get` the
            original value will be returned. This means the JSON will be
            decoded, respectively the object will be unpickled.
        """
        if password is None:
            raise exceptions.ArgumentError("A password must be given")
        if bucket is None:
            raise exceptions.ArgumentError("A bucket name must be given")

        host = ('%s:%d' % (host, port)).encode('utf-8')
        password = password.encode('utf-8')
        bucket = bucket.encode('utf-8')

        if username is None:
            # Try to connect to a protected bucket
            username = bucket
        else:
            username = username.encode('utf-8')

        self._create_options.v.v0.host = host
        self._create_options.v.v0.bucket = bucket
        self._create_options.v.v0.user = username
        self._create_options.v.v0.passwd = password

        rc = lcb.lcb_create(&self._instance, &self._create_options)
        Utils.maybe_raise(rc, 'failed to create libcouchbase instance')
        lcb.lcb_behavior_set_syncmode(self._instance, lcb.LCB_SYNCHRONOUS)

        <void>lcb.lcb_set_store_callback(self._instance, cb_store_callback);

        self._connect()

    def _connect(self):
        rc = lcb.lcb_connect(self._instance)
        Utils.maybe_raise(
            rc, 'failed to connect libcouchbase instance to server')

    @staticmethod
    def _encode_value(value, format):
        """Encode some value according to a given format

        The input value can be any Python object. The `format` specifies
        whether it should be as JSON string, pickled or not encoded at all
        (plain).
        """
        if format == FMT_JSON:
            return json.dumps(value).encode('utf-8')
        elif format == FMT_PICKLE:
            return pickle.dumps(value)
        elif format == FMT_PLAIN:
            return value
        else:
            # Unknown formats are treatedy as plain
            return value

    @staticmethod
    def _decode_value(value, format):
        """Decode some value according to a given format

        The input value is either encoded as a JSON string, pickled or not
        encoded at all (plain).
        """
        if format == FMT_JSON:
            return json.loads(value.decode('utf-8'))
        elif format == FMT_PICKLE:
            return pickle.loads(value)
        elif format == FMT_PLAIN:
            return value
        else:
            # Unknown formats are treated as plain
            return value

    @staticmethod
    def _context_dict():
        return {
            'exception': None,
            'force_format': None,
            'rv': []
        }

    def set(self, key, value, cas=None, format=None):
        """Unconditionally store the object in the Couchbase

        :param string key: key used to reference the value
        :param any value: value to be stored
        :param int cas: the CAS value for an object. This value is created
          on the server and is guaranteed to be unique for each value of
          a given key. This value is used to provide simple optimistic
          concurrency control when multiple clients or threads try to
          update an item simultaneously.
        :param format: the representation for storing the value in the
          bucket. If none is specified it will use the `default_format`.
          For more info see
          :attr:`couchbase.libcouchbase.Connection.default_format`.

        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed
        :raise: :exc:`couchbase.exceptions.KeyExistsError` if the key
          already exists on the server with a different CAS value.
        :raise: :exc:`couchbase.exceptions.ValueFormatError` if the value
          cannot be serialized with chosen encoder, e.g. if you try to
          store a dictionaty in plain mode.

        :return: (*int*) the CAS value of the object

        Simple set::

            cb.set('key', 'value')

        Force JSON document format for value::

            cb.set('foo', {'bar': 'baz'}, format=couchbase.FMT_JSON)

        Perform optimistic locking by specifying last known CAS version

            cb.set('foo', 'bar', cas=8835713818674332672)
        """
        if self._instance == NULL:
            Utils.raise_not_connected(lcb.LCB_SET)

        key = key.encode('utf-8')
        cdef char *c_key = key

        if format is None:
            format = self.default_format

        if format == FMT_PLAIN and not isinstance(value, bytes):
            raise exceptions.ValueFormatError(
                "unable to convert value for key '{0}': {1}. "
                "FMT_PLAIN expects a byte array".format(
                    key.decode('utf-8'), value))
        try:
            value = self._encode_value(value, format)
        except TypeError:
            raise exceptions.ValueFormatError(
                "unable to convert value for key '{0}': {1}. "
                "FMT_JSON expects a JSON serializable object".format(
                    key.decode('utf-8'), value))
        cdef char *c_value = value
        ctx = self._context_dict()

        cdef int num_commands = 1
        cdef lcb.lcb_store_cmd_t cmd
        cdef const lcb.lcb_store_cmd_t **commands = \
            <const lcb.lcb_store_cmd_t **>malloc(
                num_commands * sizeof(lcb.lcb_store_cmd_t *))
        if not commands:
            raise MemoryError()

        try:
            commands[0] = &cmd
            memset(&cmd, 0, sizeof(cmd))
            cmd.v.v0.operation = lcb.LCB_SET
            cmd.v.v0.key = c_key
            cmd.v.v0.nkey = len(key)
            cmd.v.v0.bytes = c_value
            cmd.v.v0.nbytes = len(value)
            cmd.v.v0.flags = format
            if cas is not None:
                cmd.v.v0.cas = cas

            rc = lcb.lcb_store(self._instance, <void *>ctx, 1, commands)
            Utils.maybe_raise(rc, 'failed to schedule set request')

            # TODO vmx 2013-04-12: Wait for all operations to be processed
            #    This should already be the case in sync mode, but I'm not
            #    sure

            if num_commands > 1:
                return ctx['rv']
            else:
                return ctx['rv'][0][1]
        finally:
            free(commands)

    def get(self, key, format=None):
        """Obtain an object stored in Couchbase by given key

        :param string key: key used to reference the value
        :param format: explicitly choose the decoer for this key. If
          none is specified the decoder will automaticall be choosen
          based on the encoder that was used to store the value. For
          more information about the formats, see
          :attr:`couchbase.libcouchbase.Connection.default_format`.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          is missing in the bucket
        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed
        :raise: :exc:`couchbase.exceptions.ValueFormatError` if the
          value cannot be deserialized with chosen decoder, e.g. if you
          try to retreive a pickled object in JSON mode.

        :return: the value associated with the key

        Simple get::

            value = cb.get('key')
        """
        if self._instance == NULL:
            Utils.raise_not_connected(lcb.LCB_SET)

        key = key.encode('utf-8')
        cdef char *c_key = key

        ctx = self._context_dict()
        ctx['force_format'] = format

        <void>lcb.lcb_set_get_callback(self._instance, cb_get_callback);

        cdef int num_commands = 1
        cdef lcb.lcb_get_cmd_t cmd
        cdef const lcb.lcb_get_cmd_t **commands = \
            <const lcb.lcb_get_cmd_t **>malloc(
                num_commands * sizeof(lcb.lcb_get_cmd_t *))
        if not commands:
            raise MemoryError()

        try:
            commands[0] = &cmd
            memset(&cmd, 0, sizeof(cmd))
            cmd.v.v0.key = c_key
            cmd.v.v0.nkey = len(key)
            rc = lcb.lcb_get(self._instance, <void *>ctx, 1, commands)

            Utils.maybe_raise(rc, 'failed to schedule get request')

            # TODO vmx 2013-04-12: Wait for all operations to be processed
            #    This should already be the case in sync mode, but I'm not
            #    sure

            if ctx['exception']:
                raise ctx['exception']

            return ctx['rv'][0][1]
        finally:
            free(commands)
