Result = namedtuple('Result', ['value', 'flags', 'cas'])


cdef void cb_store_callback(lcb.lcb_t instance, const void *cookie,
                            lcb.lcb_storage_t operation, lcb.lcb_error_t rc,
                            const lcb.lcb_store_resp_t *resp):
    ctx = <object>cookie
    cas = None
    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')

    if resp.v.v0.cas > 0:
        cas = resp.v.v0.cas
    ctx['operation'] = Const.store_names[operation]

    try:
        Utils.maybe_raise(rc, 'failed to store value', key=key, cas=cas,
                          operation=operation)
    except CouchbaseError as e:
        ctx['exception'] = e

    ctx['rv'][key] = cas


cdef void cb_get_callback(lcb.lcb_t instance, const void *cookie,
                          lcb.lcb_error_t rc,
                          const lcb.lcb_get_resp_t *resp):
    # A lot of error handling is missing here, but you get at least your
    # values back
    ctx = <object>cookie

    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')

    if rc != lcb.LCB_KEY_ENOENT or not ctx['quiet']:
        try:
            Utils.maybe_raise(rc, 'failed to get value', key=key, operation='GET')
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
                    key, val), key=key)

    if ctx['extended']:
        ctx['rv'].append((key, Result(val, flags, cas)))
    else:
        ctx['rv'].append(val)


cdef class Connection:
    cdef lcb.lcb_t _instance
    cdef lcb.lcb_create_st _create_options
    cdef public lcb.lcb_uint32_t default_format
    cdef public bint quiet
    def __cinit__(self):
        self.default_format = FMT_JSON
        memset(&self._create_options, 0, sizeof(self._create_options))

    def __init__(self, host='localhost', port=8091, username=None,
                 password=None, bucket=None, quiet=False):
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

          **quiet** = `False`
            It controlls whether to raise an exception when the client
            executes operations on non-existent keys. If it is `False`
            it will raise :exc:`couchbase.exceptions.NotFoundError`
            exceptions. When set to `True` the operations will return
            `None` silently.
        """
        if password is None:
            raise exceptions.ArgumentError("A password must be given")
        if bucket is None:
            raise exceptions.ArgumentError("A bucket name must be given")

        self.quiet = quiet

        host = '{0}:{1}'.format(host, port).encode('utf-8')
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
        <void>lcb.lcb_set_get_callback(self._instance, cb_get_callback);

        self._connect()

    def __dealloc__(self):
        if self._instance:
            lcb.lcb_destroy(self._instance)

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
        if format == FMT_PLAIN and not isinstance(value, bytes):
            raise exceptions.ValueFormatError("FMT_PLAIN expects a byte array")

        if format == FMT_JSON:
            try:
                return json.dumps(value).encode('utf-8')
            except TypeError:
                raise exceptions.ValueFormatError(
                    "FMT_JSON expects a JSON serializable object")
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
            'extended': False,
            'force_format': None,
            'rv': None
        }

    def set(self, key, value=None, cas=None, ttl=None, format=None):
        """Unconditionally store the object in the Couchbase

        :param key: if it's a string it's the key used to reference the
          value. In case of a dict, it's a multi-set where the key-value
          pairs will be stored.
        :type key: string or dict
        :param any value: value to be stored
        :param int cas: the CAS value for an object. This value is created
          on the server and is guaranteed to be unique for each value of
          a given key. This value is used to provide simple optimistic
          concurrency control when multiple clients or threads try to
          update an item simultaneously.
        :param int ttl: the time to live for an object. Values larger than
         30*24*60*60 seconds (30 days) are interpreted as absolute times
         (from the epoch).
        :param format: the representation for storing the value in the
          bucket. If none is specified it will use the `default_format`.
          For more info see
          :attr:`couchbase.libcouchbase.Connection.default_format`.

        :raise: :exc:`couchbase.exceptions.ArgumentError` if an
          argument is supplied that is not applicable in this context.
          For example setting the CAS value on a multi set.
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

        Perform optimistic locking by specifying last known CAS version::

            cb.set('foo', 'bar', cas=8835713818674332672)

        Several sets at the same time (mutli-set)::

            cb.set({'foo': 'bar', 'baz': 'value'})
        """
        if self._instance == NULL:
            Utils.raise_not_connected(lcb.LCB_SET)

        # A single key
        if not isinstance(key, dict):
            data = {key: value}
        else:
            if cas:
                raise exceptions.ArgumentError(
                    "setting `cas` is not applicable on a multi-set operation")
            data = key

        ctx = self._context_dict()
        ctx['rv'] = {}
        cdef int num_commands = len(data)
        cdef int i = 0

        cdef lcb.lcb_store_cmd_t *cmds = <lcb.lcb_store_cmd_t *>malloc(
            num_commands * sizeof(lcb.lcb_store_cmd_t))
        if not cmds:
            raise MemoryError()
        cdef const lcb.lcb_store_cmd_t **ptr_cmds = \
            <const lcb.lcb_store_cmd_t **>malloc(
                num_commands * sizeof(lcb.lcb_store_cmd_t *))
        if not ptr_cmds:
            free(cmds)
            raise MemoryError()

        # Those lists are needed as there must be a reference to the string
        # in the Python space
        keys = []
        values = []

        try:
            for key, value in data.items():
                keys.append(key.encode('utf-8'))
                if format is None:
                    format = self.default_format
                try:
                    values.append(self._encode_value(value, format))
                except exceptions.ValueFormatError as e:
                    e.msg = ("unable to convert value for key "
                             "'{0}': {1}. ").format(key, value) + e.msg
                    e.key = key
                    raise e

                ptr_cmds[i] = &cmds[i]
                memset(&cmds[i], 0, sizeof(lcb.lcb_store_cmd_t))
                cmds[i].v.v0.operation = lcb.LCB_SET
                cmds[i].v.v0.key = <char *>keys[i]
                cmds[i].v.v0.nkey = len(keys[i])
                cmds[i].v.v0.bytes = <char *>values[i]
                cmds[i].v.v0.nbytes = len(values[i])
                cmds[i].v.v0.flags = format
                if cas is not None:
                    cmds[i].v.v0.cas = cas
                if ttl is not None:
                    cmds[i].v.v0.exptime = ttl
                i += 1

            rc = lcb.lcb_store(self._instance, <void *>ctx, num_commands,
                               ptr_cmds)
            Utils.maybe_raise(rc, 'failed to schedule set request')

            # TODO vmx 2013-04-12: Wait for all operations to be processed
            #    This should already be the case in sync mode, but I'm not
            #    sure

            if ctx['exception']:
                raise ctx['exception']

            if num_commands > 1:
                return ctx['rv']
            else:
                return list(ctx['rv'].values())[0]
        finally:
            free(cmds)
            free(ptr_cmds)

    def get(self, keys, extended=False, format=None, quiet=None):
        """Obtain an object stored in Couchbase by given key

        :param keys: One or several keys to fetch
        :type key: string or list
        :param boolean extended: If set to `True`, the operation will
          return a named tuple with `value`, `flags` and `cas`,
          otherwise (by default) it returns just the value.
        :param format: explicitly choose the decoder for this key. If
          none is specified the decoder will automaticall be choosen
          based on the encoder that was used to store the value. For
          more information about the formats, see
          :attr:`couchbase.libcouchbase.Connection.default_format`.
        :param boolean quiet: causes `get` to return None instead of
          raising an exception when the key is not found. It defaults
          to the value set by
          :attr:`couchbase.libcouchbase.Connection.quiet`.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          is missing in the bucket
        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed
        :raise: :exc:`couchbase.exceptions.ValueFormatError` if the
          value cannot be deserialized with chosen decoder, e.g. if you
          try to retreive a pickled object in JSON mode.

        :return: the value associated with the key or a dict on a
          multi-get in `extended` mode.

        Simple get::

            value = cb.get('key')

        Get multiple values::

            cb.get(['foo', 'bar', 'baz'])
            # [val1, val2, val3]

        Extended get::

            value, flags, cas = cb.get('key', extended=True)

        Extended get using named tuples::

            result = cb.get('key', extended=True)
            # result.value, result.flags, result.cas

        Get multiple values in extended mode::

            cb.get(['foo', 'bar'], extended=True)
            # {'foo': (val1, flags1, cas1), 'bar': (val2, flags2, cas2)}
        """
        if self._instance == NULL:
            Utils.raise_not_connected(lcb.LCB_GET)

        if isinstance(keys, list):
            single_key = False
        else:
            keys = [keys]
            single_key = True

        ctx = self._context_dict()
        ctx['rv'] = []
        ctx['force_format'] = format
        if quiet is None:
            quiet = self.quiet
        ctx['quiet'] = quiet
        ctx['extended'] = extended
        cdef int num_cmds = len(keys)
        cdef int i = 0

        cdef lcb.lcb_get_cmd_t *cmds = <lcb.lcb_get_cmd_t *>malloc(
            num_cmds * sizeof(lcb.lcb_get_cmd_t))
        if not cmds:
            raise MemoryError()
        cdef const lcb.lcb_get_cmd_t **ptr_cmds = \
            <const lcb.lcb_get_cmd_t **>malloc(
                num_cmds * sizeof(lcb.lcb_get_cmd_t *))
        if not ptr_cmds:
            free(cmds)
            raise MemoryError()

        try:
            for i in range(len(keys)):
                keys[i] = keys[i].encode('utf-8')

                ptr_cmds[i] = &cmds[i]
                memset(&cmds[i], 0, sizeof(lcb.lcb_get_cmd_t))
                cmds[i].v.v0.key = <char *>keys[i]
                cmds[i].v.v0.nkey = len(keys[i])

            rc = lcb.lcb_get(self._instance, <void *>ctx, num_cmds, ptr_cmds)
            if rc != lcb.LCB_KEY_ENOENT or not ctx['quiet']:
                Utils.maybe_raise(rc, 'failed to schedule get request')

            # TODO vmx 2013-04-12: Wait for all operations to be processed
            #    This should already be the case in sync mode, but I'm not
            #    sure

            if ctx['exception']:
                raise ctx['exception']

            if single_key:
                if extended:
                    return ctx['rv'][0][1]
                return ctx['rv'][0]
            else:
                if extended:
                    return dict(ctx['rv'])
                return ctx['rv']
        finally:
            free(cmds)
            free(ptr_cmds)
