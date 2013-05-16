Result = namedtuple('Result', ['value', 'flags', 'cas'])


cdef void cb_store_callback(lcb.lcb_t instance, const void *cookie,
                            lcb.lcb_storage_t operation, lcb.lcb_error_t rc,
                            const lcb.lcb_store_resp_t *resp) with gil:
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
                          const lcb.lcb_get_resp_t *resp) with gil:
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

cdef void cb_remove_callback(lcb.lcb_t instance, const void *cookie,
                             lcb.lcb_error_t rc,
                             const lcb.lcb_remove_resp_t *resp) with gil:
    ctx = <object>cookie;
    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')
    try:
        if rc != lcb.LCB_KEY_ENOENT or not ctx['quiet']:
            Utils.maybe_raise(rc, 'failed to remove value', key=key)
    except CouchbaseError as e:
        ctx['exception'] = e

    if rc == lcb.LCB_SUCCESS:
        ctx['rv'][key] = True
    else:
        ctx['rv'][key] = False

cdef void cb_arithmetic_callback(lcb.lcb_t instance, const void *cookie,
                                 lcb.lcb_error_t rc,
                                 const lcb.lcb_arithmetic_resp_t *resp) with gil:
    ctx = <object>cookie;
    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')
    try:
        Utils.maybe_raise(rc, 'failed to incr/decr value', key=key)
    except CouchbaseError as e:
        ctx['exception'] = e

    rv = None
    if rc == lcb.LCB_SUCCESS:
        if ctx['extended']:
            rv = Result(resp.v.v0.value, 0, resp.v.v0.cas)
        else:
            rv = resp.v.v0.value

    ctx['rv'][key] = rv

cdef void cb_stat_callback(lcb.lcb_t instance, const void *cookie,
                           lcb.lcb_error_t rc,
                           const lcb.lcb_server_stat_resp_t *resp) with gil:
    ctx = <object>cookie

    node = None
    if resp.v.v0.server_endpoint:
        node = (<char *>resp.v.v0.server_endpoint).decode('utf-8')
    try:
        Utils.maybe_raise(rc, 'failed to fetch value', key=node,
                          operation='STATS')
    except CouchbaseError as e:
        ctx['exception'] = e

    if node:
        key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')
        val = (<char *>resp.v.v0.bytes)[:resp.v.v0.nbytes].decode('utf-8')
        ctx['rv'][key][node] = Utils.string_to_num(val)

cdef void cb_error_callback(lcb.lcb_t instance,
                            lcb.lcb_error_t err,
                            const char *desc) with gil:
    obj = <object>lcb.lcb_get_cookie(instance)
    obj._errors.append((err, desc))

cdef void cb_http_complete_callback(lcb.lcb_http_request_t request,
                                    lcb.lcb_t instance, const void *cookie,
                                    lcb.lcb_error_t rc,
                                    lcb.lcb_http_resp_t *resp) with gil:
    ctx = <object>cookie
    cdef char* _err_bytes  # In case of an error
    try:
        Utils.maybe_raise(rc, 'failed to make http request',
                          status=resp.v.v0.status)
    except CouchbaseError as e:
        # Try to capture a bit more error information, from the returned bytes
        _err_bytes = <char*>resp.v.v0.bytes
        err_bytes = _err_bytes[:resp.v.v0.nbytes].decode("utf-8")
        e.msg += ": " + err_bytes
        ctx['exception'] = e


    # cdef struct ____lcb_http_resp_t_v_v0:
    #     lcb_http_status_t status
    #     const char *path
    #     lcb_size_t npath
    #     const char *const *headers
    #     const void *bytes
    #     lcb_size_t nbytes

    status = int(resp.v.v0.status)
    cdef const char* _path = resp.v.v0.path
    path = _path[:resp.v.v0.npath].decode("utf-8")
    cdef char* _resp_bytes = <char*>resp.v.v0.bytes
    resp_bytes = _resp_bytes[:resp.v.v0.nbytes].decode("utf-8")

    # Pulling out the headers is a bit of a chore, but it has to be done...
    cdef char* _hdr
    headers = []
    i = 0
    while True:
        _hdr = <char *>resp.v.v0.headers[i]
        if _hdr == NULL:
            break
        headers.append(_hdr.decode("utf-8"))
        i += 1

    headers_dict = {}
    for i in range(0, len(headers), 2):
        hdr_key = headers[i]
        hdr_val = headers[i + 1]
        headers_dict[hdr_key] = hdr_val

    # Assemble the response dictionary
    response_data = {
        'status': status,
        'path': path,
        'content': resp_bytes,
        'headers': headers_dict,
    }

    ctx['rv'] = response_data


cdef class Connection:
    cdef lcb.lcb_t _instance
    cdef lcb.lcb_create_st _create_options
    cdef lcb.lcb_cached_config_st _cached_options
    cdef public lcb.lcb_uint32_t default_format
    cdef public bint quiet
    cdef public object _errors

    def __cinit__(self):
        self.default_format = FMT_JSON
        memset(&self._create_options, 0, sizeof(self._create_options))

    def __init__(self, host='localhost', port=8091, username=None,
                 password=None, bucket=None, quiet=False, conncache=None,
                 _no_connect_exceptions=False):
        """Connection to a bucket.

        Normally it's initialized through
        :meth:`couchbase.Couchbase.connect`

        **Class attributes**

          .. py:attribute:: default_format

            Specify the default format (default: `Couchbase.FMT_JSON')
            to encode your data before storing in Couchbase. It uses the
            flags field to store the format. Possible values are:

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

          .. py:attribute:: quiet

            It controlls whether to raise an exception when the client
            executes operations on non-existent keys (default: `False`).
            If it is `False` it will raise
            :exc:`couchbase.exceptions.NotFoundError` exceptions. When
            set to `True` the operations will return `None` silently.

        """
        if bucket is None:
            raise exceptions.ArgumentError("A bucket name must be given")

        self.quiet = quiet
        self._errors = deque(maxlen=1000)
        if isinstance(host, (tuple,list)):
            hosts_tmp = []
            for curhost in host:
                cur_hname = None
                cur_hport = None
                if isinstance(curhost, (list,tuple)):
                    cur_hname, cur_hport = curhost
                else:
                    cur_hname = curhost
                    cur_hport = port

                hosts_tmp.append("{0}:{1}".format(cur_hname, cur_hport))

            host = ";".join(hosts_tmp).encode('utf-8')

        else:
            host = '{0}:{1}'.format(host, port).encode('utf-8')

        if password:
            password = password.encode('utf-8')
            if not username:
                username = bucket
        else:
            password = "".encode('utf-8')

        if username:
            username = username.encode('utf-8')
        else:
            username = "".encode('utf-8')

        bucket = bucket.encode('utf-8')

        self._create_options.v.v0.host = host
        self._create_options.v.v0.bucket = bucket
        self._create_options.v.v0.user = username
        self._create_options.v.v0.passwd = password

        rc = -1

        if conncache:
            conncache = conncache.encode("utf-8")
            memset(&self._cached_options, 0, sizeof(lcb_cached_config_st));

            memcpy(&self._cached_options.createopt,
                   &self._create_options,
                   sizeof(lcb_create_st))

            self._cached_options.cachefile = <const char*>conncache
            rc = lcb.lcb_create_compat(lcb.LCB_CACHED_CONFIG,
                                        &self._cached_options,
                                        &self._instance,
                                        NULL)
        else:
            rc = lcb.lcb_create(&self._instance, &self._create_options)

        Utils.maybe_raise(rc, 'failed to create libcouchbase instance')

        <void>lcb.lcb_set_store_callback(self._instance, cb_store_callback);
        <void>lcb.lcb_set_get_callback(self._instance, cb_get_callback);
        <void>lcb.lcb_set_stat_callback(self._instance, cb_stat_callback);
        <void>lcb.lcb_set_remove_callback(self._instance, cb_remove_callback);
        <void>lcb.lcb_set_arithmetic_callback(self._instance,
                                              cb_arithmetic_callback);
        <void>lcb.lcb_set_error_callback(self._instance, cb_error_callback);
        <void>lcb.lcb_set_http_complete_callback(
            self._instance,
            <lcb.lcb_http_complete_callback>cb_http_complete_callback);

        lcb.lcb_set_cookie(self._instance, <void*>self);

        if _no_connect_exceptions:
            lcb.lcb_connect(self._instance)
            self._wait_common()
        else:
            self._connect()

    def __dealloc__(self):
        if self._instance:
            lcb.lcb_destroy(self._instance)

    def _connect(self):
        rc = lcb.lcb_connect(self._instance)

        Utils.maybe_raise(
            rc, 'failed to schedule connection to server')

        self._wait_common()
        errors = self.errors()
        if errors:
            Utils.maybe_raise(errors[0][0],
                    "failed to connect to server")


    def _wait_common(self):
        cdef int rv
        with nogil:
            rv = lcb.lcb_wait(self._instance)
        return rv

    def errors(self, clear_existing=True):
        """
        Get miscellaneous error information.

        This function returns error information relating to the client instance.
        This will contain error information not related to any specific operation
        and may also provide insight as to what caused a specific operation to
        fail.

        :param boolean clear_existing: If set to true, the errors will be
          cleared once they are returned. The client will keep a history of
          the last 1000 errors which were received.

        :return: a tuple of ((errnum, errdesc), ...) (which may be empty)
        """
        ret = tuple(self._errors)
        if clear_existing:
            self._errors.clear()
        return ret

    @staticmethod
    def _encode_value(value, format):
        """Encode some value according to a given format.

        The input value can be any Python object. The `format` specifies
        whether it should be as JSON string, pickled or not encoded at
        all (plain).

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
        """Decode some value according to a given format.

        The input value is either encoded as a JSON string, pickled or
        not encoded at all (plain).

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

    def __set(self, operation, key, value=None, cas=None, ttl=None, format=None):
        if self._instance == NULL:
            Utils.raise_not_connected(operation)

        if not key and key != 0 and key != "":
            # The number 0 and the empty string *might* be acceptable keys,
            # but anything else that evaluates false isn't
            raise exceptions.ArgumentError("Invalid ID: {0}".format(key))

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
                cmds[i].v.v0.operation = operation
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
            if (rc == lcb.LCB_SUCCESS):
                rc = self._wait_common()

            if ctx['exception']:
                raise ctx['exception']

            if num_commands > 1:
                return ctx['rv']
            else:
                return list(ctx['rv'].values())[0]
        finally:
            free(cmds)
            free(ptr_cmds)


    def set(self, key, value=None, cas=None, ttl=None, format=None):
        """Unconditionally store the object in Couchbase.

        :param key: if it's a string it's the key used to reference the
          value. In case of a dict, it's a multi-set where the key-value
          pairs will be stored.
        :type key: string or dict
        :param any value: value to be stored
        :param int cas: the CAS value for an object. This value is
          created on the server and is guaranteed to be unique for each
          value of a given key. This value is used to provide simple
          optimistic concurrency control when multiple clients or
          threads try to update an item simultaneously.
        :param int ttl: the time to live for an object. Values larger
          than 30*24*60*60 seconds (30 days) are interpreted as absolute
          times (from midnight, January 1, 1970, a.k.a. the epoch).
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
        :raise: :exc:`couchbase.exceptions.ValueFormatError` if the
          value cannot be serialized with chosen encoder, e.g. if you
          try to store a dictionaty in plain mode.

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
        return self.__set(lcb.LCB_SET, key, value, cas, ttl, format)

    def append(self, key, value=None, cas=None, ttl=None):
        """
        Append a string to an existing value in Couchbase.

        This follows the same conventions as
        :meth:`~couchbase.libcouchbase.Connection.set`, with the caveat
        that the `format` argument is unavailable and will always be
        `FMT_PLAIN`.

        :raise: :exc:`couchbase.exceptions.NotStoredError` if the key does
          not exist

        """
        return self.__set(lcb.LCB_APPEND, key, value, cas, ttl, FMT_PLAIN)

    def prepend(self, key, value=None, cas=None, ttl=None):
        """
        Prepend a string to an existing value in Couchbase.

        This follows the same conventions as
        :meth:`~couchbase.libcouchbase.Connection.append`

        """
        return self.__set(lcb.LCB_PREPEND, key, value, cas, ttl, FMT_PLAIN)

    def add(self, key, value=None, ttl=None, format=None):
        """
        Store an object in Couchbase unless it already exists.

        Follows the same conventions as
        :meth:`~couchbase.libcouchbase.Connection.set` but the value is
        stored only if it does not exist already. Conversely, the value
        is not stored if the key already exists.

        :raise: :exc:`couchbase.exceptions.KeyExistsError` if the key
          already exists

        """
        return self.__set(lcb.LCB_ADD, key, value, None, ttl, format)

    def replace(self, key, value=None, cas=None, ttl=None, format=None):
        """
        Store an object in Couchbase only if it already exists.

        Follows the same conventions as
        :meth:`~couchbase.libcouchbase.Connection.set`, but the value is
        stored only if a previous value already exists.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist

        """
        return self.__set(lcb.LCB_REPLACE, key, value, cas, ttl, format)

    def get(self, keys, extended=False, format=None, quiet=None):
        """Obtain an object stored in Couchbase by given key.

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
            if rc != lcb.LCB_SUCCESS:
                Utils.maybe_raise(rc, 'failed to schedule get request')
            else:
                rc = self._wait_common()
                if rc != lcb.LCB_SUCCESS:
                    Utils.maybe_raise(rc, "couldn't wait for get operation")

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

    def stats(self, stats=''):
        """Request server statistics

        Fetches stats from each node in the cluster. Without a key
        specified the server will respond with a default set of
        statistical information. It returns the a `dict` with stats keys
        and node-value pairs as a value.

        :param stats: One or several stats to query
        :type stats: string or list of string

        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed

        :return: `dict` where keys are stat keys and values are
          host-value pairs

        Find out how many items are in the bucket::

            total = 0
            for key, value in cb.stats()['total_items'].items():
                total += value

        Get memory stats (works on couchbase buckets)::

            cb.stats('memory')
            # {'mem_used': {...}, ...}
        """
        if self._instance == NULL:
            Utils.raise_not_connected(lcb.LCB_GET)

        if not isinstance(stats, list):
            stats = [stats]

        ctx = self._context_dict()
        ctx['rv'] = defaultdict(dict)
        cdef int num_cmds = len(stats)
        cdef int i = 0

        cdef lcb.lcb_server_stats_cmd_t *cmds = \
            <lcb.lcb_server_stats_cmd_t *>calloc(
                num_cmds, sizeof(lcb.lcb_server_stats_cmd_t))
        if not cmds:
            raise MemoryError()
        cdef const lcb.lcb_server_stats_cmd_t **ptr_cmds = \
            <const lcb.lcb_server_stats_cmd_t **>malloc(
                num_cmds * sizeof(lcb.lcb_server_stats_cmd_t *))
        if not ptr_cmds:
            free(cmds)
            raise MemoryError()

        try:
            for i in range(len(stats)):
                stats[i] = stats[i].encode('ascii')

                ptr_cmds[i] = &cmds[i]
                cmds[i].v.v0.name = <char *>stats[i]
                cmds[i].v.v0.nname = len(stats[i])

            rc = lcb.lcb_server_stats(self._instance, <void *>ctx, num_cmds,
                                      ptr_cmds)
            Utils.maybe_raise(rc, 'failed to schedule get request')
            if rc == lcb.LCB_SUCCESS:
                rc = self._wait_common()
                Utils.maybe_raise(rc, 'failed to wait for stats')

            if ctx['exception']:
                raise ctx['exception']

            return ctx['rv']
        finally:
            free(cmds)
            free(ptr_cmds)


    def delete(self, key, cas=None, quiet=False):
        """Remove the key-value entry for a given key in Couchbase.

        :param key: This can be a single string which is the key to
          delete, a list of strings, or a dict of strings, with the
          values being CAS values for each key (see below)

        :type key: string, dict, or tuple/list
        :param int cas: The CAS to use for the removal operation.
          If specified, the key will only be deleted from the server if
          it has the same CAS as specified. This is useful to delete a
          key only if its value has not been changed from the version
          currently visible to the client.
          If the CAS on the server does not match the one specified,
          an exception is thrown.
        :param boolean quiet:
          Follows the same semantics as `quiet` in
          :meth:`~couchbase.libcouchbase.Connection.get`

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist on the bucket
        :raise: :exc:`couchbase.exceptions.KeyExistsError` if a CAS
          was specified, but the CAS on the server had changed
        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection was closed

        :return: a boolean value or a dictionary of boolean values,
          depending on whether the `key` parameter was a string or a
          collection.


        Simple delete::

            ok = cb.delete("key")

        Don't complain if key does not exist::

            ok = cb.delete("key", quiet = True)

        Only delete if CAS matches our version::

            value, flags, cas = cb.get("key", extended = True)
            cb.delete("key", cas = cas)

        Remove multiple keys::

            oks = cb.delete(["key1", "key2", "key3"])

        Remove multiple keys with CAS::

            oks = cb.delete({
                "key1" : cas1,
                "key2" : cas2,
                "key3" : cas3
            })

        """

        if self._instance == NULL:
            Utils.raise_not_connected()

        ctx = self._context_dict()

        ctx['rv'] = {}

        if quiet is None:
            quiet = self.quiet
        ctx['quiet'] = quiet

        keys = {}

        cdef lcb.lcb_remove_cmd_t *cmds
        cdef const lcb.lcb_remove_cmd_t **cmdlist
        cdef int ii = 0

        is_single_key = False

        if isinstance(key, dict):
            keys = key

        elif isinstance(key, (list, tuple)):
            for k in key:
                keys[k] = None
        else:
            keys[key] = cas
            is_single_key = True

        cmds = <lcb.lcb_remove_cmd_t*>\
                calloc(len(keys), sizeof(lcb.lcb_remove_cmd_t))
        if not cmds:
            raise MemoryError()

        cmdlist = <const lcb.lcb_remove_cmd_t**>\
                malloc(sizeof(lcb.lcb_remove_cmd_t*) * len(keys));
        if not cmdlist:
            free(cmds)
            raise MemoryError()

        utf8_keys = []
        for cur_key, cur_cas in keys.items():
            try:
                utf8_keys.append(cur_key.encode('utf-8'))
                cmds[ii].v.v0.key = <const char*>utf8_keys[ii]
                cmds[ii].v.v0.nkey = len(utf8_keys[ii])

                if cur_cas:
                    cmds[ii].v.v0.cas = cur_cas

                cmdlist[ii] = &cmds[ii]

                ii += 1

            except:
                free(cmds)
                free(cmdlist)
                raise

        try:
            rc = lcb.lcb_remove(self._instance, <void*>ctx, ii, cmdlist)
            Utils.maybe_raise(rc, 'failed to schedule remove request')
            if rc == lcb.LCB_SUCCESS:
                rc = self._wait_common()
                Utils.maybe_raise(rc, 'failed to wait for remove request')

            if ctx['exception']:
                raise ctx['exception']

            if is_single_key:
                return list(ctx['rv'].values())[0]
            else:
                return ctx['rv']

        finally:
            free(cmds)
            free(cmdlist)

    def _arithmetic(self, key, delta, initial=None, ttl=None, extended=False):
        if self._instance == NULL:
            Utils.raise_not_connected()

        ctx = self._context_dict()
        ctx['rv'] = {}
        ctx['extended'] = extended

        cdef lcb.lcb_arithmetic_cmd_t *cmds
        cdef const lcb.lcb_arithmetic_cmd_t **cmdlist
        cdef ii
        is_single_key = True
        keys = []

        if isinstance(key, (list, tuple)):
            keys = key
            is_single_key = False
        else:
            keys = (key,)

        cmds = <lcb.lcb_arithmetic_cmd_t*>calloc(len(keys),
                sizeof(lcb.lcb_arithmetic_cmd_t))
        if not cmds:
            raise MemoryError()

        cmdlist = <const lcb.lcb_arithmetic_cmd_t**>malloc(
                len(keys) * sizeof(lcb.lcb_arithmetic_cmd_t*))
        if not cmdlist:
            free(cmds)
            raise MemoryError()

        utf8_keys = []
        ii = 0
        for cur_key in keys:
            try:
                utf8_keys.append(cur_key.encode('utf-8'))
                cmds[ii].v.v0.key = <const char*>utf8_keys[ii]
                cmds[ii].v.v0.nkey = len(utf8_keys[ii])
                cmds[ii].v.v0.delta = delta

                if isinstance(ttl, int):
                    cmds[ii].v.v0.exptime = ttl

                if isinstance(initial, int):
                    cmds[ii].v.v0.create = 1
                    cmds[ii].v.v0.initial = initial

                cmdlist[ii] = &cmds[ii]
                ii += 1

            except:
                free(cmds)
                free(cmdlist)
                raise


        try:
            rc = lcb.lcb_arithmetic(self._instance, <void*>ctx, ii, cmdlist)
            if rc != lcb.LCB_SUCCESS:
                Utils.maybe_raise(rc, 'failed to schedule arithmetic request')
            else:
                rc = self._wait_common()
                Utils.maybe_raise(rc, 'failed to wait for arithmetic request')

            if ctx['exception']:
                raise ctx['exception']

            if is_single_key:
                return list(ctx['rv'].values())[0]
            else:
                return ctx['rv']

        finally:
            free(cmds)
            free(cmdlist)


    def incr(self, key, amount=1, initial=None, ttl=None, extended=False):
        """
        Increment the numeric value of a key.

        :param key: A key or a collection of keys which are to be
          incremented
        :type key: string or list of strings

        :param int amount: an amount by which the key should be
          incremented

        :param initial: The initial value for the key, if it does not
          exist. If the key does not exist, this value is used, and
          `amount` is ignored. If this parameter is `None` then no
          initial value is used
        :type initial: int or `None`

        :param int ttl: The lifetime for the key, after which it will
          expire

        :param boolean extended: If set to true, the return value will
          be a `Result` object (similar to whatever
          :meth:`~couchbase.Couchbase.Connection.get`) returns.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist on the bucket (and `initial` was `None`)

        :raise: :exc:`couchbase.exceptions.DeltaBadvalError` if the key
          exists, but the existing value is not numeric

        :return:
          An integer or dictionary of keys and integers, indicating the
          current value of the counter. If `extended` was true, a
          `Result` object is used rather than a simple integer.
          If an operation failed, the value will be `None`. Check for
          this as a counter's value may be `0` (but would not be a
          failure)

        Simple increment::

            ok = cb.incr("key")

        Increment by 10::

            ok = cb.incr("key", amount=10)

        Increment by 20, set initial value to 5 if it does not exist::

            ok = cb.incr("key", amount=20, initial=5)

        Increment three keys, and use the 'extended' return value::

            kv = cb.incr(["foo", "bar", "baz"], extended=True)
            for key, result in kv.items():
                print "Key %s has value %d now" % (key, result.value)

        """
        return self._arithmetic(key, amount, initial=initial, ttl=ttl, extended=extended)

    def decr(self, key, amount=1, initial=None, ttl=None, extended=False):
        """
        Decrement a key in Couchbase.

        This follows the same conventions as
        :meth:`~couchbase.libcouchbase.Connection.incr`, except the
        `amount` is the value which is subtracted ratherthan added to
        the existing value

        """
        amount = -amount
        return self._arithmetic(key, amount, initial=initial, ttl=ttl, extended=extended)

    def _make_http_request(self, request_type, method, path, body,
                            content_type):
        """
        Perform an HTTP request to the Couchbase REST API.

        :param request_type: The type of request (lcb_http_type_t in
          libcouchbase).
        :type request_type: integer

        :param method: The HTTP method to use (GET, POST, PUT, DELETE).
        :type method: string

        :param path: The path to make the request against (passed through
          directly to libcouchbase).
        :type path: string

        :param body: The body of the HTTP request. Can be None.
        :type body: string or None

        :param content_type: The HTTP content type, e.g. application/json.
        :type content_type: string
        """
        if self._instance == NULL:
            Utils.raise_not_connected()

        ctx = self._context_dict()
        ctx['rv'] = {}

        cdef lcb_http_cmd_t *cmd = <lcb.lcb_http_cmd_t*>malloc(
            sizeof(lcb.lcb_http_cmd_t))
        if not cmd:
            raise MemoryError()

        cdef lcb_http_request_t *request = <lcb.lcb_http_request_t*>malloc(
            sizeof(lcb.lcb_http_request_t))
        if not request:
            raise MemoryError()

        # ctypedef enum lcb_http_type_t:
        #     LCB_HTTP_TYPE_VIEW
        #     LCB_HTTP_TYPE_MANAGEMENT
        #     LCB_HTTP_TYPE_RAW
        #     LCB_HTTP_TYPE_MAX
        cdef lcb_http_type_t req_type = request_type

        # ctypedef enum lcb_http_method_t:
        #     LCB_HTTP_METHOD_GET
        #     LCB_HTTP_METHOD_POST
        #     LCB_HTTP_METHOD_PUT
        #     LCB_HTTP_METHOD_DELETE
        #     LCB_HTTP_METHOD_MAX
        cdef lcb_http_method_t req_method
        try:
            req_method = Utils.http_method[method]
        except KeyError:
            raise ValueError("Invalid HTTP method: {0}".format(method))

        cdef int chunked = 0  # For now, don't support chunked transfer

        # Set up the HTTP request according to the libcouchbase C API:
        # cdef struct ____lcb_http_cmd_st_v_v0:
        #     const char *path
        #     lcb_size_t npath
        #     const void *body
        #     lcb_size_t nbody
        #     lcb_http_method_t method
        #     int chunked
        #     const char *content_type

        # These values need references Python-side, so give them names.
        _encoded_path = path.encode("utf-8")
        if content_type:
            _encoded_content_type = content_type.encode("utf-8")

        memset(cmd, 0, sizeof(lcb.lcb_http_cmd_t))
        cmd.v.v0.path = _encoded_path
        cmd.v.v0.npath = len(_encoded_path)
        if body:
            if hasattr(body, "encode"):
                # Strings need to be encoded to their constituent bytes
                _encoded_body = body.encode("utf-8")
            else:
                # Otherwise assume it's properly encoded bytes already
                _encoded_body = body
            cmd.v.v0.body = <char *>_encoded_body
            cmd.v.v0.nbody = len(_encoded_body)
        else:
            cmd.v.v0.body = NULL
            cmd.v.v0.nbody = 0
        cmd.v.v0.method = req_method
        cmd.v.v0.chunked = chunked
        if content_type:
            cmd.v.v0.content_type = _encoded_content_type
        else:
            cmd.v.v0.content_type = NULL

        try:
            rc = lcb.lcb_make_http_request(self._instance, <void*>ctx,
                                           req_type, cmd, request)
            if rc != lcb.LCB_SUCCESS:
                Utils.maybe_raise(rc, 'failed to schedule http request')
            else:
                rc = self._wait_common()
                Utils.maybe_raise(rc, 'failed to wait for http request')

            if ctx['exception']:
                raise ctx['exception']

            return ctx['rv']

        finally:
            free(cmd)
            free(request)

    def _http_view(self, request_type, method, path, body=None,
                   content_type=None, **params):
        """
        Marshal / unmarshal calls to the lower-level _make_http_request method.

        Provides a slightly friendlier API to the lower-level method by making
        guesses about the desired interpretation of the content_type and body
        of the request, and providing automatic unmarshaling of results (i.e.
        parsing of response JSON).

        Most parameters are the same as
        :meth:`couchbase.libcouchbase.Connection._make_http_request`, except
        that some are optional, and any other keyword arguments (`**params`)
        will be interpreted as arguments to the REST API and sent through.

        If the method is GET (or DELETE), the parameters are sent as
        query arguments and content_type will be guessed as
        "application/x-www-form-urlencoded" if it is not provided.

        If the method is POST or PUT and there is no body, the parameters are
        sent as the request body -- by default they are encoded as JSON, but
        if content_type is set to "application/x-www-form-urlencoded" they will
        be sent as a standard HTTP payload instead.

        If the method is POST and there *is* a body, the parameters are sent
        as query arguments.
        """
        if method in ("GET", "DELETE"):
            # Try to send any parameters through as query arguments, and set
            # content-type accordingly.
            if params:
                path = path + "?" + urllib.urlencode(params)
            content_type = (content_type or "application/x-www-form-urlencoded")
        elif method in ("POST", "PUT"):
            # If parameters are provided and there is no body, send them as the
            # body -- using JSON by default but URL-encoded if specified.
            if params and not body:
                if content_type == "application/x-www-form-urlencoded":
                    body = urllib.urlencode(params)
                elif not content_type:
                    body = json.dumps(params)
                    content_type = "application/json"
                else:
                    raise ValueError(
                        "Don't know how to encode parameters as %r"
                        % content_type)
            # When there are both parameters and a body, send the parameters
            # as query arguments.
            elif params:
                path = path + "?" + urllib.urlencode(params)
                content_type = (content_type or
                                "application/x-www-form-urlencoded")
            # No parameters here, but in case there's a body, make sure it's
            # encoded properly, i.e. as JSON if it's not already bytes ready
            # to go out.
            elif body and not isinstance(body, bytes):
                if not content_type or content_type == "application/json":
                    body = json.dumps(body)
                    content_type = "application/json"
        # Call the lower-level method to do the real work
        result = self._make_http_request(request_type, method, path, body,
                                         content_type)
        if 'content' in result and result['content']:
            try:
                result['json'] = json.loads(result['content'])
            except:
                pass
        return result

    def bucket_view(self, path, method="GET", body=None, **params):
        """
        Query a view on the currently connected Couchbase bucket.

        :param path: The base HTTP path to query (for instance, the path to the
          map/reduce view)
        :type path: string

        :param method: The HTTP method to use. This defaults to HTTP GET.
        :type method: string, one of "GET", "POST", "PUT", "DELETE", "HEAD"

        :param body: The HTTP payload for this request, typically in the case
          of a PUT or POST request. Optional. If this is supplied, it must be
          either a string containing valid JSON or JSON-serializable.
        :type body: anything JSON-serializable

        :param **params: Any further keyword arguments will be passed through
          the REST API. If the method is GET (the default) they will be sent
          as query arguments -- if POST, they will be encoded as JSON and sent
          in the body of the request.

        The following parameters are currently accepted by the Couchbase view
        REST API:

        :param descending: Return the documents in descending order by key.
          Optional.
        :type descending: boolean

        :param endkey: Stop returning records when the given key is reached.
          Optional. If specified, must be JSON-serializable (e.g. list, dict).
        :type endkey: anything JSON-serializable

        :param endkey_docid: Stop returning records when the given document ID
          is reached. Optional.
        :type endkey_docid: string

        :param full_set: Use the full cluster data set (only in development
          views). Optional.
        :type full_set: boolean

        :param group: Group the results using the reduce function. Optional.
        :type group: boolean

        :param group_level: Specify the level at which to group results (i.e.
          if the key has multiple elements, how many should be counted as the
          key for a group). Optional.
        :type group_level: int

        :param inclusive_end: Whether the specified end_key should be included
          in the results. Optional.
        :type inclusive_end: boolean

        :param key: Return only documents that match the given key. Optional.
          If this is supplied, it must be JSON-serializable.
        :type key: anything JSON-serializable

        :param keys: Return only documents that match keys specified within the
          given array. Optional. if this is supplied, it must be a list or
          tuple of strings or JSON-serializable types. Note that if this
          argument is given, sorting will not be applied to the results.
        :type keys: list of JSON-serializable types

        :param limit: Limit the number of returned rows to the given number.
          Optional.
        :type limit: integer

        :param on_error: Set the response in the event of an error occurring.
          Optional. Supported values are:
            "continue" -- continues to generate view information, and simply
              includes the error information in the response stream.
            "stop" -- stops immediately when an error condition occurs, and
              returns no further view information.
        :type on_error: string, one of "continue" or "stop"

        :param reduce: Use the reduce function. Optional.
        :type reduce: boolean

        :param skip: Skip this number of records before starting to return
          results. Optional.
        :type skip: integer

        :param stale: Allow the results from a view to be stale -- that is,
          not necessarily fully up to date. Optional. Supported values are:
            "false" -- forces a view update before returning any data
            "ok" -- allow stale view data to be returned
            "update_after" -- allow stale view data to be returned, but update
              the view immediately after it has returned.
        :type stale: string, one of "false", "ok", or "update_after"

        :param startkey: Return records with a key value equal to or greater
          than the given key. Optional. If supplied, this must be a valid JSON
          string or JSON-serializable.
        :type startkey: anything JSON-serializable

        :param startkey_docid: Return records starting with the given document
          ID. Optional.
        :type startkey_docid: string

        :raise: :exc:`couchbase.exceptions.HTTPError` if anything went wrong
          while processing the request. Error information will be available as
          the `status` attribute and in the `msg` attribute of the exception.

        :return:
          The decoded JSON response from Couchbase, if there is a response. In
          the case of querying a map/reduce view, this is typically a JSON
          object (decoded as a dictionary) with fields `rows` (a list of result
          rows) and `total_rows` (the count of results).

          If there is no response data, return None.

        Simple view request::

            result = cb.bucket_view("_design/dev_test/_view/test",
                                    keys=["key1", "key2"])
            for r in result['rows']:
                print("%r is a matching row!" % r)

        Uploading a design document::

            design_doc = {"views": {"map": "function(doc, meta) { ... }"}}
            result = cb.bucket_view("_design/dev_test", body=design_doc,
                                    method="PUT")
            print("Uploaded a new design document with ID %r" % result['id'])
        """
        # Some parameters need to be treated specially, and encoded as JSON
        # even when passed as query arguments.
        _jsonify_params = ("endkey", "startkey", "key", "keys")
        # Also, some types need to be turned from Python to JSON.
        _convert_params = ("descending", "full_set", "group", "reduce",
                           "inclusive_end")
        for param in _convert_params:
            if param in params:
                params[param] = json.dumps(params[param])

        if method == "GET":
            for param in _jsonify_params:
                if param in params:
                    params[param] = json.dumps(params[param])

        # Apart from the above, do no further validation on the parameters --
        # it's a low-level method and the client can deal with interpreting
        # errors.

        # Now use the lower-level method to make the actual request, handling
        # the marshaling of arguments and so on for us.
        result = self._http_view(lcb.LCB_HTTP_TYPE_VIEW,
                                 method, path, body, **params)

        # Finally, pull out the interesting part of the result (if available)
        # as the return value.
        if 'json' in result:
            return result['json']
        return None

    def management_view(self, path, method="GET", body=None, **params):
        """
        Query view in the Couchbase management REST API.

        This follows the same conventions as
        :meth:`~couchbase.libcouchbase.Connection.bucket_view`. For more
        information on the ways that the management API can be used, see
        http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-admin-restapi.html
        """
        # Since the management API is more complex than the view API, it's
        # most sensible just to pass requests through -- construct the call.
        result = self._http_view(lcb.LCB_HTTP_TYPE_MANAGEMENT,
                                 method, path, body, **params)
        if 'json' in result:
            return result['json']
        return result
