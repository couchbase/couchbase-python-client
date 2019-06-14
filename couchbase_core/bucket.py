from warnings import warn

from couchbase_core._libcouchbase import Bucket as _Base

import couchbase_core.exceptions as E
from couchbase_core.n1ql import N1QLQuery, N1QLRequest
from couchbase_core.views.iterator import View
from .views.params import make_options_string, make_dvpath
import couchbase_core._libcouchbase as _LCB

from couchbase_core import priv_constants as _P


class Bucket(_Base):
    def __init__(self, *args, **kwargs):
        """Connect to a bucket.

        :param string connection_string:
            The connection string to use for connecting to the bucket.
            This is a URI-like string allowing specifying multiple hosts
            and a bucket name.

            The format of the connection string is the *scheme*
            (``couchbase`` for normal connections, ``couchbases`` for
            SSL enabled connections); a list of one or more *hostnames*
            delimited by commas; a *bucket* and a set of options.

            like so::

                couchbase://host1,host2,host3/bucketname?option1=value1&option2=value2

            If using the SSL scheme (``couchbases``), ensure to specify
            the ``certpath`` option to point to the location of the
            certificate on the client's filesystem; otherwise connection
            may fail with an error code indicating the server's
            certificate could not be trusted.

            See :ref:`connopts` for additional connection options.

        :param string username: username to connect to bucket with

        :param string password: the password of the bucket

        :param boolean quiet: the flag controlling whether to raise an
            exception when the client executes operations on
            non-existent keys. If it is `False` it will raise
            :exc:`.NotFoundError` exceptions. When
            set to `True` the operations will return `None` silently.

        :param boolean unlock_gil: If set (which is the default), the
            bucket object will release the python GIL when possible,
            allowing other (Python) threads to function in the
            background. This should be set to true if you are using
            threads in your application (and is the default), as
            otherwise all threads will be blocked while couchbase
            functions execute.

            You may turn this off for some performance boost and you are
            certain your application is not using threads

        :param transcoder:
            Set the transcoder object to use. This should conform to the
            interface in the documentation (it need not actually be a
            subclass). This can be either a class type to instantiate,
            or an initialized instance.
        :type transcoder: :class:`.Transcoder`

        :param lockmode: The *lockmode* for threaded access.
            See :ref:`multiple_threads` for more information.

        :param tracer: An OpenTracing tracer into which
            to propagate any tracing information. Requires
            tracing to be enabled.

        :raise: :exc:`.BucketNotFoundError` or :exc:`.AuthError` if
            there is no such bucket to connect to, or if invalid
            credentials were supplied.
        :raise: :exc:`.CouchbaseNetworkError` if the socket wasn't
            accessible (doesn't accept connections or doesn't respond
            in
        :raise: :exc:`.InvalidError` if the connection string
            was malformed.

        :return: instance of :class:`~couchbase_core.bucket.Bucket`


        Initialize bucket using default options::

            from couchbase_core.bucket import Bucket
            cb = Bucket('couchbase:///mybucket')

        Connect to protected bucket::

            cb = Bucket('couchbase:///protected', password='secret')

        Connect using a list of servers::

            cb = Bucket('couchbase://host1,host2,host3/mybucket')

        Connect using SSL::

            cb = Bucket('couchbases://securehost/bucketname?certpath=/var/cb-cert.pem')

        """
        _no_connect_exceptions = kwargs.pop('_no_connect_exceptions', False)
        _cntlopts = kwargs.pop('_cntl', {})

        # The following two blocks adapt some options from 1.x to proper
        # connection string (or lcb_cntl_string()) settings.
        strcntls = {}
        if 'timeout' in kwargs:
            _depr('timeout keyword argument',
                  'operation_timeout (with float value) in connection string')
            strcntls['operation_timeout'] = str(float(kwargs.pop('timeout')))

        if 'config_cache' in kwargs:
            _depr('config_cache keyword argument',
                  'config_cache in connection string')
            strcntls['config_cache'] = kwargs.pop('config_cache')

        tc = kwargs.get('transcoder')
        if isinstance(tc, type):
            kwargs['transcoder'] = tc()

        super(Bucket, self).__init__(*args, **kwargs)
        # Enable detailed error codes for network errors:
        self._cntlstr("detailed_errcodes", "1")

        # Enable self-identification in logs
        try:
            from couchbase_core._version import __version__ as cb_version
            self._cntlstr('client_string', 'PYCBC/' + cb_version)
        except E.NotSupportedError:
            pass

        for ctl, val in strcntls.items():
            self._cntlstr(ctl, val)

        for ctl, val in _cntlopts.items():
            self._cntl(ctl, val)

        try:
            self._do_ctor_connect()
        except E.CouchbaseError as e:
            if not _no_connect_exceptions:
                raise

    def _do_ctor_connect(self, *args, **kwargs):
        """This should be overidden by subclasses which want to use a
        different sort of connection behavior
        """
        self._connect()

    def __repr__(self):
        return ('<{modname}.{cls} bucket={bucket}, nodes={nodes} at 0x{oid:x}>'
                ).format(modname=__name__, cls=self.__class__.__name__,
                         nodes=self.server_nodes, bucket=self.bucket,
                         oid=id(self))

    def _get_timeout_common(self, op):
        return self._cntl(op, value_type='timeout')

    def _set_timeout_common(self, op, value):
        value = float(value)
        if value <= 0:
            raise ValueError('Timeout must be greater than 0')

        self._cntl(op, value_type='timeout', value=value)

    def mkmeth(oldname, newname, _dst):
        def _tmpmeth(self, *args, **kwargs):
            _depr(oldname, newname)
            return _dst(self, *args, **kwargs)
        return _tmpmeth

    def _view(self, ddoc, view,
              use_devmode=False,
              params=None,
              unrecognized_ok=False,
              passthrough=False):
        """Internal method to Execute a view (MapReduce) query

        :param string ddoc: Name of the design document
        :param string view: Name of the view function to execute
        :param params: Extra options to pass to the view engine
        :type params: string or dict
        :return: a :class:`~couchbase_core.result.HttpResult` object.
        """

        if params:
            if not isinstance(params, str):
                params = make_options_string(
                    params,
                    unrecognized_ok=unrecognized_ok,
                    passthrough=passthrough)
        else:
            params = ""

        ddoc = self._mk_devmode(ddoc, use_devmode)
        url = make_dvpath(ddoc, view) + params

        ret = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                 path=url,
                                 method=_LCB.LCB_HTTP_METHOD_GET,
                                 response_format=FMT_JSON)
        return ret

    def _cntl(self, *args, **kwargs):
        """Low-level interface to the underlying C library's settings. via
        ``lcb_cntl()``.

        This method accepts an opcode and an
        optional value. Constants are intentionally not defined for
        the various opcodes to allow saner error handling when an
        unknown opcode is not used.

        .. warning::

            If you pass the wrong parameters to this API call, your
            application may crash. For this reason, this is not a public
            API call. Nevertheless it may be used sparingly as a
            workaround for settings which may have not yet been exposed
            directly via a supported API

        :param int op: Type of cntl to access. These are defined in
          libcouchbase's ``cntl.h`` header file

        :param value: An optional value to supply for the operation.
            If a value is not passed then the operation will return the
            current value of the cntl without doing anything else.
            otherwise, it will interpret the cntl in a manner that makes
            sense. If the value is a float, it will be treated as a
            timeout value and will be multiplied by 1000000 to yield the
            microsecond equivalent for the library. If the value is a
            boolean, it is treated as a C ``int``

        :param value_type: String indicating the type of C-level value
            to be passed to ``lcb_cntl()``. The possible values are:

            * ``"string"`` - NUL-terminated `const char`.
                Pass a Python string
            * ``"int"`` - C ``int`` type. Pass a Python int
            * ``"uint32_t"`` - C ``lcb_uint32_t`` type.
                Pass a Python int
            * ``"unsigned"`` - C ``unsigned int`` type.
                Pass a Python int
            * ``"float"`` - C ``float`` type. Pass a Python float
            * ``"timeout"`` - The number of seconds as a float. This is
                converted into microseconds within the extension library.

        :return: If no `value` argument is provided, retrieves the
            current setting (per the ``value_type`` specification).
            Otherwise this function returns ``None``.
        """
        return _Base._cntl(self, *args, **kwargs)

    def _cntlstr(self, key, value):
        """
        Low-level interface to the underlying C library's settings.
        via ``lcb_cntl_string()``.

        This method accepts a key and a value. It can modify the same
        sort of settings as the :meth:`~._cntl` method, but may be a
        bit more convenient to follow in code.

        .. warning::

            See :meth:`~._cntl` for warnings.

        :param string key: The setting key
        :param string value: The setting value

        See the API documentation for libcouchbase for a list of
        acceptable setting keys.
        """
        return _Base._cntlstr(self, key, value)

    @staticmethod
    def lcb_version():
        return _LCB.lcb_version()

    def flush(self):
        """
        Clears the bucket's contents.

        .. note::

            This functionality requires that the flush option be
            enabled for the bucket by the cluster administrator. You
            can enable flush on the bucket using the administrative
            console (See http://docs.couchbase.com/admin/admin/UI/ui-data-buckets.html)

        .. note::

            This is a destructive operation, as it will clear all the
            data from the bucket.

        .. note::

            A successful execution of this method means that the bucket
            will have started the flush process. This does not
            necessarily mean that the bucket is actually empty.
        """
        path = '/pools/default/buckets/{0}/controller/doFlush'
        path = path.format(self.bucket)
        return self._http_request(type=_LCB.LCB_HTTP_TYPE_MANAGEMENT,
                                  path=path, method=_LCB.LCB_HTTP_METHOD_POST)

    def _wrap_dsop(self, sdres, has_value=False):
        from couchbase_core.items import Item
        it = Item(sdres.key)
        it.cas = sdres.cas
        if has_value:
            it.value = sdres[0]
        return it

    @property
    def closed(self):
        """Returns True if the object has been closed with :meth:`_close`"""
        return self._privflags & _LCB.PYCBC_CONN_F_CLOSED

    @property
    def timeout(self):
        """
        The timeout for key-value operations, in fractions of a second.
        This timeout affects the :meth:`get` and :meth:`upsert` family
        of methods.

        ::

            # Set timeout to 3.75 seconds
            cb.timeout = 3.75

        .. seealso:: :attr:`views_timeout`, :attr:`n1ql_timeout`
        """
        return self._get_timeout_common(_LCB.LCB_CNTL_OP_TIMEOUT)

    @timeout.setter
    def timeout(self, value):
        self._set_timeout_common(_LCB.LCB_CNTL_OP_TIMEOUT, value)

    @property
    def views_timeout(self):
        """
        The timeout for view query operations. This affects the
        :meth:`query` method.

        Timeout may be specified in fractions of a second.
        .. seealso:: :attr:`timeout`
        """
        return self._get_timeout_common(_LCB.LCB_CNTL_VIEW_TIMEOUT)

    @views_timeout.setter
    def views_timeout(self, value):
        self._set_timeout_common(_LCB.LCB_CNTL_VIEW_TIMEOUT, value)

    @property
    def n1ql_timeout(self):
        """
        The timeout for N1QL query operations. This affects the
        :meth:`n1ql_query` method.

        Timeouts may also be adjusted on a per-query basis by setting the
        :attr:`couchbase_core.n1ql.N1QLQuery.timeout` property. The effective
        timeout is either the per-query timeout or the global timeout,
        whichever is lower.
        """

        return self._get_timeout_common(_LCB.LCB_CNTL_N1QL_TIMEOUT)

    @n1ql_timeout.setter
    def n1ql_timeout(self, value):
        self._set_timeout_common(_LCB.LCB_CNTL_N1QL_TIMEOUT, value)

    @property
    def compression(self):
        """
        The compression mode to be used when talking to the server.

        This can be any of the values in :module:`couchbase_core._libcouchbase`
        prefixed with `COMPRESS_`:

        .. data:: COMPRESS_NONE

        Do not perform compression in any direction.

        .. data:: COMPRESS_IN

        Decompress incoming data, if the data has been compressed at the server.

        .. data:: COMPRESS_OUT

        Compress outgoing data.

        .. data:: COMPRESS_INOUT

        Both `COMPRESS_IN` and `COMPRESS_OUT`.

        .. data:: COMPRESS_FORCE

        Setting this flag will force the client to assume that all servers
        support compression despite a HELLO not having been initially negotiated.
        """

        return self._cntl(_LCB.LCB_CNTL_COMPRESSION_OPTS, value_type='int')

    @compression.setter
    def compression(self, value):
        self._cntl(_LCB.LCB_CNTL_COMPRESSION_OPTS, value_type='int', value=value)

    @property
    def is_ssl(self):
        """
        Read-only boolean property indicating whether SSL is used for
        this connection.

        If this property is true, then all communication between this
        object and the Couchbase cluster is encrypted using SSL.

        See :meth:`__init__` for more information on connection options.
        """
        mode = self._cntl(op=_LCB.LCB_CNTL_SSL_MODE, value_type='int')
        return mode & _LCB.LCB_SSL_ENABLED != 0


    @property
    def redaction(self):
        return self._cntl(_LCB.LCB_CNTL_LOG_REDACTION,  value_type='int')

    @redaction.setter
    def redaction(self, val):
        return self._cntl(_LCB.LCB_CNTL_LOG_REDACTION, value=val, value_type='int')

    @property
    def tracing_orphaned_queue_flush_interval(self):
        """
        The tracing orphaned queue flush interval, in fractions of a second.

        ::
            # Set tracing orphaned queue flush interval to 0.5 seconds
            cb.tracing_orphaned_queue_flush_interval = 0.5

        """

        return self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL, value_type="timeout")

    @tracing_orphaned_queue_flush_interval.setter
    def tracing_orphaned_queue_flush_interval(self, val):
        self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL, value=val, value_type="timeout")

    @property
    def tracing_orphaned_queue_size(self):
        """
        The tracing orphaned queue size.

        ::
            # Set tracing orphaned queue size to 100 entries
            cb.tracing_orphaned_queue_size = 100

        """

        return self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_SIZE, value_type="uint32_t")

    @tracing_orphaned_queue_size.setter
    def tracing_orphaned_queue_size(self, val):
        self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_SIZE, value=val, value_type="uint32_t")

    @property
    def tracing_threshold_queue_flush_interval(self):
        """
        The tracing threshold queue flush interval, in fractions of a second.

        ::
            # Set tracing threshold queue flush interval to 0.5 seconds
            cb.tracing_threshold_queue_flush_interval = 0.5

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL, value_type="timeout")

    @tracing_threshold_queue_flush_interval.setter
    def tracing_threshold_queue_flush_interval(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL, value=val, value_type="timeout")

    @property
    def tracing_threshold_queue_size(self):
        """
        The tracing threshold queue size.

        ::
            # Set tracing threshold queue size to 100 entries
            cb.tracing_threshold_queue_size = 100

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_SIZE, value_type="uint32_t")

    @tracing_threshold_queue_size.setter
    def tracing_threshold_queue_size(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_SIZE, value=val, value_type="uint32_t")

    @property
    def tracing_threshold_kv(self):
        """
        The tracing threshold for KV, in fractions of a second.

        ::
            # Set tracing threshold for KV to 0.5 seconds
            cb.tracing_threshold_kv = 0.5

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_KV, value_type="timeout")

    @tracing_threshold_kv.setter
    def tracing_threshold_kv(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_KV, value=val, value_type="timeout")

    @property
    def tracing_threshold_n1ql(self):
        """
        The tracing threshold for N1QL, in fractions of a second.

        ::
            # Set tracing threshold for N1QL to 0.5 seconds
            cb.tracing_threshold_n1ql = 0.5

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_N1QL, value_type="timeout")

    @tracing_threshold_n1ql.setter
    def tracing_threshold_n1ql(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_N1QL, value=val, value_type="timeout")

    @property
    def tracing_threshold_view(self):
        """
        The tracing threshold for View, in fractions of a second.

        ::
            # Set tracing threshold for View to 0.5 seconds
            cb.tracing_threshold_view = 0.5

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_VIEW, value_type="timeout")

    @tracing_threshold_view.setter
    def tracing_threshold_view(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_VIEW, value=val, value_type="timeout")

    @property
    def tracing_threshold_fts(self):
        """
        The tracing threshold for FTS, in fractions of a second.

        ::
            # Set tracing threshold for FTS to 0.5 seconds
            cb.tracing_threshold_fts = 0.5

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_FTS, value_type="timeout")

    @tracing_threshold_fts.setter
    def tracing_threshold_fts(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_FTS, value=val, value_type="timeout")

    @property
    def tracing_threshold_analytics(self):
        """
        The tracing threshold for analytics, in fractions of a second.

        ::
            # Set tracing threshold for analytics to 0.5 seconds
            cb.tracing_threshold_analytics = 0.5

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_ANALYTICS, value_type="timeout")

    @tracing_threshold_analytics.setter
    def tracing_threshold_analytics(self, val):
        self._cntl(op=_LCB.TRACING_THRESHOLD_ANALYTICS, value=val, value_type="timeout")

    def mutate_in(self, key, *specs, **kwargs):
        """Perform multiple atomic modifications within a document.

        :param key: The key of the document to modify
        :param specs: A list of specs (See :mod:`.couchbase_core.subdocument`)
        :param bool create_doc:
            Whether the document should be create if it doesn't exist
        :param bool insert_doc: If the document should be created anew, and the
            operations performed *only* if it does not exist.
        :param bool upsert_doc: If the document should be created anew if it
            does not exist. If it does exist the commands are still executed.
        :param kwargs: CAS, etc.
        :return: A :class:`~.couchbase_core.result.SubdocResult` object.

        Here's an example of adding a new tag to a "user" document
        and incrementing a modification counter::

            import couchbase_core.subdocument as SD
            # ....
            cb.mutate_in('user',
                         SD.array_addunique('tags', 'dog'),
                         SD.counter('updates', 1))

        .. note::

            The `insert_doc` and `upsert_doc` options are mutually exclusive.
            Use `insert_doc` when you wish to create a new document with
            extended attributes (xattrs).

        .. seealso:: :mod:`.couchbase_core.subdocument`
        """

        # Note we don't verify the validity of the options. lcb does that for
        # us.
        sdflags = kwargs.pop('_sd_doc_flags', 0)

        if kwargs.pop('insert_doc', False):
            sdflags |= _P.CMDSUBDOC_F_INSERT_DOC
        if kwargs.pop('upsert_doc', False):
            sdflags |= _P.CMDSUBDOC_F_UPSERT_DOC

        # TODO: find a way of supporting this with LCB V4 API - PYCBC-584
        if _LCB.PYCBC_LCB_API>0x02FF00 and (sdflags & _P.CMDSUBDOC_F_INSERT_DOC):
            for spec in specs:
                if spec[0] ==_LCB.LCB_SDCMD_DICT_UPSERT:
                    raise E.NotSupportedError("Subdoc upsert + fulldoc insert Not supported in SDK 3 yet")

        kwargs['_sd_doc_flags'] = sdflags
        return super(Bucket, self).mutate_in(key, specs, **kwargs)

    def lookup_in(self, key, *specs, **kwargs):
        """Atomically retrieve one or more paths from a document.

        :param key: The key of the document to lookup
        :param spec: A list of specs (see :mod:`.couchbase_core.subdocument`)
        :return: A :class:`.couchbase_core.result.SubdocResult` object.
            This object contains the results and any errors of the
            operation.

        Example::

            import couchbase_core.subdocument as SD
            rv = cb.lookup_in('user',
                              SD.get('email'),
                              SD.get('name'),
                              SD.exists('friends.therock'))

            email = rv[0]
            name = rv[1]
            friend_exists = rv.exists(2)

        .. seealso:: :meth:`retrieve_in` which acts as a convenience wrapper
        """
        return super(Bucket, self).lookup_in({key: specs}, **kwargs)

    def rget(self, key, replica_index=None, quiet=None):
        """Get an item from a replica node

        :param string key: The key to fetch
        :param int replica_index: The replica index to fetch.
            If this is ``None`` then this method will return once any
            replica responds. Use :attr:`configured_replica_count` to
            figure out the upper bound for this parameter.

            The value for this parameter must be a number between 0 and
            the value of :attr:`configured_replica_count`-1.
        :param boolean quiet: Whether to suppress errors when the key is
            not found

        This method (if `replica_index` is not supplied) functions like
        the :meth:`get` method that has been passed the `replica`
        parameter::

            c.get(key, replica=True)

        .. seealso:: :meth:`get` :meth:`rget_multi`
        """
        if replica_index is not None:
            return _Base._rgetix(self, key, replica=replica_index, quiet=quiet)
        else:
            return _Base._rget(self, key, quiet=quiet)

    def rget_multi(self, keys, replica_index=None, quiet=None):
        if replica_index is not None:
            return _Base._rgetix_multi(self, keys,
                                       replica=replica_index, quiet=quiet)
        else:
            return _Base._rget_multi(self, keys, quiet=quiet)

    def query(self, query, *args, **kwargs):
        """
        Execute a N1QL query.

        This method is mainly a wrapper around the :class:`~.N1QLQuery`
        and :class:`~.N1QLRequest` objects, which contain the inputs
        and outputs of the query.

        Using an explicit :class:`~.N1QLQuery`::

            query = N1QLQuery(
                'SELECT airportname FROM `travel-sample` WHERE city=$1', "Reno")
            # Use this option for often-repeated queries
            query.adhoc = False
            for row in cb.n1ql_query(query):
                print 'Name: {0}'.format(row['airportname'])

        Using an implicit :class:`~.N1QLQuery`::

            for row in cb.n1ql_query(
                'SELECT airportname, FROM `travel-sample` WHERE city="Reno"'):
                print 'Name: {0}'.format(row['airportname'])

        With the latter form, *args and **kwargs are forwarded to the
        N1QL Request constructor, optionally selected in kwargs['iterclass'],
        otherwise defaulting to :class:`~.N1QLRequest`.

        :param query: The query to execute. This may either be a
            :class:`.N1QLQuery` object, or a string (which will be
            implicitly converted to one).
        :param kwargs: Arguments for :class:`.N1QLRequest`.
        :return: An iterator which yields rows. Each row is a dictionary
            representing a single result
        """
        if not isinstance(query, N1QLQuery):
            query = N1QLQuery(query)

        itercls = kwargs.pop('itercls', N1QLRequest)
        return itercls(query, self, *args, **kwargs)

    @staticmethod
    def _mk_devmode(n, use_devmode):
        if n.startswith('dev_') or not use_devmode:
            return n
        return 'dev_' + n

    def view_query(self, design, view, use_devmode=False, **kwargs):
        """
        Query a pre-defined MapReduce view, passing parameters.

        This method executes a view on the cluster. It accepts various
        parameters for the view and returns an iterable object
        (specifically, a :class:`~.View`).

        :param string design: The design document
        :param string view: The view function contained within the design
            document
        :param boolean use_devmode: Whether the view name should be
            transformed into a development-mode view. See documentation
            on :meth:`~.BucketManager.design_create` for more
            explanation.
        :param kwargs: Extra arguments passed to the :class:`~.View`
            object constructor.
        :param kwargs: Additional parameters passed to the
            :class:`~.View` constructor. See that class'
            documentation for accepted parameters.

        .. seealso::

            :class:`~.View`
                contains more extensive documentation and examples

            :class:`couchbase_v2.views.params.Query`
                contains documentation on the available query options

            :class:`~.SpatialQuery`
                contains documentation on the available query options
                for Geospatial views.

        .. note::

            To query a spatial view, you must explicitly use the
            :class:`.SpatialQuery`. Passing key-value view parameters
            in ``kwargs`` is not supported for spatial views.

        """
        design = self._mk_devmode(design, use_devmode)
        itercls = kwargs.pop('itercls', View)
        return itercls(self, design, view, **kwargs)


def _depr(fn, usage, stacklevel=3):
    """Internal convenience function for deprecation warnings"""
    warn('{0} is deprecated. Use {1} instead'.format(fn, usage),
         stacklevel=stacklevel, category=DeprecationWarning)