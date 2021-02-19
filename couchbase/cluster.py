import asyncio

from couchbase_core.asynchronous.client import AsyncClientMixin
from couchbase.mutation_state import MutationState
from couchbase.management.queries import QueryIndexManager
from couchbase.management.search import SearchIndexManager
from couchbase.management.analytics import AnalyticsIndexManager
from couchbase.analytics import AnalyticsOptions
from couchbase_core.mapper import identity
from .auth import NoBucketException, Authenticator
from .management.users import UserManager
from .management.buckets import BucketManager
from couchbase.management.admin import Admin
from couchbase.diagnostics import DiagnosticsResult
from couchbase.search import SearchResult, SearchOptions, SearchQuery
from .analytics import AnalyticsResult
from .n1ql import QueryResult
from couchbase_core.n1ql import _N1QLQuery
from .options import OptionBlock, OptionBlockDeriv, QueryBaseOptions, LockMode, enum_value
from .bucket import Bucket, CoreClient, PingOptions
from couchbase_core.cluster import _Cluster as CoreCluster
from .exceptions import AlreadyShutdownException, InvalidArgumentException, \
    SearchException, QueryException, AnalyticsException, CouchbaseException, NetworkException
import couchbase_core._libcouchbase as _LCB
from couchbase_core._pyport import raise_from
from couchbase_core.cluster import *
from .result import *
from random import choice
from enum import Enum
from copy import deepcopy
from datetime import timedelta


T = TypeVar('T')


CallableOnOptionBlock = Callable[[OptionBlockDeriv, Any], Any]


class DiagnosticsOptions(OptionBlock):

    @overload
    def __init__(self,
                 report_id=None # type: str
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        """

        :param str report_id: An id that is appended on to the :class:`~.DiagnosticsResult`.  Helps with
            disambiguating reports when you have several running.
        """
        super(DiagnosticsOptions, self).__init__(**kwargs)


class QueryScanConsistency(Enum):
    """
    QueryScanConsistency

    This can be:

    NOT_BOUNDED
        Which means we just return what is currently in the indexes, or
    REQUEST_PLUS
        which means we 'read our own writes'.  Slower, since the query has to wait for the indexes to catch up.
    """
    REQUEST_PLUS = "request_plus"
    NOT_BOUNDED = "not_bounded"


class QueryProfile(Enum):
    """
    QueryProfile

    You can chose to set this to:

    OFF
        No query profiling data will be collected.
    PHASES
        Profile will have details on phases.
    TIMINGS
        Profile will have phases, and details on the query plan execution as well.
    """
    OFF = 'off'
    PHASES = 'phases'
    TIMINGS = 'timings'


class NamedClass(type):
    def __new__(cls, name, bases=tuple(), namespace=dict()):
        super(NamedClass, cls).__new__(cls, name, bases=bases, namespace=namespace)


class QueryOptions(QueryBaseOptions):
    VALID_OPTS = {'timeout': {'timeout': timedelta.total_seconds},
                  'read_only': {'readonly':identity},
                  'scan_consistency': {'consistency': enum_value},
                  'adhoc': {'adhoc': identity},
                  'client_context_id': {},
                  'consistent_with': {'consistent_with': identity},
                  'max_parallelism': {},
                  'positional_parameters': {},
                  'named_parameters': {},
                  'pipeline_batch': {'pipeline_batch': identity},
                  'pipeline_cap': {'pipeline_cap': identity},
                  'profile': {'profile': enum_value},
                  'raw': {},
                  'scan_wait': {},
                  'scan_cap': {'scan_cap': identity},
                  'metrics': {'metrics': identity},
                  'flex_index': {'flex_index': int}}

    TARGET_CLASS = _N1QLQuery

    @overload
    def __init__(self,
                 timeout=None,                # type: timedelta
                 read_only=None,              # type: bool
                 scan_consistency=None,       # type: QueryScanConsistency
                 adhoc=None,                  # type: bool
                 client_context_id=None,      # type: str
                 consistent_with=None,        # type: MutationState
                 max_parallelism=None,        # type: int
                 positional_parameters=None,  # type: Iterable[JSON]
                 named_parameters=None,       # type: dict[str, JSON]
                 pipeline_batch=None,         # type: int
                 pipeline_cap=None,           # type: int
                 profile=None,                # type: QueryProfile
                 raw=None,                    # type: dict[str, JSON]
                 scan_wait=None,              # type: timedelta
                 scan_cap=None,               # type: int
                 metrics=False,               # type: bool
                 flex_index=False                # type: bool
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        """
        QueryOptions
        Various options for queries

        :param timedelta timeout:
            Uses this timeout value, rather than the default for the cluster. See :meth:`~Cluster.query_timeout`.
        :param bool read_only:
            Hint to the server that this is a read-only query.
        :param QueryScanConsistency scan_consistency:
            Specify the level of consistency for the query.  Overrides any setting in consistent_with.  Can be either
            :meth:`~.QueryScanConsistency.NOT_BOUNDED`, which means return what is in the index now, or
            :meth:`~.QueryScanConsistency.REQUEST_PLUS`, which means you can read your own writes.  Slower, but when
            you need it you have it.
        :param bool adhoc:
            Specifies if the prepared statement logic should be executed internally.
        :param str client_context_id:
            Specifies a context ID string which is mirrored back from the query engine on response.
        :param MutationState consistent_with:
            Specifies custom scan consistency through “at_plus” with mutation state token vectors.
        :param int max_parallelism:
            The maximum number of logical cores to use in parallel for this query.
        :param Iterable[JSON] positional_parameters:
            Specifies the parameters used in the query, when positional notation ($1, $2, etc...) is used.
        :param dict[str,JSON] named_parameters:
            Specifies the parameters used in the query, when named parameter notation ($foo, $bar, etc...) is used.
        :param int pipeline_batch:
            Specifies pipeline batching characteristics.
        :param int pipeline_cap:
            Specifies pipeline cap characteristics.
        :param QueryProfile profile:
            Specifies the profiling level to use.
        :param dict[str,JSON] raw:
            This is a way to to specify the query payload to support unknown commands and be future-compatible.
        :param timedelta scan_wait:
            Specifies maximum amount of time to wait for a scan.
        :param int scan_cap:
            Specifies the scan cap characteristics.
        :param bool metrics:
            Specifies whether or not to include metrics with the :class:`~.QueryResult`.
        :param bool flex_index
            Specifies whether this query may make use of Search indexes
        """
        super(QueryOptions, self).__init__(**kwargs)


class ClusterTimeoutOptions(dict):
    KEY_MAP = {'kv_timeout': 'operation_timeout',
               'query_timeout': 'query_timeout',
               'views_timeout': 'views_timeout',
               'config_total_timeout': 'config_total_timeout'}
    @overload
    def __init__(self,
                 query_timeout=None,                  # type: timedelta
                 kv_timeout=None,                     # type: timedelta
                 views_timeout=None,                  # type: timedelta
                 config_total_timeout=None            # type: timedelta
        ):
        pass

    def __init__(self, **kwargs):
        """
        ClusterTimeoutOptions
        These will be the default timeouts for queries, kv operations or views for the entire cluster

        :param timedelta query_timeout: Timeout for query operations.
        :param timedelta kv_timeout: Timeout for KV operations.
        :param timedelta views_timeout: Timeout for View operations.
        :param timedelta config_total_timeout: Timeout for complete bootstrap configuration
        """
        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        for k, v in self.items():
            if v is None or k not in self.KEY_MAP.keys():
                continue
            elif k in self.KEY_MAP:
                opts[self.KEY_MAP[k]] = v.total_seconds()
            else:
                opts[k] = v
        return opts


class Compression(Enum):
    """
    Can be one of:
        NONE:
            The client will not compress or decompress the data.
        IN:
            The data coming back from the server will be decompressed, if it was compressed.
        OUT:
            The data coming into server will be compressed.
        INOUT:
            The data will be compressed on way in, decompressed on way out of server.
        FORCE:
            By default the library will send a HELLO command to the server to determine whether compression
            is supported or not.  Because commands may be
            pipelined prior to the scheduing of the HELLO command it is possible that the first few commands may not be
            compressed when schedule due to the library not yet having negotiated settings with the server. Setting this flag
            will force the client to assume that all servers support compression despite a HELLO not having been intially
            negotiated.
    """
    @classmethod
    def from_int(cls, val):
        if val == 0:
            return cls.NONE
        elif val == 1:
            return cls.IN
        elif val == 2:
            return cls.OUT
        elif val == 3:
            return cls.INOUT
        elif val == 7:
            # note that the lcb flag is a 4, but when you set "force" in the connection
            # string, it sets it as INOUT|FORCE.
            return cls.FORCE
        else:
            raise InvalidArgumentException("cannot convert {} to a Compression".format(val))

    NONE='off'
    IN='inflate_only'
    OUT='deflate_only'
    INOUT='on'
    FORCE='force'


class ClusterTracingOptions(dict):
    INT_KEYS = ['tracing_threshold_queue_size',
                'tracing_orphaned_queue_size']
    KEYS = ['tracing_threshold_kv', 'tracing_threshold_view', 'tracing_threshold_query', 'tracing_threshold_search',
            'tracing_threshold_analytics', 'tracing_threshold_queue_size', 'tracing_threshold_queue_flush_interval',
            'tracing_orphaned_queue_size', 'tracing_orphaned_queue_flush_interval']
    @overload
    def __init__(self,
                 tracing_threshold_kv=None,                      # type: timedelta
                 tracing_threshold_view=None,                    # type: timedelta
                 tracing_threshold_query=None,                   # type: timedelta
                 tracing_threshold_search=None,                  # type: timedelta
                 tracing_threshold_analytics=None,               # type: timedelta
                 tracing_threshold_queue_size=None,              # type: int
                 tracing_threshold_queue_flush_interval=None,    # type: timedelta
                 tracing_orphaned_queue_size=None,               # type: int
                 tracing_orphaned_queue_flush_interval=None,     # type: timedelta
                 ):
        pass

    def __init__(self, **kwargs):
        """
        ClusterTracingOptions
        These parameters control when/how our request tracing will log slow requests or orphaned responses.

        :param timedelta tracing_threshold_kv:  Any KV request taking longer than this will be traced.
        :param timedelta tracing_threshold_view: Any View request taking longer than this will be traced.
        :param timedelta tracing_threshold_query: Any Query request taking longer than this will be traced.
        :param timedelta tracing_threshold_search: Any Search request taking longer than this will be traced.
        :param timedelta tracing_threshold_analytics: Any Analytics request taking longer than this will be traced
        :param int tracing_threshold_queue_size: Limits the number of requests traced.
        :param timedelta tracing_threshold_queue_flush_interval: Interval between flushes of the threshold queues.
        :param int tracing_orphaned_queue_size: Limits the number of orphaned requests traced.
        :param timedelta tracing_orphaned_queue_flush_interval:  Interval between flushes of the orphaned queue.
        """
        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        for k, v in self.items():
            if v is None or k not in self.KEYS:
                continue
            elif isinstance(v, timedelta):
                opts[k] = v.total_seconds()
            else:
                opts[k] = v
        return opts


class ClusterOptions(dict):
    KEYS = ['timeout_options', 'tracing_options', 'log_redaction', 'compression', 'compression_min_size',
            'compression_min_ratio', 'certpath']

    @overload
    def __init__(self,
                 authenticator,                      # type: Authenticator
                 timeout_options=None,               # type: ClusterTimeoutOptions
                 tracing_options=None,               # type: ClusterTracingOptions
                 log_redaction=None,                 # type: bool
                 compression=None,                   # type: Compression
                 compression_min_size=None,          # type: int
                 compression_min_ratio=None,         # type: float
                 lockmode=None                       # type: LockMode
                 ):
        pass

    def __init__(self,
                 authenticator,     # type: Authenticator
                 **kwargs):
        """
        Options to set when creating a cluster.  Note the authenticator is mandatory, all the
        others are optional.

        :param Authenticator authenticator: :class:`~.Authenticator` to use - see :class:`~.PasswordAuthenticator` and
            :class:`~.CertAuthenticator`.
        :param ClusterTimeoutOptions timeout_options: A :class:`~.ClusterTimeoutOptions` object, with various optional timeouts.
        :param ClusterTracingOptions tracing_options: A :class:`~.ClusterTracingOptions` object, with various options for tracing.
        :param bool log_redaction: Turn log redaction on/off.
        :param Compression compression: A :class:`~.Compression` value for this cluster.
        :param int compression_min_size: Min size of the data before compression kicks in.
        :param float compression_min_ratio: A `float` representing the minimum compression ratio to use when compressing.
        """
        super(ClusterOptions, self).__init__(**kwargs)
        self['authenticator'] = authenticator

    def update_connection_string(self, connstr, **kwargs):

        opts = self.as_dict(**kwargs)
        if len(opts) == 0:
            return connstr
        conn = ConnectionString.parse(connstr)
        for k, v in opts.items():
            conn.set_option(k, v)
        return conn.encode()

    def split_args(self, **kwargs):
        # return a tuple with args we recognize and those we don't, which
        # should be kwargs in connect
        ours = {}
        theirs = {}
        for k, v in kwargs.items():
            if k in self.KEYS:
                ours[k] = v
            else:
                theirs[k] = v
        return (ours, theirs,)

    def as_dict(self, **kwargs):
        # the kwargs override or add to existing args.  So you could do something like:
        # opts.as_dict(tracing_options=TracingOptions(tracing_threshold_kv=timedelta(seconds=1)),
        #                                             compression=Compression.NONE)
        # and expect those values to override the corresponding ones in the output.
        #

        # first, get the nested dicts and update if necessary
        for k in ['timeout_options', 'tracing_options']:
            obj = self.get(k, {}).update(kwargs.pop(k, {}))
            if obj:
                self.update({k:obj})
        # now, update the top-level ones
        self.update(kwargs)

        # now convert final product
        opts = {}
        for k, v in self.items():
            if v is None or k not in self.KEYS:
                continue
            elif k in ['timeout_options', 'tracing_options']:
                opts.update(v.as_dict())
            elif k in ['compression_min_size', 'log_redaction']:
                opts[k] = str(int(v))
            elif k == 'compression':
                opts[k] = v.value
            else:
                opts[k] = v
        return opts


class Cluster(CoreClient):

    @internal
    def __init__(self,
                 connection_string,         # type: str
                 options=None,              # type: ClusterOptions
                 bucket_factory=Bucket,     # type: Any
                 **kwargs                   # type: Any
                 ):
        self._authenticator = kwargs.pop('authenticator', None)
        self.__is_6_5 = None
        # copy options if they exist, as we mutate it
        cluster_opts = deepcopy(options) or ClusterOptions(self._authenticator)
        if not self._authenticator:
            self._authenticator = cluster_opts.pop('authenticator', None)
            if not self._authenticator:
                raise InvalidArgumentException("Authenticator is mandatory")
        async_items = {k: kwargs.pop(k) for k in list(kwargs.keys()) if k in {'_iops', '_flags'}}
        # fixup any overrides to the ClusterOptions here as well
        args, kwargs = cluster_opts.split_args(**kwargs)
        self.connstr = cluster_opts.update_connection_string(connection_string, **args)
        self.__admin = None
        self._cluster = CoreCluster(self.connstr, bucket_factory=bucket_factory)  # type: CoreCluster
        self._cluster.authenticate(self._authenticator)
        credentials = self._authenticator.get_credentials()
        self._clusteropts = dict(**credentials.get('options', {}))
        # TODO: eliminate the 'mock hack' and ClassicAuthenticator, then you can remove this as well.
        self._clusteropts.update(kwargs)
        self._adminopts = dict(**self._clusteropts)
        self._clusteropts.update(async_items)
        self.connstr = cluster_opts.update_connection_string(self.connstr, **self._clusteropts)

        # PYCBC-949 remove certpath, it is not accepted by super(Cluster)
        # (it has been copied into self.connstr)
        self._clusteropts.pop('certpath', None)
        self._adminopts.pop('certpath', None)

        super(Cluster, self).__init__(connection_string=str(self.connstr), _conntype=_LCB.LCB_TYPE_CLUSTER, **self._clusteropts)

    @classmethod
    def connect(cls,
                connection_string,  # type: str
                options=None,       # type: ClusterOptions
                **kwargs
                ):
        # type: (...) -> Cluster
        """
        Create a Cluster object.
        An Authenticator must be provided, either as the authenticator named parameter, or within the options argument.

        :param connection_string: the connection string for the cluster.
        :param options: options for the cluster.
        :param Any kwargs: Override corresponding value in options.
        """
        return cls(connection_string, options, **kwargs)

    def _do_ctor_connect(self, *args, **kwargs):
        super(Cluster,self)._do_ctor_connect(*args,**kwargs)

    def _check_for_shutdown(self):
        if not self._cluster:
            raise AlreadyShutdownException("This cluster has already been shutdown")

    @property
    @internal
    def _admin(self):
        self._check_for_shutdown()
        if not self.__admin:
            c = ConnectionString.parse(self.connstr)
            if not c.bucket:
                c.bucket = self._adminopts.pop('bucket', None)
            self.__admin = Admin(connection_string=str(c), **self._adminopts)
        return self.__admin

    def bucket(self,
               name    # type: str
               ):
        # type: (...) -> Bucket
        """
        Open a bucket on this cluster.  This doesn't create a bucket, merely opens an existing bucket.

        :param name: Name of bucket to open.
        :return: The :class:~.bucket.Bucket` you requested.
        :raise: :exc:`~.exceptions.BucketDoesNotExistException` if the bucket has not been created on this cluster.
        """
        self._check_for_shutdown()
        if not self.__admin:
            self._adminopts['bucket'] = name
        return self._cluster.open_bucket(name, admin=self._admin)

    # Temporary, helpful with working around CCBC-1204.  We should be able to get rid of this
    # logic when this issue is fixed.
    def _is_6_5_plus(self):
        self._check_for_shutdown()

        # lets just check once.  Below, we will only set this if we are sure about the value.
        if self.__is_6_5 is not None:
            return self.__is_6_5

        try:
            response = self._admin.http_request(path="/pools").value
            v = response.get("implementationVersion")
            # lets just get first 3 characters -- the string should be X.Y.Z-XXXX-YYYY and we only care about
            # major and minor version
            self.__is_6_5 = (float(v[:3]) >= 6.5)
        except NetworkException as e:
            # the cloud doesn't let us query this endpoint, and so lets assume this is a cloud instance.  However
            # lets not actually set the __is_6_5 flag as this also could be a transient error.  That means cloud
            # instances check every time, but this is only temporary.
            return True
        except ValueError:
            # this comes from the conversion to float -- the mock says "CouchbaseMock..."
            self.__is_6_5 = True
        return self.__is_6_5

    def query(self,
              statement,            # type: str
              *options,             # type: Union[QueryOptions,Any]
              **kwargs              # type: Any
              ):
        # type: (...) -> QueryResult
        """
        Perform a N1QL query.

        :param statement: the N1QL query statement to execute
        :param options: A QueryOptions object or the positional parameters in the query.
        :param kwargs: Override the corresponding value in the Options.  If they don't match
          any value in the options, assumed to be named parameters for the query.

        :return: The results of the query or error message
            if the query failed on the server.

        :raise: :exc:`~.exceptions.QueryException` - for errors involving the query itself.  Also any exceptions
            raised by underlying system - :class:`~.exceptions.TimeoutException` for instance.

        """
        # we could have multiple positional parameters passed in, one of which may or may not be
        # a QueryOptions.  Note if multiple QueryOptions are passed in for some strange reason,
        # all but the last are ignored.
        self._check_for_shutdown()
        itercls = kwargs.pop('itercls', QueryResult)
        opt = QueryOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, QueryOptions):
                opt = o
                opts.remove(o)

        # if not a 6.5 cluster, we need to query against a bucket.  We think once
        # CCBC-1204 is addressed, we can just use the cluster's instance
        return self._maybe_operate_on_an_open_bucket(CoreClient.query,
                                                     QueryException,
                                                     opt.to_query_object(statement, *opts, **kwargs),
                                                     itercls=itercls,
                                                     err_msg="Query requires an open bucket")

    # gets a random bucket from those the cluster has opened
    def _get_an_open_bucket(self, err_msg):
        clients = [v() for k, v in self._cluster._buckets.items()]
        clients = [v for v in clients if v]
        if clients:
            return choice(clients)
        raise NoBucketException(err_msg)

    def _maybe_operate_on_an_open_bucket(self,
                                         verb,
                                         failtype,
                                         *args,
                                         **kwargs):
        if self._is_6_5_plus():
            kwargs.pop('err_msg', None)
            return self._operate_on_cluster(verb, failtype, *args, **kwargs)
        return self._operate_on_an_open_bucket(verb, failtype, *args, **kwargs)

    def _operate_on_an_open_bucket(self,
                                   verb,
                                   failtype,
                                   *args,
                                   **kwargs):
        try:
            return verb(self._get_an_open_bucket(kwargs.pop('err_msg', 'Cluster has no open buckets')),
                        *args,
                        **kwargs)
        except Exception as e:
            raise_from(failtype(params=CouchbaseException.ParamType(message='Cluster operation on bucket failed',
                                                                    inner_cause=e)), e)

    def _operate_on_cluster(self,
                            verb,
                            failtype,  # type: Type[CouchbaseException]
                            *args,
                            **kwargs):

        try:
            return verb(self, *args, **kwargs)
        except Exception as e:
            raise_from(failtype(params=CouchbaseException.ParamType(message="Cluster operation failed", inner_cause=e)), e)

    # for now this just calls functions.  We can return stuff if we need it, later.
    def _sync_operate_on_entire_cluster(self,
                                        verb,
                                        *args,
                                        **kwargs):
        clients = [v() for k, v in self._cluster._buckets.items()]
        clients = [v for v in clients if v]
        clients.append(self)
        results = []
        for c in clients:
            results.append(verb(c, *args, **kwargs))
        return results

    async def _operate_on_entire_cluster(self,
                                         verb,
                                         failtype,
                                         *args,
                                         **kwargs):
        # if you don't have a cluster client yet, then you don't have any other buckets open either, so
        # this is the same as operate_on_cluster
        if not self._cluster._buckets:
            return self._operate_on_cluster(verb, failtype, *args, **kwargs)

        async def coroutine(client, verb, *args, **kwargs):
            return verb(client, *args, **kwargs)
        # ok, lets loop over all the buckets, and the clusterclient.  And lets do it async so it isn't miserably
        # slow.  So we will create a list of tasks and execute them together...
        tasks = [asyncio.ensure_future(coroutine(self, verb, *args, **kwargs))]
        for name, c in self._cluster._buckets.items():
            client = c()
            if client:
                tasks.append(coroutine(client, verb, *args, **kwargs))
        done, pending = await asyncio.wait(tasks)
        results = []
        for d in done:
            results.append(d.result())
        return results

    def analytics_query(self,       # type: Cluster
                        statement,  # type: str,
                        *options,   # type: AnalyticsOptions
                        **kwargs
                        ):
        # type: (...) -> AnalyticsResult
        """
        Executes an Analytics query against the remote cluster and returns a AnalyticsResult with the results
        of the query.

        :param statement: the analytics statement to execute
        :param options: the optional parameters that the Analytics service takes based on the Analytics RFC.
        :return: An AnalyticsResult object with the results of the query or error message if the query failed on the server.
        :raise: :exc:`~.exceptions.AnalyticsException` errors associated with the analytics query itself.
            Also, any exceptions raised by the underlying platform - :class:`~.exceptions.TimeoutException`
            for example.
        """
        # following the query implementation, but this seems worth revisiting soon
        self._check_for_shutdown()
        itercls = kwargs.pop('itercls', AnalyticsResult)
        opt = AnalyticsOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, AnalyticsOptions):
                opt = o
                opts.remove(o)

        return self._maybe_operate_on_an_open_bucket(CoreClient.analytics_query,
                                                     AnalyticsException,
                                                     opt.to_query_object(statement, *opts, **kwargs),
                                                     itercls=itercls,
                                                     err_msg='Analytics queries require an open bucket')

    def search_query(self,
                     index,     # type: str
                     query,     # type: SearchQuery
                     *options,  # type: SearchOptions
                     **kwargs
                     ):
        # type: (...) -> SearchResult
        """
        Executes a Search or FTS query against the remote cluster and returns a SearchResult implementation with the
        results of the query.

        .. code-block:: python

            from couchbase.search import MatchQuery, SearchOptions

            it = cb.search('name', MatchQuery('nosql'), SearchOptions(limit=10))
            for hit in it:
                print(hit)

        :param str index: Name of the index to use for this query.
        :param query: the fluent search API to construct a query for FTS.
        :param options: the options to pass to the cluster with the query.
        :param kwargs: Overrides corresponding value in options.
        :return: A :class:`~.search.SearchResult` object with the results of the query or error message if the query
            failed on the server.
        :raise: :exc:`~.exceptions.SearchException` Errors related to the query itself.
            Also, any exceptions raised by the underlying platform - :class:`~.exceptions.TimeoutException`
            for example.

        """
        self._check_for_shutdown()

        def do_search(dest):
            search_params = SearchOptions.gen_search_params_cls(index, query, *options, **kwargs)
            return search_params.itercls(search_params.body, dest, **search_params.iterargs)

        return self._maybe_operate_on_an_open_bucket(do_search, SearchException, err_msg="No buckets opened on cluster")

    _root_diag_data = {'id', 'version', 'sdk'}

    def diagnostics(self,
                    *options,   # type: DiagnosticsOptions
                    **kwargs
                    ):
        # type: (...) -> DiagnosticsResult
        """
        Creates a diagnostics report that can be used to determine the healthfulness of the Cluster.
        :param options:  Options for the diagnostics
        :return: A :class:`~.diagnostics.DiagnosticsResult` object with the results of the query or error message if the query failed on the server.

        """
        self._check_for_shutdown()
        result = self._sync_operate_on_entire_cluster(CoreClient.diagnostics, **forward_args(kwargs, *options))
        return DiagnosticsResult(result)

    def ping(self,
             *options,  # type: PingOptions
             **kwargs
             ):
        # type: (...) -> PingResult
        """
        Actively contacts each of the  services and returns their pinged status.

        :param options: Options for sending the ping request.
        :param kwargs: Overrides corresponding value in options.
        :return: A :class:`~.result.PingResult` representing the state of all the pinged services.
        :raise: :class:`~.exceptions.CouchbaseException` for various communication issues.
        """

        return PingResult(CoreClient.ping(self, **forward_args(kwargs, *options)))

    def users(self):
        # type: (...) -> UserManager
        """
        Get the UserManager.

        :return: A :class:`~.management.UserManager` with which you can create or update cluster users and roles.
        """
        self._check_for_shutdown()
        return UserManager(self._admin)

    def query_indexes(self):
        # type: (...) -> QueryIndexManager
        """
        Get the QueryIndexManager.

        :return:  A :class:`~.management.QueryIndexManager` with which you can create or modify query indexes on
            the cluster.
        """
        self._check_for_shutdown()
        return QueryIndexManager(self._admin)

    def search_indexes(self):
        # type: (...) -> SearchIndexManager
        """
        Get the SearchIndexManager.

        :return:  A :class:`~.management.SearchIndexManager` with which you can create or modify search (FTS) indexes
            on the cluster.
        """

        self._check_for_shutdown()
        return SearchIndexManager(self._admin)

    def analytics_indexes(self):
        # type: (...) -> AnalyticsIndexManager
        """
        Get the AnalyticsIndexManager.

        :return:  A :class:`~.management.AnalyticsIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        self._check_for_shutdown()
        return AnalyticsIndexManager(self)

    def buckets(self):
        # type: (...) -> BucketManager
        """
        Get the BucketManager.

        :return: A :class:`~.management.BucketManager` with which you can create or modify buckets on the cluster.
        """
        self._check_for_shutdown()
        return BucketManager(self._admin)

    def disconnect(self):
        # type: (...) -> None
        """
        Closes and cleans up any resources used by the Cluster and any objects it owns.

        :return: None
        :raise: Any exceptions raised by the underlying platform.

        """
        # in this context, if we invoke the _cluster's destructor, that will do same for
        # all the buckets we've opened, unless they are stored elswhere and are actively
        # being used.
        self._cluster = None
        self.__admin = None

    # Only useful for 6.5 DP testing
    def _is_dev_preview(self):
        self._check_for_shutdown()
        return self._admin.http_request(path="/pools").value.get("isDeveloperPreview", False)

    @property
    def query_timeout(self):
        # type: (...) -> timedelta
        """
        The timeout for N1QL query operations, as a `timedelta`. This affects the
        :meth:`query` method.  This can be set in :meth:`connect` by
        passing in a :class:`ClusterOptions` with the query_timeout set
        to the desired time.

        Timeouts may also be adjusted on a per-query basis by setting the
        :attr:`timeout` property in the options to the n1ql_query method.
        The effective timeout is either the per-query timeout or the global timeout,
        whichever is lower.
        """
        self._check_for_shutdown()
        return timedelta(seconds=self._get_timeout_common(_LCB.LCB_CNTL_QUERY_TIMEOUT))

    @property
    def tracing_threshold_query(self):
        # type: (...) -> timedelta
        """
        The tracing threshold for query response times, as `timedelta`.  This can be set in the :meth:`connect`
        by passing in a :class:`~.ClusterOptions` with the desired tracing_threshold_query set in it.
        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_QUERY, value_type="timeout"))

    @property
    def tracing_threshold_search(self):
        # type: (...) -> timedelta
        """
        The tracing threshold for search response times, as `timedelta`.  This can be set in the :meth:`connect`
        by passing in a :class:`~.ClusterOptions` with the desired tracing_threshold_search set in it.
        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_SEARCH,
                                            value_type="timeout"))

    @property
    def tracing_threshold_analytics(self):
        # type: (...) -> timedelta
        """
        The tracing threshold for analytics, as `timedelta`.  This can be set in the :meth:`connect`
        by passing in a :class:`~.ClusterOptions` with the desired tracing_threshold_analytics set in it.
        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_ANALYTICS,
                                            value_type="timeout"))

    @property
    def tracing_orphaned_queue_flush_interval(self):
        # type: (...) -> timedelta
        """
        Returns the interval that the orphaned responses are logged, as a `timedelta`. This can be set in the
        :meth:`connect` by passing in a :class:`~.ClusterOptions` with the desired interval set in it.
        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
                                                    value_type="timeout"))

    @property
    def tracing_orphaned_queue_size(self):
        # type: (...) -> int
        """
        Returns the tracing orphaned queue size. This can be set in the :meth:`connect` by passing in a
        :class:`~.ClusterOptions` with the size set in it.
        """

        return self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_SIZE, value_type="uint32_t")

    @property
    def tracing_threshold_queue_flush_interval(self):
        # type: (...) -> timedelta
        """
        The tracing threshold queue flush interval, as a `timedelta`.  This can be set in the :meth:`connect` by
        passing in a :class:`~.ClusterOptions` with the desired interval set in it.
        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL,
                                            value_type="timeout"))

    @property
    def tracing_threshold_queue_size(self):
        # type: (...) -> int
        """
        The tracing threshold queue size. This can be set in the :meth:`connect` by
        passing in a :class:`~.ClusterOptions` with the desired size set in it.
        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_SIZE, value_type="uint32_t")

    @property
    def redaction(self):
        # type: (...) -> bool
        """
        Returns whether or not the logs will redact sensitive information.
        """
        return bool(self._cntl(_LCB.LCB_CNTL_LOG_REDACTION, value_type='int'))

    @property
    def compression(self):
        # type: (...) -> Compression
        """
        Returns the compression mode to be used when talking to the server. See :class:`Compression` for
        details.  This can be set in the :meth:`connect` by passing in a :class:`~.ClusterOptions`
        with the desired compression set in it.
        """

        return Compression.from_int(
            self._cntl(_LCB.LCB_CNTL_COMPRESSION_OPTS, value_type='int')
        )

    @property
    def compression_min_size(self):
        # type: (...) -> int
        """
        Minimum size (in bytes) of the document payload to be compressed when compression enabled. This can be set
        in the :meth:`connect` by passing in a :class:`~.ClusterOptions` with the desired compression set in it.
        """
        return self._cntl(_LCB.LCB_CNTL_COMPRESSION_MIN_SIZE, value_type='uint32_t')

    @property
    def compression_min_ratio(self):
        # type: (...) -> float
        """
        Minimum compression ratio (compressed / original) of the compressed payload to allow sending it to cluster.
        This can be set in the :meth:`connect` by passing in a :class:`~.ClusterOptions` with the desired
        ratio set in it.
        """
        return self._cntl(_LCB.LCB_CNTL_COMPRESSION_MIN_RATIO, value_type='float')

    @property
    def is_ssl(self):
        # type: (...) -> bool
        """
        Read-only boolean property indicating whether SSL is used for
        this connection.

        If this property is true, then all communication between this
        object and the Couchbase cluster is encrypted using SSL.

        See :meth:`__init__` for more information on connection options.
        """
        mode = self._cntl(op=_LCB.LCB_CNTL_SSL_MODE, value_type='int')
        return mode & _LCB.LCB_SSL_ENABLED != 0


class AsyncCluster(AsyncClientMixin, Cluster):
    @classmethod
    def connect(cls, connection_string=None, *args, **kwargs):
        return cls(connection_string=connection_string, *args, **kwargs)

