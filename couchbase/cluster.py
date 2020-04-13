import asyncio
from typing import *
from couchbase.management.admin import Admin


import couchbase.search as SEARCH
from couchbase_core.mutation_state import MutationState
from couchbase_core.asynchronous import AsyncClientFactory
from couchbase.management.queries import QueryIndexManager
from couchbase.management.search import SearchIndexManager
from couchbase.management.analytics import AnalyticsIndexManager
from couchbase.analytics import AnalyticsOptions
from .management.users import UserManager
from .management.buckets import BucketManager
from couchbase.management.admin import Admin
from couchbase.diagnostics import DiagnosticsResult, EndPointDiagnostics
from couchbase.search import SearchResult, SearchOptions
from .analytics import AnalyticsResult
from .n1ql import QueryResult
from couchbase_core.n1ql import N1QLQuery
from .options import OptionBlock, OptionBlockDeriv
from .bucket import Bucket, CoreClient, PingOptions
from couchbase_core.cluster import _Cluster as CoreCluster, Authenticator as CoreAuthenticator
from .exceptions import CouchbaseException, AlreadyShutdownException, InvalidArgumentsException, \
    SearchException, DiagnosticsException, QueryException, ArgumentException, AnalyticsException
import couchbase_core._libcouchbase as _LCB
from couchbase_core._pyport import raise_from
from couchbase.options import OptionBlockTimeOut
from datetime import timedelta
from couchbase_core.cluster import *
from .result import *
from random import choice


T = TypeVar('T')


CallableOnOptionBlock = Callable[[OptionBlockDeriv, Any], Any]


class DiagnosticsOptions(OptionBlock):
    def __init__(self,
                 report_id = None # type: str
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        super(DiagnosticsOptions, self).__init__(**kwargs)




class QueryScanConsistency(object):
    REQUEST_PLUS="request_plus"
    NOT_BOUNDED="not_bounded"

    def __init__(self, val):
        if val == self.REQUEST_PLUS or val == self.NOT_BOUNDED:
            self._value = val
        else:
            raise InvalidArgumentsException("QueryScanConsistency can only be {} or {}".format(self.REQUEST_PLUS, self.NOT_BOUNDED))

    @classmethod
    def request_plus(cls):
        return cls(cls.REQUEST_PLUS)

    @classmethod
    def not_bounded(cls):
        return cls(cls.NOT_BOUNDED)

    def as_string(self):
        return getattr(self, '_value', self.NOT_BOUNDED)


class QueryProfile(object):
  OFF='off'
  PHASES='phases'
  TIMINGS='timings'

  @classmethod
  def off(cls):
    return cls(cls.OFF)

  @classmethod
  def phases(cls):
    return cls(cls.PHASES)

  @classmethod
  def timings(cls):
    return cls(cls.TIMINGS)

  def __init__(self, val):
    if val == self.OFF or val == self.PHASES or val==self.TIMINGS:
      self._value = val
    else:
      raise InvalidArgumentsException("QueryProfile can only be {}, {}, {}".format(self.OFF, self.TIMINGS, self.PHASES))

  def as_string(self):
    return getattr(self, '_value', self.OFF)


class QueryOptions(OptionBlockTimeOut):
    VALID_OPTS = ['timeout', 'read_only', 'scan_consistency', 'adhoc', 'client_context_id', 'consistent_with',
                  'max_parallelism', 'positional_parameters', 'named_parameters', 'pipeline_batch', 'pipeline_cap',
                  'profile', 'raw', 'scan_wait', 'scan_cap', 'metrics']

    @overload
    def __init__(self,
                 timeout=None,                # type: timedelta
                 read_only=None,              # type: bool
                 scan_consistency=None,       # type: QueryScanConsistency
                 adhoc=None,                  # type: bool
                 client_context_id=None,      # type: str
                 consistent_with=None,        # type: MutationState
                 max_parallelism=None,        # type: int
                 positional_parameters=None,  # type: Iterable[str]
                 named_parameters=None,       # type: Dict[str, str]
                 pipeline_batch=None,         # type: int
                 pipeline_cap=None,           # type: int
                 profile=None,                # type: QueryProfile
                 raw=None,                    # type: Dict[str, Any]
                 scan_wait=None,              # type: timedelta
                 scan_cap=None,               # type: int
                 metrics=False                # type: bool
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        super(QueryOptions, self).__init__(**kwargs)

    def to_n1ql_query(self, statement, *options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        args = self.copy()
        args.update(kwargs)

        # now lets get positional parameters.  Actual positional
        # params OVERRIDE positional_parameters
        positional_parameters = args.pop('positional_parameters', [])
        if options and len(options) > 0:
            positional_parameters = options

        # now the named parameters.  NOTE: all the kwargs that are
        # not VALID_OPTS must be named parameters, and the kwargs
        # OVERRIDE the list of named_parameters
        new_keys = list(filter(lambda x: x not in self.VALID_OPTS, args.keys()))
        named_parameters = args.pop('named_parameters', {})
        for k in new_keys:
            named_parameters[k] = args[k]

        query = N1QLQuery(statement, *positional_parameters, **named_parameters)
        # now lets try to setup the options.  TODO: rework this after beta.3
        # but for now we will use the existing N1QLQuery.  Could be we can
        # add to it, etc...

        # default to false on metrics
        query.metrics = args.get('metrics', False)

        # TODO: there is surely a cleaner way...
        for k in self.VALID_OPTS:
            v = args.get(k, None)
            if v:
                if k == 'scan_consistency':
                    query.consistency = v.as_string()
                if k == 'consistent_with':
                    query.consistent_with = v
                if k == 'adhoc':
                    query.adhoc = v
                if k == 'timeout':
                    query.timeout = v.total_seconds()
                if k == 'scan_cap':
                    query.scan_cap = v
                if k == 'pipeline_batch':
                    query.pipeline_batch = v
                if k == 'pipeline_cap':
                    query.pipeline_cap = v
                if k == 'read_only':
                    query.readonly = v
                if k == 'profile':
                    query.profile = v.as_string()
        return query

        # this will change the options for export.
        # NOT USED CURRENTLY


    def as_dict(self):
        for key, val in self.items():
            if key == 'positional_parameters':
                self.pop(key, None)
                self['args'] = val
            if key == 'named_parameters':
                self.pop(key, None)
                for k, v in val.items():
                    self["${}".format(k)] = v
            if key == 'scan_consistency':
                self[key] = val.as_string()
            if key == 'consistent_with':
                self[key] = val.encode()
            if key == 'profile':
                self[key] = val.as_string()
            if key == 'scan_wait':
                # scan_wait should be in ms
                self[key] = val.total_seconds() * 1000
        if self.get('consistent_with', None):
            self['scan_consistency'] = 'at_plus'
        return self


class Cluster(CoreClient):
    class ClusterOptions(OptionBlock):
        def __init__(self,
                     authenticator,  # type: CoreAuthenticator
                     **kwargs
                     ):
            super(ClusterOptions, self).__init__()
            self['authenticator'] = authenticator

    def __init__(self,
                 connection_string,  # type: str
                 *options,           # type: ClusterOptions
                 bucket_factory=Bucket,  # type: Any
                 **kwargs            # type: Any
                 ):
        """
        Create a Cluster object.
        An Authenticator must be provided, either as the authenticator named parameter, or within the options argument.
        :param str connection_string: the connection string for the cluster.
        :param Callable bucket_factory: factory for producing couchbase.bucket.Bucket derivatives
        :param ClusterOptions options: options for the cluster.
        :param Any kwargs: Override corresponding value in options.
        """
        self.connstr = connection_string
        async_items = {k: kwargs.pop(k) for k in list(kwargs.keys()) if k in {'_iops', '_flags'}}
        cluster_opts = forward_args(kwargs, *options)  # type: Dict[str,Any]
        self._authenticator = cluster_opts.pop('authenticator', None)  # type: Authenticator
        if not self._authenticator:
            raise ArgumentException("Authenticator is mandatory")

        self.__admin = None
        self._cluster = CoreCluster(connection_string, bucket_factory=bucket_factory)  # type: CoreCluster
        self._cluster.authenticate(self._authenticator)
        credentials = self._authenticator.get_credentials()
        self._clusteropts = dict(**credentials.get('options', {}))
        self._clusteropts.update(cluster_opts)
        self._adminopts = dict(**self._clusteropts)
        self._clusteropts.update(async_items)
        #self._clusteropts['bucket'] = "default"
        super(Cluster, self).__init__(connection_string=str(self.connstr), _conntype=_LCB.LCB_TYPE_CLUSTER, **self._clusteropts)

    @classmethod
    def connect(cls, connection_string,  # type: str
                *options,  # type: ClusterOptions
                **kwargs
                ):
        """
        Create a Cluster object.
        An Authenticator must be provided, either as the authenticator named parameter, or within the options argument.
        :param str connection_string: the connection string for the cluster.
        :param ClusterOptions options: options for the cluster.
        :param Any kwargs: Override corresponding value in options.
        """
        return cls(connection_string, *options, **kwargs)

    def _do_ctor_connect(self, *args, **kwargs):
        super(Cluster,self)._do_ctor_connect(*args,**kwargs)

    def _check_for_shutdown(self):
        if not self._cluster:
            raise AlreadyShutdownException("This cluster has already been shutdown")

    @property
    def _admin(self):
        self._check_for_shutdown()
        if not self.__admin:
            self.__admin = Admin(connection_string=str(self.connstr), **self._adminopts)
        return self.__admin

    # TODO: There should be no reason for these kwargs.  However, our tests against the mock
    # will all fail with auth errors without it...  So keeping it just for now, but lets fix it
    # and remove this for 3.0.0
    def bucket(self,
               name    # type: str
               ):
        # type: (...) -> Bucket
        self._check_for_shutdown()
        return self._cluster.open_bucket(name, admin=self._admin)

    # Temporary, helpful with working around CCBC-1204
    def _is_6_5_plus(self):
        self._check_for_shutdown()
        response = self._admin.http_request(path="/pools").value
        v = response.get("implementationVersion")
        # lets just get first 3 characters -- the string should be X.Y.Z-XXXX-YYYY and we only care about
        # major and minor version
        try:
            return float(v[:3]) >= 6.5
        except ValueError:
            # the mock says "CouchbaseMock..."
            return True

    def query(self,
              statement,            # type: str
              *options,             # type: QueryOptions
              **kwargs              # type: Any
              ):
        # type: (...) -> QueryResult
        """
        Perform a N1QL query.

        :param str statement: the N1QL query statement to execute
        :param QueryOptions options: the optional parameters that the Query service takes.
        :param Any options: if present, assumed to be the positional parameters in the query.
        :param Any kwargs: Override the corresponding value in the Options.  If they don't match
          any value in the options, assumed to be named parameters for the query.

        :return: An :class:`QueryResult` object with the results of the query or error message
            if the query failed on the server.

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
                                                     opt.to_n1ql_query(statement, *opts, **kwargs),
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
        Executes an Analytics query against the remote cluster and returns a AnalyticsResult with the results of the query.
        :param statement: the analytics statement to execute
        :param options: the optional parameters that the Analytics service takes based on the Analytics RFC.
        :return: An AnalyticsResult object with the results of the query or error message if the query failed on the server.
        Throws Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except ServiceNotFoundException - service does not exist or cannot be located.
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
                                                     opt.to_analytics_query(statement, *opts, **kwargs),
                                                     itercls=itercls,
                                                     err_msg='Analytics queries require an open bucket')

    def search_query(self,
                     index,     # type: str
                     query,     # type: search.SearchQuery
                     *options,  # type: SearchOptions
                     **kwargs
                     ):
        # type: (...) -> SearchResult
        """
        Executes a Search or F.T.S. query against the remote cluster and returns a SearchResult implementation with the results of the query.

        .. code-block:: python

            it = cb.search('name', ft.MatchQuery('nosql'), SearchOptions(limit=10))
            for hit in it:
                print(hit)

        :param str index: Name of the index to use for this query.
        :param couchbase.search.SearchQuery query: the fluent search API to construct a query for F.T.S.
        :param QueryOptions options: the options to pass to the cluster with the query.
        :param Any kwargs: Overrides corresponding value in options.
        :return: A SearchResult object with the results of the query or error message if the query failed on the server.
        Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except    ServiceNotFoundException - service does not exist or cannot be located.

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
        :param DiagnosticsOptions options:  Options for the diagnostics
        :return: A DiagnosticsResult object with the results of the query or error message if the query failed on the server.

        """
        self._check_for_shutdown()
        result = self._sync_operate_on_entire_cluster(CoreClient.diagnostics, **forward_args(kwargs, *options))
        return DiagnosticsResult(result)

    def ping(self,
             *options,  # type: PingOptions
             **kwargs
             ):
        # type: (...) -> PingResult
        bucket = self._get_an_open_bucket()
        if bucket:
            return PingResult(bucket.ping(*options, **kwargs))
        raise NoBucketException("ping requires a bucket be opened first")

    def users(self):
        # type: (...) -> UserManager
        self._check_for_shutdown()
        return UserManager(self._admin)

    def query_indexes(self):
        # type: (...) -> QueryIndexManager
        self._check_for_shutdown()
        return QueryIndexManager(self._admin)

    def search_indexes(self):
        # type: (...) -> SearchIndexManager
        self._check_for_shutdown()
        return SearchIndexManager(self._admin)

    def analytics_indexes(self):
        # type: (...) -> AnalyticsIndexManager
        self._check_for_shutdown()
        return AnalyticsIndexManager(self)

    def buckets(self):
        # type: (...) -> BucketManager
        self._check_for_shutdown()
        return BucketManager(self._admin)

    def disconnect(self):
        # type: (...) -> None
        """
        Closes and cleans up any resources used by the Cluster and any objects it owns.

        :return: None
        :except Any exceptions raised by the underlying platform

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
    def n1ql_timeout(self):
        # type: (...) -> timedelta
        """
        The timeout for N1QL query operations. This affects the
        :meth:`n1ql_query` method.

        Timeouts may also be adjusted on a per-query basis by setting the
        :attr:`timeout` property in the options to the n1ql_query method.
        The effective timeout is either the per-query timeout or the global timeout,
        whichever is lower.
        """
        self._check_for_shutdown()
        return timedelta(seconds=self._get_timeout_common(_LCB.LCB_CNTL_QUERY_TIMEOUT))

    @n1ql_timeout.setter
    def n1ql_timeout(self,
                     value  # type: timedelta
                     ):
        # type: (...) -> None
        self._check_for_shutdown()
        self._set_timeout_common(_LCB.LCB_CNTL_QUERY_TIMEOUT, value.total_seconds())

    @property
    def tracing_threshold_n1ql(self):
        """
        The tracing threshold for N1QL, as `timedelta`

        ::
            # Set tracing threshold for N1QL to 0.5 seconds
            cb.tracing_threshold_n1ql = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_QUERY, value_type="timeout"))

    @tracing_threshold_n1ql.setter
    def tracing_threshold_n1ql(self,
                               val  # type: timedelta
                               ):
        self._cntl(op=_LCB.TRACING_THRESHOLD_QUERY, value=val.total_seconds(), value_type="timeout")


    @property
    def tracing_threshold_fts(self):
        """
        The tracing threshold for FTS, as `timedelta`.
        ::
            # Set tracing threshold for FTS to 0.5 seconds
            cluster.tracing_threshold_fts = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_SEARCH,
                                                                 value_type="timeout"))

    @tracing_threshold_fts.setter
    def tracing_threshold_fts(self,
                              val   # type: timedelta
                              ):
        self._cntl(op=_LCB.TRACING_THRESHOLD_SEARCH,
                                        value=val.total_seconds(),
                                        value_type="timeout")

    @property
    def tracing_threshold_analytics(self):
        """
        The tracing threshold for analytics, as `timedelta`.

        ::
            # Set tracing threshold for analytics to 0.5 seconds
            cluster.tracing_threshold_analytics = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_ANALYTICS,
                                            value_type="timeout"))

    @tracing_threshold_analytics.setter
    def tracing_threshold_analytics(self,
                                    val     # type: timedelta
                                    ):
        self._cntl(op=_LCB.TRACING_THRESHOLD_ANALYTICS,
                                        value=val.total_seconds(),
                                        value_type="timeout")
    @property
    def tracing_orphaned_queue_flush_interval(self):
        """
        The tracing orphaned queue flush interval, as a `timedelta`

        ::
            # Set tracing orphaned queue flush interval to 0.5 seconds
            cluster.tracing_orphaned_queue_flush_interval = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
                                                    value_type="timeout"))

    @tracing_orphaned_queue_flush_interval.setter
    def tracing_orphaned_queue_flush_interval(self,
                                              val   # type: timedelta
                                              ):
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             op=_LCB.TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
                                             value=val.total_seconds(),
                                             value_type="timeout")

    @property
    def tracing_orphaned_queue_size(self):
        """
        The tracing orphaned queue size.

        ::
            # Set tracing orphaned queue size to 100 entries
            cluster.tracing_orphaned_queue_size = 100

        """

        return self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_SIZE, value_type="uint32_t")

    @tracing_orphaned_queue_size.setter
    def tracing_orphaned_queue_size(self,
                                    val     # type: int
                                    ):
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             op=_LCB.TRACING_ORPHANED_QUEUE_SIZE,
                                             value=val,
                                             value_type="uint32_t")

    @property
    def tracing_threshold_queue_flush_interval(self):
        """
        The tracing threshold queue flush interval, as a `timedelta`

        ::
            # Set tracing threshold queue flush interval to 0.5 seconds
            cluster.tracing_threshold_queue_flush_interval = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL,
                                            value_type="timeout"))

    @tracing_threshold_queue_flush_interval.setter
    def tracing_threshold_queue_flush_interval(self,
                                               val  # type: timedelta
                                               ):
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             op=_LCB.TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL,
                                             value=val.total_seconds(),
                                             value_type="timeout")

    @property
    def tracing_threshold_queue_size(self):
        """
        The tracing threshold queue size.

        ::
            # Set tracing threshold queue size to 100 entries
            cluster.tracing_threshold_queue_size = 100

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_SIZE, value_type="uint32_t")

    @tracing_threshold_queue_size.setter
    def tracing_threshold_queue_size(self, val):
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             op=_LCB.TRACING_THRESHOLD_QUEUE_SIZE,
                                             value=val,
                                             value_type="uint32_t")

    @property
    def redaction(self):
        return bool(self._cntl(_LCB.LCB_CNTL_LOG_REDACTION, value_type='int'))

    @redaction.setter
    def redaction(self,
                  val   # type: bool
                  ):
        val = 1 if val else 0
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             _LCB.LCB_CNTL_LOG_REDACTION,
                                             value=val,
                                             value_type='int')

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
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             _LCB.LCB_CNTL_COMPRESSION_OPTS,
                                             value=value,
                                             value_type='int')

    @property
    def compression_min_size(self):
        """
        Minimum size (in bytes) of the document payload to be compressed when compression enabled.

        :type: int
        """
        return self._cntl(_LCB.LCB_CNTL_COMPRESSION_MIN_SIZE, value_type='uint32_t')

    @compression_min_size.setter
    def compression_min_size(self,
                             value  # type: int
                             ):
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             _LCB.LCB_CNTL_COMPRESSION_MIN_SIZE,
                                             value_type='uint32_t',
                                             value=value)

    @property
    def compression_min_ratio(self):
        """
        Minimum compression ratio (compressed / original) of the compressed payload to allow sending it to cluster.

        :type: float
        """
        return self._cntl(_LCB.LCB_CNTL_COMPRESSION_MIN_RATIO, value_type='float')

    @compression_min_ratio.setter
    def compression_min_ratio(self, value):
        self._sync_operate_on_entire_cluster(CoreClient._cntl,
                                             _LCB.LCB_CNTL_COMPRESSION_MIN_RATIO,
                                             value_type='float',
                                             value=value)

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


class AsyncCluster(AsyncClientFactory.gen_async_client(Cluster)):
    @classmethod
    def connect(cls, connection_string=None, *args, **kwargs):
        return cls(connection_string=connection_string, *args, **kwargs)


ClusterOptions = Cluster.ClusterOptions
