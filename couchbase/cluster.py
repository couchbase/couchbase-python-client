from typing import *

from couchbase.management.queries import QueryIndexManager
from .management.users import UserManager
from .management.buckets import BucketManager
from couchbase.management.admin import Admin
from couchbase.diagnostics import DiagnosticsResult, EndPointDiagnostics, IDiagnosticsResult
from couchbase.fulltext import ISearchResult, SearchResult, SearchOptions
from couchbase_core.fulltext import Query, Facet
from .analytics import AnalyticsResult
from .n1ql import QueryResult, IQueryResult
from .options import OptionBlock, forward_args, OptionBlockDeriv
from .bucket import BucketOptions, Bucket, CoreClient
from couchbase_core.cluster import Cluster as SDK2Cluster, Authenticator as SDK2Authenticator
from .exceptions import SearchException, DiagnosticsException, QueryException, ArgumentError, AnalyticsException
from couchbase_core import abstractmethod
import multiprocessing
from multiprocessing.pool import ThreadPool
import couchbase.exceptions
import couchbase_core._libcouchbase as _LCB

T = TypeVar('T')


class QueryMetrics(object):
    pass


CallableOnOptionBlock = Callable[[OptionBlockDeriv, Any], Any]


def options_to_func(orig,  # type: U
                    verb  # type: CallableOnOptionBlock
                    ):
    class invocation:
        # type: (...) -> Callable[[T,Tuple[OptionBlockDeriv,...],Any],Any]
        def __init__(self,  # type: T
                       *options,  # type: OptionBlockDeriv
                       **kwargs  # type: Any
                       ):
            # type: (...) -> None
            self.orig=orig
            self.options=options
            self.kwargs=kwargs

        def __call__(self, *args, **kwargs):
            # type: (...) -> Callable[[T,Tuple[OptionBlockDeriv,...],Any],Any]
            def invocator(self, *options, **kwargs):
                return verb(self, forward_args(kwargs, *options))
            return invocator

    return invocation(orig)


class AnalyticsOptions(OptionBlock):
    pass


class QueryOptions(OptionBlock, IQueryResult):
    @property
    @abstractmethod
    def is_live(self):
        return False

    def __init__(self, statement=None, parameters=None, timeout=None):

        """
        Executes a N1QL query against the remote cluster returning a IQueryResult with the results of the query.
        :param statement: N1QL query
        :param options: the optional parameters that the Query service takes. See The N1QL Query API for details or a SDK 2.0 implementation for detail.
        :return: An IQueryResult object with the results of the query or error message if the query failed on the server.
        :except Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except ServiceNotFoundException - service does not exist or cannot be located.

        """
        super(QueryOptions, self).__init__(statement=statement, parameters=parameters, timeout=timeout)


class Cluster(object):
    clusterbucket = None  # type: CoreClient

    class ClusterOptions(OptionBlock):
        def __init__(self,
                     authenticator,  # type: SDK2Authenticator
                     **kwargs
                     ):
            super(ClusterOptions, self).__init__()
            self['authenticator'] = authenticator

    @overload
    def __init__(self,
                 connection_string,  # type: str
                 options  # type: ClusterOptions
                 ):
        pass

    def __init__(self,
                 connection_string,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs
                 ):
        """
        Create a Cluster object.
        An Authenticator must be provided either as the first argument or within the options argument.
        :param connection_string: the connection string for the cluster
        :param options: options for the cluster
        :type Cluster.ClusterOptions
        """
        self.connstr=connection_string
        cluster_opts=forward_args(kwargs, *options)
        authenticator=cluster_opts.pop('authenticator',None)
        if not authenticator:
            raise ArgumentError("Authenticator is mandatory")
        cluster_opts.update(bucket_class=lambda connstr, bname=None, **kwargs: Bucket(connstr,name=bname,admin=self.admin,**kwargs))
        self._cluster = SDK2Cluster(connection_string, **cluster_opts)  # type: SDK2Cluster
        self._authenticate(authenticator)

    @staticmethod
    def connect(connection_string,  # type: str
                *options,  # type: ClusterOptions
                **kwargs
                ):
        return Cluster(connection_string, *options, **kwargs)

    def _authenticate(self,
                      authenticator=None,  # type: SDK2Authenticator
                      username=None,  # type: str
                      password=None  # type: str
                      ):
        self._cluster.authenticate(authenticator, username, password)
        credentials = authenticator.get_credentials()
        self._clusteropts = credentials.get('options', {})
        self._clusteropts['bucket'] = "default"
        self._clusterclient=None
        auth=credentials.get('options')
        self.admin = Admin(auth.get('username'), auth.get('password'), connstr=str(self.connstr))

    def bucket(self,
               name,  # type: str,
               *options,  # type: BucketOptions
               **kwargs
               ):
        # type: (...) -> Bucket
        args=forward_args(kwargs,*options)
        args.update(bname=name)
        return self._cluster.open_bucket(name, **args)

    class QueryParameters(OptionBlock):
        def __init__(self, *args, **kwargs):
            super(Cluster.QueryParameters, self).__init__(*args, **kwargs)

    @overload
    def query(self,
              statement,
              parameters=None,
              timeout=None):
        pass

    @overload
    def query(self,
              statement,  # type: str,
              *options  # type: QueryOptions
              ):
        # type: (...) -> IQueryResult
        pass

    def query(self,
              statement,  # type: str
              *options,  # type: QueryOptions
              **kwargs  # type: Any
              ):
        # type: (...) -> IQueryResult
        """
        Perform a N1QL query.

        :param str statement: the N1QL query statement to execute
        :param QueryOptions options: the optional parameters that the Query service takes.
            See The N1QL Query API for details or a SDK 2.0 implementation for detail.

        :return: An :class:`IQueryResult` object with the results of the query or error message
            if the query failed on the server.

        """
        return QueryResult(self._operate_on_cluster(CoreClient.query, QueryException, statement, **(forward_args(kwargs, *options))))

    def _operate_on_cluster(self, verb, failtype, *args, **kwargs):
        if not self._clusterclient:
            self._clusterclient = CoreClient(str(self.connstr), _conntype=_LCB.LCB_TYPE_CLUSTER, **self._clusteropts)
        try:
            return verb(self._clusterclient, *args, **kwargs)
        except Exception as e:
            raise failtype(str(e))

    def analytics_query(self,  # type: Cluster
                        statement,  # type: str,
                        *options,  # type: AnalyticsOptions
                        **kwargs
                        ):
        # type: (...) -> IAnalyticsResult
        """
        Executes an Analytics query against the remote cluster and returns a IAnalyticsResult with the results of the query.
        :param statement: the analytics statement to execute
        :param options: the optional parameters that the Analytics service takes based on the Analytics RFC.
        :return: An IAnalyticsResult object with the results of the query or error message if the query failed on the server.
        Throws Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except ServiceNotFoundException - service does not exist or cannot be located.
        """

        return AnalyticsResult(self._operate_on_cluster(CoreClient.analytics_query, AnalyticsException, statement, **forward_args(kwargs,*options)))

    @overload
    def search_query(self,
                     index,  # type: str
                     query,  # type: Union[str, Query]
                     facets=None  # type: Mapping[str,Facet]
                     ):
        pass

    @overload
    def search_query(self,
                     index,  # type: str
                     query,  # type: Union[str, Query]
                     options,  # type: SearchOptions
                     ):
        pass

    def search_query(self,
                     index,  # type: str
                     query,  # type: Union[str, Query]
                     *options,  # type: SearchOptions
                     **kwargs
                     ):
        # type: (...) -> ISearchResult
        """
        Executes a Search or FTS query against the remote cluster and returns a ISearchResult implementation with the results of the query.

        :param query: the fluent search API to construct a query for FTS
        :param options: the options to pass to the cluster with the query based off the FTS/Search RFC
        :return: An ISearchResult object with the results of the query or error message if the query failed on the server.
        Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except    ServiceNotFoundException - service does not exist or cannot be located.

        """
        return SearchResult(self._operate_on_cluster(CoreClient.search, SearchException, index, query, **forward_args(kwargs, *options)))

    _root_diag_data = {'id', 'version', 'sdk'}

    def diagnostics(self,
                    reportId=None,  # type: str
                    timeout=None
                    ):
        # type: (...) -> IDiagnosticsResult
        """
        Creates a diagnostics report that can be used to determine the healthfulness of the Cluster.
        :param reportId - an optional string name for the generated report.
        :return:A IDiagnosticsResult object with the results of the query or error message if the query failed on the server.

        """

        pool = ThreadPool(processes=1)
        diag_results_async_result = pool.apply_async(self._operate_on_cluster,
                                                     (CoreClient.diagnostics, DiagnosticsException))
        try:
            diag_results = diag_results_async_result.get(timeout)
        except multiprocessing.TimeoutError as e:
            raise couchbase.exceptions.TimeoutError(params=dict(inner_cause=e))

        final_results = {'services': {}}

        for k, v in diag_results.items():
            if k in Cluster._root_diag_data:
                final_results[k] = v
            else:
                for item in v:
                    final_results['services'][k] = EndPointDiagnostics(k, item)
        return DiagnosticsResult(final_results)

    def users(self):
        # type: (...) -> UserManager
        return UserManager(self.admin)

    def query_indexes(self):
        # type: (...) -> QueryIndexManager
        return QueryIndexManager(self.admin)

    def nodes(self):
        # type: (...) -> INodeManager
        return self._cluster

    def buckets(self):
        # type: (...) -> BucketManager
        return BucketManager(self.admin)

    def disconnect(self,
                   options=None  # type: DisconnectOptions
                   ):
        # type: (...) -> None
        """
        Closes and cleans up any resources used by the Cluster and any objects it owns. Note the name is platform idiomatic.

        :param options - TBD
        :return: None
        :except Any exceptions raised by the underlying platform

        """
        raise NotImplementedError("To be implemented in full SDK3 release")

    def manager(self):
        # type: (...) -> ClusterManager
        """

        Manager
        Returns a ClusterManager object for managing resources at the Cluster level.

        Caveats and notes:
        It is acceptable for a Cluster object to have a static Connect method which takes a Configuration for ease of use.
        To facilitate testing/mocking, it's acceptable for this structure to derive from an interface at the implementers discretion.
        The "Get" and "Set" prefixes are considered platform idiomatic and can be adjusted to various platform idioms.
        The Configuration is passed in via the ctor; overloads for connection strings and various other platform specific configuration are also passed this way.
        If a language does not support ctor overloading, then an equivalent method can be used on the object.

        :return:
        """
        return self._cluster.cluster_manager()


QueryParameters = Cluster.QueryParameters
ClusterOptions = Cluster.ClusterOptions