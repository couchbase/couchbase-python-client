import abc

from uuid import UUID

from typing import *

from .n1ql import QueryResult, IQueryResult
from .options import OptionBlock, forward_args, OptionBlockDeriv
from .bucket import BucketOptions, Bucket, CoreBucket
from couchbase_core.cluster import Cluster as SDK2Cluster, Authenticator as SDK2Authenticator
from .exceptions import SearchException, DiagnosticsException, QueryException

T = TypeVar('T')


class QueryMetrics(object):
    pass


CallableOnOptionBlock = Callable[[OptionBlockDeriv, Any], Any]


def options_to_func(orig,  # type: U
                    verb  # type: CallableOnOptionBlock
                    ):
    class invocation:
        # type: (...)->Callable[[T,Tuple[OptionBlockDeriv,...],Any],Any]
        def __init__(self,  # type: T
                       *options,  # type: OptionBlockDeriv
                       **kwargs  # type: Any
                       ):
            # type: (...)->None
            self.orig=orig
            self.options=options
            self.kwargs=kwargs

        def __call__(self, *args, **kwargs):
            # type: (...)->Callable[[T,Tuple[OptionBlockDeriv,...],Any],Any]
            def invocator(self, *options, **kwargs):
                return verb(self, forward_args(kwargs, *options))
            return invocator

    return invocation(orig)


class QueryOptions(OptionBlock, IQueryResult):
    @property
    @abc.abstractmethod
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


class Cluster:
    class ClusterOptions(OptionBlock):
        pass

    def __init__(self,
                 connection_string=None,  # type: str
                 *options  # type: ClusterOptions
                 ):
        cluster_opts=forward_args(None, *options)
        cluster_opts.update(bucket_class=lambda connstr, bname=None, **kwargs: Bucket(connstr,bname, BucketOptions(**kwargs)))
        self._cluster = SDK2Cluster(connection_string, **cluster_opts)  # type: SDK2Cluster

    def authenticate(self,
                     authenticator=None,  # type: SDK2Authenticator
                     username=None,  # type: str
                     password=None  # type: str
                     ):
        self._cluster.authenticate(authenticator, username, password)

    def bucket(self,
               name,  # type: str,
               *options,  # type: BucketOptions
               **kwargs
               ):
        # type: (...)->Bucket
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
        # type: (...)->IQueryResult
        pass

    def query(self,
              statement,  # type: str
              *options,  # type: QueryOptions
              **kwargs  # type: Any
              ):
        # type: (...) -> QueryResult
        """
        Perform a N1QL query.

        :param str statement: the N1QL query statement to execute
        :param QueryOptions options: the optional parameters that the Query service takes.
            See The N1QL Query API for details or a SDK 2.0 implementation for detail.

        :return: An :class:`IQueryResult` object with the results of the query or error message
            if the query failed on the server.

        """
        return QueryResult(self._operate_on_first_bucket(CoreBucket.query, QueryException, statement, **forward_args(kwargs, *options)))

    def _operate_on_first_bucket(self, verb, failtype, *args, **kwargs):
        first_bucket = next(iter(self._cluster._buckets.items()), None)  # type: Optional[couchbase.CoreBucket]
        if not first_bucket:
            raise failtype("Need at least one bucket active to perform search")
        return verb(first_bucket[1]()._bucket, *args, **kwargs)

    def analytics_query(self,
                        statement,  # type: str,
                        *options,  # type: AnalyticsOptions
                        **kwargs
                        ):
        # type: (...)->IAnalyticsResult
        """
        Executes an Analytics query against the remote cluster and returns a IAnalyticsResult with the results of the query.
        :param statement: the analytics statement to execute
        :param options: the optional parameters that the Analytics service takes based on the Analytics RFC.
        :return: An IAnalyticsResult object with the results of the query or error message if the query failed on the server.
        Throws Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except ServiceNotFoundException - service does not exist or cannot be located.
        """
        return self.query(statement, *options, **kwargs)

    def search_query(self,
                     index,  # type: str
                     query,  # type: str
                     *options,  # type: SearchOptions
                     **kwargs
                     ):
        # type: (...)->ISearchResult
        """
        Executes a Search or FTS query against the remote cluster and returns a ISearchResult implementation with the results of the query.

        :param query: the fluent search API to construct a query for FTS
        :param options: the options to pass to the cluster with the query based off the FTS/Search RFC
        :return: An ISearchResult object with the results of the query or error message if the query failed on the server.
        Any exceptions raised by the underlying platform - HTTP_TIMEOUT for example.
        :except    ServiceNotFoundException - service does not exist or cannot be located.

        """
        return self._operate_on_first_bucket(CoreBucket.search, SearchException, index, query, **forward_args(kwargs, *options))


    def diagnostics(self,
                    reportId=None  # type: str
                    ):
        # type: (...)->IDiagnosticsResult
        """
        Creates a diagnostics report that can be used to determine the healthfulness of the Cluster.
        :param reportId - an optional string name for the generated report.
        :return:A IDiagnosticsResult object with the results of the query or error message if the query failed on the server.

        """
        return self._operate_on_first_bucket(CoreBucket.diagnostics, DiagnosticsException)

    def users(self):
        # type: (...)->IUserManager
        raise NotImplementedError("To be implemented in SDK3 full release")

    def indexes(self):
        # type: (...)->IIndexManager
        raise NotImplementedError("To be implemented in SDK3 full release")

    def nodes(self):
        # type: (...)->INodeManager
        return self._cluster

    def buckets(self):
        # type: (...)->IBucketManager
        return self._cluster._buckets

    def disconnect(self,
                   options=None  # type: DisconnectOptions
                   ):
        # type: (...)->None
        """
        Closes and cleans up any resources used by the Cluster and any objects it owns. Note the name is platform idiomatic.

        :param options - TBD
        :return: None
        :except Any exceptions raised by the underlying platform

        """
        raise NotImplementedError("To be implemented in full SDK3 release")

    def manager(self):
        # type: (...)->ClusterManager
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