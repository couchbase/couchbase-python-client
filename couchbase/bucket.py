from couchbase.management.collections import CollectionManager
from couchbase.management.views import ViewIndexManager
from couchbase.management.admin import Admin
from couchbase.management.views import DesignDocumentNamespace
from couchbase_core.client import Client as CoreClient
import couchbase_core._libcouchbase as _LCB
from .collection import CBCollection, CoreClientDatastructureWrap
from .options import OptionBlockTimeOut
from .result import *
from .collection import Scope
from datetime import timedelta
from enum import Enum
import logging
from couchbase_core.asynchronous.client import AsyncClientMixin


class ViewScanConsistency(Enum):
    NOT_BOUNDED = 'ok'
    REQUEST_PLUS = 'false'
    UPDATE_AFTER = 'update_after'


class ViewOrdering(Enum):
    DESCENDING  = 'true'
    ASCENDING = 'false'


class ViewErrorMode(Enum):
    CONTINUE = 'continue'
    STOP = 'stop'


class ViewOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,               # type: timedelta
                 scan_consistency=None,      # type: ViewScanConsistency
                 skip=None,                  # type: int
                 limit=None,                 # type: int
                 startkey=None,              # type: Any
                 endkey=None,                # type: Any
                 startkey_docid=None,        # type: str
                 endkey_docid=None,          # type: str
                 inclusive_end=None,         # type: bool
                 group=None,                 # type: bool
                 group_level=None,           # type: int
                 key=None,                   # type: Any
                 keys=None,                  # type: Any
                 order=None,                 # type: ViewOrdering
                 reduce=None,                # type: bool
                 on_error=None,              # type: ViewErrorMode
                 debug=None,                 # type: bool
                 raw=None,                   # type: Tuple(str,str)
                 namespace=None              # type: DesignDocumentNamespace
                 ):
        pass

    def __init__(self, **kwargs):
        # massage them into the form needed by the existing
        # view infrastructure... TODO: get rid of this when
        # we kill off the v2 stuff
        val = kwargs.pop('scan_consistency', None)
        if val:
            kwargs['stale'] = val.value()
        val = kwargs.pop('order', None)
        if val:
            kwargs['descending'] = val.value()
        val = kwargs.pop('on_error', None)
        if val:
            kwargs['on_error'] = val.value()
        val = kwargs.pop('raw', None)
        if val:
            kwargs[val[0]] =  val[1]
        val = kwargs.pop('namespace', None)
        if val:
           kwargs['use_devmode'] = (val == DesignDocumentNamespace.DEVELOPMENT)
        super(ViewOptions, self).__init__(**kwargs)


class PingOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 report_id=None,     # type: str
                 service_types=None  # type: Iterable[ServiceType]
                 ):
        """
        Create options used for ping command.

        :param timedelta timeout: Currently not implemented, coming soon.
        :param str report_id: Add an id to the request, which you can track in logging, etc...
        :param Iterable[ServiceType] service_types: Restrict the ping to the services passed in here.
        """
        pass

    def __init__(self,
                 **kwargs
                 ):
        if 'service_types' in kwargs:
            kwargs['service_types'] = list(map(lambda x : x.value, kwargs['service_types']))

        super(PingOptions, self).__init__(**kwargs)


class Bucket(CoreClientDatastructureWrap):
    @internal
    def __init__(self,
                 connection_string=None,            # type: str
                 name=None,                         # type: str
                 collection_factory=CBCollection,   # type: Type[CBCollection]
                 admin=None,                        # type: Admin
                 *options,
                 **kwargs
                 ):
        # type: (...) -> None
        """
        Connect to a bucket.
        Typically invoked by :meth:`couchbase.cluster.Cluster.open_bucket`

        :param str connection_string:
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

        :param str name: name of bucket.

        :param string username: username to connect to bucket with

        :param string password: the password of the bucket

        :param boolean quiet: the flag controlling whether to raise an
            exception when the client executes operations on
            non-existent keys. If it is `False` it will raise
            :exc:`.DocumentNotFoundException` exceptions. When
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

        :raise: :exc:`.BucketNotFoundException` or :exc:`.AuthenticationException` if
            there is no such bucket to connect to, or if invalid
            credentials were supplied.
        :raise: :exc:`.CouchbaseNetworkException` if the socket wasn't
            accessible (doesn't accept connections or doesn't respond
            in
        :raise: :exc:`.InvalidException` if the connection string
            was malformed.

        :return: instance of :class:`~couchbase.bucket.Bucket`


        """
        self._name = name or kwargs.get('bucket', None)
        self._connstr = connection_string
        self._bucket_args = forward_args(kwargs, *options)
        self._bucket_args['bucket'] = name
        self._collection_factory = collection_factory

        super(Bucket,self).__init__(connection_string, **self._bucket_args)
        self._admin = admin

    @property
    def _bucket(self):
        return self

    @property
    def name(self):
        # type: (...) -> str
        """
        Get the name of this bucket.

        :return: Name of this bucket.
        :rtype: str
        """
        return self._name

    def scope(self,
              scope_name  # type: str
              ):
        # type: (...) -> Scope
        """
        Open the named scope.

        :param scope_name: Name of scope to open on this bucket.
        :return: the named scope
        """
        return Scope(self, scope_name)

    def default_collection(self):
        # type: (...) -> CBCollection
        """
        Open the default collection.

        :return: the default :class:`Collection` object.
        """
        return Scope(self).default_collection()

    def collection(self,
                   collection_name  # type: str
                   ):
        # type: (...) -> CBCollection
        """
        Open a collection in the default scope.

        :param collection_name: collection name
        :return: the default :class:`.Collection` object.
        """
        return Scope(self).collection(collection_name)

    def collections(self  # type: Bucket
                    ):
        # type: (...) -> CollectionManager
        """
        Get the CollectionManager.

        :return: the :class:`.management.CollectionManager` for this bucket.
        """
        return CollectionManager(self._admin, self._name)

    def view_query(self,
                   design_doc,      # type: str
                   view_name,       # type: str
                   *view_options,   # type: ViewOptions
                   **kwargs
                   ):
        # type: (...) -> ViewResult
        """
        Run a View Query

        :param str design_doc: design document
        :param str view_name: view name
        :param ViewOptions view_options: Options to use when querying a view index.
        :param kwargs: Override corresponding option in options.
        :return: A :class:`ViewResult` containing the view results
        """
        final_kwargs={'itercls':ViewResult}
        final_kwargs.update(kwargs)
        res = CoreClient.view_query(self, design_doc, view_name, **forward_args(final_kwargs, *view_options))
        return res

    def view_indexes(self  # type: Bucket
                     ):
        # type: (...) -> ViewIndexManager
        """
        Get the ViewIndexManager for this bucket.

        :return: The :class:`.management.ViewIndexManager` for this bucket.
        """
        return ViewIndexManager(self, self._admin, self._name)

    def ping(self,
             *options,   # type: PingOptions
             **kwargs
             ):
        # type: (...) -> PingResult
        """
        Actively contacts each of the  services and returns their pinged status.

        :param options: Options for sending the ping request.
        :param kwargs: Overrides corresponding value in options.
        :return: A :class:`PingResult` representing the state of all the pinged services.
        :raise: CouchbaseException for various communication issues.
        """
        return PingResult(super(Bucket,self).ping(**forward_args(kwargs, *options)))

    @property
    def kv_timeout(self):
        # type: (...) -> timedelta
        """
        The default timeout for all kv operations on this bucket.
        ::
            # Set the default kv timeout to 10 seconds:
            bucket.kv_timeout = timedelta(seconds=10)

            # Get the current default:
            timeout = bucket.kv_timeout
        """
        return timedelta(seconds=self._get_timeout_common(_LCB.LCB_CNTL_OP_TIMEOUT))

    @property
    def views_timeout(self):
        # type: (...) -> timedelta
        """
        The default timeout for all view operations on this bucket.
        ::
            # Set the default view timeout to 10 seconds:
            cb.view_timeout = timedelta(seconds=10)

            # Get the default view timeout:
            timeout = cb.view_timeout
        """
        # lets use the private function in the private _bucket for now.  Soon
        # that _bucket will be gone and we will have migrated this all into here.
        return timedelta(seconds=self._get_timeout_common(_LCB.LCB_CNTL_VIEW_TIMEOUT))

    @property
    def tracing_orphaned_queue_flush_interval(self):
        """
        The tracing orphaned queue flush interval, as a `timedelta`

        ::
            # Set tracing orphaned queue flush interval to 0.5 seconds
            cb.tracing_orphaned_queue_flush_interval = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
                                                    value_type="timeout"))

    @property
    def tracing_orphaned_queue_size(self):
        """
        The tracing orphaned queue size.

        ::
            # Set tracing orphaned queue size to 100 entries
            cb.tracing_orphaned_queue_size = 100

        """

        return self._cntl(op=_LCB.TRACING_ORPHANED_QUEUE_SIZE, value_type="uint32_t")

    @property
    def tracing_threshold_queue_flush_interval(self):
        """
        The tracing threshold queue flush interval, as a `timedelta`

        ::
            # Set tracing threshold queue flush interval to 0.5 seconds
            cb.tracing_threshold_queue_flush_interval = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL,
                                                    value_type="timeout"))

    @property
    def tracing_threshold_queue_size(self):
        """
        The tracing threshold queue size.

        ::
            # Set tracing threshold queue size to 100 entries
            cb.tracing_threshold_queue_size = 100

        """

        return self._cntl(op=_LCB.TRACING_THRESHOLD_QUEUE_SIZE, value_type="uint32_t")

    @property
    def tracing_threshold_kv(self):
        """
        The tracing threshold for KV, as a `timedelta`.

        ::
            # Set tracing threshold for KV to 0.5 seconds
            cb.tracing_threshold_kv = timedelta(seconds=0.5)

        """

        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_KV, value_type="timeout"))

    @property
    def tracing_threshold_view(self):
        """
        The tracing threshold for View, as `timedelta`.

        ::
            # Set tracing threshold for View to 0.5 seconds
            cb.tracing_threshold_view = timedelta(seconds=0.5)

        """
        return timedelta(seconds=self._cntl(op=_LCB.TRACING_THRESHOLD_VIEW, value_type="timeout"))


class AsyncBucket(AsyncClientMixin, Bucket):
    pass
