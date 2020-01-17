from couchbase.management import CollectionManager, ViewIndexManager
from couchbase.management.admin import Admin
from couchbase_core.supportability import uncommitted, volatile
from couchbase_core.client import Client as CoreClient
from .collection import CBCollection, CollectionOptions
from .options import OptionBlock
from .result import *
from .collection import Scope


class BucketOptions(OptionBlock):
    pass


class ViewOptions(OptionBlock):
    pass


class Bucket(object):
    _bucket = None  # type: CoreClient

    @overload
    def __init__(self,
                 connection_string,  # type: str
                 name=None,
                 admin=None,  # type: Admin
                 *options  # type: BucketOptions
                 ):
        # type: (...) -> None
        pass

    def __init__(self,
                 connection_string,  # type: str
                 name=None,
                 corebucket_class=CBCollection,  # type: Type[CoreClient]
                 admin=None,  # type: Admin
                 *options,
                 **kwargs
                ):
        # type: (...) -> None
        """
        Connect to a bucket.
        Typically invoked by :meth:`couchbase.cluster.Cluster.open_bucket`

        :param str name: name of bucket.

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

        :return: instance of :class:`~couchbase.bucket.Bucket`


        """
        self._name = name
        self._connstr=connection_string
        self._bucket_args=forward_args(kwargs, *options)
        self._bucket_args['bucket']=name
        self._corebucket_class=corebucket_class

        self._bucket = CoreClient(connection_string, **self._bucket_args)
        self._admin = admin

    @property
    def name(self):
        # type: (...) -> str
        return self._name

    @volatile
    def scope(self,
              scope_name  # type: str
              ):
        # type: (...) -> Scope
        """
        Open the named scope.

        :param scope_name:
        :return: the named scope
        :rtype: Scope
        """
        return Scope(self, scope_name)

    def default_collection(self,
                           options=None  # type: CollectionOptions
                           ):
        # type: (...) -> CBCollection
        """
        Open the default collection.

        :param CollectionOptions options: any options to pass to the Collection constructor
        :return: the default :class:`Collection` object.
        """
        return Scope(self).default_collection()

    @volatile
    def collection(self,
                   collection_name,  # type: str
                   options=None  # type: CollectionOptions
                   ):
        # type: (...) -> CBCollection
        """
        Open a collection in the default scope.

        :param collection_name: collection name
        :param CollectionOptions options: any options to pass to the Collection constructor
        :return: the default :class:`.Collection` object.
        """
        return Scope(self).collection(collection_name)

    def collections(self):
        return CollectionManager(self._admin, self._name)

    @overload
    def view_query(self,
                   design_doc,  # type: str
                   view_name,  # type: str
                   ):
        pass

    def view_query(self,
                   design_doc,  # type: str
                   view_name,  # type: str
                   *view_options, # type: ViewOptions
                   **kwargs
                   ):
        # type: (...) -> ViewResult
        """
        Run a View Query
        :param str design_doc: design document
        :param str view_name: view name
        :param view_options:
        :return: ViewResult containing the view results
        """
        cb = self._bucket  # type: CoreClient
        res = cb.view_query(design_doc, view_name, **forward_args(kwargs, *view_options))
        return ViewResult(res)

    def views(self  # type: Bucket
              ):
        # type: (...)->ViewIndexManager
        return ViewIndexManager(self._bucket, self._name)
