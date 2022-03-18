from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    overload)

from couchbase.collection import Collection
from couchbase.logic import BlockingWrapper
from couchbase.logic.bucket import BucketLogic
from couchbase.management.collections import CollectionManager
from couchbase.management.views import ViewIndexManager
from couchbase.result import PingResult, ViewResult
from couchbase.scope import Scope
from couchbase.views import ViewQuery, ViewRequest

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.cluster import Cluster
    from couchbase.diagnostics import ServiceType
    from couchbase.options import PingOptions, ViewOptions


class Bucket(BucketLogic):

    def __init__(self, cluster,  # type: Cluster
                 bucket_name  # type: str
                 ):
        super().__init__(cluster, bucket_name)
        self._open_bucket()

    @BlockingWrapper.block(True)
    def _open_bucket(self, **kwargs):
        connected = super()._open_or_close_bucket(open_bucket=True, **kwargs)
        self._set_connected(connected)

    @BlockingWrapper.block(True)
    def _close_bucket(self, **kwargs):
        super()._open_or_close_bucket(open_bucket=False, **kwargs)
        self._destroy_connection()

    def close(self):
        # only close if we are connected
        if self.connected:
            self._close_bucket()

    def default_scope(self
                      ) -> Scope:
        return self.scope(Scope.default_name())

    def scope(self, name  # type: str
              ) -> Scope:
        return Scope(self, name)

    def collection(self, collection_name):
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self):
        scope = self.default_scope()
        print('creating scope')
        return scope.collection(Collection.default_name())

    @BlockingWrapper.block(PingResult)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
             ) -> PingResult:
        return super().ping(*opts, **kwargs)

    def view_query(self,
                   design_doc,      # type: str
                   view_name,       # type: str
                   *view_options,   # type: ViewOptions
                   **kwargs
                   ) -> ViewResult:

        query = ViewQuery.create_view_query_object(
            self.name, design_doc, view_name, *view_options, **kwargs
        )
        return ViewResult(ViewRequest.generate_view_request(self.connection,
                                                            self.loop,
                                                            query.as_encodable()))

    def collections(self) -> CollectionManager:
        """
        Get the CollectionManager.

        :return: the :class:`.management.collections.CollectionManager` for this bucket.
        """
        return CollectionManager(self.connection, self.name)

    def view_indexes(self) -> ViewIndexManager:
        """
        Get the ViewIndexManager for this bucket.

        :return: The :class:`.management.ViewIndexManager` for this bucket.
        """
        return ViewIndexManager(self.connection, self.name)


"""
@TODO:  remove the code below for the 4.1 release

Everything below should be removed in the 4.1 release.
All options should come from couchbase.options, or couchbase.management.options

"""


class PingOptionsDeprecated(dict):
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
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


PingOptions = PingOptionsDeprecated  # noqa: F811
