from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict)

from acouchbase.collection import Collection
from acouchbase.logic import AsyncWrapper
from acouchbase.management.collections import CollectionManager
from acouchbase.scope import Scope
from acouchbase.views import AsyncViewRequest, ViewQuery
from couchbase.logic.bucket import BucketLogic
from couchbase.management.views import ViewIndexManager
from couchbase.result import PingResult, ViewResult

if TYPE_CHECKING:
    from acouchbase.cluster import Cluster
    from couchbase.options import PingOptions, ViewOptions


class AsyncBucket(BucketLogic):

    def __init__(self, cluster,  # type: Cluster
                 bucket_name  # type: str
                 ):
        super().__init__(cluster, bucket_name)
        self._close_ftr = None
        self._connect_ftr = self._open_bucket()

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._cluster.loop

    # def _connect_bucket(self):
    #     """
    #     **INTERNAL**
    #     Used w/in wrappers if a collection op is called when not connected
    #     """
    #     if self._connect_ftr is not None:
    #         return

    #     if not self._cluster.connected:
    #         self._connect_ftr = self.loop.create_future()
    #         ft = self._cluster.on_connect()
    #         ft.add_done_callback(partial(AsyncWrapper.chain_connect_callbacks, self))
    #     else:
    #         self._connect_ftr = super()._open_or_close_bucket(open_bucket=True)

    @AsyncWrapper.inject_bucket_open_callbacks()
    def _open_bucket(self, **kwargs) -> Awaitable:
        super()._open_or_close_bucket(open_bucket=True, **kwargs)

    @AsyncWrapper.inject_close_callbacks()
    def _close_bucket(self, **kwargs):
        super()._open_or_close_bucket(open_bucket=False, **kwargs)

    def on_connect(self) -> Awaitable:
        if not (self._connect_ftr or self.connected):
            self._connect_ftr = self._open_bucket()
            self._close_ftr = None

        return self._connect_ftr

    async def close(self) -> None:
        if self.connected and not self._close_ftr:
            self._close_ftr = self._close_bucket()
            self._connect_ftr = None

        await self._close_ftr
        super()._destroy_connection()

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
        return scope.collection(Collection.default_name())

    @AsyncWrapper.inject_cluster_callbacks(PingResult)
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
        return ViewResult(AsyncViewRequest.generate_view_request(self.connection,
                                                                 self.loop,
                                                                 query.as_encodable()))

    def collections(self) -> CollectionManager:
        """
        Get the CollectionManager.

        :return: the :class:`.management.collections.CollectionManager` for this bucket.
        """
        return CollectionManager(self.connection, self.loop, self.name)

    def view_indexes(self) -> ViewIndexManager:
        """
        Get the ViewIndexManager for this bucket.

        :return: The :class:`.management.ViewIndexManager` for this bucket.
        """
        return ViewIndexManager(self.connection, self.loop, self.name)


Bucket = AsyncBucket
