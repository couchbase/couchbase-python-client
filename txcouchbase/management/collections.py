from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.management.logic.collections_logic import (CollectionManagerLogic,
                                                          CollectionSpec,
                                                          ScopeSpec)

if TYPE_CHECKING:
    from couchbase.management.options import (CreateCollectionOptions,
                                              CreateScopeOptions,
                                              DropCollectionOptions,
                                              DropScopeOptions,
                                              GetAllScopesOptions)


class CollectionManager(CollectionManagerLogic):

    def __init__(self, connection, loop, bucket_name):
        super().__init__(connection, loop, bucket_name)

    def create_scope(self,
                     scope_name,      # type: str
                     *options,        # type: CreateScopeOptions
                     **kwargs         # type: Dict[str, Any]
                     ) -> Deferred[None]:
        return Deferred.fromFuture(super().create_scope(scope_name, *options, **kwargs))

    def drop_scope(self,
                   scope_name,      # type: str
                   *options,        # type: DropScopeOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> Deferred[None]:
        return Deferred.fromFuture(super().drop_scope(scope_name, *options, **kwargs))

    def get_all_scopes(self,
                       *options,        # type: GetAllScopesOptions
                       **kwargs         # type: Dict[str, Any]
                       ) -> Deferred[Iterable[ScopeSpec]]:
        return Deferred.fromFuture(super().get_all_scopes(*options, **kwargs))

    def create_collection(self,
                          collection,     # type: CollectionSpec
                          *options,       # type: CreateCollectionOptions
                          **kwargs        # type: Dict[str, Any]
                          ) -> Deferred[None]:
        return Deferred.fromFuture(super().create_collection(collection, *options, **kwargs))

    def drop_collection(self,
                        collection,     # type: CollectionSpec
                        *options,       # type: DropCollectionOptions
                        **kwargs        # type: Dict[str, Any]
                        ) -> Deferred[None]:
        return Deferred.fromFuture(super().drop_collection(collection, *options, **kwargs))
