from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable)

from acouchbase.management.logic import CollectionMgmtWrapper
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
        super().__init__(connection, bucket_name)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @CollectionMgmtWrapper.inject_callbacks(None, CollectionManagerLogic._ERROR_MAPPING)
    def create_scope(self,
                     scope_name,      # type: str
                     *options,        # type: CreateScopeOptions
                     **kwargs         # type: Dict[str, Any]
                     ) -> Awaitable[None]:
        super().create_scope(scope_name, *options, **kwargs)

    @CollectionMgmtWrapper.inject_callbacks(None, CollectionManagerLogic._ERROR_MAPPING)
    def drop_scope(self,
                   scope_name,      # type: str
                   *options,        # type: DropScopeOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> Awaitable[None]:
        super().drop_scope(scope_name, *options, **kwargs)

    @CollectionMgmtWrapper.inject_callbacks((ScopeSpec, CollectionSpec), CollectionManagerLogic._ERROR_MAPPING)
    def get_all_scopes(self,
                       *options,        # type: GetAllScopesOptions
                       **kwargs         # type: Dict[str, Any]
                       ) -> Awaitable[Iterable[ScopeSpec]]:
        super().get_all_scopes(*options, **kwargs)

    @CollectionMgmtWrapper.inject_callbacks(None, CollectionManagerLogic._ERROR_MAPPING)
    def create_collection(self,
                          collection,     # type: CollectionSpec
                          *options,       # type: CreateCollectionOptions
                          **kwargs        # type: Dict[str, Any]
                          ) -> Awaitable[None]:
        super().create_collection(collection, *options, **kwargs)

    @CollectionMgmtWrapper.inject_callbacks(None, CollectionManagerLogic._ERROR_MAPPING)
    def drop_collection(self,
                        collection,     # type: CollectionSpec
                        *options,       # type: DropCollectionOptions
                        **kwargs        # type: Dict[str, Any]
                        ) -> Awaitable[None]:
        super().drop_collection(collection, *options, **kwargs)
