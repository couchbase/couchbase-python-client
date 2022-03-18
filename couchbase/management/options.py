from typing import (TYPE_CHECKING,
                    Optional,
                    overload)

from couchbase.management.logic.analytics_logic import AnalyticsLinkType

if TYPE_CHECKING:
    from datetime import timedelta


class CreateBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class UpdateBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetAllBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class FlushBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)

# Collection Management API


class GetAllScopesOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class CreateCollectionOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropCollectionOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class CreateScopeOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropScopeOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)

# User Management API


class UserOptions(dict):

    def __init__(self,
                 domain_name="local",     # type: str
                 timeout=None            # type: timedelta
                 ):
        """
        Base class for options used w/in the User Management API

        :param str domain_name: name of the user domain (local | external). Defaults to local.
        :param timedelta timeout: the time allowed for the operation to be terminated. This is controlled by the client.
        """
        kwargs = {"domain_name": domain_name}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetUserOptions(UserOptions):
    def __init__(self, **kwargs):
        """Get User Options
        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllUsersOptions(UserOptions):
    pass


class UpsertUserOptions(UserOptions):
    pass


class DropUserOptions(UserOptions):
    pass


class GetRolesOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropGroupOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetGroupOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetAllGroupsOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class UpsertGroupOptions(dict):
    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


# Query Index Management

class GetAllQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 scope_name=None,       # type: str
                 collection_name=None   # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Get all query indexes options

        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreateQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 ignore_if_exists=None,  # type: bool
                 num_replicas=None,     # type: int
                 deferred=None,         # type: bool
                 condition=None,        # type: str
                 scope_name=None,       # type: str
                 collection_name=None   # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Query Index creation options

        :param timeout: operation timeout in seconds
        :param ignore_if_exists: don't throw an exception if index already exists
        :param num_replicas: number of replicas
        :param deferred: whether the index creation should be deferred
        :param condition: 'where' condition for partial index creation
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        if 'ignore_if_exists' not in kwargs:
            kwargs['ignore_if_exists'] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreatePrimaryQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 index_name=None,        # type: str
                 timeout=None,           # type: timedelta
                 ignore_if_exists=None,  # type: bool
                 num_replicas=None,      # type: int
                 deferred=None,          # type: bool
                 condition=None,         # type: str
                 scope_name=None,        # type: str
                 collection_name=None    # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Query Primary Index creation options

        :param index_name: name of primary index
        :param timeout: operation timeout in seconds
        :param ignore_if_exists: don't throw an exception if index already exists
        :param num_replicas: number of replicas
        :param deferred: whether the index creation should be deferred
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        if 'ignore_if_exists' not in kwargs:
            kwargs['ignore_if_exists'] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 ignore_if_not_exists=None,   # type: bool
                 timeout=None,                # type: timedelta
                 scope_name=None,             # type: str
                 collection_name=None         # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Drop query index options

        :param ignore_if_exists: don't throw an exception if index already exists
        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(DropQueryIndexOptions, self).__init__(**kwargs)


class DropPrimaryQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 index_name=None,            # str
                 ignore_if_not_exists=None,  # type: bool
                 timeout=None,               # type: timedelta
                 scope_name=None,            # type: str
                 collection_name=None        # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Drop primary index options

        :param index_name: name of primary index
        :param timeout: operation timeout in seconds
        :param ignore_if_exists: don't throw an exception if index already exists
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(DropPrimaryQueryIndexOptions, self).__init__(**kwargs)


class WatchQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 watch_primary=None,      # type: bool
                 timeout=None,            # type: timedelta
                 scope_name=None,         # type: str
                 collection_name=None     # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Watch query index options

        :param watch_primary: If True, watch primary indexes
        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class BuildDeferredQueryIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 scope_name=None,       # type: str
                 collection_name=None   # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        """
        Build deferred query indexes options

        :param timeout: operation timeout in seconds
        :param scope_name:
            **UNCOMMITTED**
            scope_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Nme of the scope where the index belongs
        :param collection_name:
            **UNCOMMITTED**
            collection_name is an uncommitted API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

            Name of the collection where the index belongs

        """
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(BuildDeferredQueryIndexOptions, self).__init__(**kwargs)

# Analytics Management Options


class CreateDataverseOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,              # type: timedelta
                 ignore_if_exists=False     # type: bool
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def ignore_if_exists(self):
        return self.get('ignore_if_exists', False)


class DropDataverseOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,                  # type: timedelta
                 ignore_if_not_exists=False     # type: bool
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def ignore_if_not_exists(self):
        return self.get('ignore_if_not_exists', False)


class CreateDatasetOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,                  # type: Optional[timedelta]
                 ignore_if_exists=None,        # type: Optional[bool]
                 condition=None,                # type: Optional[str]
                 dataverse_name=None      # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def ignore_if_exists(self) -> bool:
        return self.get('ignore_if_exists', False)

    @property
    def condition(self) -> Optional[str]:
        return self.get('condition', None)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')


class DropDatasetOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,                  # type: Optional[timedelta]
                 ignore_if_not_exists=None,    # type: Optional[bool]
                 dataverse_name=None,           # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def ignore_if_not_exists(self) -> bool:
        return self.get('ignore_if_not_exists', False)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')


class GetAllDatasetOptions(dict):
    @overload
    def __init__(self,
                 timeout=None                  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreateAnalyticsIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,              # type: Optional[timedelta]
                 ignore_if_exists=None,    # type: Optional[bool]
                 dataverse_name=None,  # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def ignore_if_exists(self) -> bool:
        return self.get('ignore_if_exists', False)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')


class DropAnalyticsIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,                  # type: Optional[timedelta]
                 ignore_if_not_exists=None,    # type: Optional[bool]
                 dataverse_name=None       # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')

    @property
    def ignore_if_not_exists(self) -> bool:
        return self.get('ignore_if_not_exists', False)


class GetAllAnalyticsIndexesOptions(dict):
    @overload
    def __init__(self,
                 timeout=None                  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ConnectLinkOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,              # type: Optional[timedelta]
                 dataverse_name=None,  # type: Optional[str]
                 link_name=None,         # type: Optional[str]
                 force=None                # type: Optional[bool]
                 ):
        pass

    def __init__(self, **kwargs):
        super(ConnectLinkOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')

    @property
    def link_name(self) -> str:
        return self.get('link_name', 'Local')

    @property
    def force(self) -> bool:
        return self.get('force', False)


class DisconnectLinkOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,              # type: Optional[timedelta]
                 dataverse_name=None,  # type: Optional[str]
                 link_name=None         # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        super(DisconnectLinkOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')

    @property
    def link_name(self) -> str:
        return self.get('link_name', 'Local')


class GetPendingMutationsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None                  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreateLinkAnalyticsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None                  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ReplaceLinkAnalyticsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None                  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropLinkAnalyticsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None                  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetLinksAnalyticsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None,          # type: Optional[timedelta]
                 dataverse_name=None,   # type: Optional[str]
                 name=None,             # type: Optional[str]
                 link_type=None,        # type: Optional[AnalyticsLinkType]
                 ):
        pass

    def __init__(self, **kwargs):
        super(GetLinksAnalyticsOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self) -> str:
        return self.get('dataverse_name', 'Default')

    @property
    def name(self) -> Optional[str]:
        return self.get('name', None)

    @property
    def link_type(self) -> Optional[AnalyticsLinkType]:
        return self.get('link_type', None)

# Search Index Management Options


class UpsertSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllSearchIndexesOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetSearchIndexedDocumentsCountOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PauseIngestSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ResumeIngestSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class AllowQueryingSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DisallowQueryingSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class FreezePlanSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UnfreezePlanSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class AnalyzeDocumentSearchIndexOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetSearchIndexStatsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllSearchIndexStatsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None           # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

# Views


class GetDesignDocumentOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllDesignDocumentsOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UpsertDesignDocumentOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropDesignDocumentOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PublishDesignDocumentOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


# Eventing Function Management API

class EventingFunctionOptions(dict):
    def __init__(self, **kwargs):
        """
        EventingFunctionOptions
        Various options for eventing function management API

        :param timeout:
            Uses this timeout value, rather than the default for the cluster.
        :type timeout: timedelta
        """
        super(EventingFunctionOptions, self).__init__(**kwargs)


class UpsertFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DeployFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PauseFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ResumeFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UndeployFunctionOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class FunctionsStatusOptions(dict):
    @overload
    def __init__(self,
                 timeout=None    # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)
