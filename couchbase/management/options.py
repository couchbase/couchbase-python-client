#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import (TYPE_CHECKING,
                    Optional,
                    overload)

from couchbase.management.logic.analytics_logic import AnalyticsLinkType

if TYPE_CHECKING:
    from datetime import timedelta


class CreateBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s create bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class UpdateBucketOptions(dict):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s update bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropBucketOptions(dict):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s update bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetAllBucketOptions(dict):
    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetBucketOptions(dict):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s get bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class FlushBucketOptions(dict):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s flush bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)

# Collection Management API


class GetAllScopesOptions(dict):
    """Available options to for a :class:`~couchbase.management.collections.CollectionManager`'s get all
    scopes operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class CreateCollectionOptions(dict):
    """Available options to for a :class:`~couchbase.management.collections.CollectionManager`'s create collection
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropCollectionOptions(dict):
    """Available options to for a :class:`~couchbase.management.collections.CollectionManager`'s drop collection
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class CreateScopeOptions(dict):
    """Available options to for a :class:`~couchbase.management.collections.CollectionManager`'s create scope
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: timedelta
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropScopeOptions(dict):
    """Available options to for a :class:`~couchbase.management.collections.CollectionManager`'s drop scope
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

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
    """
    Base class User Management API options
    """

    def __init__(self,
                 domain_name="local",     # type: Optional[str]
                 timeout=None            # type: Optional[timedelta]
                 ):

        kwargs = {"domain_name": domain_name}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetUserOptions(UserOptions):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s get user
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str, optional): The user's domain name (either ``local`` or ``external``). Defaults
            to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllUsersOptions(UserOptions):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s get all users
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str, optional): The user's domain name (either ``local`` or ``external``). Defaults
            to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """


class UpsertUserOptions(UserOptions):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s upsert user
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str, optional): The user's domain name (either ``local`` or ``external``). Defaults
            to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """


class ChangePasswordOptions(UserOptions):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s change password user
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str, optional): The user's domain name (either ``local`` or ``external``). Defaults
            to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """


class DropUserOptions(UserOptions):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s drop user
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str, optional): The user's domain name (either ``local`` or ``external``). Defaults
            to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """


class GetRolesOptions(dict):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s get roles
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class DropGroupOptions(dict):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s drop group
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetGroupOptions(dict):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s get group
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class GetAllGroupsOptions(dict):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s get all
    groups operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


class UpsertGroupOptions(dict):
    """Available options to for a :class:`~couchbase.management.users.UserManager`'s upsert group
    operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

    def __init__(
        self,
        timeout=None  # type: Optional[timedelta]
    ):
        kwargs = {}
        if timeout:
            kwargs["timeout"] = timeout

        super().__init__(**kwargs)


# Query Index Management

class GetAllQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s get
    all indexes operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the indexes.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the indexes.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """  # noqa: E501
    @overload
    def __init__(self,
                 timeout=None,          # type: Optional[timedelta]
                 scope_name=None,       # type: Optional[str]
                 collection_name=None   # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreateQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s create
    index operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the index.
        condition (str, optional): Specifies the 'where' condition for partial index creation.
        deferred (bool, optional): Specifies whether this index creation should be deferred until
            a later point in time when they can be explicitly built together.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the
            index already existing when determining whether the call was successful.
        num_replicas (int, optional): The number of replicas of this index that should be created.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """  # noqa: E501
    @overload
    def __init__(self,
                 timeout=None,          # type: Optional[timedelta]
                 ignore_if_exists=None,  # type: Optional[bool]
                 num_replicas=None,     # type: Optional[int]
                 deferred=None,         # type: Optional[bool]
                 condition=None,        # type: Optional[str]
                 scope_name=None,       # type: Optional[str]
                 collection_name=None   # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        if 'ignore_if_exists' not in kwargs:
            kwargs['ignore_if_exists'] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class CreatePrimaryQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s create
    primary index operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the index.
        condition (str, optional): Specifies the 'where' condition for partial index creation.
        deferred (bool, optional): Specifies whether this index creation should be deferred until
            a later point in time when they can be explicitly built together.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the
            index already existing when determining whether the call was successful.
        index_name (str, optional): Specifies the name of the primary index.
        num_replicas (int, optional): The number of replicas of this index that should be created.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """  # noqa: E501
    @overload
    def __init__(self,
                 index_name=None,        # type: Optional[str]
                 timeout=None,           # type: Optional[timedelta]
                 ignore_if_exists=None,  # type: Optional[bool]
                 num_replicas=None,      # type: Optional[int]
                 deferred=None,          # type: Optional[bool]
                 condition=None,         # type: Optional[str]
                 scope_name=None,        # type: Optional[str]
                 collection_name=None    # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        if 'ignore_if_exists' not in kwargs:
            kwargs['ignore_if_exists'] = False
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DropQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s drop
    index operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the index.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the
            index not existing when determining whether the call was successful.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """  # noqa: E501
    @overload
    def __init__(self,
                 ignore_if_not_exists=None,   # type: Optional[bool]
                 timeout=None,                # type: Optional[timedelta]
                 scope_name=None,             # type: Optional[str]
                 collection_name=None         # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(DropQueryIndexOptions, self).__init__(**kwargs)


class DropPrimaryQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s drop
    primary index operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional): ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the index.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the
            index not existing when determining whether the call was successful.
        index_name (str, optional): Specifies the name of the primary index.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """  # noqa: E501
    @overload
    def __init__(self,
                 index_name=None,            # type: Optional[str]
                 ignore_if_not_exists=None,  # type: Optional[bool]
                 timeout=None,               # type: Optional[timedelta]
                 scope_name=None,            # type: Optional[str]
                 collection_name=None        # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super(DropPrimaryQueryIndexOptions, self).__init__(**kwargs)


class WatchQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s watch
    indexes operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the index.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
        watch_primary (bool, optional): Specifies whether the primary indexes should
            be watched as well.
    """  # noqa: E501
    @overload
    def __init__(self,
                 watch_primary=None,      # type: Optional[bool]
                 timeout=None,            # type: Optional[timedelta]
                 scope_name=None,         # type: Optional[str]
                 collection_name=None     # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class BuildDeferredQueryIndexOptions(dict):
    """Available options to for a :class:`~couchbase.management.queries.QueryIndexManager`'s build
    deferred indexes operation.

    .. note::
        All management options should be imported from `couchbase.management.options`.

    Args:
        collection_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the collection of the index.
        scope_name (str, optional):  ** DEPRECATED ** - use `~couchbase.management.queries.CollectionQueryIndexManager`.
            Specifies the scope of the index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """  # noqa: E501
    @overload
    def __init__(self,
                 timeout=None,          # type: Optional[timedelta]
                 scope_name=None,       # type: Optional[str]
                 collection_name=None   # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
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
