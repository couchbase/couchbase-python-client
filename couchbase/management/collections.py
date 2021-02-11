from couchbase.options import OptionBlockTimeOut, forward_args
from couchbase.management.admin import Admin
from typing import *
from .generic import GenericManager
from couchbase_core import mk_formstr
from couchbase_core.supportability import deprecated

from couchbase.exceptions import ErrorMapper, NotSupportedWrapper, HTTPException, ScopeNotFoundException, \
    ScopeAlreadyExistsException, CollectionNotFoundException, CollectionAlreadyExistsException
from datetime import timedelta


class CollectionsErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...)->Mapping[str, CBErrorType]
        return {HTTPException: {'.*Scope with.*name.*already exists': ScopeAlreadyExistsException,
                                '.*Scope with.*name.*not found': ScopeNotFoundException,
                                '.*Collection with.*name.*not found': CollectionNotFoundException,
                                '.*Collection with.*name.*already exists': CollectionAlreadyExistsException,
                                '.*collection_not_found.*': CollectionNotFoundException,
                                '.*scope_not_found.*': ScopeNotFoundException}}


class CollectionManager(GenericManager):
    def __init__(self,  # type: CollectionManager
                 admin_bucket,  # type: Admin
                 bucket_name  # type: str
                 ):
        super(CollectionManager, self).__init__(admin_bucket)
        self.base_path = 'pools/default/buckets/{}/scopes'.format(bucket_name)

    @deprecated(instead="get_all_scopes")
    def get_scope(self,           # type: CollectionManager
                  scope_name,     # type: str
                  *options,       # type: GetScopeOptions
                  **kwargs        # type: Any
                  ):
        # type: (...) -> ScopeSpec
        """
        Gets a scope. This will fetch a manifest and then pull the scope out of it.

        :param str scope_name: name of the scope.
        :param GetScopeOptions options: scope options
        :param GetScopeOptions kwargs: overrides for scope options
        :return: a ScopeSpec containing information about the specified scope.
        :raises: ScopeNotFoundException
        """
        try:
          return next(s for s in self.get_all_scopes(*options, **kwargs) if s.name == scope_name)
        except StopIteration:
          raise ScopeNotFoundException("no scope with name {}".format(scope_name))


    @NotSupportedWrapper.a_400_or_404_means_not_supported
    def get_all_scopes(self,            # type: CollectionManager
                       *options,        # type: GetAllScopesOptions
                       **kwargs         # type: Any
                       ):
        # type: (...) -> Iterable[ScopeSpec]
        """
        Gets all scopes. This will fetch a manifest and then pull the scopes out of it.

        :param GetAllScopesOptions options: (currently just timeout).
        :param kwargs: keyword version of options
        :return: An Iterable[ScopeSpec] containing all scopes in the associated bucket.
        """
        kwargs.update({ 'path':   self.base_path,
                        'method': 'GET' })
        response = self._admin_bucket.http_request(**forward_args(kwargs, *options))
        # now lets turn the response into a list of ScopeSpec...
        # the response looks like:
        # {'uid': '0', 'scopes': [{'name': '_default', 'uid': '0', 'collections': [{'name': '_default', 'uid': '0'}]}]}
        retval = list()
        for s in response.value['scopes']:
            scope = ScopeSpec(s['name'], list())
            for c in s['collections']:
                scope.collections.append(CollectionSpec(c['name'], scope.name))
            retval.append(scope)
        return retval

    @CollectionsErrorHandler.mgmt_exc_wrap
    def create_collection(self,           # type: CollectionManager
                          collection,     # type: CollectionSpec
                          *options,       # type: CreateCollectionOptions
                          **kwargs        # type: Any
                          ):
        """
        Creates a new collection.

        :param CollectionSpec collection: specification of the collection.
        :param CreateCollectionOptions options:  options (currently just timeout).
        :param kwargs: keyword version of 'options'
        :return:
        :raises: InvalidArgumentsException
        :raises: CollectionAlreadyExistsException
        :raises: ScopeNotFoundException
        """

        params = {
            'name': collection.name
        }
        if collection.max_ttl:
            params['maxTTL'] = int(collection.max_ttl.total_seconds())

        form = mk_formstr(params)
        kwargs.update({'path': '{}/{}/collections'.format(self.base_path, collection.scope_name),
                       'method': 'POST',
                       'content_type': 'application/x-www-form-urlencoded',
                       'content': form})
        return self._admin_bucket.http_request(**forward_args(kwargs, *options))

    @CollectionsErrorHandler.mgmt_exc_wrap
    def drop_collection(self,           # type: CollectionManager
                        collection,     # type: CollectionSpec
                        *options,       # type: DropCollectionOptions
                        **kwargs        # type: Any
                        ):
        # type: (...) -> None
        """
        Removes a collection.

        :param CollectionSpec collection: namspece of the collection.
        :param DropCollectionOptions options: (currently just timeout).
        :param kwargs: keyword version of `options`
        :raises: CollectionNotFoundException
        """
        kwargs.update({ 'path': '{}/{}/collections/{}'.format(self.base_path, collection.scope_name, collection.name),
                        'method': 'DELETE'})
        self._admin_bucket.http_request(**forward_args(kwargs, *options))

    @CollectionsErrorHandler.mgmt_exc_wrap
    def create_scope(self,            # type: CollectionManager
                     scope_name,      # type: str
                     *options,        # type: CreateScopeOptions
                     **kwargs         # type: Any
                     ):
        # type: (...) -> None
        """
        Creates a new scope.

        :param str scope_name: name of the scope.
        :param CreateScopeOptions options: options (currently just timeout).
        :param kwargs: keyword version of `options`
        :return:

        :raises: InvalidArgumentsException
        Any exceptions raised by the underlying platform
        Uri
        POST http://localhost:8091/pools/default/buckets/<bucket>/collections -d name=<scope_name>
        """
        params = {
            'name': scope_name
        }

        form = mk_formstr(params)
        kwargs.update({'path': self.base_path,
                       'method': 'POST',
                       'content_type': 'application/x-www-form-urlencoded',
                       'content': form})

        self._admin_bucket.http_request(**forward_args(kwargs, *options))

    @CollectionsErrorHandler.mgmt_exc_wrap
    def drop_scope(self,            # type: CollectionManager
                   scope_name,      # type: str
                   *options,        # type: DropScopeOptions
                   **kwargs         # type: Any
                   ):
        """
        Removes a scope.

        :param str scope_name: name of the scope
        :param DropScopeOptions options: (currently just timeout)
        :param kwargs: keyword version of `options`

        :raises: ScopeNotFoundException
        """

        kwargs.update({ 'path': '{}/{}'.format(self.base_path, scope_name),
                        'method': 'DELETE'})
        self._admin_bucket.http_request(**forward_args(kwargs, *options))


class CollectionSpec(object):
    def __init__(self,
                 collection_name,           # type: str
                 scope_name='_default',     # type: str
                 max_ttl=None               # type: timedelta
                 ):
        self._name, self._scope_name = collection_name, scope_name
        self._max_ttl = max_ttl

    @property
    def name(self):
        # type: (...) -> str
        return self._name

    @property
    def scope_name(self):
        # type: (...) -> str
        return self._scope_name

    @property
    def max_ttl(self):
        # type: (...) -> timedelta
        return self._max_ttl


class ScopeSpec(object):
    def __init__(self,
                 name,  # type : str
                 collections,  # type: Iterable[CollectionSpec]
                 ):
        self._name, self._collections = name, collections

    @property
    def name(self):
        # type: (...) -> str
        return self._name

    @property
    def collections(self):
        # type: (...) -> Iterable[CollectionSpec]
        return self._collections


class GetAllScopesOptions(OptionBlockTimeOut):
    pass


class GetScopeOptions(GetAllScopesOptions):
    pass


class CreateCollectionOptions(OptionBlockTimeOut):
    pass


class DropCollectionOptions(OptionBlockTimeOut):
    pass


class CreateScopeOptions(OptionBlockTimeOut):
    pass


class DropScopeOptions(OptionBlockTimeOut):
    pass
