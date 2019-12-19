from ..options import OptionBlock
from couchbase.management.admin import Admin
from typing import *
from .generic import GenericManager
from couchbase_core import mk_formstr

from couchbase_core.exceptions import ErrorMapper, NotSupportedWrapper, HTTPError
from couchbase.exceptions import ScopeNotFoundException, ScopeAlreadyExistsException, CollectionNotFoundException, CollectionAlreadyExistsException

class CollectionsErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...)->Mapping[str, CBErrorType]
        return {HTTPError: {'Scope with this name already exists': ScopeAlreadyExistsException,
                            'Scope with this name is not found': ScopeNotFoundException,
                            'Collection with this name is not found': CollectionNotFoundException,
                            'Collection with this name already exists': CollectionAlreadyExistsException}}

class CollectionManager(GenericManager):
    def __init__(self,  # type: CollectionManager
                 admin_bucket,  # type: Admin
                 bucket_name  # type: str
                 ):
        super(CollectionManager, self).__init__(admin_bucket)
        self.bucket_name = bucket_name

    def get_scope(self,  # type: CollectionManager
                  scope_name,  # type: str
                  *options  # type: GetScopeOptions
                  ):
        # type: (...) -> ScopeSpec
        """
        Get Scope
        Gets a scope. This will fetch a manifest and then pull the scope out of it.
        Signature
        ScopeSpec GetScope(string scope_name,  [options])
        Parameters
        Required:
        scope_name: string - name of the scope.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        ScopeNotFoundException
        Any exceptions raised by the underlying platform
        Uri
        GET /pools/default/buckets/<bucket>/collections
        """
        try:
          return next(s for s in self.get_all_scopes() if s.name == scope_name)
        except StopIteration:
          raise ScopeNotFoundException("no scope with name {}".format(scope_name))


    @NotSupportedWrapper.a_404_means_not_supported
    def get_all_scopes(self):
        # type: (...) -> Iterable[ScopeSpec]
        """Get All Scopes
        Gets all scopes. This will fetch a manifest and then pull the scopes out of it.
        Signature
        iterable<ScopeSpec> GetAllScopes([options])
        Parameters
        Required:
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        Any exceptions raised by the underlying platform
        Uri
        GET /pools/default/buckets/<bucket>/collections"""
        path = "/pools/default/buckets/{}/collections".format(self.bucket_name)
        response = self._admin_bucket.http_request(path=path, method='GET')
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
    def create_collection(self,  # type: CollectionManager
                          collection,  # type: CollectionSpec
                          *options  # type: CreateCollectionOptions
                          ):
        """
        Create Collection
        Creates a new collection.
        Signature
        void CreateCollection(CollectionSpec collection, [options])
        Parameters
        Required:
        collection: CollectionSpec - specification of the collection.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        InvalidArgumentsException
        CollectionAlreadyExistsException
        ScopeNotFoundException
        Any exceptions raised by the underlying platform
        Uri
        POST http://localhost:8091/pools/default/buckets/<bucket>/collections/<scope_name> -d name=<collection_name>
        """
        path = "pools/default/buckets/{}/collections/{}".format(self.bucket_name, collection.scope_name)

        params = {
            'name': collection.name
        }

        form = mk_formstr(params)
        return self._admin_bucket.http_request(path=path,
                                               method='POST',
                                               content_type='application/x-www-form-urlencoded',
                                               content=form)

    @CollectionsErrorHandler.mgmt_exc_wrap
    def drop_collection(self,  # type: CollectionManager
                        collection,  # type: CollectionSpec
                        *options # type: DropCollectionOptions
                        ):
        """Drop Collection
        Removes a collection.
        Signature
        void DropCollection(CollectionSpec collection, [options])
        Parameters
        Required:
        collection: CollectionSpec - namspece of the collection.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        CollectionNotFoundException
        Any exceptions raised by the underlying platform
        Uri
        DELETE http://localhost:8091/pools/default/buckets/<bucket>/collections/<scope_name>/<collection_name>
        """
        path = "pools/default/buckets/{}/collections/{}/{}".format(self.bucket_name, collection.scope_name, collection.name)
        self._admin_bucket.http_request(path, "DELETE")

    @CollectionsErrorHandler.mgmt_exc_wrap
    def create_scope(self,  # type: CollectionManager
                     scope_name,  # type: str
                     *options  # type: CreateScopeOptions
                     ):
        """Create Scope
        Creates a new scope.
        Signature
        Void CreateScope(string scope_name, [options])
        Parameters
        Required:
        scope_name: String - name of the scope.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        InvalidArgumentsException
        Any exceptions raised by the underlying platform
        Uri
        POST http://localhost:8091/pools/default/buckets/<bucket>/collections -d name=<scope_name>
        """
        path = "pools/default/buckets/{}/collections".format(self.bucket_name)

        params = {
            'name': scope_name
        }

        form = mk_formstr(params)
        self._admin_bucket.http_request(path=path,
                                        method='POST',
                                        content_type='application/x-www-form-urlencoded',
                                        content=form)

    @CollectionsErrorHandler.mgmt_exc_wrap
    def drop_scope(self,  # type: CollectionManager
                   scope_name,  # type: str
                   *options  # type: DropScopeOptions
                   ):
        """
        Drop Scope
        Removes a scope.
        Signature
        void DropScope(string scope_name, [options])
        Parameters
        Required:
        collectionName: string - name of the collection.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        ScopeNotFoundException
        Any exceptions raised by the underlying platform
        Uri
        DELETE http://localhost:8091/pools/default/buckets/<bucket>/collections/<scope_name>
        """
        path = "pools/default/buckets/{}/collections/{}".format(self.bucket_name, scope_name)
        self._admin_bucket.http_request(path=path, method='DELETE')

class CollectionSpec(object):
    def __init__(self,
                 collection_name,  # type: str
                 scope_name = '_default'  # type: str
                 ):
        self._name, self._scope_name=collection_name,scope_name
    @property
    def name(self):
        # type: (...) -> str
        return self._name

    @property
    def scope_name(self):
        # type: (...) -> str
        return self._scope_name


class ScopeSpec(object):
    def __init__(self,
                 name, # type : str
                 collections, # type: Iterable[CollectionSpec]
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


class InsertCollectionOptions(OptionBlock):
    pass


class InsertScopeOptions(OptionBlock):
    pass


class GetScopeOptions(object):
    pass


class GetAllScopesOptions(object):
    pass


class CreateCollectionOptions(object):
    pass


class DropCollectionOptions(object):
    pass


class CreateScopeOptions(object):
    pass


class DropScopeOptions(object):
    pass

