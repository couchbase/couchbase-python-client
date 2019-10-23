from ..options import OptionBlock
from couchbase.management.admin import Admin
from typing import *
from .generic import GenericManager
from couchbase_core import mk_formstr


class CollectionManager(GenericManager):
    def __init__(self,  # type: CollectionManager
                 admin_bucket,  # type: Admin
                 bucket_name  # type: str
                 ):
        super(CollectionManager, self).__init__(admin_bucket)
        self.bucket_name = bucket_name

    def collection_exists(self,  # type: CollectionManager
                          collection,  # type: ICollectionSpec
                          *options  # type: CollectionExistsOptions
                          ):
        # type: (...)->bool
        """
        Checks for existence of a collection. This will fetch a manifest and then interrogate it to check that the scope name exists and then that the collection name exists within that scope.

        :param ICollectionSpec collection: spec of the collection.
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        :param options:
        :return whether the collection exists.
        :rtype bool
        :raises: InvalidArgumentsException
        Uri
        GET /pools/default/buckets/<bucket>/collections"""
        colls = self.get_all_collections()
        return collection in colls

    def get_all_collections(self):
        return self._admin_bucket.http_request("/pools/default/buckets/{}/collections".format(self.bucket_name), "GET")

    def scope_exists(self,  # type: CollectionManager
                     scope_name,  # type: str
                     *options  # type: ScopeExistsOptions
                     ):
        # type: (...)->bool
        """
        Scope Exists
        Checks for existence of a scope. This will fetch a manifest and then interrogate it to check that the scope name exists.
        Signature
        boolean ScopeExists(String scopeName,  [options])
        Parameters
        Required:
        scopeName: string - name of the scope.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        Any exceptions raised by the underlying platform
        Uri
        GET /pools/default/buckets/<bucket>/collections"""

    def get_scope(self,  # type: CollectionManager
                  scopeName,  # type: str
                  options  # type: GetScopeOptions
                  ):
        # type: (...)->IScopeSpec
        """
        Get Scope
        Gets a scope. This will fetch a manifest and then pull the scope out of it.
        Signature
        IScopeSpec GetScope(string scopeName,  [options])
        Parameters
        Required:
        scopeName: string - name of the scope.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        ScopeNotFoundException
        Any exceptions raised by the underlying platform
        Uri
        GET /pools/default/buckets/<bucket>/collections
        """

    def get_all_scopes(self,  # type: CollectionManager
                       options  # type: GetAllScopesOptions
                       ):
        # type: (...)->Iterable[IScopeSpec]
        """Get All Scopes
        Gets all scopes. This will fetch a manifest and then pull the scopes out of it.
        Signature
        iterable<IScopeSpec> GetAllScopes([options])
        Parameters
        Required:
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        Any exceptions raised by the underlying platform
        Uri
        GET /pools/default/buckets/<bucket>/collections"""

    def create_collection(self,  # type: CollectionManager
                          collection,  # type: ICollectionSpec
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
        path = "pools/default/buckets/default/collections/{}".format(collection.scope_name)

        params = {
            'name': collection.name
        }

        form = mk_formstr(params)
        return self._admin_bucket.http_request(path=path,
                                              method='POST',
                                              content_type='application/x-www-form-urlencoded',
                                              content=form)

    def drop_collection(self,  # type: CollectionManager
                        collection,  # type: ICollectionSpec
                        options  # type: DropCollectionOptions
                        ):
        """Drop Collection
        Removes a collection.
        Signature
        void DropCollection(ICollectionSpec collection, [options])
        Parameters
        Required:
        collection: ICollectionSpec - namspece of the collection.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        CollectionNotFoundException
        Any exceptions raised by the underlying platform
        Uri
        DELETE http://localhost:8091/pools/default/buckets/<bucket>/collections/<scope_name>/<collection_name>
        """

    def create_scope(self,  # type: CollectionManager
                     scope_name,  # type: str
                     *options  # type: CreateScopeOptions
                     ):
        """Create Scope
        Creates a new scope.
        Signature
        Void CreateScope(string scopeName, [options])
        Parameters
        Required:
        scopeName: String - name of the scope.
        Optional:
        Timeout or timeoutMillis (int/duration) - the time allowed for the operation to be terminated. This is controlled by the client.
        Returns
        Throws
        InvalidArgumentsException
        Any exceptions raised by the underlying platform
        Uri
        POST http://localhost:8091/pools/default/buckets/<bucket>/collections -d name=<scope_name>"""

        path = "pools/default/buckets/default/collections"

        params = {
            'name': scope_name
        }

        form = mk_formstr(params)
        self._admin_bucket.http_request(path=path,
                                              method='POST',
                                              content_type='application/x-www-form-urlencoded',
                                              content=form)

    def drop_scope(self,  # type: CollectionManager
                   scopeName,  # type: str
                   options  # type: DropScopeOptions
                   ):
        """
        Drop Scope
        Removes a scope.
        Signature
        void DropScope(string scopeName, [options])
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
        DELETE http://localhost:8091/pools/default/buckets/<bucket>/collections/<scope_name>"""

    def flush_collection(self,  # type: CollectionManager
                         collection,  # type: ICollectionSpec
                         options  # type: FlushCollectionOptions
                         ):
        pass

class ICollectionSpec(object):
    def __init__(self,
                 collection_name,  # type: str
                 scope_name  # type: str
                 ):
        self._name, self._scope_name=collection_name,scope_name
    @property
    def name(self):
        # type: (...)->str
        return self._name

    @property
    def scope_name(self):
        # type: (...)->str
        return self._scope_name


class IScopeSpec(object):
    @property
    def name(self):
        # type: (...)->str
        pass

    @property
    def collections(self):
        # type: (...)->Iterable[ICollectionSpec]
        pass


class InsertCollectionOptions(OptionBlock):
    pass


class InsertScopeOptions(OptionBlock):
    pass


class CollectionExistsOptions(object):
    pass


class ScopeExistsOptions(object):
    pass


class GetScopeOptions(object):
    pass


class GetAllScopesOptions(object):
    pass


class CreateCollectionOptions(object):
    pass


class IScopeSpec(object):
    pass


class DropCollectionOptions(object):
    pass


class CreateScopeOptions(object):
    pass


class DropScopeOptions(object):
    pass


class FlushCollectionOptions(object):
    pass
