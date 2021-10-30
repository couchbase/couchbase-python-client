import asyncio
from typing import Any, Iterable

from couchbase_core import mk_formstr
from couchbase.options import forward_args
from couchbase.management.admin import Admin
from couchbase.exceptions import NotSupportedWrapper
from couchbase.management.collections import (CollectionsErrorHandler, ScopeSpec,
                                              GetAllScopesOptions, CreateScopeOptions,
                                              DropScopeOptions, DropCollectionOptions,
                                              CollectionSpec, CreateCollectionOptions)


class ACollectionManager(object):
    _HANDLE_ERRORS_ASYNC = True

    def __init__(self,  # type: "ACollectionManager"
                 admin_bucket,  # type: Admin
                 bucket_name  # type: str
                 ):
        self._admin_bucket = admin_bucket
        self._base_path = "pools/default/buckets/{}/scopes".format(bucket_name)

    @CollectionsErrorHandler.mgmt_exc_wrap_async
    def create_scope(self,            # type: "ACollectionManager"
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
        kwargs.update({'path': self._base_path,
                       'method': 'POST',
                       'content_type': 'application/x-www-form-urlencoded',
                       'content': form})

        result = self._admin_bucket.http_request(
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    @CollectionsErrorHandler.mgmt_exc_wrap_async
    def drop_scope(self,            # type: "ACollectionManager"
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

        kwargs.update({'path': '{}/{}'.format(self._base_path, scope_name),
                       'method': 'DELETE'})
        result = self._admin_bucket.http_request(
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    @NotSupportedWrapper.a_400_or_404_means_not_supported_async
    def get_all_scopes(self,            # type: "ACollectionManager"
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
        kwargs.update({'path': self._base_path,
                       'method': 'GET'})
        result = self._admin_bucket.http_request(
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(response):
            # now lets turn the response into a list of ScopeSpec...
            # the response looks like:
            # {'uid': '0', 'scopes': [{'name': '_default', 'uid': '0', 'collections': [{'name': '_default', 'uid': '0'}]}]}
            retval = list()
            for s in response.value['scopes']:
                scope = ScopeSpec(s['name'], list())
                for c in s['collections']:
                    scope.collections.append(
                        CollectionSpec(c['name'], scope.name))
                retval.append(scope)
            ft.set_result(retval)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    @CollectionsErrorHandler.mgmt_exc_wrap_async
    def create_collection(self,           # type: "ACollectionManager"
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
        kwargs.update({'path': '{}/{}/collections'.format(self._base_path, collection.scope_name),
                       'method': 'POST',
                       'content_type': 'application/x-www-form-urlencoded',
                       'content': form})
        result = self._admin_bucket.http_request(
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(response):
            ft.set_result(response)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    @CollectionsErrorHandler.mgmt_exc_wrap_async
    def drop_collection(self,           # type: "ACollectionManager"
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
        kwargs.update({'path': '{}/{}/collections/{}'.format(self._base_path, collection.scope_name, collection.name),
                       'method': 'DELETE'})
        result = self._admin_bucket.http_request(
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(response):
            ft.set_result(response)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft
