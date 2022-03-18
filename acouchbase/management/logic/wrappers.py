from __future__ import annotations

from functools import wraps

from acouchbase.logic import build_exception, call_async_fn
from couchbase.exceptions import MissingConnectionException
from couchbase.management.logic import (analyze_search_index_document,
                                        get_all_analytics_indexes,
                                        get_all_bucket_settings,
                                        get_all_datasets,
                                        get_all_design_documents,
                                        get_all_eventing_functions,
                                        get_all_groups,
                                        get_all_query_indexes,
                                        get_all_scopes,
                                        get_all_search_index_stats,
                                        get_all_search_indexes,
                                        get_all_users,
                                        get_bucket_settings,
                                        get_design_document,
                                        get_eventing_function,
                                        get_eventing_functions_status,
                                        get_group,
                                        get_links,
                                        get_roles,
                                        get_search_index,
                                        get_search_index_stats,
                                        get_user)


class BucketMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_bucket':
                            retval = get_bucket_settings(res, return_cls)
                        elif fn.__name__ == 'get_all_buckets':
                            retval = get_all_bucket_settings(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    print(f'err context: {exc.error_context()}')
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class CollectionMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_all_scopes':
                            retval = get_all_scopes(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class UserMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_user':
                            retval = get_user(res, return_cls)
                        elif fn.__name__ == 'get_all_users':
                            retval = get_all_users(res, return_cls)
                        elif fn.__name__ == 'get_roles':
                            retval = get_roles(res, return_cls)
                        elif fn.__name__ == 'get_group':
                            retval = get_group(res, return_cls)
                        elif fn.__name__ == 'get_all_groups':
                            retval = get_all_groups(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class AnalyticsMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_all_datasets':
                            retval = get_all_datasets(res, return_cls)
                        elif fn.__name__ == 'get_all_indexes':
                            retval = get_all_analytics_indexes(res, return_cls)
                        elif fn.__name__ == 'get_pending_mutations':
                            retval = res.raw_result.get('stats', None)
                        elif fn.__name__ == 'get_links':
                            retval = get_links(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class QueryIndexMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_all_indexes':
                            retval = get_all_query_indexes(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class SearchIndexMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_index':
                            retval = get_search_index(res, return_cls)
                        elif fn.__name__ == 'get_all_indexes':
                            retval = get_all_search_indexes(res, return_cls)
                        elif fn.__name__ == 'get_indexed_documents_count':
                            retval = res.raw_result.get('count', 0)
                        elif fn.__name__ == 'analyze_document':
                            retval = analyze_search_index_document(res)
                        elif fn.__name__ == 'get_index_stats':
                            retval = get_search_index_stats(res)
                        elif fn.__name__ == 'get_all_index_stats':
                            retval = get_all_search_index_stats(res)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class ViewIndexMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_design_document':
                            retval = get_design_document(res, return_cls)
                        elif fn.__name__ == 'get_all_design_documents':
                            retval = get_all_design_documents(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator


class EventingFunctionMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, error_map):   # noqa: C901

        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = res
                    else:
                        if fn.__name__ == 'get_function':
                            retval = get_eventing_function(res, return_cls)
                        elif fn.__name__ == 'get_all_functions':
                            retval = get_all_eventing_functions(res, return_cls)
                        elif fn.__name__ == 'functions_status':
                            retval = get_eventing_functions_status(res, return_cls)
                        else:
                            retval = return_cls(res)
                    print(f'{fn.__name__} returning {retval}')

                    self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    print(f'error: {exc}, {exc_info}, {error_msg}')
                    err_msg = error_msg
                    if exc_info and not err_msg:
                        err_msg = exc_info.get('error_msg', None)
                    excptn = build_exception(exc, exc_info=exc_info, error_map=error_map, error_msg=err_msg)
                    self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    print(f'calling {fn.__name__}')
                    call_async_fn(ft, self, fn, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator
