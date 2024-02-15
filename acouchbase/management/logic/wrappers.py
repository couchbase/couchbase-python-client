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

from __future__ import annotations

from functools import wraps

from acouchbase.logic import call_async_fn
from couchbase._utils import Overload, OverloadType
from couchbase.exceptions import ErrorMapper, MissingConnectionException
from couchbase.logic.supportability import Supportability
from couchbase.management.logic import (ManagementType,
                                        handle_analytics_index_mgmt_response,
                                        handle_bucket_mgmt_response,
                                        handle_collection_mgmt_response,
                                        handle_eventing_function_mgmt_response,
                                        handle_query_index_mgmt_response,
                                        handle_search_index_mgmt_response,
                                        handle_user_mgmt_response,
                                        handle_view_index_mgmt_response)


def build_mgmt_exception(exc, mgmt_type, error_map):
    return ErrorMapper.build_exception(exc, mapping=error_map)


mgmt_overload_registry = {}


class AsyncMgmtWrapper:

    @classmethod   # noqa: C901
    def inject_callbacks(cls, return_cls, mgmt_type, error_map, overload_type=None):   # noqa: C901

        def decorator(fn):
            if overload_type is not None:
                mgmt_overload = mgmt_overload_registry.get(fn.__qualname__)
                if mgmt_overload is None:
                    mgmt_overload = mgmt_overload_registry[fn.__qualname__] = Overload(fn.__qualname__)
                if overload_type is OverloadType.DEFAULT:
                    mgmt_overload.register_default(fn)
                else:
                    mgmt_overload.register(fn)

            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(ret):
                    if return_cls is None:
                        retval = None
                    elif return_cls is True:
                        retval = ret
                    else:
                        if mgmt_type == ManagementType.BucketMgmt:
                            retval = handle_bucket_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.CollectionMgmt:
                            retval = handle_collection_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.UserMgmt:
                            retval = handle_user_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.QueryIndexMgmt:
                            retval = handle_query_index_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.AnalyticsIndexMgmt:
                            retval = handle_analytics_index_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.SearchIndexMgmt:
                            retval = handle_search_index_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.ViewIndexMgmt:
                            retval = handle_view_index_mgmt_response(ret, fn.__name__, return_cls)
                        elif mgmt_type == ManagementType.EventingFunctionMgmt:
                            retval = handle_eventing_function_mgmt_response(ret, fn.__name__, return_cls)
                        else:
                            retval = None

                    if not ft.done():
                        self.loop.call_soon_threadsafe(ft.set_result, retval)

                def on_err(exc, exc_info=None, error_msg=None):
                    excptn = build_mgmt_exception(exc, mgmt_type, error_map)
                    if not ft.done():
                        self.loop.call_soon_threadsafe(ft.set_exception, excptn)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err

                if not self._connection:
                    exc = MissingConnectionException('Not connected.  Cannot perform bucket management operation.')
                    ft.set_exception(exc)
                else:
                    func = mgmt_overload_registry.get(fn.__qualname__, fn)
                    # work-around for PYCBC-1375, I doubt users are calling the index mgmt method
                    # using fields=[...], but in the event they do (as we do in the tests) this corrects
                    # the kwarg name.
                    if ('QueryIndexManager' in fn.__qualname__
                        and fn.__qualname__.endswith('create_index')
                            and 'fields' in kwargs):
                        kwargs['keys'] = kwargs.pop('fields')
                        Supportability.method_kwarg_deprecated('fields', 'keys')
                    call_async_fn(ft, self, func, *args, **kwargs)

                return ft

            return wrapped_fn

        return decorator
