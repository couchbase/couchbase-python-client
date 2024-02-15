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

import json
from datetime import timedelta
from enum import Enum
from functools import wraps

from couchbase._utils import Overload, OverloadType
from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap,
                                  HTTPException)
from couchbase.exceptions import exception as BaseCouchbaseException
from couchbase.logic.supportability import Supportability


class ManagementType(Enum):
    BucketMgmt = 'bucket_mgmt'
    CollectionMgmt = 'collection_mgmt'
    UserMgmt = 'user_mgmt'
    QueryIndexMgmt = 'query_index_mgmt'
    SearchIndexMgmt = 'search_index_mgmt'
    AnalyticsIndexMgmt = 'analytics_index_mgmt'
    ViewIndexMgmt = 'view_index_mgmt'
    EventingFunctionMgmt = 'eventing_function_mgmt'


"""

Bucket mgmt helpers for parsing returned results

"""


def get_bucket_settings(res, return_cls):
    raw_settings = res.raw_result.get('bucket_settings', None)
    bucket_settings = None
    if raw_settings:
        bucket_settings = return_cls.transform_from_dest(raw_settings)

    return bucket_settings


def get_all_bucket_settings(res, return_cls):
    raw_buckets = res.raw_result.get('buckets', None)
    buckets = []
    if raw_buckets:
        for b in raw_buckets:
            bucket_settings = return_cls.transform_from_dest(b)
            buckets.append(bucket_settings)

    return buckets


def get_bucket_describe_result(res, return_cls):
    bucket_info = res.raw_result.get('bucket_info', None)
    if bucket_info:
        return return_cls(**bucket_info)
    return None


"""

Collection mgmt helpers for parsing returned results

"""


def get_all_scopes(res, return_cls):
    scopes = []
    raw_scopes = res.raw_result.get('scopes', None)
    # TODO: better exception?
    if raw_scopes:
        for s in raw_scopes:
            scope = return_cls[0](s['name'], list())
            for c in s['collections']:
                scope.collections.append(
                    return_cls[1](c['name'],
                                  c['scope_name'],
                                  timedelta(seconds=c['max_expiry']),
                                  history=c.get('history')))
            scopes.append(scope)

    return scopes


"""

User mgmt helpers for parsing returned results

"""


def get_user(res, return_cls):
    raw_user = res.raw_result.get('user_and_metadata', None)
    user = None
    if raw_user:
        user = return_cls.create_user_and_metadata(raw_user)

    return user


def get_all_users(res, return_cls):
    users = []
    raw_users = res.raw_result.get('users', None)
    if raw_users:
        for u in raw_users:
            user = return_cls.create_user_and_metadata(u)
            users.append(user)

    return users


def get_roles(res, return_cls):
    roles = []
    raw_roles = res.raw_result.get('roles', None)
    if raw_roles:
        for r in raw_roles:
            role = return_cls.create_role_and_description(r)
            roles.append(role)

    return roles


def get_group(res, return_cls):
    raw_group = res.raw_result.get('group', None)
    group = None
    if raw_group:
        group = return_cls.create_group(raw_group)

    return group


def get_all_groups(res, return_cls):
    groups = []
    raw_groups = res.raw_result.get('groups', None)
    if raw_groups:
        for g in raw_groups:
            group = return_cls.create_group(g)
            groups.append(group)

    return groups


"""

Analytics mgmt helpers for parsing returned results

"""


def get_all_datasets(res, return_cls):
    datasets = []
    raw_datasets = res.raw_result.get('datasets', None)
    if raw_datasets:
        datasets = [return_cls(**ds) for ds in raw_datasets]

    return datasets


def get_all_analytics_indexes(res, return_cls):
    indexes = []
    raw_indexes = res.raw_result.get('indexes', None)
    if raw_indexes:
        indexes = [return_cls(**ds) for ds in raw_indexes]

    return indexes


def get_links(res, return_cls):
    analytics_links = []
    cb_links = res.raw_result.get('couchbase_links', None)
    if cb_links and len(cb_links) > 0:
        analytics_links.extend(map(lambda l: return_cls[0].link_from_server_json(l), cb_links))
    s3_links = res.raw_result.get('s3_links', None)
    if s3_links and len(s3_links) > 0:
        analytics_links.extend(map(lambda l: return_cls[1].link_from_server_json(l), s3_links))
    azure_blob_links = res.raw_result.get('azure_blob_links', None)
    if azure_blob_links and len(azure_blob_links) > 0:
        analytics_links.extend(
            map(lambda l: return_cls[2].link_from_server_json(l), azure_blob_links))

    return analytics_links


def get_all_query_indexes(res, return_cls):
    indexes = []
    raw_indexes = res.raw_result.get('indexes', None)
    if raw_indexes:
        indexes = [return_cls.from_server(idx) for idx in raw_indexes]

    return indexes


"""

Search index mgmt helpers for parsing returned results

"""


def get_search_index(res, return_cls):
    raw_index = res.raw_result.get('index', None)
    index = None
    if raw_index:
        index = return_cls.from_server(raw_index)

    return index


def get_all_search_indexes(res, return_cls):
    indexes = []
    raw_indexes = res.raw_result.get('indexes', None)
    if raw_indexes:
        indexes = [return_cls.from_server(idx) for idx in raw_indexes]

    return indexes


def analyze_search_index_document(res):
    output = {}
    analysis = res.raw_result.get('analysis', None)
    if analysis:
        output['analysis'] = json.loads(analysis)
    status = res.raw_result.get('status', None)
    if status:
        output['status'] = status

    return output


def get_search_index_stats(res):
    raw_stats = res.raw_result.get('stats', None)
    stats = None
    if raw_stats:
        stats = json.loads(raw_stats)

    return stats


def get_all_search_index_stats(res):
    raw_stats = res.raw_result.get('stats', None)
    stats = None
    if raw_stats:
        stats = json.loads(raw_stats)

    return stats


"""

View index mgmt helpers for parsing returned results

"""


def get_design_document(res, return_cls):
    raw_ddoc = res.raw_result.get('design_document', None)
    ddoc = None
    if raw_ddoc:
        ddoc = return_cls.from_json(raw_ddoc)

    return ddoc


def get_all_design_documents(res, return_cls):
    ddocs = []
    raw_ddocs = res.raw_result.get('design_documents', None)
    if raw_ddocs:
        ddocs = [return_cls.from_json(ddoc) for ddoc in raw_ddocs]

    return ddocs


"""

Eventing mgmt helpers for parsing returned results

"""


def get_eventing_function(res, return_cls):
    raw_func = res.raw_result.get('function', None)
    func = None
    if raw_func:
        func = return_cls.from_server(raw_func)

    return func


def get_all_eventing_functions(res, return_cls):
    functions = []
    raw_functions = res.raw_result.get('functions', None)
    if raw_functions:
        functions = [return_cls.from_server(f) for f in raw_functions]

    return functions


def get_eventing_functions_status(res, return_cls):
    raw_status = res.raw_result.get('status', None)
    status = None
    if raw_status:
        status = return_cls.from_server(raw_status)

    return status


"""

"""


def handle_mgmt_exception(exc, mgmt_type, error_map):
    raise ErrorMapper.build_exception(exc, mapping=error_map)


def handle_bucket_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_bucket':
        retval = get_bucket_settings(ret, return_cls)
    elif fn_name == 'get_all_buckets':
        retval = get_all_bucket_settings(ret, return_cls)
    elif fn_name == 'bucket_describe':
        retval = get_bucket_describe_result(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


def handle_collection_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_all_scopes':
        retval = get_all_scopes(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


def handle_user_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_user':
        retval = get_user(ret, return_cls)
    elif fn_name == 'get_all_users':
        retval = get_all_users(ret, return_cls)
    elif fn_name == 'get_roles':
        retval = get_roles(ret, return_cls)
    elif fn_name == 'get_group':
        retval = get_group(ret, return_cls)
    elif fn_name == 'get_all_groups':
        retval = get_all_groups(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


def handle_query_index_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_all_indexes':
        retval = get_all_query_indexes(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


def handle_analytics_index_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_all_datasets':
        retval = get_all_datasets(ret, return_cls)
    elif fn_name == 'get_all_indexes':
        retval = get_all_analytics_indexes(ret, return_cls)
    elif fn_name == 'get_pending_mutations':
        retval = ret.raw_result.get('stats', None)
    elif fn_name == 'get_links':
        retval = get_links(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


def handle_search_index_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_index':
        retval = get_search_index(ret, return_cls)
    elif fn_name == 'get_all_indexes':
        retval = get_all_search_indexes(ret, return_cls)
    elif fn_name == 'get_indexed_documents_count':
        retval = ret.raw_result.get('count', 0)
    elif fn_name == 'analyze_document':
        retval = analyze_search_index_document(ret)
    elif fn_name == 'get_index_stats':
        retval = get_search_index_stats(ret)
    elif fn_name == 'get_all_index_stats':
        retval = get_all_search_index_stats(ret)
    else:
        retval = return_cls(ret)

    return retval


def handle_view_index_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_design_document':
        retval = get_design_document(ret, return_cls)
    elif fn_name == 'get_all_design_documents':
        retval = get_all_design_documents(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


def handle_eventing_function_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_function':
        retval = get_eventing_function(ret, return_cls)
    elif fn_name == 'get_all_functions':
        retval = get_all_eventing_functions(ret, return_cls)
    elif fn_name == 'functions_status':
        retval = get_eventing_functions_status(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


mgmt_overload_registry = {}


class BlockingMgmtWrapper:

    @classmethod  # noqa: C901
    def block(cls, return_cls, mgmt_type, error_map, overload_type=None):  # noqa: C901
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
                try:
                    func = mgmt_overload_registry.get(fn.__qualname__, fn)
                    # work-around for PYCBC-1375, I doubt users are calling the index mgmt method
                    # using fields=[...], but in the event they do (as we do in the tests) this corrects
                    # the kwarg name.
                    if ('QueryIndexManager' in fn.__qualname__
                        and fn.__qualname__.endswith('create_index')
                            and 'fields' in kwargs):
                        kwargs['keys'] = kwargs.pop('fields')
                        Supportability.method_kwarg_deprecated('fields', 'keys')
                    ret = func(self, *args, **kwargs)
                    if isinstance(ret, BaseCouchbaseException):
                        handle_mgmt_exception(ret, mgmt_type, error_map)
                    if return_cls is None:
                        return None
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
                    return retval
                except HTTPException as e:
                    raise e
                except CouchbaseException as e:
                    raise e
                except Exception as ex:
                    if isinstance(ex, (TypeError, ValueError)):
                        raise ex
                    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
                    excptn = exc_cls(message=str(ex))
                    raise excptn

            return wrapped_fn
        return decorator
