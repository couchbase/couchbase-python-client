#  Copyright 2016-2026. Couchbase, Inc.
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

import importlib
import pathlib
import pickle
import pkgutil
import re
import warnings
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, call

import pytest

import couchbase
import couchbase.exceptions as E
import couchbase.protocol.classic._collection_impl as CI
import couchbase.protocol.classic._error_mapping as EM
from couchbase.common.exceptions import _ExceptionInfo
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.protocol.classic._management._analytics_mgmt_types import ANALYTICS_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._bucket_mgmt_types import BUCKET_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._collection_mgmt_types import COLLECTION_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._eventing_function_mgmt_types import EVENTING_FUNCTION_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._query_index_mgmt_types import QUERY_INDEX_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._search_index_mgmt_types import SEARCH_INDEX_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._user_mgmt_types import USER_MGMT_ERROR_MAP
from couchbase.protocol.classic._management._view_index_mgmt_types import VIEW_INDEX_MGMT_ERROR_MAP
from couchbase.protocol.classic._pycbc_core import pycbc_exception, pycbc_result
from couchbase.protocol.classic._result import _mutation_from_core
from couchbase.protocol.classic._subdocument import _subdoc_error
from couchbase.result import MultiMutationResult, MutationResult
from couchbase.subdocument import SubDocStatus
from tests.test_features import TestProtocol

# The packages behind an extra, which the modules that use them import at the top, so an install
# without the extra cannot import those modules.
_OPTIONAL_DEPENDENCIES = frozenset({'opentelemetry'})

# What is checked here is how classic maps its binding's errors.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

# Every code the core can hand back, and the class it becomes.  Written out rather than derived
# from ERROR_MAP, which is the thing under test: a table that reads its expectations out of the
# mapping agrees with any mapping, including a wrong one.
_CODE_CASES = [
    (2, 'RequestCanceledException'),
    (3, 'InvalidArgumentException'),
    (4, 'ServiceUnavailableException'),
    (5, 'InternalServerFailureException'),
    (6, 'AuthenticationException'),
    (7, 'TemporaryFailException'),
    (8, 'ParsingFailedException'),
    (9, 'CasMismatchException'),
    (10, 'BucketNotFoundException'),
    (11, 'CollectionNotFoundException'),
    (12, 'UnsupportedOperation'),
    (13, 'AmbiguousTimeoutException'),
    (14, 'UnAmbiguousTimeoutException'),
    (15, 'FeatureUnavailableException'),
    (16, 'ScopeNotFoundException'),
    (17, 'QueryIndexNotFoundException'),
    (18, 'QueryIndexAlreadyExistsException'),
    (21, 'RateLimitedException'),
    (22, 'QuotaLimitedException'),
    (101, 'DocumentNotFoundException'),
    (102, 'DocumentUnretrievableException'),
    (103, 'DocumentLockedException'),
    (104, 'ValueTooLargeException'),
    (105, 'DocumentExistsException'),
    (107, 'DurabilityInvalidLevelException'),
    (108, 'DurabilityImpossibleException'),
    (109, 'DurabilitySyncWriteAmbiguousException'),
    (110, 'DurabilitySyncWriteInProgressException'),
    (111, 'DurableWriteReCommitInProgressException'),
    (113, 'PathNotFoundException'),
    (114, 'PathMismatchException'),
    (115, 'PathInvalidException'),
    (116, 'PathTooBigException'),
    (117, 'PathTooDeepException'),
    (118, 'ValueTooDeepException'),
    (119, 'InvalidValueException'),
    (120, 'DocumentNotJsonException'),
    (121, 'NumberTooBigException'),
    (122, 'DeltaInvalidException'),
    (123, 'PathExistsException'),
    (124, 'XattrUnknownMacroException'),
    (126, 'XattrInvalidKeyComboException'),
    (127, 'XattrUnknownVirtualAttributeException'),
    (128, 'XattrCannotModifyVirtualAttributeException'),
    (130, 'XattrNoAccessException'),
    (131, 'DocumentNotLockedException'),
    (132, 'CannotReviveLivingDocumentException'),
    (133, 'MutationTokenOutdatedException'),
    (134, 'RangeScanCompletedException'),
    (135, 'DocumentNotFoundOnReplicaException'),
    (136, 'ReplicaIndexOutOfBoundsException'),
    (137, 'ReplicaIndexCurrentlyUnavailableException'),
    (303, 'DatasetNotFoundException'),
    (304, 'DataverseNotFoundException'),
    (305, 'DatasetAlreadyExistsException'),
    (306, 'DataverseAlreadyExistsException'),
    (502, 'DesignDocumentNotFoundException'),
    (5000, 'InternalSDKException'),
    (5001, 'HTTPException'),
    (5002, 'UnsuccessfulOperationException'),
]

# Outside every range the core allocates from, so it reaches the fallback.
_UNMAPPED_CODE = 99999

# name -> the module that defines it.  The request builders import these by name, so the scan in
# test_string_table_inventory_is_complete dedupes on the name rather than the module.
_STRING_TABLES = {
    'KV_ERROR_CONTEXT_MAPPING': 'couchbase.protocol.classic._error_mapping',
    'QUERY_ERROR_MAPPING': 'couchbase.protocol.classic._error_mapping',
    'BUCKET_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._bucket_mgmt_types',
    'VIEW_INDEX_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._view_index_mgmt_types',
    'SEARCH_INDEX_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._search_index_mgmt_types',
    'EVENTING_FUNCTION_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._eventing_function_mgmt_types',
    'COLLECTION_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._collection_mgmt_types',
    'USER_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._user_mgmt_types',
    'ANALYTICS_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._analytics_mgmt_types',
    'QUERY_INDEX_MGMT_ERROR_MAP': 'couchbase.protocol.classic._management._query_index_mgmt_types',
}

_CODE_TABLES = {'ERROR_MAP': 60}

_TABLES_BY_NAME: dict[str, dict[str, type[E.CouchbaseException]]] = {
    'KV_ERROR_CONTEXT_MAPPING': EM.KV_ERROR_CONTEXT_MAPPING,
    'QUERY_ERROR_MAPPING': EM.QUERY_ERROR_MAPPING,
    'BUCKET_MGMT_ERROR_MAP': BUCKET_MGMT_ERROR_MAP,
    'VIEW_INDEX_MGMT_ERROR_MAP': VIEW_INDEX_MGMT_ERROR_MAP,
    'SEARCH_INDEX_MGMT_ERROR_MAP': SEARCH_INDEX_MGMT_ERROR_MAP,
    'EVENTING_FUNCTION_MGMT_ERROR_MAP': EVENTING_FUNCTION_MGMT_ERROR_MAP,
    'COLLECTION_MGMT_ERROR_MAP': COLLECTION_MGMT_ERROR_MAP,
    'USER_MGMT_ERROR_MAP': USER_MGMT_ERROR_MAP,
    'ANALYTICS_MGMT_ERROR_MAP': ANALYTICS_MGMT_ERROR_MAP,
    'QUERY_INDEX_MGMT_ERROR_MAP': QUERY_INDEX_MGMT_ERROR_MAP,
}

# One row per table entry: the table, the pattern exactly as the table spells it, a server string
# that pattern accepts, and the class it produces.  The samples are written to match the pattern,
# not transcribed from a server; whether a pattern still matches what a given server build sends is
# a different question and needs a cluster to answer.
_STRING_CASES = [
    ('KV_ERROR_CONTEXT_MAPPING',
     'key_value_locked',
     'key_value_locked',
     'DocumentLockedException'),
    ('KV_ERROR_CONTEXT_MAPPING',
     'key_value_temporary_failure',
     'key_value_temporary_failure',
     'TemporaryFailException'),

    ('QUERY_ERROR_MAPPING',
     r'.*Keyspace not found.*',
     '12003 Keyspace not found in CB datastore: default:travel-sample',
     'KeyspaceNotFoundException'),
    ('QUERY_ERROR_MAPPING',
     r'.*Scope not found.*',
     '12021 Scope not found in CB datastore default:travel-sample',
     'ScopeNotFoundException'),
    ('QUERY_ERROR_MAPPING',
     r'.*No index available.*',
     '4000 No index available on keyspace default:travel-sample',
     'QueryIndexNotFoundException'),
    ('QUERY_ERROR_MAPPING',
     r'.*[iI]ndex.*not found.*',
     '12016 Index idx-name not found',
     'QueryIndexNotFoundException'),
    ('QUERY_ERROR_MAPPING',
     r'.*[iI]ndex.*already exists.*',
     '4300 The index idx-name already exists',
     'QueryIndexAlreadyExistsException'),

    ('BUCKET_MGMT_ERROR_MAP',
     'Bucket with given name (already|still) exists',
     'Bucket with given name already exists',
     'BucketAlreadyExistsException'),
    ('BUCKET_MGMT_ERROR_MAP',
     'Requested resource not found',
     'Requested resource not found.',
     'BucketDoesNotExistException'),
    ('BUCKET_MGMT_ERROR_MAP',
     r'.*non existent bucket.*',
     'Attempt to modify a non existent bucket "test-bucket"',
     'BucketDoesNotExistException'),
    ('BUCKET_MGMT_ERROR_MAP',
     r'.*Flush is disabled for the bucket.*',
     'Flush is disabled for the bucket',
     'BucketNotFlushableException'),
    ('BUCKET_MGMT_ERROR_MAP',
     r'.*Limit\(s\) exceeded\s+\[.*\].*',
     'Limit(s) exceeded [num_concurrent_requests]',
     'RateLimitedException'),
    ('BUCKET_MGMT_ERROR_MAP',
     r'.*is supported only with developer preview enabled.*',
     'Storage mode magma is supported only with developer preview enabled',
     'FeatureUnavailableException'),

    ('VIEW_INDEX_MGMT_ERROR_MAP',
     'not_found',
     'not_found',
     'DesignDocumentNotFoundException'),
    ('VIEW_INDEX_MGMT_ERROR_MAP',
     r'.*Limit\(s\) exceeded\s+\[.*\].*',
     'Limit(s) exceeded [num_views_per_ddoc]',
     'RateLimitedException'),

    ('SEARCH_INDEX_MGMT_ERROR_MAP',
     r'.*[iI]ndex not found.*',
     'rest_auth: preparePerms, err: index not found',
     'SearchIndexNotFoundException'),
    ('SEARCH_INDEX_MGMT_ERROR_MAP',
     r'.*[Cc]reate[Ii]ndex, [Pp]repare failed, err: [Ee]xceeds indexes limit',
     'CreateIndex, Prepare failed, err: exceeds indexes limit',
     'QuotaLimitedException'),

    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_APP_NOT_FOUND_TS',
     'ERR_APP_NOT_FOUND_TS',
     'EventingFunctionNotFoundException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_HANDLER_COMPILATION',
     'ERR_HANDLER_COMPILATION',
     'EventingFunctionCompilationFailureException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_APP_NOT_BOOTSTRAPPED',
     'ERR_APP_NOT_BOOTSTRAPPED',
     'EventingFunctionNotBootstrappedException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_APP_NOT_DEPLOYED',
     'ERR_APP_NOT_DEPLOYED',
     'EventingFunctionNotDeployedException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_APP_NOT_UNDEPLOYED',
     'ERR_APP_NOT_UNDEPLOYED',
     'EventingFunctionNotUnDeployedException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_COLLECTION_MISSING',
     'ERR_COLLECTION_MISSING',
     'EventingFunctionCollectionNotFoundException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_APP_ALREADY_DEPLOYED',
     'ERR_APP_ALREADY_DEPLOYED',
     'EventingFunctionAlreadyDeployedException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     r'.*ERR_SRC_MB_SAME',
     'ERR_SRC_MB_SAME',
     'EventingFunctionIdenticalKeyspaceException'),

    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*Scope with.*name.*already exists',
     'Scope with this name already exists',
     'ScopeAlreadyExistsException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*Scope with.*name.*not found',
     'Scope with this name not found',
     'ScopeNotFoundException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*Collection with.*name.*not found',
     'Collection with this name not found',
     'CollectionNotFoundException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*Collection with.*name.*already exists',
     'Collection with this name already exists',
     'CollectionAlreadyExistsException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*collection_not_found.*',
     'collection_not_found',
     'CollectionNotFoundException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*scope_not_found.*',
     'scope_not_found',
     'ScopeNotFoundException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*Maximum number of collections has been reached for scope.*',
     'Maximum number of collections has been reached for scope "test-scope"',
     'QuotaLimitedException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     r'.*Limit\(s\) exceeded\s+\[.*\].*',
     'Limit(s) exceeded [num_collections]',
     'RateLimitedException'),

    ('USER_MGMT_ERROR_MAP',
     'Unknown group.*',
     'Unknown group test-group',
     'GroupNotFoundException'),
    ('USER_MGMT_ERROR_MAP',
     'Unknown user.*',
     'Unknown user test-user',
     'UserNotFoundException'),
    ('USER_MGMT_ERROR_MAP',
     'Group was not found.*',
     'Group was not found',
     'GroupNotFoundException'),
    ('USER_MGMT_ERROR_MAP',
     'User was not found.*',
     'User was not found',
     'UserNotFoundException'),
    ('USER_MGMT_ERROR_MAP',
     'Not found.*',
     'Not found.',
     'FeatureUnavailableException'),
    ('USER_MGMT_ERROR_MAP',
     'Method Not Allowed.*',
     'Method Not Allowed',
     'FeatureUnavailableException'),
    ('USER_MGMT_ERROR_MAP',
     r'.*Limit\(s\) exceeded\s+\[.*\].*',
     'Limit(s) exceeded [num_concurrent_requests]',
     'RateLimitedException'),

    ('ANALYTICS_MGMT_ERROR_MAP',
     r'.*24055.*already exists',
     'Error Code 24055: Link test-link already exists',
     'AnalyticsLinkExistsException'),
    ('ANALYTICS_MGMT_ERROR_MAP',
     r'.*24006.*does not exist',
     'Error Code 24006: Link test-link does not exist',
     'AnalyticsLinkNotFoundException'),
    ('ANALYTICS_MGMT_ERROR_MAP',
     r'.*24034.*Cannot find',
     'Error Code 24034: Cannot find dataverse test-dataverse',
     'DataverseNotFoundException'),
    ('ANALYTICS_MGMT_ERROR_MAP',
     r'.*24025.*Cannot find analytics (collection)*',
     'Error Code 24025: Cannot find analytics collection test-dataset',
     'DatasetNotFoundException'),
    ('ANALYTICS_MGMT_ERROR_MAP',
     r'.*24039.*An analytics\s+(scope)*.*already exists',
     'Error Code 24039: An analytics scope with name test-dataverse already exists',
     'DataverseAlreadyExistsException'),
    ('ANALYTICS_MGMT_ERROR_MAP',
     r'.*24040.*An analytics\s+(collection)*.*already exists',
     'Error Code 24040: An analytics collection with name test-dataset already exists',
     'DatasetAlreadyExistsException'),

    ('QUERY_INDEX_MGMT_ERROR_MAP',
     r'.*[iI]ndex.*already exists.*',
     '4300 The index idx-name already exists',
     'QueryIndexAlreadyExistsException'),
    ('QUERY_INDEX_MGMT_ERROR_MAP',
     r'.*[iI]ndex.*[nN]ot [fF]ound.*',
     '12016 Index idx-name Not Found',
     'QueryIndexNotFoundException'),
]

# Strings two patterns in the same table both accept.  The winner is the earlier key, so the
# tables are ordered data and a reordering is a behavior change.
_PRECEDENCE_CASES = [
    ('QUERY_ERROR_MAPPING',
     'Index build failed: Keyspace not found in CB datastore',
     'KeyspaceNotFoundException',
     'QueryIndexNotFoundException'),
    ('COLLECTION_MGMT_ERROR_MAP',
     'Scope with the given name exists; Collection with the given name already exists',
     'ScopeAlreadyExistsException',
     'CollectionAlreadyExistsException'),
    ('USER_MGMT_ERROR_MAP',
     'Not found. Unknown user test-user',
     'FeatureUnavailableException',
     'UserNotFoundException'),
]

# The tables are matched with re.match, so a key that does not open with a wildcard only matches at
# the start of the string.  Each of these carries the table's own text and still maps to nothing.
_ANCHORED_CASES = [
    ('BUCKET_MGMT_ERROR_MAP', 'Attempt failed: Bucket with given name already exists'),
    ('BUCKET_MGMT_ERROR_MAP', 'Error: Requested resource not found'),
    ('USER_MGMT_ERROR_MAP', 'Error: Unknown user test-user'),
    ('VIEW_INDEX_MGMT_ERROR_MAP', 'error: not_found'),
]

# The response body is read as a plain string first and only then as JSON, and the JSON reader
# takes a different field per shape.
_RESPONSE_BODY_CASES = [
    ('BUCKET_MGMT_ERROR_MAP',
     'Flush is disabled for the bucket',
     'BucketNotFlushableException'),
    ('BUCKET_MGMT_ERROR_MAP',
     '"Requested resource not found"',
     'BucketDoesNotExistException'),
    ('BUCKET_MGMT_ERROR_MAP',
     '{"errors": [{"code": 1, "msg": "Flush is disabled for the bucket"}]}',
     'BucketNotFlushableException'),
    ('VIEW_INDEX_MGMT_ERROR_MAP',
     '{"errors": {"name": "not_found"}}',
     'DesignDocumentNotFoundException'),
    ('EVENTING_FUNCTION_MGMT_ERROR_MAP',
     '{"name": "ERR_APP_NOT_FOUND_TS"}',
     'EventingFunctionNotFoundException'),
    ('BUCKET_MGMT_ERROR_MAP',
     'nothing here matches and it is not json',
     'HTTPException'),
    # An `errors` that is neither a list nor an object names nothing, so the code decides.
    ('BUCKET_MGMT_ERROR_MAP',
     '{"errors": "nothing here matches"}',
     'HTTPException'),
]


# Every context_type the core writes, written out for the same reason as _CODE_CASES.
_CONTEXT_TYPE_NAMES = ['AnalyticsErrorContext',
                       'HTTPErrorContext',
                       'KeyValueErrorContext',
                       'QueryErrorContext',
                       'SearchErrorContext',
                       'SubdocumentErrorContext',
                       'ViewErrorContext']

_CODE_CASE_IDS = [f'{code}-{name}' for code, name in _CODE_CASES]
_STRING_CASE_IDS = [f'{table}-{pattern}' for table, pattern, _, _ in _STRING_CASES]
_ANCHORED_CASE_IDS = [f'{table}-{sample}' for table, sample in _ANCHORED_CASES]


class _CoreError(pycbc_exception):
    """Stands in for the exception object the C++ core hands to ErrorMapper.

    Subclassing the real core type rather than writing a stub keeps the isinstance checks in
    build_exception honest and costs nothing: every input build_exception reads is a method, and a
    Python subclass may override all five.  That is what lets the whole mapping be characterized
    with no cluster and no request.
    """

    def __init__(self,
                 err_code: int = 5001,
                 category: str = 'couchbase.common',
                 strerror: str = 'request_canceled',
                 context: dict[str, Any] | None = None,
                 info: _ExceptionInfo | None = None) -> None:
        super().__init__()
        self._err_code = err_code
        self._category = category
        self._strerror = strerror
        self._error_context = context
        self._error_info = info

    def err(self) -> int:
        return self._err_code

    def err_category(self) -> str:
        return self._category

    def strerror(self) -> str:
        return self._strerror

    def error_context(self) -> dict[str, Any] | None:
        return self._error_context

    def error_info(self) -> _ExceptionInfo | None:
        return self._error_info


def _http_context(body: str | None = None, **kwargs: Any) -> dict[str, Any]:
    context: dict[str, Any] = {'context_type': 'HTTPErrorContext'}
    if body is not None:
        context['http_body'] = body
    context.update(kwargs)
    return context


def _query_context(body: str | None = None, **kwargs: Any) -> dict[str, Any]:
    context = _http_context(body, **kwargs)
    context['context_type'] = 'QueryErrorContext'
    return context


def _kv_context(retry_reasons: list[str] | None = None, **kwargs: Any) -> dict[str, Any]:
    context: dict[str, Any] = {'context_type': 'KeyValueErrorContext'}
    if retry_reasons is not None:
        context['retry_reasons'] = retry_reasons
    context.update(kwargs)
    return context


def _mapped_from_message(text: str, mapping: dict[str, type[E.CouchbaseException]]) -> E.CouchbaseException:
    core_error = _CoreError(context=_http_context(''), info={'error_message': text})
    return EM.ErrorMapper.build_exception(core_error, mapping=mapping)


class ErrorMappingUnitTests:
    """Characterization of the exception a core error becomes, and of the text it carries.

    ErrorMapper.build_exception picks a class from three inputs (the core error code, the error
    context and the caller-supplied mapping), and CouchbaseException then renders a message from a
    fourth, the core's strerror.  Neither the choice nor the rendering is specified anywhere except
    in the code that performs it, so this file is the specification: every table entry, every
    dispatch branch and every message shape is asserted against the value it produces today.

    Nothing here touches a cluster.  The inputs are _CoreError instances, so a run needs the
    extension module and nothing else.
    """

    # No TEST_MANIFEST: the mapper and the exception classes have one definition, shared by all
    # three apis, so there is nothing for a generated copy to be in parity with.

    # The integer branch, which runs when the core sends no error context.
    @pytest.mark.parametrize('code, expected', _CODE_CASES, ids=_CODE_CASE_IDS)
    def test_code_maps_to_exception_type(self, code: int, expected: str) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=code))
        assert type(exc).__name__ == expected

    def test_code_table_covers_every_error_map_entry(self) -> None:
        assert set(_CODE_CASES) == set((code, cls.__name__) for code, cls in EM.ERROR_MAP.items())

    # The table names each class directly, so a class defined twice would map to a copy that
    # `except` against the published name does not catch.
    def test_error_map_holds_the_classes_couchbase_exceptions_publishes(self) -> None:
        for cls in EM.ERROR_MAP.values():
            assert getattr(E, cls.__name__) is cls

    def test_unmapped_code_falls_back_to_couchbase_exception(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=_UNMAPPED_CODE))
        assert type(exc) is E.CouchbaseException

    @pytest.mark.parametrize('table, pattern, sample, expected', _STRING_CASES, ids=_STRING_CASE_IDS)
    def test_string_table_entry_maps_to_exception_type(self,
                                                       table: str,
                                                       pattern: str,
                                                       sample: str,
                                                       expected: str) -> None:
        mapping = _TABLES_BY_NAME[table]
        assert pattern in mapping, f'{table} no longer holds {pattern!r}.'
        exc = _mapped_from_message(sample, mapping)
        assert type(exc).__name__ == expected

    # Guards the parametrization above.  An entry added to a table with no row here would be
    # unpinned rather than a failure, which is the shape of miss this whole file exists to catch.
    def test_string_case_table_covers_every_table_entry(self) -> None:
        for name, mapping in _TABLES_BY_NAME.items():
            covered = set(pattern for table, pattern, _, _ in _STRING_CASES if table == name)
            assert covered == set(mapping), f'{name} entries without a case: {set(mapping) - covered}.'

    # And guards the inventory itself, against a table added in a module nobody thought to list.
    def test_string_table_inventory_is_complete(self) -> None:
        found: dict[str, int] = {}
        for name, table in self._error_tables(str).items():
            found[name] = len(table)
        assert found == {name: len(_TABLES_BY_NAME[name]) for name in _STRING_TABLES}

    def test_code_table_inventory_is_complete(self) -> None:
        found = {name: len(table) for name, table in self._error_tables(int).items()}
        assert found == _CODE_TABLES

    @pytest.mark.parametrize('table, sample, expected, also_matches', _PRECEDENCE_CASES)
    def test_first_matching_pattern_wins(self, table: str, sample: str, expected: str, also_matches: str) -> None:
        mapping = _TABLES_BY_NAME[table]
        exc = _mapped_from_message(sample, mapping)
        assert type(exc).__name__ == expected
        assert expected != also_matches

    @pytest.mark.parametrize('table, sample', _ANCHORED_CASES, ids=_ANCHORED_CASE_IDS)
    def test_pattern_is_anchored_at_the_start_of_the_string(self, table: str, sample: str) -> None:
        exc = _mapped_from_message(sample, _TABLES_BY_NAME[table])
        assert type(exc).__name__ == 'HTTPException'

    @pytest.mark.parametrize('table, body, expected', _RESPONSE_BODY_CASES)
    def test_response_body_shape_selects_the_exception_type(self, table: str, body: str, expected: str) -> None:
        core_error = _CoreError(context=_http_context(body))
        exc = EM.ErrorMapper.build_exception(core_error, mapping=_TABLES_BY_NAME[table])
        assert type(exc).__name__ == expected

    def test_error_message_is_preferred_over_the_response_body(self) -> None:
        core_error = _CoreError(context=_http_context('Flush is disabled for the bucket'),
                                info={'error_message': 'Requested resource not found'})
        exc = EM.ErrorMapper.build_exception(core_error, mapping=BUCKET_MGMT_ERROR_MAP)
        assert type(exc).__name__ == 'BucketDoesNotExistException'

    # A key-value context is matched against its retry reasons alone: build_exception calls
    # _parse_kv_context with no err_content, so the reasons are the only strings it ever sees.
    def test_key_value_context_maps_a_retry_reason(self) -> None:
        core_error = _CoreError(err_code=101,
                                category='couchbase.key_value',
                                strerror='document_not_found',
                                context=_kv_context(retry_reasons=['key_value_locked']))
        exc = EM.ErrorMapper.build_exception(core_error)
        assert type(exc) is E.DocumentLockedException

    def test_key_value_context_takes_the_default_mapping(self) -> None:
        core_error = _CoreError(err_code=101,
                                context=_kv_context(retry_reasons=['key_value_temporary_failure']))
        exc = EM.ErrorMapper.build_exception(core_error, mapping=None)
        assert type(exc) is E.TemporaryFailException

    def test_query_context_takes_the_default_mapping(self) -> None:
        core_error = _CoreError(context=_query_context('12003 Keyspace not found in CB datastore'))
        exc = EM.ErrorMapper.build_exception(core_error)
        assert type(exc) is E.KeyspaceNotFoundException

    # Regression guard (PYCBC-1949): a query context is also an HTTP context, and the query branch
    # used to run second and discard error_message, so the same text mapped under an HTTP context
    # and not under a query one.
    def test_query_context_maps_the_error_message(self) -> None:
        message = '4300 The index idx-name already exists'
        mapped = _mapped_from_message(message, QUERY_INDEX_MGMT_ERROR_MAP)
        assert type(mapped) is E.QueryIndexAlreadyExistsException

        core_error = _CoreError(context=_query_context(''), info={'error_message': message})
        exc = EM.ErrorMapper.build_exception(core_error, mapping=QUERY_INDEX_MGMT_ERROR_MAP)
        assert type(exc) is E.QueryIndexAlreadyExistsException

    # Regression guard (PYCBC-1949): a context type this module does not know used to raise
    # AttributeError out of build_exception in place of the SDK exception.
    def test_unknown_context_type_degrades_to_the_base_context(self) -> None:
        core_error = _CoreError(context={'context_type': 'NoSuchErrorContext', 'retry_attempts': 2})
        exc = EM.ErrorMapper.build_exception(core_error)
        assert type(exc) is E.HTTPException
        assert type(exc.error_context) is E.ErrorContext
        assert exc.error_context.retry_attempts == 2

    def test_missing_context_type_degrades_to_the_base_context(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(context={'retry_attempts': 2}))
        assert type(exc.error_context) is E.ErrorContext
        assert exc.error_context.retry_attempts == 2

    @pytest.mark.parametrize('context_type', _CONTEXT_TYPE_NAMES)
    def test_context_type_selects_the_context_class(self, context_type: str) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            exc = EM.ErrorMapper.build_exception(_CoreError(context={'context_type': context_type}))
        assert type(exc.error_context).__name__ == context_type

    def test_from_dict_is_deprecated_and_still_resolves_the_class(self) -> None:
        with pytest.warns(CouchbaseDeprecationWarning, match='ErrorContext.from_dict'):
            context = E.ErrorContext.from_dict(context_type='QueryErrorContext', statement='SELECT 1')
        assert type(context) is E.QueryErrorContext
        assert context.statement == 'SELECT 1'

    def test_context_types_are_the_ones_the_core_writes(self) -> None:
        assert sorted(EM.CONTEXT_TYPES) == _CONTEXT_TYPE_NAMES
        source = pathlib.Path(couchbase.__file__).parents[1] / 'src' / 'error_contexts.hxx'
        if not source.exists():
            pytest.skip('the extension source is not beside the package')
        pattern = (r'PyUnicode_FromString\("(\w+)"\);\s*'
                   r'PyDict_SetItem\(error_context, pycbc::keys::context_type')
        written = re.findall(pattern, source.read_text())
        assert sorted(written) == _CONTEXT_TYPE_NAMES

    def test_last_exception_is_read_from_the_context(self) -> None:
        text = 'UNAVAILABLE: name resolution failed, caused by DNS lookup timed out'
        context = E.ErrorContext(retry_attempts=3, last_exception=text)
        assert context.last_exception == text
        assert text in repr(context)
        assert E.ErrorContext(retry_attempts=0).last_exception is None
        assert E.KeyValueErrorContext(last_exception=text).last_exception == text

    def test_generic_context_has_only_the_common_fields(self) -> None:
        context = E.GenericErrorContext(retry_attempts=1, resource_name='travel-sample')
        assert isinstance(context, E.ErrorContext)
        assert context.retry_attempts == 1
        assert not hasattr(context, 'resource_name')
        assert repr(context) == "GenericErrorContext({'retry_attempts': 1, 'resource_name': 'travel-sample'})"
        exc = E.CouchbaseException('precondition failed', context=context)
        assert "context=GenericErrorContext({'retry_attempts': 1" in repr(exc)

    def test_message_is_the_core_string_with_underscores_replaced(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=101, strerror='document_not_found'))
        assert exc.message == 'document not found'

    def test_message_is_the_first_argument(self) -> None:
        assert E.CouchbaseException('explicit').message == 'explicit'
        assert E.CouchbaseException().message is None
        # Everything after `message` is keyword-only, so a second positional argument is refused
        # rather than bound to a parameter it was not meant for.
        positional: tuple[Any, ...] = (None, 'a second positional argument')
        with pytest.raises(TypeError):
            E.CouchbaseException(*positional)

    # The core operation get_replica runs reports a missing document as a plain not-found.
    def test_on_replica_keeps_the_not_found_and_its_context(self) -> None:
        context = {'context_type': 'KeyValueErrorContext', 'key': 'a-key', 'retry_attempts': 0}
        not_found = EM.ErrorMapper.build_exception(_CoreError(err_code=101,
                                                              category='couchbase.key_value',
                                                              strerror='document_not_found',
                                                              context=context))
        assert isinstance(not_found, E.DocumentNotFoundException)
        exc = EM.ErrorMapper.on_replica(not_found)
        assert type(exc) is E.DocumentNotFoundOnReplicaException
        assert isinstance(exc, E.DocumentNotFoundException)
        assert exc.error_code == 135
        assert exc.message == 'document not found on replica'
        assert exc.error_context is not_found.error_context
        assert exc.inner_cause is not_found

    def test_error_code_reports_the_core_error_code(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=103))
        assert exc.error_code == 103
        assert E.CouchbaseException(error_code=7).error_code == 7

    @pytest.mark.parametrize('kwargs, expected', [
        ({'message': 'a message'}, '<message=a message>'),
        ({'message': 'a message', 'context': E.GenericErrorContext(reason='CAS_MISMATCH')},
         "<message=a message, context=GenericErrorContext({'reason': 'CAS_MISMATCH'})>"),
        ({}, '<>'),
        ({'exc_info': {'cinfo': ('src/errors.cxx', 42)}}, '<C Source=src/errors.cxx:42>'),
        ({'exc_info': {'inner_cause': 'a cause'}}, '<Inner cause=a cause>'),
    ])
    def test_repr_shape(self, kwargs: dict[str, Any], expected: str) -> None:
        assert repr(E.CouchbaseException(**kwargs)) == expected

    def test_repr_carries_the_message(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=101,
                                                        category='couchbase.key_value',
                                                        strerror='document_not_found'))
        assert repr(exc) == ('DocumentNotFoundException(<ec=101, category=couchbase.key_value, '
                             'message=document not found>)')
        assert exc.message == 'document not found'

    def test_reading_error_context_does_not_change_the_repr(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=103,
                                                        category='couchbase.key_value',
                                                        strerror='document_locked'))
        before = repr(exc)
        assert 'context=' not in before
        assert exc.error_context is exc.error_context
        assert repr(exc) == before

    # A mapped exception holds plain values, not the C-extension error, so it pickles and loads as
    # the same class with the same rendering.
    def test_mapped_exception_pickles(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=101,
                                                        category='couchbase.key_value',
                                                        strerror='document_not_found',
                                                        context=_kv_context(key='k')))
        loaded = pickle.loads(pickle.dumps(exc))
        assert type(loaded) is E.DocumentNotFoundException
        assert repr(loaded) == repr(exc)
        assert (loaded.message, loaded.error_code) == ('document not found', 101)

    def test_subclass_repr_is_prefixed_with_the_class_name(self) -> None:
        exc = E.DocumentLockedException(message='a message')
        assert repr(exc) == 'DocumentLockedException(<message=a message>)'

    def test_str_is_repr(self) -> None:
        exc = EM.ErrorMapper.build_exception(_CoreError(err_code=101, strerror='document_not_found'))
        assert str(exc) == repr(exc)

    @staticmethod
    def _multi_result(**values: Any) -> pycbc_result[dict[str, Any]]:
        # `pycbc_result` cannot be subclassed and Python cannot set its flag.
        return cast('pycbc_result[dict[str, Any]]', SimpleNamespace(raw_result=values, multi_resp_all_okay=False))

    def test_multi_op_errors_are_mapped_before_the_result_is_built(self) -> None:
        # A multi operation cannot raise per key, so its core errors arrive as values; the result
        # a caller holds must never see one.
        ret = self._multi_result(a=_CoreError(err_code=101, strerror='document_not_found'), b=pycbc_result())
        result = MultiMutationResult(*CI._parse_sub_ops(
            ret, None, True, lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k)))
        assert list(result.exceptions) == ['a']
        assert type(result.exceptions['a']) is E.DocumentNotFoundException
        assert list(result.results) == ['b']
        assert result.all_ok is False

    def test_multi_op_raises_the_first_error_after_recording_the_keys_before_it(self) -> None:
        first = _CoreError(err_code=101, strerror='document_not_found')
        second = _CoreError(err_code=105, strerror='cas_mismatch')
        ok: pycbc_result[dict[str, Any]] = pycbc_result()
        ret = self._multi_result(ok=ok, first=first, second=second)
        obs = MagicMock()
        with pytest.raises(E.DocumentNotFoundException) as exc_info:
            CI._parse_sub_ops(ret, obs, False, lambda k, v: v)
        assert obs.process_multi_sub_op.call_args_list == [call(ok), call(first, exc_val=exc_info.value)]

    @staticmethod
    def _error_tables(key_type: type) -> dict[str, dict[Any, Any]]:
        """Every module-level dict in the couchbase package that maps key_type to an exception."""
        tables: dict[str, dict[Any, Any]] = {}
        for module_info in pkgutil.walk_packages(couchbase.__path__, 'couchbase.'):
            if module_info.name.startswith('couchbase.tests'):
                continue
            try:
                module = importlib.import_module(module_info.name)
            except ModuleNotFoundError as e:
                if (e.name or '').partition('.')[0] in _OPTIONAL_DEPENDENCIES:
                    continue
                raise
            for name, obj in vars(module).items():
                if not isinstance(obj, dict) or not obj:
                    continue
                if all(isinstance(k, key_type) for k in obj) and all(
                        isinstance(v, type) and issubclass(v, E.CouchbaseException) for v in obj.values()):
                    tables[name] = obj
        return tables


class SubdocStatusMappingUnitTests:

    @pytest.mark.parametrize('status, expected', [
        (0xce, E.XattrInvalidKeyComboException),
        (0xcf, E.XattrInvalidKeyComboException),
        (0xd0, E.XattrUnknownMacroException),
        (0xd1, E.XattrUnknownVirtualAttributeException),
        (0xd2, E.XattrCannotModifyVirtualAttributeException),
        (0xd5, E.XattrUnknownMacroException),
        (0xd6, E.CannotReviveLivingDocumentException),
    ])
    def test_xattr_and_revive_statuses_map_to_their_class(self, status: int,
                                                          expected: type[E.CouchbaseException]) -> None:
        # Folded the way the core folds them (status.cxx), so a lookup_in spec raises the class
        # the same status raises for a whole operation.
        assert status in set(SubDocStatus)
        error = _subdoc_error(status, 'some.path', 'some-key')
        assert type(error) is expected
        assert 'Path=some.path, key=some-key.' in str(error)

    def test_success_status_has_no_error(self) -> None:
        assert _subdoc_error(0, 'some.path', 'some-key') is None

    def test_unknown_subdocument_status_reports_the_status(self) -> None:
        # Regression guard (PYCBC-1926): every other branch of _subdoc_error builds a subclass
        # that reads correctly; the fallback, whose whole job is to describe a status
        # nothing else recognised, was the one that constructed the base positionally and so
        # described nothing.  A caller saw `CouchbaseException: <exception str() failed>`.
        error = _subdoc_error(0xFFFF, 'some.path', 'some-key')
        assert type(error) is E.CouchbaseException
        message = str(error)
        assert 'Status=65535' in message
        assert 'path=some.path' in message
        assert 'key=some-key' in message
