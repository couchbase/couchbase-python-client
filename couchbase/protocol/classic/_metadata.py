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

import json
from typing import TYPE_CHECKING

from couchbase.common.analytics import (AnalyticsStatus,
                                        _AnalyticsMetaDataRecord,
                                        _AnalyticsMetricsRecord,
                                        _AnalyticsProblemRecord)
from couchbase.common.n1ql import (QueryStatus,
                                   _QueryMetaDataRecord,
                                   _QueryMetricsRecord,
                                   _QueryProblemRecord)
from couchbase.common.views import _ViewMetaDataRecord

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import (CppAnalyticsMetaData,
                                                                          CppAnalyticsProblem,
                                                                          CppQueryMetaData,
                                                                          CppQueryProblem,
                                                                          CppViewMetadata)


def _query_problem_from_core(raw: CppQueryProblem) -> _QueryProblemRecord:
    return _QueryProblemRecord(code=raw['code'],
                               message=raw['message'],
                               reason=raw.get('reason'),
                               retry=raw.get('retry'))


def _query_metadata_from_core(raw: CppQueryMetaData | None) -> _QueryMetaDataRecord | None:
    """The classic adapter: the core's metadata dict into a record, or None when there is none.

    The dict is read and never written, so the core's response is left as the core produced it.
    """
    if raw is None:
        return None

    signature = raw.get('signature')
    profile = raw.get('profile')
    metrics = raw.get('metrics')
    return _QueryMetaDataRecord(
        request_id=raw['request_id'],
        client_context_id=raw['client_context_id'],
        status=QueryStatus.from_str(raw.get('status', 'unknown')),
        signature=None if signature is None else json.loads(signature),
        profile=None if profile is None else json.loads(profile),
        warnings=tuple(_query_problem_from_core(w) for w in raw.get('warnings') or ()),
        errors=tuple(_query_problem_from_core(e) for e in raw.get('errors') or ()),
        metrics=None if not metrics else _QueryMetricsRecord(
            elapsed_time_ns=metrics.get('elapsed_time', 0),
            execution_time_ns=metrics.get('execution_time', 0),
            result_count=metrics.get('result_count', 0),
            result_size=metrics.get('result_size', 0),
            mutation_count=metrics.get('mutation_count', 0),
            sort_count=metrics.get('sort_count', 0),
            error_count=metrics.get('error_count', 0),
            warning_count=metrics.get('warning_count', 0)))


def _analytics_problem_from_core(raw: CppAnalyticsProblem) -> _AnalyticsProblemRecord:
    return _AnalyticsProblemRecord(code=raw['code'], message=raw['message'])


def _analytics_metadata_from_core(raw: CppAnalyticsMetaData | None) -> _AnalyticsMetaDataRecord | None:
    """The classic adapter: the core's metadata dict into a record, or None when there is none.

    The dict is read and never written, so the core's response is left as the core produced it.
    The core sends no ``mutation_count`` or ``sort_count``, so both are None in the record.
    """
    if raw is None:
        return None

    signature = raw.get('signature')
    metrics = raw.get('metrics')
    return _AnalyticsMetaDataRecord(
        request_id=raw['request_id'],
        client_context_id=raw['client_context_id'],
        status=AnalyticsStatus.from_str(raw.get('status', 'unknown')),
        signature=None if signature is None else json.loads(signature),
        warnings=tuple(_analytics_problem_from_core(w) for w in raw.get('warnings') or ()),
        errors=tuple(_analytics_problem_from_core(e) for e in raw.get('errors') or ()),
        metrics=None if not metrics else _AnalyticsMetricsRecord(
            elapsed_time_ns=metrics.get('elapsed_time', 0),
            execution_time_ns=metrics.get('execution_time', 0),
            result_count=metrics.get('result_count', 0),
            result_size=metrics.get('result_size', 0),
            error_count=metrics.get('error_count', 0),
            processed_objects=metrics.get('processed_objects', 0),
            warning_count=metrics.get('warning_count', 0),
            mutation_count=None,
            sort_count=None))


def _view_metadata_from_core(raw: CppViewMetadata | None) -> _ViewMetaDataRecord | None:
    """The classic adapter: the core's metadata dict into a record, or None when there is none.

    The dict is read and never written, so the core's response is left as the core produced it.
    The core always sends both keys, with None for what the server left out: ``total_rows`` for a
    reduced query, and ``debug_info`` unless the query asked for it.
    """
    if not raw:
        return None

    return _ViewMetaDataRecord(total_rows=raw.get('total_rows'),
                               debug_info=raw.get('debug_info'))
