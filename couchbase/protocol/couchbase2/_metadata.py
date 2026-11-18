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

"""The metadata the gateway's answers carry, as the records classic fills."""

from __future__ import annotations

import json

from couchbase.common.n1ql import (QueryStatus,
                                   _QueryMetaDataRecord,
                                   _QueryMetricsRecord,
                                   _QueryProblemRecord)
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2


def query_metadata(response: query_pb2.QueryResponse) -> _QueryMetaDataRecord | None:
    """The metadata a stream's last message carries, or `None` from any other.

    Only `metrics` and `profile` have presence on the wire.  An unset status is the enum's zero,
    which is RUNNING, so a message without one reads as RUNNING where classic reads UNKNOWN; the
    gateway always sends one.  An empty signature, and a profile that is absent or JSON null,
    read as None.
    """
    if not response.HasField('meta_data'):
        return None
    raw = response.meta_data
    metrics = None
    if raw.HasField('metrics'):
        m = raw.metrics
        metrics = _QueryMetricsRecord(elapsed_time_ns=m.elapsed_time.ToNanoseconds(),
                                      execution_time_ns=m.execution_time.ToNanoseconds(),
                                      result_count=m.result_count,
                                      result_size=m.result_size,
                                      mutation_count=m.mutation_count,
                                      sort_count=m.sort_count,
                                      error_count=m.error_count,
                                      warning_count=m.warning_count)
    warnings = tuple(_QueryProblemRecord(code=w.code, message=w.message, reason=None, retry=None) for w in raw.warnings)
    # The gateway reports a query's errors as the call's status, so the metadata carries none.
    return _QueryMetaDataRecord(request_id=raw.request_id,
                                client_context_id=raw.client_context_id,
                                status=_status(raw.status),
                                signature=json.loads(raw.signature) if raw.signature else None,
                                profile=json.loads(raw.profile) if raw.HasField('profile') and raw.profile else None,
                                warnings=warnings,
                                errors=(),
                                metrics=metrics)


def _status(value: int) -> QueryStatus:
    # A status this SDK cannot name comes from a newer gateway, and reads as UNKNOWN.
    try:
        name = query_pb2.QueryResponse.MetaData.Status.Name(value)
    except ValueError:
        return QueryStatus.UNKNOWN
    return QueryStatus.__members__.get(name.removeprefix('STATUS_'), QueryStatus.UNKNOWN)
