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

"""How a SQL++ query's request to the gateway is made, for the cluster's and a scope's builders.

A query's request is built at the call: its options are checked, its message built, and its span's
attributes read.  Where it runs comes from its `query_context`, which a scope's builder supplies.

Parameters are encoded as JSON here and sent as bytes, never through the caller's serializer, which
decodes rows only.  A named parameter is sent without its leading `$`, which the gateway adds.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from google.protobuf.duration_pb2 import Duration

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._query_args import QueryArgs, query_args
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.mutation_state import MutationState
from couchbase.common.n1ql import QueryProfile, QueryScanConsistency
from couchbase.common.options import QueryOptions
from couchbase.protocol.couchbase2._cluster_types import ClusterOptionsBlock, QueryRequest
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2

if TYPE_CHECKING:
    from couchbase.common._observability.handler import OpAttributeOptions
    from couchbase.protocol.couchbase2._core import ClusterCore
    from couchbase.serializer import Serializer

_SCAN_CONSISTENCY = {
    QueryScanConsistency.NOT_BOUNDED: query_pb2.QueryRequest.SCAN_CONSISTENCY_NOT_BOUNDED,
    QueryScanConsistency.REQUEST_PLUS: query_pb2.QueryRequest.SCAN_CONSISTENCY_REQUEST_PLUS,
}

_PROFILE_MODE = {
    QueryProfile.OFF: query_pb2.QueryRequest.PROFILE_MODE_OFF,
    QueryProfile.PHASES: query_pb2.QueryRequest.PROFILE_MODE_PHASES,
    QueryProfile.TIMINGS: query_pb2.QueryRequest.PROFILE_MODE_TIMINGS,
}

_TUNING = ('max_parallelism', 'pipeline_batch', 'pipeline_cap', 'scan_cap')

_UINT32_MAX = 2**32 - 1

# Each name is backticked or bare.  A scope name cannot hold a `.`, so a bare context splits at its last.
_QUERY_CONTEXT = re.compile(r'(?:default:)?(?:`([^`]+)`|([^`]+))\.(?:`([^`]+)`|([^`.]+))')


def query_message(statement: str, args: QueryArgs) -> query_pb2.QueryRequest:
    opts = args.options
    request = query_pb2.QueryRequest(statement=statement,
                                     client_context_id=opts.get('client_context_id') or str(uuid.uuid4()))

    location = _parse_query_context(opts.get('query_context'))
    if location is not None:
        request.bucket_name, request.scope_name = location

    for key, wire in (('read_only', 'read_only'), ('flex_index', 'flex_index'), ('preserve_expiry', 'preserve_expiry')):
        if opts.get(key) is not None:
            setattr(request, wire, bool(opts[key]))
    if opts.get('adhoc') is False:
        request.prepared = True

    _set_consistency(request, opts.get('scan_consistency'), opts.get('consistent_with'))
    if opts.get('profile') is not None:
        request.profile_mode = _PROFILE_MODE[_enum(QueryProfile, opts['profile'], 'profile')]

    request.positional_parameters.extend(_encode(value) for value in args.positional)
    for name, value in args.named.items():
        request.named_parameters[name] = _encode(value)

    _set_tuning(request.tuning_options, opts)
    return request


def create_query_request(core: ClusterCore, statement: str, *options: object, **kwargs: Any) -> QueryRequest:
    stated: dict[str, Any] = {}
    for option in options:
        if isinstance(option, QueryOptions):
            stated = dict(option)
    core.validate_options(ClusterOptionsBlock.Query, {**stated, **kwargs})
    args = query_args(QueryOptions, options, kwargs)
    span = args.options.pop('span', None)
    parent_span = args.options.pop('parent_span', None)
    serializer: Serializer = args.options.pop('serializer', None) or core.settings.default_serializer
    timeout = timeout_seconds(args.options, core.query_timeout)
    args.options.pop('timeout', None)
    message = query_message(statement, args)
    # A statement is recorded only when it is parameterized, which keeps literal values out of the span.
    parameters = {key: value for key, value in (('positional_parameters', args.positional),
                                                ('named_parameters', args.named)) if value}
    attributes: OpAttributeOptions = {
        'statement': statement,
        'query_params': parameters,
        'parent_span': ObservableRequestHandler.maybe_get_parent_span(span=span, parent_span=parent_span),
        # The request is lazy, so its span starts when it is sent rather than when it was built.
        'use_now_as_start_time': True,
    }
    if message.HasField('bucket_name') and message.HasField('scope_name'):
        attributes['bucket_name'], attributes['scope_name'] = message.bucket_name, message.scope_name
    return QueryRequest(message=message,
                        serializer=serializer,
                        timeout=timeout,
                        readonly=bool(message.read_only),
                        span_attributes=attributes)


def _parse_query_context(value: object) -> tuple[str, str] | None:
    """A context that is not exactly a bucket and a scope sets neither; the statement then names its keyspace."""
    match = _QUERY_CONTEXT.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        return None
    bucket, bare_bucket, scope, bare_scope = match.groups()
    return bucket or bare_bucket, scope or bare_scope


def _set_consistency(request: query_pb2.QueryRequest, consistency: Any, state: Any) -> None:
    if state is not None:
        if not (isinstance(state, MutationState) and state._sv):
            raise InvalidArgumentException('consistent_with expects a MutationState holding at least one token.')
        level = None if consistency is None else _enum(QueryScanConsistency, consistency, 'scan_consistency')
        if level not in (None, QueryScanConsistency.NOT_BOUNDED):
            raise InvalidArgumentException('consistent_with cannot be combined with another scan_consistency.')
        request.consistent_with.extend(kv_pb2.MutationToken(bucket_name=token.bucket_name,
                                                            vbucket_id=token.partition_id,
                                                            vbucket_uuid=token.partition_uuid,
                                                            seq_no=token.sequence_number)
                                       for token in state._sv)
        return
    if consistency is None:
        return
    level = _enum(QueryScanConsistency, consistency, 'scan_consistency')
    if level is QueryScanConsistency.AT_PLUS:
        raise InvalidArgumentException('Cannot set scan_consistency to AT_PLUS.  Use consistent_with instead.')
    request.scan_consistency = _SCAN_CONSISTENCY[level]


def _set_tuning(tuning: query_pb2.QueryRequest.TuningOptions, opts: Mapping[str, Any]) -> None:
    # The SDK's metrics default to off, and the gateway's to on.
    tuning.disable_metrics = not opts.get('metrics', False)
    for key in _TUNING:
        value = opts.get(key)
        if value is None:
            continue
        if not isinstance(value, int) or not 0 <= value <= _UINT32_MAX:
            raise InvalidArgumentException(f'Expected {key} to be an int from 0 to {_UINT32_MAX}, got {value!r}.')
        setattr(tuning, key, value)
    scan_wait = opts.get('scan_wait')
    if scan_wait is not None:
        if not isinstance(scan_wait, timedelta):
            raise InvalidArgumentException(f'Expected scan_wait to be a timedelta, got {scan_wait!r}.')
        tuning.scan_wait.CopyFrom(_duration(scan_wait))


def _duration(value: timedelta) -> Duration:
    duration = Duration()
    duration.FromTimedelta(value)
    return duration


def _enum(enum_type: Any, value: Any, name: str) -> Any:
    try:
        return value if isinstance(value, enum_type) else enum_type(value)
    except ValueError:
        raise InvalidArgumentException(f'{value!r} is not a valid {name}.') from None


def _encode(value: Any) -> bytes:
    return json.dumps(value).encode('utf-8')
