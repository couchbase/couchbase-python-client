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

"""The requests a collection's builder returns, and the records the gateway's answers become.

The records are the ones classic fills, so a result class cannot tell which protocol answered.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Any,
                    Generic,
                    TypeAlias,
                    TypeVar)

import grpc
from google.protobuf.message import Message

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.exceptions import (CouchbaseException,
                                         InternalServerFailureException,
                                         PathNotFoundException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.result import (_CounterRecord,
                                     _ExistsRecord,
                                     _GetRecord,
                                     _LookupInFieldRecord,
                                     _LookupInRecord,
                                     _MutateInFieldRecord,
                                     _MutateInRecord,
                                     _MutationRecord,
                                     _MutationTokenRecord)
from couchbase.common.subdocument import SubDocOp
from couchbase.protocol.couchbase2._error_mapping import map_status

if TYPE_CHECKING:
    from google.rpc import status_pb2

    from couchbase.common._observability import ObservableRequestHandler
    from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2
    from couchbase.serializer import Serializer
    from couchbase.transcoder import Transcoder

_STATUS_CODES = {code.value[0]: code for code in grpc.StatusCode}

_M = TypeVar('_M', bound=Message)

if TYPE_CHECKING:
    _Read: TypeAlias = (kv_pb2.GetResponse
                        | kv_pb2.GetAndTouchResponse
                        | kv_pb2.GetAndLockResponse)
    _Mutation: TypeAlias = (kv_pb2.InsertResponse
                            | kv_pb2.UpsertResponse
                            | kv_pb2.ReplaceResponse
                            | kv_pb2.RemoveResponse
                            | kv_pb2.TouchResponse
                            | kv_pb2.AppendResponse
                            | kv_pb2.PrependResponse
                            | kv_pb2.MutateInResponse)


class CollectionOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    Append = 'AppendOptions'
    AppendMulti = 'AppendMultiOptions'
    Decrement = 'DecrementOptions'
    DecrementMulti = 'DecrementMultiOptions'
    Exists = 'ExistsOptions'
    ExistsMulti = 'ExistsMultiOptions'
    Get = 'GetOptions'
    GetAndLock = 'GetAndLockOptions'
    GetAndLockMulti = 'GetAndLockMultiOptions'
    GetAndTouch = 'GetAndTouchOptions'
    GetMulti = 'GetMultiOptions'
    Increment = 'IncrementOptions'
    IncrementMulti = 'IncrementMultiOptions'
    Insert = 'InsertOptions'
    InsertMulti = 'InsertMultiOptions'
    LookupIn = 'LookupInOptions'
    MutateIn = 'MutateInOptions'
    Prepend = 'PrependOptions'
    PrependMulti = 'PrependMultiOptions'
    Remove = 'RemoveOptions'
    RemoveMulti = 'RemoveMultiOptions'
    Replace = 'ReplaceOptions'
    ReplaceMulti = 'ReplaceMultiOptions'
    Touch = 'TouchOptions'
    TouchMulti = 'TouchMultiOptions'
    Unlock = 'UnlockOptions'
    UnlockMulti = 'UnlockMultiOptions'
    Upsert = 'UpsertOptions'
    UpsertMulti = 'UpsertMultiOptions'


@dataclass(frozen=True)
class GetRequest:
    message: kv_pb2.GetRequest
    transcoder: Transcoder
    with_expiry: bool
    timeout: float
    obs_handler: ObservableRequestHandler | None


@dataclass(frozen=True)
class MultiOptions:
    """A multi operation's options for each key's single operation, and whether a key's error is raised."""
    by_key: dict[str, dict[str, Any]]
    return_exceptions: bool


@dataclass(frozen=True)
class KvRequest(Generic[_M]):
    message: _M
    timeout: float
    obs_handler: ObservableRequestHandler | None


@dataclass(frozen=True)
class ReadRequest(Generic[_M]):
    """A request whose answer carries a document, which `transcoder` decodes."""
    message: _M
    transcoder: Transcoder
    timeout: float
    obs_handler: ObservableRequestHandler | None


@dataclass(frozen=True)
class SubdocSpec:
    """A spec as the caller wrote it, read back against the answer to the spec sent in its place."""
    opcode: SubDocOp
    path: str
    original_index: int


@dataclass(frozen=True)
class LookupInRequest:
    """`specs` is in the order the specs were sent, which is not the caller's; see `xattrs_first`."""
    message: kv_pb2.LookupInRequest
    specs: tuple[SubdocSpec, ...]
    serializer: Serializer
    timeout: float
    obs_handler: ObservableRequestHandler | None


@dataclass(frozen=True)
class MutateInRequest:
    """`specs` is in the order the specs were sent, which is not the caller's; see `xattrs_first`."""
    message: kv_pb2.MutateInRequest
    specs: tuple[SubdocSpec, ...]
    serializer: Serializer
    timeout: float
    obs_handler: ObservableRequestHandler | None


def xattrs_first(xattr: Sequence[bool]) -> list[int]:
    """The order to send a request's specs in: the indexes of the xattr specs, then the rest, each as written.

    The server needs the xattr specs first, and the gateway moves them there itself, but puts its
    answers back by the inverse of that move (ING-2851): whenever an xattr spec follows a body spec,
    the answers come back at the wrong indexes.  Sent already in this order, the gateway's move
    changes nothing, so the answers line up with what was sent and the SDK puts them back itself.
    """
    return ([i for i, x in enumerate(xattr) if x]
            + [i for i, x in enumerate(xattr) if not x])


def get_record(response: _Read, with_expiry: bool = False) -> _GetRecord:
    """The gateway always sends the expiry, and the record carries it only when the caller asked.

    A projection matching none of its paths comes back from the gateway as JSON `null` (ING-2850),
    and is returned as it came.
    """
    expiry = response.expiry.seconds if with_expiry and response.HasField('expiry') else None
    return _GetRecord(cas=response.cas,
                      value=_content(response),
                      flags=response.content_flags,
                      expiry=expiry)


def mutation_record(response: _Mutation) -> _MutationRecord:
    return _MutationRecord(cas=response.cas, mutation_token=_mutation_token(response))


def counter_record(response: kv_pb2.IncrementResponse | kv_pb2.DecrementResponse) -> _CounterRecord:
    return _CounterRecord(cas=response.cas, content=response.content, mutation_token=_mutation_token(response))


def exists_record(response: kv_pb2.ExistsResponse) -> _ExistsRecord:
    """The gateway says only whether the document exists and its CAS, so the rest is unknown."""
    return _ExistsRecord(cas=response.cas,
                         exists=response.result,
                         deleted=None,
                         flags=None,
                         expiry=None,
                         sequence_number=None,
                         datatype=None)


def lookup_in_record(response: kv_pb2.LookupInResponse,
                     specs: tuple[SubdocSpec, ...],
                     key: str) -> _LookupInRecord:
    """Each spec's answer as classic's field record, in the order the caller wrote the specs.

    The gateway answers `exists` with the content `true` or `false` and no status, where classic
    reports a path that is not there as a `PathNotFoundException`, so that is how `false` is read.
    A field that fails is the exception its status maps to, with its message.  The gateway does not
    say whether the document is a tombstone, so `deleted` is unknown.
    """
    if len(response.specs) != len(specs):
        raise InternalServerFailureException(f'The gateway answered {len(specs)} lookup_in specs with '
                                             f'{len(response.specs)} results.')
    fields = []
    for spec, answer in zip(specs, response.specs):
        error = _field_error(answer.status, spec, key) if answer.HasField('status') else None
        value = answer.content if error is None else None
        if spec.opcode == SubDocOp.EXISTS and error is None:
            if value == b'false':
                error = PathNotFoundException(f'Path could not be found. Path={spec.path}, key={key}.')
            value = None
        fields.append(_LookupInFieldRecord(path=spec.path,
                                           value=value or None,
                                           original_index=spec.original_index,
                                           exists=error is None,
                                           opcode=spec.opcode.value,
                                           status=None,
                                           ec=None,
                                           error=error))
    fields.sort(key=lambda f: f.original_index)
    return _LookupInRecord(cas=response.cas, fields=tuple(fields), deleted=None)


def mutate_in_record(response: kv_pb2.MutateInResponse, specs: tuple[SubdocSpec, ...]) -> _MutateInRecord:
    """Only a counter's answer carries content, the counter's new value."""
    if len(response.specs) != len(specs):
        raise InternalServerFailureException(f'The gateway answered {len(specs)} mutate_in specs with '
                                             f'{len(response.specs)} results.')
    fields = sorted((_MutateInFieldRecord(path=spec.path,
                                          value=answer.content or None,
                                          original_index=spec.original_index,
                                          opcode=spec.opcode.value,
                                          status=None,
                                          ec=None,
                                          error=None)
                     for spec, answer in zip(specs, response.specs)),
                    key=lambda f: f.original_index)
    return _MutateInRecord(cas=response.cas,
                           mutation_token=mutation_record(response).mutation_token,
                           fields=tuple(fields),
                           deleted=None)


def _field_error(status: status_pb2.Status, spec: SubdocSpec, key: str) -> CouchbaseException | None:
    code = _STATUS_CODES.get(status.code, grpc.StatusCode.UNKNOWN)
    if code == grpc.StatusCode.OK:
        return None
    return map_status(code, status.message, status, readonly=True).exception(path=spec.path, key=key)


def _mutation_token(response: _Mutation | kv_pb2.IncrementResponse | kv_pb2.DecrementResponse
                    ) -> _MutationTokenRecord | None:
    if not response.HasField('mutation_token'):
        return None
    raw = response.mutation_token
    return _MutationTokenRecord(partition_id=raw.vbucket_id,
                                partition_uuid=raw.vbucket_uuid,
                                sequence_number=raw.seq_no,
                                bucket_name=raw.bucket_name)


def _content(response: _Read) -> bytes:
    # The SDK never asks for compression, so compressed content is as wrong as none.
    if response.WhichOneof('content') != 'content_uncompressed':
        raise InternalServerFailureException('The gateway answered a get without uncompressed content.')
    return response.content_uncompressed
