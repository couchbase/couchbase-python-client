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

__all__ = [
    'ClusterInfoResult',
    'ContentProxy',
    'ContentSubdocProxy',
    'CounterResult',
    'DiagnosticsResult',
    'ExistsResult',
    'GetReplicaResult',
    'GetResult',
    'LookupInReplicaResult',
    'LookupInResult',
    'MultiCounterResult',
    'MultiExistsResult',
    'MultiGetReplicaResult',
    'MultiGetResult',
    'MultiMutationResult',
    'MultiResult',
    'MultiResultType',
    'MutateInResult',
    'MutationResult',
    'MutationToken',
    'OperationResult',
    'PingResult',
    'Result',
    'ScanResult',
    'ValueResult',
]

import json
import sys
from collections.abc import Callable
from dataclasses import (asdict,
                         dataclass,
                         field)
from datetime import datetime
from typing import (Any,
                    Generic,
                    TypedDict,
                    cast)

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

from couchbase.common._supportability import Supportability
from couchbase.common.diagnostics import (ClusterState,
                                          EndpointDiagnosticsReport,
                                          EndpointPingReport,
                                          EndpointState,
                                          ServiceType,
                                          _EndpointDiagnosticsRecord,
                                          _EndpointPingRecord)
from couchbase.common.exceptions import (CouchbaseException,
                                         InvalidArgumentException,
                                         InvalidIndexException,
                                         PathNotFoundException)
from couchbase.common.serializer import Serializer
from couchbase.common.subdocument import SubDocOp
from couchbase.common.transcoder import Transcoder

# Each record below holds one key-value response in a shape no protocol owns. A protocol fills it
# from its own wire format, and the result class reads it without knowing which protocol did.
# Document bodies stay encoded and expiries stay in seconds since the epoch, so the decoding and
# the datetime conversion run in the accessor, once, whichever protocol filled the record.


@dataclass(frozen=True)
class _MutationTokenRecord:
    partition_id: int
    partition_uuid: int
    sequence_number: int
    bucket_name: str


@dataclass(frozen=True)
class _GetRecord:
    cas: int
    value: bytes
    flags: int
    expiry: int | None


@dataclass(frozen=True)
class _GetReplicaRecord:
    cas: int
    value: bytes
    flags: int
    is_replica: bool


@dataclass(frozen=True)
class _ExistsRecord:
    cas: int
    exists: bool
    deleted: bool | None
    flags: int | None
    expiry: int | None
    sequence_number: int | None
    datatype: int | None


@dataclass(frozen=True)
class _MutationRecord:
    cas: int
    mutation_token: _MutationTokenRecord | None


@dataclass(frozen=True)
class _CounterRecord:
    cas: int
    content: int
    mutation_token: _MutationTokenRecord | None


class _ErrorCodeDict(TypedDict):
    code: int
    message: str


# The subdocument field records keep the core's key names and order, because `value` hands each
# field to the caller as a dict.  `status` and `ec` are carried for that dict alone: a protocol
# with no equivalent leaves them None, and nothing here reads them.  `error` is what a protocol
# resolved the field's outcome to, and `content_as` raises it.  It is left out of equality because
# exceptions compare by identity, and it is derived from the status and path, which are compared.
@dataclass(frozen=True)
class _LookupInFieldRecord:
    path: str
    value: bytes | None
    original_index: int
    exists: bool
    opcode: int
    status: int | None
    ec: _ErrorCodeDict | None
    error: CouchbaseException | None = field(compare=False)


@dataclass(frozen=True)
class _MutateInFieldRecord:
    path: str
    value: bytes | None
    original_index: int
    opcode: int
    status: int | None
    ec: _ErrorCodeDict | None
    error: CouchbaseException | None = field(compare=False)


@dataclass(frozen=True)
class _LookupInRecord:
    cas: int
    fields: tuple[_LookupInFieldRecord, ...]
    deleted: bool | None


@dataclass(frozen=True)
class _LookupInReplicaRecord:
    cas: int
    fields: tuple[_LookupInFieldRecord, ...]
    deleted: bool | None
    is_replica: bool


@dataclass(frozen=True)
class _MutateInRecord:
    cas: int
    mutation_token: _MutationTokenRecord | None
    fields: tuple[_MutateInFieldRecord, ...]
    deleted: bool | None


@dataclass(frozen=True)
class _ScanItemBodyRecord:
    cas: int
    value: bytes
    flags: int
    expiry: int | None
    sequence_number: int
    datatype: int | None


@dataclass(frozen=True)
class _ScanItemRecord:
    id: str
    body: _ScanItemBodyRecord | None


@dataclass(frozen=True)
class _DiagnosticsRecord:
    """``services`` pairs each :class:`ServiceType` value with its endpoints, in the order the protocol sent them."""
    id: str
    sdk: str
    version: int
    services: tuple[tuple[str, tuple[_EndpointDiagnosticsRecord, ...]], ...]


@dataclass(frozen=True)
class _PingRecord:
    """``services`` pairs each :class:`ServiceType` value with its endpoints, in the order the protocol sent them."""
    id: str
    sdk: str
    version: int
    services: tuple[tuple[str, tuple[_EndpointPingRecord, ...]], ...]


@dataclass(frozen=True)
class _ServerVersionRecord:
    """A server's version as parts, and as the server spelled it when the protocol sends a string.

    A part the protocol does not carry, or that does not parse, is ``None``.
    """
    full: str | None
    major: int | None
    minor: int | None
    patch: int | None
    build: int | None
    edition: str | None


@dataclass(frozen=True)
class _ClusterInfoNodeRecord:
    uuid: str
    otp_node: str
    status: str
    hostname: str
    os: str
    version: _ServerVersionRecord
    services: tuple[str, ...]


class _ClusterNodeDict(TypedDict):
    uuid: str
    otp_node: str
    status: str
    hostname: str
    os: str
    version: str | None
    services: list[str]


@dataclass(frozen=True)
class _ClusterInfoBucketRecord:
    uuid: str
    name: str


@dataclass(frozen=True)
class _ClusterInfoRecord:
    nodes: tuple[_ClusterInfoNodeRecord, ...]
    buckets: tuple[_ClusterInfoBucketRecord, ...]
    services: frozenset[str]


def _expiry_as_datetime(expiry: int | None) -> datetime | None:
    # 0 is how the server says the document never expires.
    if expiry:
        return datetime.fromtimestamp(expiry)
    return None


def _decode_subdoc_fields(serializer: Serializer,
                          fields: tuple[_LookupInFieldRecord | _MutateInFieldRecord, ...]
                          ) -> list[dict[str, Any]]:
    decoded = []
    for record in fields:
        entry = asdict(record)
        del entry['error']
        encoded = entry.pop('value')
        if encoded:
            entry['value'] = _decode_subdoc_value(serializer, encoded)
        decoded.append(entry)
    return decoded


def _decode_subdoc_value(serializer: Serializer, encoded: bytes | None) -> Any:
    if not encoded:
        return None
    return serializer.deserialize(encoded)


def _subdoc_field_at(fields: tuple[_LookupInFieldRecord | _MutateInFieldRecord, ...],
                     index: int
                     ) -> _LookupInFieldRecord | _MutateInFieldRecord:
    if index > len(fields) - 1 or index < 0:
        raise InvalidIndexException(f"Provided index is invalid. Index={index}.")
    return fields[index]


def _parse_subdocument_content_as(serializer: Serializer,
                                  fields: tuple[_LookupInFieldRecord | _MutateInFieldRecord, ...],
                                  index: int
                                  ) -> Any:
    record = _subdoc_field_at(fields, index)
    if record.opcode == SubDocOp.EXISTS:
        return _parse_subdocument_exists(fields, index)
    if record.error is not None:
        raise record.error
    return _decode_subdoc_value(serializer, record.value)


def _parse_subdocument_exists(fields: tuple[_LookupInFieldRecord | _MutateInFieldRecord, ...],
                              index: int
                              ) -> bool:
    record = _subdoc_field_at(fields, index)
    if record.error is None:
        return True
    if isinstance(record.error, PathNotFoundException):
        return False
    raise record.error


class Result:
    """The base of the results of the key-value operations."""

    def __init__(self,
                 record: Any,
                 transcoder: Transcoder | None = None,
                 key: str | None = None,
                 ) -> None:
        self._record = record
        self._transcoder = transcoder
        self._decoded_value: Any | None = None
        self._key = key

    @property
    def value(self) -> Any:
        """The content of the document, if it exists."""
        if self._decoded_value is None:
            self._decoded_value = self._decode_value()
        return self._decoded_value

    @property
    def cas(self) -> int:
        """The CAS of the document."""
        return self._record.cas

    @property
    def flags(self) -> int:
        """The document's flags, which the transcoder reads to decode it."""
        # Only a response that carries a document carries its flags; the rest read 0.
        return 0

    @property
    def key(self) -> str | None:
        """The key of the document the operation was on, if known."""
        return self._key

    @property
    def success(self) -> bool:
        """Whether the operation succeeded."""
        return self._record.cas != 0

    def _decode_value(self) -> Any | None:
        return None


_T = TypeVar('_T')


class ContentProxy:
    """What a result's ``content_as`` returns: ``content_as[dict]`` is the content as a ``dict``."""

    def __init__(self, content: Any) -> None:
        self._content = content

    def __getitem__(self, type_: type[_T] | Callable[[Any], _T]) -> _T:
        """Get the content, converted by calling ``type_`` on it.

        Args:
            type_: The type, or any callable, to convert the content with.

        Returns:
            The converted content.
        """
        return cast(Callable[[Any], _T], type_)(self._content)


class ContentSubdocProxy:
    """What a sub-document result's ``content_as`` returns: ``content_as[dict](0)`` is the first field as a ``dict``."""

    def __init__(self,
                 serializer: Serializer,
                 fields: tuple[_LookupInFieldRecord | _MutateInFieldRecord, ...],
                 ) -> None:
        self._serializer = serializer
        self._fields = fields

    def _parse_content_at_index(self, index: int, type_: type[_T] | Callable[[Any], _T]) -> _T:
        item = _parse_subdocument_content_as(self._serializer, self._fields, index)
        return cast(Callable[[Any], _T], type_)(item)

    def __getitem__(self, type_: type[_T] | Callable[[Any], _T]) -> Callable[[int], _T]:
        """Get a function that takes a field's index and returns the field, converted by calling ``type_`` on it.

        Args:
            type_: The type, or any callable, to convert the field with.

        Returns:
            The function.
        """
        return lambda index: self._parse_content_at_index(index, type_)


class GetReplicaResult(Result):
    """The result of reading a document from the active node or a replica."""

    _record: _GetReplicaRecord

    def __init__(self,
                 record: _GetReplicaRecord,
                 transcoder: Transcoder | None = None,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, transcoder=transcoder, key=key)

    @property
    def flags(self) -> int:
        """The document's flags, which the transcoder reads to decode it."""
        return self._record.flags

    @property
    def is_active(self) -> bool:
        """Whether the document was read from the active node.

        .. deprecated:: 4.0.3
            Use :attr:`is_replica` instead.
        """
        Supportability.class_property_deprecated('GetReplicaResult.is_active', 'GetReplicaResult.is_replica')
        return not self._record.is_replica

    @property
    def is_replica(self) -> bool:
        """Whether the document was read from a replica."""
        return self._record.is_replica

    @property
    def content_as(self) -> ContentProxy:
        """The contents of the document.

        Get the value as a dict::

            res = collection.get_replica(key)
            value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    def _decode_value(self) -> Any | None:
        if self._transcoder is None:
            return None
        return self._transcoder.decode_value(self._record.value, self._record.flags)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class GetResult(Result):
    """The result of reading a document."""

    _record: _GetRecord

    def __init__(self,
                 record: _GetRecord,
                 transcoder: Transcoder | None = None,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, transcoder=transcoder, key=key)

    @property
    def flags(self) -> int:
        """The document's flags, which the transcoder reads to decode it."""
        return self._record.flags

    @property
    def expiry_time(self) -> datetime | None:
        """The expiry of the document, if it was requested."""
        return _expiry_as_datetime(self._record.expiry)

    @property
    def expiryTime(self) -> datetime | None:
        """The expiry of the document, if it was requested.

        .. deprecated:: 4.0.0
            Use :attr:`expiry_time` instead.
        """
        Supportability.class_property_deprecated('GetResult.expiryTime', 'GetResult.expiry_time')
        return _expiry_as_datetime(self._record.expiry)

    @property
    def content_as(self) -> ContentProxy:
        """The contents of the document.

        Get the value as a dict::

            res = collection.get(key)
            value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    def _decode_value(self) -> Any | None:
        if self._transcoder is None:
            return None
        return self._transcoder.decode_value(self._record.value, self._record.flags)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class ExistsResult(Result):
    """The result of checking whether a document exists."""

    _record: _ExistsRecord

    def __init__(self,
                 record: _ExistsRecord,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, key=key)

    @property
    def flags(self) -> int:
        """The document's flags, which the transcoder reads to decode it."""
        flags = self._record.flags
        return 0 if flags is None else flags

    @property
    def exists(self) -> bool:
        """Whether the document exists."""
        return self._record.exists

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class _MutationTokenDict(TypedDict):
    partition_id: int
    partition_uuid: int
    sequence_number: int
    bucket_name: str


class MutationToken:
    """Identifies one mutation, so a later query or search can be made consistent with it.

    Pass it to :class:`~couchbase.mutation_state.MutationState`.
    """

    def __init__(self, token: _MutationTokenDict) -> None:
        self._token = token

    @property
    def partition_id(self) -> int:
        """The ID of the partition the document is in."""
        return self._token['partition_id']

    @property
    def partition_uuid(self) -> int:
        """The UUID of the partition's history when the mutation was made."""
        return self._token['partition_uuid']

    @property
    def sequence_number(self) -> int:
        """The mutation's sequence number within the partition."""
        return self._token['sequence_number']

    @property
    def bucket_name(self) -> str:
        """The name of the bucket the document is in."""
        return self._token['bucket_name']

    def as_tuple(self) -> tuple[int, int, int, str]:
        """Get the token's fields as a tuple.

        Returns:
            tuple[int, int, int, str]: The partition ID, partition UUID, sequence number and bucket name.
        """
        return (self.partition_id, self.partition_uuid,
                self.sequence_number, self.bucket_name)

    def as_dict(self) -> _MutationTokenDict:
        """Get the token's fields as a dict.

        Returns:
            dict[str, int | str]: The token's fields, by name.
        """
        return self._token

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._token})'

    def __hash__(self) -> int:
        return hash(self.as_tuple())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MutationToken):
            return False
        return (self.partition_id == other.partition_id
                and self.partition_uuid == other.partition_uuid
                and self.sequence_number == other.sequence_number
                and self.bucket_name == other.bucket_name)


class MutationResult(Result):
    """The result of an operation that changed a document."""

    _record: _MutationRecord | _CounterRecord | _MutateInRecord

    def __init__(self,
                 record: _MutationRecord | _CounterRecord | _MutateInRecord,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, key=key)
        self._mutation_token: MutationToken | None = None

    def mutation_token(self) -> MutationToken | None:
        """Get the operation's mutation token, if it has one.

        Returns:
            :class:`.MutationToken` | None: The operation's mutation token.
        """
        token = self._record.mutation_token
        if token is not None and self._mutation_token is None:
            # A fresh dict each build, so the token a caller holds is the caller's to change.
            self._mutation_token = MutationToken({'partition_id': token.partition_id,
                                                  'partition_uuid': token.partition_uuid,
                                                  'sequence_number': token.sequence_number,
                                                  'bucket_name': token.bucket_name})
        return self._mutation_token

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class LookupInResult(Result):
    """The result of reading paths within a document."""

    _record: _LookupInRecord

    def __init__(self,
                 record: _LookupInRecord,
                 serializer: Serializer,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, key=key)
        self._serializer = serializer

    def exists(self, index: int) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """
        return _parse_subdocument_exists(self._record.fields, index)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """A proxy to return the value at the specified index.

        Get first value as a dict::

            res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
            value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self._serializer, self._record.fields)

    def _decode_value(self) -> Any | None:
        return _decode_subdoc_fields(self._serializer, self._record.fields)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class LookupInReplicaResult(Result):
    """The result of reading paths within a document from the active node or a replica."""

    _record: _LookupInReplicaRecord

    def __init__(self,
                 record: _LookupInReplicaRecord,
                 serializer: Serializer,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, key=key)
        self._serializer = serializer

    def exists(self, index: int) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """
        return _parse_subdocument_exists(self._record.fields, index)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """A proxy to return the value at the specified index.

        Get first value as a dict::

            res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
            value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self._serializer, self._record.fields)

    @property
    def is_replica(self) -> bool:
        """Whether the document was read from a replica."""
        return self._record.is_replica

    def _decode_value(self) -> Any | None:
        return _decode_subdoc_fields(self._serializer, self._record.fields)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class MutateInResult(MutationResult):
    """The result of changing paths within a document."""

    _record: _MutateInRecord

    def __init__(self,
                 record: _MutateInRecord,
                 serializer: Serializer,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, key=key)
        self._serializer = serializer

    @property
    def content_as(self) -> ContentSubdocProxy:
        """A proxy to return the value at the specified index.

        Get first value as a str::

            res = collection.mutate_in(key, (SD.upsert("city", "New City"),
                                            SD.replace("faa", "CTY")))
            value = res.content_as[str](0)
        """
        return ContentSubdocProxy(self._serializer, self._record.fields)

    def _decode_value(self) -> Any | None:
        return _decode_subdoc_fields(self._serializer, self._record.fields)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class CounterResult(MutationResult):
    """The result of incrementing or decrementing a counter document."""

    _record: _CounterRecord

    def __init__(self,
                 record: _CounterRecord,
                 key: str | None = None,
                 ) -> None:
        super().__init__(record, key=key)

    # Uncomment and delete previous property when ready to remove cas CounterResult.
    # cas = RemoveProperty('cas')

    @property
    def cas(self) -> int:
        """The CAS of the document.

        .. deprecated:: 4.0.3
        """
        Supportability.class_property_deprecated('CounterResult.cas')
        return self._record.cas

    @property
    def content(self) -> int:
        """The value of the document after the operation completed."""
        return self._record.content

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class ScanResult(Result):
    """One document a range scan returned."""

    _record: _ScanItemRecord

    def __init__(self,
                 record: _ScanItemRecord,
                 ids_only: bool,
                 transcoder: Transcoder | None,
                 ) -> None:
        super().__init__(record, transcoder=transcoder)
        self._ids_only = ids_only

    @property
    def id(self) -> str:
        """The document's ID."""
        return self._record.id

    @property
    def ids_only(self) -> bool:
        """Whether the scan was asked for IDs only, in which case the result carries no document."""
        return self._ids_only

    @property
    def cas(self) -> int:
        """The CAS of the document."""
        if self.ids_only:
            raise InvalidArgumentException(("No cas available when scan is requested with "
                                            "`ScanOptions` ids_only set to True."))
        body = self._record.body
        return 0 if body is None else body.cas

    @property
    def flags(self) -> int:
        """The document's flags, which the transcoder reads to decode it."""
        body = self._record.body
        return 0 if body is None else body.flags

    @property
    def expiry_time(self) -> datetime | None:
        """The expiry of the document, if it was requested."""
        if self.ids_only:
            raise InvalidArgumentException(("No expiry_time available when scan is requested with "
                                            "`ScanOptions` ids_only set to True."))
        body = self._record.body
        return None if body is None else _expiry_as_datetime(body.expiry)

    @property
    def content_as(self) -> ContentProxy:
        """The contents of the document.

        Get the value as a dict::

            for res in collection.scan(RangeScan()):
                value = res.content_as[dict]

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the scan was asked for IDs only.
        """
        if self.ids_only:
            raise InvalidArgumentException(("No content available when scan is requested with "
                                            "`ScanOptions` ids_only set to True."))
        return ContentProxy(self.value)

    def _decode_value(self) -> Any | None:
        body = self._record.body
        if body is None or self._transcoder is None:
            return None
        return self._transcoder.decode_value(body.value, body.flags)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class MultiResult:
    """The base of the results of ``get_multi`` and the replica ``*_multi`` operations."""

    def __init__(self,
                 results: dict[str, Any],
                 all_ok: bool,
                 result_type: type[GetReplicaResult] | type[GetResult]) -> None:
        self._results = results
        self._all_ok = all_ok
        self._result_type = result_type

    @property
    def all_ok(self) -> bool:
        """Whether every key's operation succeeded."""
        return self._all_ok

    @property
    def exceptions(self) -> dict[str, CouchbaseException]:
        """The exception each failed key's operation raised, by key."""
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, self._result_type) and not isinstance(v, list):
                exc[k] = v
        return exc


# get_any_replica_multi maps each key to a single GetReplicaResult, get_all_replicas_multi maps
# each key to a list of them.  Defaulted to Any so that a bare MultiGetReplicaResult annotation
# keeps meaning what it means today.
_ReplicaValueType = TypeVar('_ReplicaValueType',
                            bound=GetReplicaResult | list[GetReplicaResult],
                            default=Any)


class MultiGetReplicaResult(MultiResult, Generic[_ReplicaValueType]):
    """The result of ``get_any_replica_multi`` or ``get_all_replicas_multi``."""

    def __init__(self,
                 results: dict[str, _ReplicaValueType | CouchbaseException],
                 all_ok: bool) -> None:
        super().__init__(results, all_ok, GetReplicaResult)

    @property
    def results(self) -> dict[str, _ReplicaValueType]:
        """The result of each key whose operation succeeded, by key.

        A :class:`.GetReplicaResult` for ``get_any_replica_multi``, a list of them for ``get_all_replicas_multi``.
        """
        res = {}
        for k, v in self._results.items():
            okay = (isinstance(v, GetReplicaResult) or
                    isinstance(v, list) and all(map(lambda r: isinstance(r, GetReplicaResult), v)))
            if okay:
                res[k] = v
        return res

    def __repr__(self) -> str:
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'{type(self).__name__}({", ".join(output_results)})'


class MultiGetResult(MultiResult):
    """The result of ``get_multi``."""

    def __init__(self,
                 results: dict[str, GetResult | CouchbaseException],
                 all_ok: bool) -> None:
        super().__init__(results, all_ok, GetResult)

    @property
    def results(self) -> dict[str, GetResult]:
        """The result of each key whose operation succeeded, by key."""
        res = {}
        for k, v in self._results.items():
            if isinstance(v, GetResult):
                res[k] = v
        return res

    def __repr__(self) -> str:
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'{type(self).__name__}({", ".join(output_results)})'


class MultiExistsResult:
    """The result of ``exists_multi``."""

    def __init__(self,
                 results: dict[str, ExistsResult | CouchbaseException],
                 all_ok: bool) -> None:
        self._results = results
        self._all_ok = all_ok

    @property
    def all_ok(self) -> bool:
        """Whether every key's operation succeeded."""
        return self._all_ok

    @property
    def exceptions(self) -> dict[str, CouchbaseException]:
        """The exception each failed key's operation raised, by key."""
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, ExistsResult):
                exc[k] = v
        return exc

    @property
    def results(self) -> dict[str, ExistsResult]:
        """The result of each key whose operation succeeded, by key."""
        res = {}
        for k, v in self._results.items():
            if isinstance(v, ExistsResult):
                res[k] = v
        return res

    def __repr__(self) -> str:
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'{type(self).__name__}({", ".join(output_results)})'


class MultiMutationResult:
    """The result of a multi-key mutation such as ``upsert_multi`` or ``remove_multi``."""

    def __init__(self,
                 results: dict[str, MutationResult | CouchbaseException],
                 all_ok: bool) -> None:
        self._results = results
        self._all_ok = all_ok

    @property
    def all_ok(self) -> bool:
        """Whether every key's operation succeeded."""
        return self._all_ok

    @property
    def exceptions(self) -> dict[str, CouchbaseException]:
        """The exception each failed key's operation raised, by key."""
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, MutationResult):
                exc[k] = v
        return exc

    @property
    def results(self) -> dict[str, MutationResult]:
        """The result of each key whose operation succeeded, by key."""
        res = {}
        for k, v in self._results.items():
            if isinstance(v, MutationResult):
                res[k] = v
        return res

    def __repr__(self) -> str:
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'{type(self).__name__}({", ".join(output_results)})'


MultiResultType = MultiGetResult | MultiMutationResult


class MultiCounterResult:
    """The result of ``increment_multi`` or ``decrement_multi``."""

    def __init__(self,
                 results: dict[str, CounterResult | CouchbaseException],
                 all_ok: bool) -> None:
        self._results = results
        self._all_ok = all_ok

    @property
    def all_ok(self) -> bool:
        """Whether every key's operation succeeded."""
        return self._all_ok

    @property
    def exceptions(self) -> dict[str, CouchbaseException]:
        """The exception each failed key's operation raised, by key."""
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, CounterResult):
                exc[k] = v
        return exc

    @property
    def results(self) -> dict[str, CounterResult]:
        """The result of each key whose operation succeeded, by key."""
        res = {}
        for k, v in self._results.items():
            if isinstance(v, CounterResult):
                res[k] = v
        return res

    def __repr__(self) -> str:
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'{type(self).__name__}({", ".join(output_results)})'


class _ClusterReport(Result):
    """The five document fields of a :class:`Result`, answered with fixed values until they are removed.

    A report describes the cluster rather than a document, so none of them means anything here.  It
    stays a :class:`Result` subclass, never initialized as one, so that ``isinstance`` keeps its answer
    until the fields and the base go together.
    """

    def _document_field_deprecated(self, name: str) -> None:
        Supportability.class_property_deprecated(f'{type(self).__name__}.{name}',
                                                 message='A report describes the cluster, not a document.')

    @property
    def cas(self) -> int:
        """Always 0.

        .. deprecated:: 4.7.0
        """
        self._document_field_deprecated('cas')
        return 0

    @property
    def value(self) -> Any | None:
        """Always ``None``.

        .. deprecated:: 4.7.0
        """
        self._document_field_deprecated('value')
        return None

    @property
    def flags(self) -> int:
        """Always 0.

        .. deprecated:: 4.7.0
        """
        self._document_field_deprecated('flags')
        return 0

    @property
    def key(self) -> str | None:
        """Always ``None``.

        .. deprecated:: 4.7.0
        """
        self._document_field_deprecated('key')
        return None

    @property
    def success(self) -> bool:
        """Always ``False``, whatever the report says of the cluster.

        .. deprecated:: 4.7.0
        """
        self._document_field_deprecated('success')
        return False


class DiagnosticsResult(_ClusterReport):
    """The state of the SDK's connections to the cluster, as ``diagnostics`` reports it."""

    def __init__(self, record: _DiagnosticsRecord) -> None:
        self._record = record
        self._endpoints = {ServiceType(service): [EndpointDiagnosticsReport(e) for e in endpoints]
                           for service, endpoints in record.services}

    @property
    def id(self) -> str:
        """The report's ID."""
        return self._record.id

    @property
    def version(self) -> int:
        """The version of the report's format."""
        return self._record.version

    @property
    def sdk(self) -> str:
        """The name of the SDK which generated this report."""
        return self._record.sdk

    @property
    def endpoints(self) -> dict[ServiceType, list[EndpointDiagnosticsReport]]:
        """The report on each endpoint, by service."""
        return self._endpoints

    @property
    def state(self) -> ClusterState:
        """The state of the SDK's connections, summed over every endpoint."""
        num_found = 0
        num_connected = 0
        for endpoints in self._endpoints.values():
            for endpoint in endpoints:
                num_found += 1
                if endpoint.state == EndpointState.Connected:
                    num_connected += 1

        if num_found == num_connected:
            return ClusterState.Online
        if num_connected > 0:
            return ClusterState.Degraded
        return ClusterState.Offline

    def as_json(self) -> str:
        """Get the report as JSON.

        Returns:
            str: The report as a JSON string.
        """
        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class PingResult(_ClusterReport):
    """The outcome of pinging the cluster's services, as ``ping`` reports it."""

    def __init__(self, record: _PingRecord) -> None:
        self._record = record
        self._endpoints = {ServiceType(service): [EndpointPingReport(e) for e in endpoints]
                           for service, endpoints in record.services}

    @property
    def id(self) -> str:
        """The report's ID."""
        return self._record.id

    @property
    def version(self) -> int:
        """The version of the report's format."""
        return self._record.version

    @property
    def sdk(self) -> str:
        """The name of the SDK which generated this report."""
        return self._record.sdk

    @property
    def endpoints(self) -> dict[ServiceType, list[EndpointPingReport]]:
        """The report on each endpoint pinged, by service."""
        return self._endpoints

    def as_json(self) -> str:
        """Get the report as JSON.

        Returns:
            str: The report as a JSON string.
        """
        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class ClusterInfoResult:
    """What the cluster reports about its nodes, buckets and version.

    The version is known only when every node runs the same one.
    """

    def __init__(self, record: _ClusterInfoRecord) -> None:
        self._record = record
        versions = {node.version for node in record.nodes}
        # A cluster whose nodes run different versions reports no version at all.
        self._version = versions.pop() if len(versions) == 1 else None

    @property
    def nodes(self) -> list[_ClusterNodeDict]:
        """The cluster's nodes, each as a dict of its UUID, node name, status, hostname, OS, version and services."""
        return [{'uuid': node.uuid,
                 'otp_node': node.otp_node,
                 'status': node.status,
                 'hostname': node.hostname,
                 'os': node.os,
                 'version': node.version.full,
                 'services': list(node.services)} for node in self._record.nodes]

    @property
    def server_version(self) -> str | None:
        """The cluster's version as ``major.minor.patch-build``, if known."""
        v = self._version
        if v is None or v.major is None or v.minor is None or v.patch is None:
            return None
        if v.build is None:
            return f'{v.major}.{v.minor}.{v.patch}'
        return f'{v.major}.{v.minor}.{v.patch}-{v.build}'

    @property
    def server_version_short(self) -> float | None:
        """The cluster's version as ``major.minor``, if known."""
        v = self._version
        if v is None or v.major is None or v.minor is None:
            return None
        return float(f'{v.major}.{v.minor}')

    @property
    def server_version_full(self) -> str | None:
        """The cluster's version as the server spells it, if known."""
        return None if self._version is None else self._version.full

    @property
    def server_version_build(self) -> int | None:
        """The build number of the cluster's version, if known."""
        return None if self._version is None else self._version.build

    @property
    def is_enterprise(self) -> bool | None:
        """Whether the cluster is Enterprise Edition, or ``None`` if that is not known."""
        return self._is_edition('ENTERPRISE')

    @property
    def is_community(self) -> bool | None:
        """Whether the cluster is Community Edition, or ``None`` if that is not known."""
        return self._is_edition('COMMUNITY')

    def _is_edition(self, edition: str) -> bool | None:
        if self._version is None or self._version.edition is None:
            return None
        return self._version.edition.upper() == edition

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class OperationResult:
    """The CAS and mutation token of an operation.

    .. deprecated:: 4.0.0
    """

    def __init__(self, cas: int | None, mut_token: MutationToken | None) -> None:
        self._mutation_token = mut_token
        self._cas = cas

    @property
    def cas(self) -> int | None:
        """The CAS of the document."""
        return self._cas

    def mutation_token(self) -> MutationToken | None:
        """Get the operation's mutation token, if it has one.

        Returns:
            :class:`.MutationToken` | None: The operation's mutation token.
        """
        return self._mutation_token

    def __repr__(self) -> str:
        return f'{type(self).__name__}(mutation_token={self._mutation_token}, cas={self._cas})'


class ValueResult:
    """The CAS, mutation token and value of an operation.

    .. deprecated:: 4.0.0
    """

    def __init__(self, cas: int | None, mut_token: MutationToken | None, value: Any) -> None:
        Supportability.type_deprecated('ValueResult')
        self._mutation_token = mut_token
        self._cas = cas
        self._value = value

    @property
    def cas(self) -> int | None:
        """The CAS of the document."""
        return self._cas

    @property
    def value(self) -> Any | None:
        """The content of the document."""
        return self._value

    def mutation_token(self) -> MutationToken | None:
        """Get the operation's mutation token, if it has one.

        Returns:
            :class:`.MutationToken` | None: The operation's mutation token.
        """
        return self._mutation_token

    def __repr__(self) -> str:
        return f'{type(self).__name__}(mutation_token={self._mutation_token}, cas={self._cas})'
