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

# !! AUTO-GENERATED from tools/autogen/config/protos.yaml: DO NOT EDIT MANUALLY !!
# Regenerate with `python -m tools.autogen protos generate --write`.

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TransactionBeginAttemptRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ...) -> None: ...

class TransactionBeginAttemptResponse(_message.Message):
    __slots__ = ("transaction_id", "attempt_id")
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    transaction_id: str
    attempt_id: str
    def __init__(self, transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class TransactionCommitRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id", "attempt_id")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    attempt_id: str
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class TransactionCommitResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TransactionRollbackRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id", "attempt_id")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    attempt_id: str
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class TransactionRollbackResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TransactionGetRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id", "attempt_id", "scope_name", "collection_name", "key")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    attempt_id: str
    scope_name: str
    collection_name: str
    key: str
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class TransactionGetResponse(_message.Message):
    __slots__ = ("cas", "value")
    CAS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    cas: int
    value: bytes
    def __init__(self, cas: _Optional[int] = ..., value: _Optional[bytes] = ...) -> None: ...

class TransactionInsertRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id", "attempt_id", "scope_name", "collection_name", "key", "value")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    attempt_id: str
    scope_name: str
    collection_name: str
    key: str
    value: bytes
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class TransactionInsertResponse(_message.Message):
    __slots__ = ("cas",)
    CAS_FIELD_NUMBER: _ClassVar[int]
    cas: int
    def __init__(self, cas: _Optional[int] = ...) -> None: ...

class TransactionReplaceRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id", "attempt_id", "scope_name", "collection_name", "key", "cas", "value")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    attempt_id: str
    scope_name: str
    collection_name: str
    key: str
    cas: int
    value: bytes
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., cas: _Optional[int] = ..., value: _Optional[bytes] = ...) -> None: ...

class TransactionReplaceResponse(_message.Message):
    __slots__ = ("cas",)
    CAS_FIELD_NUMBER: _ClassVar[int]
    cas: int
    def __init__(self, cas: _Optional[int] = ...) -> None: ...

class TransactionRemoveRequest(_message.Message):
    __slots__ = ("bucket_name", "transaction_id", "attempt_id", "scope_name", "collection_name", "key", "cas")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    transaction_id: str
    attempt_id: str
    scope_name: str
    collection_name: str
    key: str
    cas: int
    def __init__(self, bucket_name: _Optional[str] = ..., transaction_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., cas: _Optional[int] = ...) -> None: ...

class TransactionRemoveResponse(_message.Message):
    __slots__ = ("cas",)
    CAS_FIELD_NUMBER: _ClassVar[int]
    cas: int
    def __init__(self, cas: _Optional[int] = ...) -> None: ...
