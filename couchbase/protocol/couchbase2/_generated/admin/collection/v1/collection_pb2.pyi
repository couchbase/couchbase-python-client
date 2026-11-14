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

from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListCollectionsRequest(_message.Message):
    __slots__ = ("bucket_name",)
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    def __init__(self, bucket_name: _Optional[str] = ...) -> None: ...

class ListCollectionsResponse(_message.Message):
    __slots__ = ("scopes", "manifest_uid")
    class Collection(_message.Message):
        __slots__ = ("name", "max_expiry_secs", "history_retention_enabled")
        NAME_FIELD_NUMBER: _ClassVar[int]
        MAX_EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
        HISTORY_RETENTION_ENABLED_FIELD_NUMBER: _ClassVar[int]
        name: str
        max_expiry_secs: int
        history_retention_enabled: bool
        def __init__(self, name: _Optional[str] = ..., max_expiry_secs: _Optional[int] = ..., history_retention_enabled: bool = ...) -> None: ...
    class Scope(_message.Message):
        __slots__ = ("name", "collections")
        NAME_FIELD_NUMBER: _ClassVar[int]
        COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
        name: str
        collections: _containers.RepeatedCompositeFieldContainer[ListCollectionsResponse.Collection]
        def __init__(self, name: _Optional[str] = ..., collections: _Optional[_Iterable[_Union[ListCollectionsResponse.Collection, _Mapping]]] = ...) -> None: ...
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    scopes: _containers.RepeatedCompositeFieldContainer[ListCollectionsResponse.Scope]
    manifest_uid: int
    def __init__(self, scopes: _Optional[_Iterable[_Union[ListCollectionsResponse.Scope, _Mapping]]] = ..., manifest_uid: _Optional[int] = ...) -> None: ...

class CreateScopeRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class CreateScopeResponse(_message.Message):
    __slots__ = ("manifest_uid",)
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    manifest_uid: int
    def __init__(self, manifest_uid: _Optional[int] = ...) -> None: ...

class DeleteScopeRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class DeleteScopeResponse(_message.Message):
    __slots__ = ("manifest_uid",)
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    manifest_uid: int
    def __init__(self, manifest_uid: _Optional[int] = ...) -> None: ...

class CreateCollectionRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "max_expiry_secs", "history_retention_enabled")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    MAX_EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_ENABLED_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    max_expiry_secs: int
    history_retention_enabled: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., max_expiry_secs: _Optional[int] = ..., history_retention_enabled: bool = ...) -> None: ...

class CreateCollectionResponse(_message.Message):
    __slots__ = ("manifest_uid",)
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    manifest_uid: int
    def __init__(self, manifest_uid: _Optional[int] = ...) -> None: ...

class UpdateCollectionRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "max_expiry_secs", "history_retention_enabled")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    MAX_EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_ENABLED_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    max_expiry_secs: int
    history_retention_enabled: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., max_expiry_secs: _Optional[int] = ..., history_retention_enabled: bool = ...) -> None: ...

class UpdateCollectionResponse(_message.Message):
    __slots__ = ("manifest_uid",)
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    manifest_uid: int
    def __init__(self, manifest_uid: _Optional[int] = ...) -> None: ...

class DeleteCollectionRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ...) -> None: ...

class DeleteCollectionResponse(_message.Message):
    __slots__ = ("manifest_uid",)
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    manifest_uid: int
    def __init__(self, manifest_uid: _Optional[int] = ...) -> None: ...
